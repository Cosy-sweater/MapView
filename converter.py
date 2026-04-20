import osmium
import osmium.geom
import mercantile
import sqlite3
import json
import hashlib
import multiprocessing as mp
from shapely.wkb import loads as load_wkb
from shapely.wkb import dumps as dump_wkb
from shapely.geometry import box
from shapely.validation import make_valid
from tqdm import tqdm

from constants import allowed_tags

# --- ПРАВИЛЬНЫЙ НАБОР ЗУМОВ ---
# Мы останавливаемся на 14! Большие зумы клиент растянет сам без потери качества.
OPTIMIZED_ZOOMS = [6, 8, 10, 12, 14]


# ==========================================
# 1. WORKER: Продвинутая обработка геометрии
# ==========================================
def geometry_worker(worker_id, task_queue, result_queue, progress_queue):
    def is_important(zoom, tags):
        if 'highway' in tags:
            hw = tags['highway']
            if zoom <= 6: return hw in ['motorway', 'trunk']
            if zoom <= 8: return hw in ['motorway', 'trunk', 'primary']
            if zoom <= 10: return hw in ['motorway', 'trunk', 'primary', 'secondary']
            if zoom <= 12: return hw not in ['footway', 'path', 'steps', 'pedestrian', 'track', 'service']
            return True

        if 'building' in tags: return zoom >= 13
        if any(k in tags for k in ['natural', 'landuse', 'waterway']): return zoom >= 8
        return zoom >= 14

    def sanitize_geometry(geom):
        """Очистка коллекций и лечение битой топологии"""
        if geom.is_empty:
            return []

        # Лечим геометрию (исправляет самопересечения и баги OSM)
        if not geom.is_valid:
            geom = make_valid(geom)

        if geom.geom_type in ['LineString', 'MultiLineString', 'Polygon', 'MultiPolygon']:
            return [geom]
        elif geom.geom_type == 'GeometryCollection':
            return [g for g in geom.geoms if
                    g.geom_type in ['LineString', 'MultiLineString', 'Polygon', 'MultiPolygon']]
        return []

    while True:
        batch = task_queue.get()
        if batch is None: break

        processed_batch = []
        for wkb_hex, tags_dict in batch:
            try:
                shapely_geom = load_wkb(wkb_hex, hex=True)

                # Лечим сырую геометрию ДО любых трансформаций
                if not shapely_geom.is_valid:
                    shapely_geom = make_valid(shapely_geom)

                bounds = shapely_geom.bounds
                width = bounds[2] - bounds[0]
                height = bounds[3] - bounds[1]

                for zoom in OPTIMIZED_ZOOMS:
                    if not is_important(zoom, tags_dict): continue

                    pixel_size_deg = 360.0 / (256.0 * (2 ** zoom))

                    if shapely_geom.geom_type in ['Polygon', 'MultiPolygon']:
                        if width < pixel_size_deg and height < pixel_size_deg: continue
                    elif shapely_geom.geom_type in ['LineString', 'MultiLineString']:
                        if shapely_geom.length < (pixel_size_deg * 2): continue

                    tolerance = pixel_size_deg * 1.5
                    simplified_geom = shapely_geom.simplify(tolerance, preserve_topology=True)

                    if simplified_geom.is_empty: continue

                    intersecting_tiles = mercantile.tiles(bounds[0], bounds[1], bounds[2], bounds[3], [zoom])

                    for tile in intersecting_tiles:
                        tile_bounds = mercantile.bounds(tile)
                        tile_box = box(tile_bounds.west, tile_bounds.south, tile_bounds.east, tile_bounds.north)

                        clipped_geom = simplified_geom.intersection(tile_box)

                        # Очищаем и лечим обрезанные куски
                        valid_geoms = sanitize_geometry(clipped_geom)
                        for valid_geom in valid_geoms:
                            clipped_wkb_bytes = dump_wkb(valid_geom)
                            processed_batch.append((zoom, tile.x, tile.y, clipped_wkb_bytes, tags_dict))

            except BaseException:
                pass

        if processed_batch:
            result_queue.put(processed_batch)
        progress_queue.put(1)


# ==========================================
# 2. CONSUMER: Запись в БД (Дедупликация)
# ==========================================
def db_writer(db_path, result_queue):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # ЭКСТРЕМАЛЬНАЯ ОПТИМИЗАЦИЯ ЗАПИСИ
    cursor.execute("PRAGMA synchronous = OFF;")
    cursor.execute("PRAGMA journal_mode = MEMORY;")

    cursor.executescript("""
        CREATE TABLE IF NOT EXISTS tags (id INTEGER PRIMARY KEY AUTOINCREMENT, hash TEXT UNIQUE, tags_json TEXT);
        CREATE TABLE IF NOT EXISTS geometries (id INTEGER PRIMARY KEY AUTOINCREMENT, hash TEXT UNIQUE, geometry_wkb BLOB);
        CREATE TABLE IF NOT EXISTS tiles (zoom INTEGER, tile_x INTEGER, tile_y INTEGER, geom_id INTEGER, tag_id INTEGER);
    """)
    conn.commit()

    tag_cache = {}
    geom_cache = {}

    def get_tag_id(tags_dict):
        tags_json = json.dumps(tags_dict, sort_keys=True)
        tag_hash = hashlib.md5(tags_json.encode('utf-8')).hexdigest()
        if tag_hash in tag_cache: return tag_cache[tag_hash]

        cursor.execute("INSERT OR IGNORE INTO tags (hash, tags_json) VALUES (?, ?)", (tag_hash, tags_json))
        cursor.execute("SELECT id FROM tags WHERE hash = ?", (tag_hash,))
        tag_id = cursor.fetchone()[0]
        tag_cache[tag_hash] = tag_id
        return tag_id

    def get_geom_id(wkb_bytes):
        geom_hash = hashlib.md5(wkb_bytes).hexdigest()
        if geom_hash in geom_cache: return geom_cache[geom_hash]

        cursor.execute("INSERT OR IGNORE INTO geometries (hash, geometry_wkb) VALUES (?, ?)", (geom_hash, wkb_bytes))
        cursor.execute("SELECT id FROM geometries WHERE hash = ?", (geom_hash,))
        geom_id = cursor.fetchone()[0]
        geom_cache[geom_hash] = geom_id
        return geom_id

    write_count = 0
    while True:
        batch = result_queue.get()
        if batch is None:
            break

        for zoom, tile_x, tile_y, clipped_wkb_bytes, tags_dict in batch:
            tag_id = get_tag_id(tags_dict)
            geom_id = get_geom_id(clipped_wkb_bytes)
            cursor.execute(
                "INSERT INTO tiles (zoom, tile_x, tile_y, geom_id, tag_id) VALUES (?, ?, ?, ?, ?)",
                (zoom, tile_x, tile_y, geom_id, tag_id)
            )

        write_count += 1
        if write_count % 500 == 0:  # Редкие коммиты для скорости
            conn.commit()

    conn.commit()
    print("\nСоздание индексов (около 10 секунд)...")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_tiles_zxy ON tiles(zoom, tile_x, tile_y);")
    cursor.execute("VACUUM;")
    cursor.execute("ANALYZE;")
    conn.commit()
    conn.close()


# ==========================================
# 3. PRODUCER: Чтение файла OSM
# ==========================================
class ParallelTilePipeline(osmium.SimpleHandler):
    def __init__(self, task_queue, pbar):
        super().__init__()
        self.wkbfab = osmium.geom.WKBFactory()
        self.task_queue = task_queue
        self.pbar = pbar
        self.batch = []
        self.BATCH_SIZE = 1000  # Увеличен батч для снижения нагрузки на потоки
        self.batches_sent = 0

    def flush_batch(self):
        if self.batch:
            self.task_queue.put(self.batch)
            self.pbar.update(len(self.batch))
            self.batch = []
            self.batches_sent += 1

    def way(self, w):
        # ИСПРАВЛЕНИЕ БАГА ТОПОЛОГИИ: Исключили 'natural', 'landuse' и 'building'
        # Линии - это только дороги и реки.
        if 'highway' not in w.tags and 'waterway' not in w.tags:
            return

        try:
            wkb = self.wkbfab.create_linestring(w)
            tags = {k.k: k.v for k in w.tags if k.k in allowed_tags}
            if tags:
                self.batch.append((wkb, tags))
                if len(self.batch) >= self.BATCH_SIZE: self.flush_batch()
        except Exception:
            pass

    def area(self, a):
        # Полигоны - это здания, водоемы, парки.
        if 'building' not in a.tags and 'natural' not in a.tags and 'landuse' not in a.tags:
            return

        try:
            wkb = self.wkbfab.create_multipolygon(a)
            tags = {k.k: k.v for k in a.tags if k.k in allowed_tags}
            if tags:
                self.batch.append((wkb, tags))
                if len(self.batch) >= self.BATCH_SIZE: self.flush_batch()
        except Exception:
            pass


if __name__ == '__main__':
    pbf_file = "mapfiles/cyprus.osm.pbf"
    db_file = "vector_tiles.sqlite"

    num_workers = max(1, mp.cpu_count() - 2)
    print(f"Запуск: {num_workers} воркеров. Используемые зумы: {OPTIMIZED_ZOOMS}")

    task_queue = mp.Queue(maxsize=1000)
    result_queue = mp.Queue(maxsize=1000)
    progress_queue = mp.Queue()

    db_process = mp.Process(target=db_writer, args=(db_file, result_queue))
    db_process.start()

    workers = []
    for i in range(num_workers):
        p = mp.Process(target=geometry_worker, args=(i, task_queue, result_queue, progress_queue))
        p.start()
        workers.append(p)

    # --- ЭТАП 1: Чтение OSM ---
    print("\n--- ЭТАП 1: Извлечение геометрии из OSM ---")
    with tqdm(desc="Чтение файла", unit=" obj") as pbar:
        handler = ParallelTilePipeline(task_queue, pbar)
        handler.apply_file(pbf_file, locations=True, idx='flex_mem')
        handler.flush_batch()

    total_batches = handler.batches_sent

    # Сигнал остановки воркерам
    for _ in range(num_workers):
        task_queue.put(None)

    # --- ЭТАП 2: Математика ---
    print("\n--- ЭТАП 2: Обработка геометрии и нарезка ---")
    with tqdm(total=total_batches, desc="Упрощение и Тайлинг", unit=" batch") as pbar_geom:
        batches_processed = 0
        while batches_processed < total_batches:
            progress_queue.get()  # Ждем сигнал готовности батча от воркера
            batches_processed += 1
            pbar_geom.update(1)

    for p in workers:
        p.join()

    # --- ЭТАП 3: Финализация БД ---
    print("\n--- ЭТАП 3: Запись и сжатие БД ---")
    result_queue.put(None)
    db_process.join()

    print("\nПайплайн успешно завершен!")