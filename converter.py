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
from tqdm import tqdm

from constants import allowed_tags, ZOOM_LEVELS


# ==========================================
# 1. WORKER: Обработка геометрии (Математика)
# ==========================================
def geometry_worker(worker_id, task_queue, result_queue):
    def is_important(zoom, tags):
        # Логика для дорог
        if 'highway' in tags:
            hw = tags['highway']
            if zoom <= 7:
                return hw in ['motorway', 'trunk']
            if zoom <= 10:
                return hw in ['motorway', 'trunk', 'primary', 'secondary']
            return True

        # Логика для зданий
        if 'building' in tags:
            return zoom >= 12

        return True

    """
    Воркер берет сырую геометрию, упрощает ее и режет на тайлы.
    Выполняется параллельно в нескольких процессах.
    """
    while True:
        batch = task_queue.get()
        if batch is None:  # Сигнал об остановке
            break

        processed_batch = []
        for wkb_hex, tags_dict in batch:
            try:
                # PyOsmium выдает HEX-строку, конвертируем в Shapely
                shapely_geom = load_wkb(wkb_hex, hex=True)
                bounds = shapely_geom.bounds

                for zoom in ZOOM_LEVELS:
                    if not is_important(zoom, tags_dict):
                        continue
                    tolerance = 360.0 / (64 * (2 ** zoom))
                    simplified_geom = shapely_geom.simplify(tolerance, preserve_topology=True)

                    if simplified_geom.is_empty:
                        continue

                    intersecting_tiles = mercantile.tiles(bounds[0], bounds[1], bounds[2], bounds[3], [zoom])

                    for tile in intersecting_tiles:
                        tile_bounds = mercantile.bounds(tile)
                        tile_box = box(tile_bounds.west, tile_bounds.south, tile_bounds.east, tile_bounds.north)

                        clipped_geom = simplified_geom.intersection(tile_box)
                        if clipped_geom.is_empty:
                            continue

                        # Переводим в бинарный WKB для компактной передачи и хранения
                        clipped_wkb_bytes = dump_wkb(clipped_geom)
                        processed_batch.append((zoom, tile.x, tile.y, clipped_wkb_bytes, tags_dict))

            except BaseException as e:
                # Игнорируем сбойную геометрию
                pass

        # Отправляем результаты в БД
        if processed_batch:
            result_queue.put(processed_batch)


# ==========================================
# 2. CONSUMER: Запись в БД (Дедупликация)
# ==========================================
def db_writer(db_path, result_queue):
    """
    Отдельный процесс для эксклюзивной записи в SQLite.
    Хранит кэш тегов и геометрий в оперативной памяти для дедупликации.
    """
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Создаем таблицы (как в предыдущем примере)
    cursor.executescript("""
        CREATE TABLE IF NOT EXISTS tags (
            id INTEGER PRIMARY KEY AUTOINCREMENT, hash TEXT UNIQUE, tags_json TEXT
        );
        CREATE TABLE IF NOT EXISTS geometries (
            id INTEGER PRIMARY KEY AUTOINCREMENT, hash TEXT UNIQUE, geometry_wkb BLOB
        );
        CREATE TABLE IF NOT EXISTS tiles (
            zoom INTEGER, tile_x INTEGER, tile_y INTEGER, geom_id INTEGER, tag_id INTEGER
        );
        CREATE INDEX IF NOT EXISTS idx_tiles_zxy ON tiles(zoom, tile_x, tile_y);
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

    # Чтение из очереди и запись
    write_count = 0
    while True:
        batch = result_queue.get()
        if batch is None:  # Сигнал об остановке
            break

        for zoom, tile_x, tile_y, clipped_wkb_bytes, tags_dict in batch:
            tag_id = get_tag_id(tags_dict)
            geom_id = get_geom_id(clipped_wkb_bytes)
            cursor.execute(
                "INSERT INTO tiles (zoom, tile_x, tile_y, geom_id, tag_id) VALUES (?, ?, ?, ?, ?)",
                (zoom, tile_x, tile_y, geom_id, tag_id)
            )

        # Коммит каждые 100 батчей для скорости
        write_count += 1
        if write_count % 100 == 0:
            conn.commit()

    # В методе finalize()
    conn.commit()
    cursor.execute("PRAGMA journal_mode = DELETE;")  # Отключаем логи транзакций
    # cursor.execute("DROP INDEX IF EXISTS idx_tiles_zxy;")  # Удаляем временные индексы, если нужно
    cursor.execute("VACUUM;")  # Полная пересборка файла базы (самый важный шаг)
    cursor.execute("ANALYZE;")  # Оптимизация статистики запросов
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

        # Мы группируем объекты в батчи, чтобы не перегружать
        # межпроцессное взаимодействие (IPC) мелкими пересылками
        self.batch = []
        self.BATCH_SIZE = 500

    def flush_batch(self):
        if self.batch:
            self.task_queue.put(self.batch)
            self.pbar.update(len(self.batch))
            self.batch = []

    def way(self, w):
        if 'highway' not in w.tags: return

        try:
            wkb = self.wkbfab.create_linestring(w)
            tags = {k.k: k.v for k in w.tags if k.k in allowed_tags}
            self.batch.append((wkb, tags))
            if len(self.batch) >= self.BATCH_SIZE:
                self.flush_batch()
        except Exception:
            pass

    def area(self, a):
        if 'building' not in a.tags and 'natural' not in a.tags: return
        try:
            wkb = self.wkbfab.create_multipolygon(a)
            tags = {k.k: k.v for k in a.tags if k.k in allowed_tags}
            self.batch.append((wkb, tags))
            if len(self.batch) >= self.BATCH_SIZE:
                self.flush_batch()
        except Exception:
            pass


if __name__ == '__main__':
    pbf_file = "mapfiles/cyprus.osm.pbf"
    db_file = "vector_tiles.sqlite"

    # Настраиваем количество процессов. Оставляем 1 ядро для БД и 1 для чтения
    num_workers = max(1, mp.cpu_count() - 2)
    print(f"Запуск с {num_workers} процессами-воркерами.")

    # Ограничиваем размер очереди, чтобы не забить оперативную память,
    # если чтение работает быстрее, чем обработка геометрии
    task_queue = mp.Queue(maxsize=1000)
    result_queue = mp.Queue(maxsize=1000)

    # Запуск процесса базы данных
    db_process = mp.Process(target=db_writer, args=(db_file, result_queue))
    db_process.start()

    # Запуск процессов-воркеров
    workers = []
    for i in range(num_workers):
        p = mp.Process(target=geometry_worker, args=(i, task_queue, result_queue))
        p.start()
        workers.append(p)

    print(f"Чтение {pbf_file}...")

    # Запускаем прогресс-бар.
    # Поскольку PBF читается стримом, мы не знаем итоговое количество объектов,
    # поэтому tqdm будет показывать только счетчик и скорость (it/s).
    with tqdm(desc="Извлечено объектов", unit=" obj") as pbar:
        handler = ParallelTilePipeline(task_queue, pbar)

        # Читаем файл. idx='flex_mem' нужен для кэширования координат узлов
        handler.apply_file(pbf_file, locations=True, idx='flex_mem')
        handler.flush_batch()  # Докидываем остатки

    print("\nЧтение завершено. Ожидание завершения обработки геометрии...")

    # Отправляем сигнал воркерам о завершении работы
    for _ in range(num_workers):
        task_queue.put(None)

    # Ждем завершения воркеров
    for p in workers:
        p.join()

    print("Геометрия обработана. Ожидание записи в БД...")

    # Отправляем сигнал процессу БД о завершении
    result_queue.put(None)
    db_process.join()

    print("Пайплайн успешно завершен! База данных готова.")