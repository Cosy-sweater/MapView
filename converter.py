import os
import yaml
import zlib
import sqlite3
import mercantile
import mapbox_vector_tile
import multiprocessing as mp
from collections import defaultdict
from shapely.wkb import loads as load_wkb
from shapely.geometry import shape, mapping, box
from shapely.validation import make_valid
from shapely.ops import transform
import osmium
from tqdm import tqdm

# ==========================================
# Настройки оптимизации
# ==========================================
MVT_EXTENT = 4096
OPTIMIZED_ZOOMS = [6, 8, 10, 12, 14]


def load_allowed_tags():
    try:
        with open("render_tags.yaml", 'r') as f:
            return set(yaml.safe_load(f).get('tags', []))
    except FileNotFoundError:
        # Базовый набор на случай отсутствия конфига
        return {'highway', 'building', 'waterway', 'natural', 'landuse', 'amenity'}


ALLOWED_TAGS = load_allowed_tags()


# ==========================================
# 1. Фильтрация и Эвристика (Тот самый буст скорости)
# ==========================================
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
    return zoom >= 12


def project_to_mvt_pixels(geom, tile):
    bounds = mercantile.xy_bounds(tile)
    # ИСПРАВЛЕНИЕ: xy_bounds возвращает left, bottom, right, top
    w, s, e, n = bounds.left, bounds.bottom, bounds.right, bounds.top

    def transform_coords(lon, lat):
        try:
            # Для старых версий Shapely (передаются числа float)
            mx, my = mercantile.xy(lon, lat)
            x = int((mx - w) / (e - w) * MVT_EXTENT)
            y = int((n - my) / (n - s) * MVT_EXTENT)
            return x, y
        except TypeError:
            # Для Shapely 2.0+ (передаются массивы NumPy)
            import numpy as np
            mx = lon * 20037508.34 / 180.0
            my = np.log(np.tan((90.0 + lat) * np.pi / 360.0)) * 20037508.34 / 180.0
            x = ((mx - w) / (e - w) * MVT_EXTENT).astype(int)
            y = ((n - my) / (n - s) * MVT_EXTENT).astype(int)
            return x, y

    return transform(transform_coords, geom)

# ==========================================
# Stage 1 Worker: Геометрия -> Тайлы
# ==========================================
def process_geometry_batch(batch):
    """Воркер: Принимает батч WKB, режет на тайлы, возвращает локальные координаты"""
    results = defaultdict(list)

    # Bbox для обрезки внутри MVT тайла (с небольшим буфером от швов)
    mvt_bbox = box(-100, -100, MVT_EXTENT + 100, MVT_EXTENT + 100)

    for wkb_hex, tags_dict in batch:
        try:
            geom = load_wkb(wkb_hex, hex=True)
            if not geom.is_valid:
                geom = make_valid(geom)

            bounds = geom.bounds
            width = bounds[2] - bounds[0]
            height = bounds[3] - bounds[1]

            for zoom in OPTIMIZED_ZOOMS:
                if not is_important(zoom, tags_dict): continue



                pixel_size_deg = 360.0 / (256.0 * (2 ** zoom))

                # Отбрасываем невидимый мусор
                if geom.geom_type in ['Polygon', 'MultiPolygon']:
                    if width < pixel_size_deg and height < pixel_size_deg: continue
                elif geom.geom_type in ['LineString', 'MultiLineString']:
                    if geom.length < (pixel_size_deg * 2): continue

                if 'building' in tags_dict:
                    tolerance = pixel_size_deg * 0.1
                else:
                    tolerance = pixel_size_deg * 1.5

                simplified_geom = geom.simplify(tolerance, preserve_topology=True)
                if simplified_geom.is_empty: continue

                # Находим пересекаемые тайлы
                intersecting_tiles = mercantile.tiles(bounds[0], bounds[1], bounds[2], bounds[3], [zoom])

                for tile in intersecting_tiles:
                    # 1. Переводим координаты в локальные пиксели тайла (0..4096)
                    local_geom = project_to_mvt_pixels(simplified_geom, tile)

                    # 2. Обрезаем геометрию по границе тайла
                    clipped_geom = local_geom.intersection(mvt_bbox)
                    if clipped_geom.is_empty: continue

                    # 3. Фильтруем коллекции, возникшие при обрезке
                    geoms_to_add = []
                    if clipped_geom.geom_type == 'GeometryCollection':
                        geoms_to_add = [g for g in clipped_geom.geoms if
                                        g.geom_type in ['LineString', 'MultiLineString', 'Polygon', 'MultiPolygon']]
                    elif clipped_geom.geom_type in ['LineString', 'MultiLineString', 'Polygon', 'MultiPolygon']:
                        geoms_to_add = [clipped_geom]

                    for g in geoms_to_add:
                        results[(zoom, tile.x, tile.y)].append({
                            'geometry': mapping(g),
                            'properties': tags_dict
                        })
        except Exception as e:
            # Выводим ошибку в консоль, чтобы больше не ловить "0 тайлов" вслепую
            print(f"Ошибка в геометрии: {e}")
            pass

    return results


# ==========================================
# Stage 2 Worker: Сборка MVT Protobuf
# ==========================================
def process_mvt_worker(args):
    """Воркер: Принимает список фичей для одного тайла -> возвращает байты MVT"""
    tile_key, features = args
    z, x, y = tile_key

    layer = {"name": "osm", "features": features}
    try:
        # mapbox_vector_tile.encode - самая тяжелая операция, теперь она в пуле процессов
        mvt_data = mapbox_vector_tile.encode([layer], default_options={"extents": MVT_EXTENT})
        return z, x, y, mvt_data
    except Exception:
        return None


# ==========================================
# OSM Parser (Продюсер)
# ==========================================
class FastOsmHandler(osmium.SimpleHandler):
    def __init__(self):
        super().__init__()
        self.wkbfab = osmium.geom.WKBFactory()
        self.batch = []
        self.BATCH_SIZE = 2000
        self.pool = mp.Pool(max(1, mp.cpu_count() - 1))
        self.async_results = []

    def _flush(self):
        if self.batch:
            # Асинхронно отправляем батч в Stage 1
            res = self.pool.apply_async(process_geometry_batch, (self.batch,))
            self.async_results.append(res)
            self.batch = []

    def way(self, w):
        if 'highway' not in w.tags and 'waterway' not in w.tags: return
        tags = {k.k: k.v for k in w.tags if k.k in ALLOWED_TAGS}
        if not tags: return
        try:
            wkb = self.wkbfab.create_linestring(w)
            self.batch.append((wkb, tags))
            if len(self.batch) >= self.BATCH_SIZE: self._flush()
        except Exception:
            pass

    def area(self, a):
        if 'building' not in a.tags and 'natural' not in a.tags and 'landuse' not in a.tags: return
        tags = {k.k: k.v for k in a.tags if k.k in ALLOWED_TAGS}
        if not tags: return
        try:
            wkb = self.wkbfab.create_multipolygon(a)
            self.batch.append((wkb, tags))
            if len(self.batch) >= self.BATCH_SIZE: self._flush()
        except Exception:
            pass

    def finish_and_aggregate(self):
        self._flush()
        aggregated_tiles = defaultdict(list)

        print("\n[Stage 1] Расчет геометрии и обрезка по тайлам...")
        for res in tqdm(self.async_results, total=len(self.async_results), desc="Обработка батчей"):
            batch_result = res.get()  # Получаем словари от воркеров
            # Объединяем результаты в один большой словарь (Group By Tile)
            for tile_key, features in batch_result.items():
                aggregated_tiles[tile_key].extend(features)

        return aggregated_tiles


# ==========================================
# Запись в MBTiles
# ==========================================
def write_to_mbtiles(db_path, mvt_generator, total_tiles):
    if os.path.exists(db_path): os.remove(db_path)
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    cursor.execute("PRAGMA synchronous = OFF;")
    cursor.execute("PRAGMA journal_mode = WAL;")
    cursor.execute("CREATE TABLE metadata (name text, value text);")
    cursor.execute("CREATE TABLE tiles (zoom_level integer, tile_column integer, tile_row integer, tile_data blob);")
    cursor.execute("CREATE UNIQUE INDEX tile_index ON tiles (zoom_level, tile_column, tile_row);")

    batch = []
    print(f"\n[Stage 2] Сериализация {total_tiles} тайлов в MVT и запись в БД...")
    for result in tqdm(mvt_generator, total=total_tiles, desc="Генерация MVT"):
        if result:
            z, x, y, mvt_data = result
            compressed = zlib.compress(mvt_data)
            tms_y = (1 << z) - 1 - y  # TMS инверсия для MBTiles
            batch.append((z, x, tms_y, compressed))

            if len(batch) >= 500:
                cursor.executemany("INSERT INTO tiles VALUES (?, ?, ?, ?)", batch)
                batch = []
                conn.commit()

    if batch:
        cursor.executemany("INSERT INTO tiles VALUES (?, ?, ?, ?)", batch)

    conn.commit()
    conn.isolation_level = None
    cursor.execute("VACUUM;")
    conn.close()


if __name__ == '__main__':
    PBF_FILE = "mapfiles/cyprus.osm.pbf"
    DB_FILE = "cyprus_fast.mbtiles"

    # 1. Читаем и режем геометрию
    handler = FastOsmHandler()
    print("Чтение OSM PBF (извлечение нужных путей/полигонов)...")
    handler.apply_file(PBF_FILE, locations=True, idx='flex_mem')

    # Агрегация словаря
    tiles_dict = handler.finish_and_aggregate()
    total_tiles = len(tiles_dict)

    # 2. Параллельное кодирование MVT
    with mp.Pool(max(1, mp.cpu_count() - 1)) as pool:
        # imap_unordered мгновенно отдает готовые тайлы
        mvt_generator = pool.imap_unordered(process_mvt_worker, tiles_dict.items())
        write_to_mbtiles(DB_FILE, mvt_generator, total_tiles)

    print(f"\nГотово! MBTiles успешно создан: {DB_FILE}")