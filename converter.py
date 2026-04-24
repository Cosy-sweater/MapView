import os
import zlib
import sqlite3
import yaml
import threading
import mercantile
import mapbox_vector_tile
import multiprocessing as mp
from collections import defaultdict
from shapely.wkb import loads as load_wkb
from shapely.geometry import mapping, box
from shapely.validation import make_valid
from shapely.ops import transform
import osmium
from tqdm import tqdm

MVT_EXTENT = 4096

# ==========================================
# Загрузка конфигурации
# ==========================================
with open("config.yaml", 'r', encoding='utf-8') as f:
    CONFIG = yaml.safe_load(f)

ZOOMS = CONFIG.get('zooms', [6, 8, 10, 12, 14])
TOLERANCES_LINES = CONFIG.get('tolerances', {}).get('lines', {})
TOLERANCES_POLYS = CONFIG.get('tolerances', {}).get('polygons', {})
HW_RULES = CONFIG.get('highways', {})
WATER_RULES = CONFIG.get('waterways', {})
LAYER_RULES = CONFIG.get('layers', {})


def get_layer_and_zoom(tags):
    if 'highway' in tags:
        return 'highway', HW_RULES.get(tags['highway'], 14)

    if 'waterway' in tags:
        return 'waterway', WATER_RULES.get(tags['waterway'], 12)

    if tags.get('natural') == 'water':
        return 'waterway', LAYER_RULES.get('natural', {}).get('min_zoom', 6)

    if 'building' in tags:
        return 'building', LAYER_RULES.get('building', {}).get('min_zoom', 13)

    if 'natural' in tags or 'landuse' in tags:
        return 'natural', LAYER_RULES.get('natural', {}).get('min_zoom', 6)

    return None, 99


def project_to_mvt_pixels(geom, tile):
    bounds = mercantile.xy_bounds(tile)
    w, s, e, n = bounds.left, bounds.bottom, bounds.right, bounds.top

    def transform_coords(lon, lat):
        try:
            mx, my = mercantile.xy(lon, lat)
            return int((mx - w) / (e - w) * MVT_EXTENT), int((n - my) / (n - s) * MVT_EXTENT)
        except TypeError:
            import numpy as np
            mx = lon * 20037508.34 / 180.0
            my = np.log(np.tan((90.0 + lat) * np.pi / 360.0)) * 20037508.34 / 180.0
            return ((mx - w) / (e - w) * MVT_EXTENT).astype(int), ((n - my) / (n - s) * MVT_EXTENT).astype(int)

    return transform(transform_coords, geom)


# ==========================================
# Worker 1: Геометрия -> Тайлы (ОБНОВЛЕНО)
# ==========================================
def process_geometry_batch(batch):
    results = defaultdict(lambda: defaultdict(list))
    # Буфер обрезки 128 MVT единиц (предотвращает артефакты на самых краях)
    mvt_bbox = box(-128, -128, MVT_EXTENT + 128, MVT_EXTENT + 128)

    for wkb_hex, tags_dict in batch:
        try:
            layer_name, min_zoom = get_layer_and_zoom(tags_dict)
            if not layer_name: continue

            geom = load_wkb(wkb_hex, hex=True)
            if not geom.is_valid: geom = make_valid(geom)
            if geom.is_empty: continue

            bounds = geom.bounds
            is_poly = geom.geom_type in ['Polygon', 'MultiPolygon']

            for zoom in ZOOMS:
                if zoom < min_zoom: continue

                # Градусов в одном пикселе на экваторе для текущего зума
                pixel_size_deg = 360.0 / (256.0 * (2 ** zoom))

                base_tol = TOLERANCES_POLYS.get(zoom, 1.0) if is_poly else TOLERANCES_LINES.get(zoom, 1.0)

                # ИСПРАВЛЕНИЕ 1: Сжимаем ДО нарезки на тайлы (гарантирует отсутствие разрывов)
                if layer_name == 'building':
                    # Здания вообще не трогаем, чтобы не искажать форму
                    simplified_geom = geom
                else:
                    # Дороги и леса сжимаем в градусной сетке
                    tolerance = pixel_size_deg * base_tol * 0.5
                    simplified_geom = geom.simplify(tolerance, preserve_topology=True)

                if simplified_geom.is_empty: continue
                if not simplified_geom.is_valid:
                    simplified_geom = make_valid(simplified_geom)

                # ИСПРАВЛЕНИЕ 2: Фильтр "сараев" на уровне базы данных.
                # Если это здание на зуме <= 13 и оно очень маленькое в градусах — пропускаем
                if layer_name == 'building' and zoom <= 14:
                    if (bounds[2] - bounds[0]) < pixel_size_deg and (bounds[3] - bounds[1]) < pixel_size_deg:
                        continue

                intersecting_tiles = mercantile.tiles(bounds[0], bounds[1], bounds[2], bounds[3], [zoom])

                for tile in intersecting_tiles:
                    # Переводим УЖЕ сжатую геометрию в локальные координаты
                    local_geom = project_to_mvt_pixels(simplified_geom, tile)

                    # Обрезаем с небольшим буфером (mvt_bbox с запасом -128..+128)
                    clipped_geom = local_geom.intersection(mvt_bbox)
                    if clipped_geom.is_empty: continue

                    valid_geoms = []
                    if clipped_geom.geom_type == 'GeometryCollection':
                        valid_geoms = [g for g in clipped_geom.geoms if
                                       g.geom_type in ['LineString', 'MultiLineString', 'Polygon', 'MultiPolygon']]
                    elif clipped_geom.geom_type in ['LineString', 'MultiLineString', 'Polygon', 'MultiPolygon']:
                        valid_geoms = [clipped_geom]

                    for g in valid_geoms:
                        results[(zoom, tile.x, tile.y)][layer_name].append({
                            'geometry': mapping(g),
                            'properties': tags_dict
                        })
        except Exception:
            pass

    return {k: dict(v) for k, v in results.items()}


# ==========================================
# Worker 2: MVT кодирование
# ==========================================
def process_mvt_worker(args):
    tile_key, layers_dict = args
    z, x, y = tile_key
    mvt_layers = [{"name": name, "features": feats} for name, feats in layers_dict.items()]
    try:
        mvt_data = mapbox_vector_tile.encode(mvt_layers, default_options={"extents": MVT_EXTENT})
        return z, x, y, mvt_data
    except Exception:
        return None


# ==========================================
# Парсер OSM (Продюсер)
# ==========================================
class FastOsmHandler(osmium.SimpleHandler):
    def __init__(self):
        super().__init__()
        self.wkbfab = osmium.geom.WKBFactory()
        self.batch = []
        self.BATCH_SIZE = 5000  # Увеличенный батч для ускорения
        self.pool = mp.Pool(max(1, mp.cpu_count() - 1))
        self.async_results = []

        # Индикаторы прогресса, чтобы скрипт не казался зависшим
        self.nodes_count = 0
        self.ways_count = 0

        # Ограничитель очереди, чтобы не забить всю оперативную память
        self.sema = threading.Semaphore(mp.cpu_count() * 2)

    def node(self, n):
        self.nodes_count += 1
        if self.nodes_count % 500000 == 0:
            print(f"\r[OSM C++ Index] Загружено узлов: {self.nodes_count}...", end="")

    def _flush(self):
        if self.batch:
            self.sema.acquire()  # Ждем, если воркеры перегружены

            def callback(result):
                self.async_results.append(result)
                self.sema.release()  # Освобождаем слот

            self.pool.apply_async(process_geometry_batch, (self.batch,), callback=callback)
            self.batch = []

    def way(self, w):
        if self.ways_count == 0: print("\n[OSM Parser] Начат парсинг Линий (Ways)...")
        self.ways_count += 1

        tags = {k.k: k.v for k in w.tags}
        if get_layer_and_zoom(tags)[0]:
            try:
                self.batch.append((self.wkbfab.create_linestring(w), tags))
                if len(self.batch) >= self.BATCH_SIZE: self._flush()
            except Exception:
                pass

    def area(self, a):
        tags = {k.k: k.v for k in a.tags}
        if get_layer_and_zoom(tags)[0]:
            try:
                self.batch.append((self.wkbfab.create_multipolygon(a), tags))
                if len(self.batch) >= self.BATCH_SIZE: self._flush()
            except Exception:
                pass

    def finish_and_aggregate(self):
        self._flush()
        aggregated_tiles = defaultdict(lambda: defaultdict(list))

        print(f"\n[Stage 1] Слияние данных по тайлам (Батчей: {len(self.async_results)})...")
        # Закрываем пул и ждем завершения всех задач
        self.pool.close()
        self.pool.join()

        for res in tqdm(self.async_results):
            for tile_key, layers_dict in res.items():
                for layer_name, features in layers_dict.items():
                    aggregated_tiles[tile_key][layer_name].extend(features)
        return aggregated_tiles


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
    print(f"\n[Stage 2] Сериализация {total_tiles} MVT тайлов...")
    for result in tqdm(mvt_generator, total=total_tiles):
        if result:
            z, x, y, mvt_data = result
            batch.append((z, x, (1 << z) - 1 - y, zlib.compress(mvt_data)))
            if len(batch) >= 500:
                cursor.executemany("INSERT INTO tiles VALUES (?, ?, ?, ?)", batch)
                batch = []

    if batch: cursor.executemany("INSERT INTO tiles VALUES (?, ?, ?, ?)", batch)
    conn.commit()
    conn.isolation_level = None
    cursor.execute("VACUUM;")
    conn.close()


if __name__ == '__main__':
    handler = FastOsmHandler()
    # locations=True требует времени на построение индекса в ОЗУ
    print("Инициализация библиотеки Osmium (чтение .pbf)...")
    handler.apply_file("mapfiles/cyprus.osm.pbf", locations=True, idx='flex_mem')

    tiles_dict = handler.finish_and_aggregate()

    with mp.Pool(max(1, mp.cpu_count() - 1)) as pool:
        mvt_generator = pool.imap_unordered(process_mvt_worker, tiles_dict.items())
        write_to_mbtiles("cyprus_fast.mbtiles", mvt_generator, len(tiles_dict))