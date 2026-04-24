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
# 1. КОНФИГУРАЦИЯ И КОНСТАНТЫ
# ==========================================
with open("config.yaml", 'r', encoding='utf-8') as f:
    CONFIG = yaml.safe_load(f)

ZOOMS = CONFIG.get('zooms', [6, 8, 10, 12, 14])
MAX_DB_ZOOM = max(ZOOMS) if ZOOMS else 14

TOLERANCES_LINES = CONFIG.get('tolerances', {}).get('lines', {})
TOLERANCES_POLYS = CONFIG.get('tolerances', {}).get('polygons', {})

HW_RULES = CONFIG.get('highways', {})
WATER_RULES = CONFIG.get('waterways', {})
LAYER_RULES = CONFIG.get('layers', {})


# ==========================================
# 2. ПРАВИЛА СЛОЕВ И ТЕГОВ
# ==========================================
def get_layer_and_zoom(tags):
    """Определяет слой и минимальный зум на основе тегов OSM."""
    # 1. Дороги (динамические слои: highway_motorway, highway_primary и т.д.)
    if 'highway' in tags:
        hw_type = tags['highway']
        rule = HW_RULES.get(hw_type)
        if rule:
            min_z = rule.get('min_zoom', 14) if isinstance(rule, dict) else rule
            return f"highway_{hw_type}", min_z

    # 2. Вода (Разделяем линии рек и полигоны озер)
    if 'waterway' in tags:
        min_z = WATER_RULES.get(tags['waterway'], 12)
        return "waterway", min_z
    if tags.get('natural') == 'water' or tags.get('landuse') == 'reservoir':
        min_z = LAYER_RULES.get('water', {}).get('min_zoom', 6)
        return "water_poly", min_z

    # 3. Здания
    if 'building' in tags:
        min_z = LAYER_RULES.get('building', {}).get('min_zoom', 14)
        return "building", min_z

    # 4. Природа (леса, парки)
    if tags.get('natural') in ['wood', 'forest'] or tags.get('landuse') in ['forest', 'grass', 'meadow']:
        min_z = LAYER_RULES.get('natural', {}).get('min_zoom', 6)
        return "greenery", min_z

    return None, 99


def project_to_mvt_pixels(geom, tile):
    """Переводит координаты из градусов в локальную пиксельную сетку тайла 0..4096."""
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
# 3. МАТЕМАТИКА ГЕОМЕТРИИ (WORKER 1)
# ==========================================
def process_geometry_batch(batch):
    """Параллельный воркер: фильтрует, сжимает и нарезает геометрию на тайлы."""
    results = defaultdict(lambda: defaultdict(list))
    # Увеличенный буфер обрезки (256 единиц), предотвращает артефакты на границах тайлов
    mvt_bbox = box(-256, -256, MVT_EXTENT + 256, MVT_EXTENT + 256)

    for wkb_hex, tags_dict in batch:
        try:
            layer_name, min_zoom = get_layer_and_zoom(tags_dict)
            if not layer_name: continue

            geom = load_wkb(wkb_hex, hex=True)
            if not geom.is_valid:
                geom = make_valid(geom)
            if geom.is_empty: continue

            bounds = geom.bounds
            is_poly = geom.geom_type in ['Polygon', 'MultiPolygon']

            # Защита от Overzooming: пакуем объекты в максимальный доступный слой БД
            effective_min_zoom = min(min_zoom, MAX_DB_ZOOM)

            for zoom in ZOOMS:
                if zoom < effective_min_zoom: continue

                pixel_size_deg = 360.0 / (256.0 * (2 ** zoom))

                # --- СЖАТИЕ ГЕОМЕТРИИ ---
                if layer_name == 'building':
                    # Здания сохраняем максимально точными
                    simplified_geom = geom
                    # Фильтр "сараев" на низких зумах
                    if zoom <= 13 and (bounds[2] - bounds[0]) < pixel_size_deg and (
                            bounds[3] - bounds[1]) < pixel_size_deg:
                        continue
                else:
                    base_tol = TOLERANCES_POLYS.get(zoom, 1.0) if is_poly else TOLERANCES_LINES.get(zoom, 1.0)
                    tolerance = pixel_size_deg * base_tol

                    # Отключаем preserve_topology на мелких зумах, чтобы агрессивно резать сложные петли рек
                    preserve = True if zoom >= 12 else False
                    simplified_geom = geom.simplify(tolerance, preserve_topology=preserve)

                if simplified_geom.is_empty: continue

                # --- ЛЕЧЕНИЕ ЗЕЛЕНИ ---
                if not simplified_geom.is_valid:
                    simplified_geom = make_valid(simplified_geom)
                    if is_poly:
                        # Ультимативное исправление сломанных полигонов
                        simplified_geom = simplified_geom.buffer(0)

                intersecting_tiles = mercantile.tiles(bounds[0], bounds[1], bounds[2], bounds[3], [zoom])

                for tile in intersecting_tiles:
                    local_geom = project_to_mvt_pixels(simplified_geom, tile)
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
# 4. КОДИРОВАНИЕ MVT (WORKER 2)
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
# 5. ПАРСИНГ OSM (PRODUCER)
# ==========================================
class FastOsmHandler(osmium.SimpleHandler):
    def __init__(self):
        super().__init__()
        self.wkbfab = osmium.geom.WKBFactory()
        self.batch = []
        self.BATCH_SIZE = 5000
        self.pool = mp.Pool(max(1, mp.cpu_count() - 1))
        self.async_results = []
        self.nodes_count = 0
        self.ways_count = 0
        self.sema = threading.Semaphore(mp.cpu_count() * 2)

    def node(self, n):
        self.nodes_count += 1
        if self.nodes_count % 500000 == 0:
            print(f"\r[OSM Index] Загружено координат: {self.nodes_count}...", end="")

    def _flush(self):
        if self.batch:
            self.sema.acquire()

            def callback(result):
                self.async_results.append(result)
                self.sema.release()

            self.pool.apply_async(process_geometry_batch, (self.batch,), callback=callback)
            self.batch = []

    def way(self, w):
        if self.ways_count == 0: print("\n[OSM Parser] Обработка линий...")
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
        print(f"\n[Stage 1] Слияние геометрии (Обработано батчей: {len(self.async_results)})...")
        self.pool.close()
        self.pool.join()
        for res in tqdm(self.async_results):
            for tile_key, layers_dict in res.items():
                for layer_name, features in layers_dict.items():
                    aggregated_tiles[tile_key][layer_name].extend(features)
        return aggregated_tiles


# ==========================================
# 6. ЗАПИСЬ В БАЗУ ДАННЫХ
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


def run(map_path, output_path):
    handler = FastOsmHandler()
    print("Инициализация библиотеки Osmium (чтение .pbf)...")
    handler.apply_file(map_path, locations=True, idx='flex_mem')

    tiles_dict = handler.finish_and_aggregate()

    with mp.Pool(max(1, mp.cpu_count() - 1)) as pool:
        mvt_generator = pool.imap_unordered(process_mvt_worker, tiles_dict.items())
        write_to_mbtiles(output_path, mvt_generator, len(tiles_dict))


if __name__ == "__main__":
    run("mapfiles/cyprus.osm.pbf", "cyprus_fast.mbtiles")