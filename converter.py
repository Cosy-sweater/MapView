import os
import zlib
import sqlite3
import yaml
import threading
import mercantile
import mapbox_vector_tile
import multiprocessing as mp
import math
import numpy as np
import traceback
from collections import defaultdict
from shapely.wkb import loads as load_wkb
from shapely.geometry import mapping, box, MultiPolygon
from shapely.validation import make_valid
from shapely.ops import transform
from shapely.geometry.polygon import orient
import osmium
from tqdm import tqdm


MVT_EXTENT = 4096

with open("config.yaml", 'r', encoding='utf-8') as f:
    CONFIG = yaml.safe_load(f)

ZOOMS = [6, 8, 10, 12, 13, 14, 15]
MAX_ZOOM = max(ZOOMS)

TOLERANCES_LINES = CONFIG.get('tolerances', {}).get('lines', {})
TOLERANCES_POLYGONS = CONFIG.get('tolerances', {}).get('polygons', {})


def get_layer_and_zoom(tags, area_sqm=0):
    if 'highway' in tags:
        hw = tags['highway']
        if hw in ['motorway', 'trunk']: return f"highway_{hw}", 6
        if hw in ['primary', 'secondary']: return f"highway_{hw}", 8
        if hw in ['tertiary', 'unclassified', 'residential']: return f"highway_{hw}", 11
        if hw in ['service', 'pedestrian', 'path', 'footway', 'cycleway']: return "highway_minor", 13

    if 'railway' in tags and tags['railway'] == 'rail': return "railway", 10
    if 'waterway' in tags: return "waterway", 10
    if tags.get('natural') in ['water', 'bay'] or tags.get('landuse') == 'reservoir': return "water_poly", 6

    if 'building' in tags:
        if 0 < area_sqm < 50: return "building_small", 14
        return "building_large", 13

    if tags.get('natural') in ['wood', 'scrub', 'heath', 'grassland'] or tags.get('landuse') in ['forest', 'grass', 'meadow']:
        return "greenery", 8
    if tags.get('leisure') in ['park', 'garden', 'pitch', 'nature_reserve']: return "greenery", 10
    if 'landuse' in tags:
        if tags['landuse'] in ['residential', 'commercial', 'industrial', 'farmland', 'cemetery']: return f"landuse_{tags['landuse']}", 10
    if tags.get('amenity') in ['parking', 'university', 'school', 'hospital']: return "amenity_area", 13
    return None, 99


def wgs84_to_mercator(lon, lat, z=None):
    lon = np.asarray(lon)
    lat = np.asarray(lat)
    x = lon * 20037508.34 / 180.0
    lat = np.clip(lat, -89.9, 89.9)
    y = np.log(np.tan((90.0 + lat) * np.pi / 360.0)) * (20037508.34 / math.pi)
    if z is not None: return x, y, np.asarray(z)
    return x, y


def get_mvt_transformer(w, s, tile_size_m):
    def merc_to_mvt(x, y, z=None):
        x = np.asarray(x)
        y = np.asarray(y)
        mvt_x = (x - w) / tile_size_m * MVT_EXTENT
        mvt_y = (y - s) / tile_size_m * MVT_EXTENT
        if z is not None: return mvt_x, mvt_y, np.asarray(z)
        return mvt_x, mvt_y
    return merc_to_mvt


def enforce_mvt_topology(geom):
    def round_coords(x, y, z=None):
        return np.round(x), np.round(y)

    geom = transform(round_coords, geom)

    if geom.geom_type in ['Polygon', 'MultiPolygon']:
        geom = geom.buffer(0)
        if geom.is_empty: return None
        if geom.geom_type == 'Polygon':
            return orient(geom, sign=-1.0)
        elif geom.geom_type == 'MultiPolygon':
            return MultiPolygon([orient(p, sign=-1.0) for p in geom.geoms])
    elif geom.geom_type in ['LineString', 'MultiLineString']:
        if not geom.is_valid: geom = make_valid(geom)
        return geom if not geom.is_empty else None
    return None


def process_geometry_batch(batch):
    stats = {'input_objects': len(batch), 'wkb_parse_errors': 0, 'projection_errors': 0, 'out_of_bounds': 0,
             'successful_features': 0}
    results = defaultdict(lambda: defaultdict(list))

    for wkb_data, tags_dict in batch:
        try:
            geom_wgs84 = load_wkb(wkb_data)
            if not geom_wgs84.is_valid: geom_wgs84 = make_valid(geom_wgs84)
            geom_merc = transform(wgs84_to_mercator, geom_wgs84)

            is_poly = geom_merc.geom_type in ['Polygon', 'MultiPolygon']
            layer_name, min_zoom = get_layer_and_zoom(tags_dict, geom_merc.area if is_poly else 0)
            if not layer_name: continue

            intersecting_tiles = mercantile.tiles(geom_wgs84.bounds[0], geom_wgs84.bounds[1], geom_wgs84.bounds[2],
                                                  geom_wgs84.bounds[3], ZOOMS)

            for tile in intersecting_tiles:
                if tile.z < min_zoom: continue

                w, s, e, n = mercantile.xy_bounds(tile)
                tile_size_m = e - w
                tile_bbox_merc = box(w, s, e, n)

                clipped_merc = geom_merc.intersection(tile_bbox_merc)
                if clipped_merc.is_empty: continue

                if not layer_name.startswith('building'):
                    tol = (TOLERANCES_POLYGONS.get(tile.z, 1.0) if is_poly else TOLERANCES_LINES.get(tile.z, 1.0)) * (
                                tile_size_m / MVT_EXTENT)
                    simplified = clipped_merc.simplify(tol, preserve_topology=True)
                else:
                    simplified = clipped_merc

                mvt_geom = transform(get_mvt_transformer(w, s, tile_size_m), simplified)

                valid_g = enforce_mvt_topology(mvt_geom)
                if valid_g:
                    subs = valid_g.geoms if valid_g.geom_type in ['GeometryCollection', 'MultiPolygon',
                                                                  'MultiLineString'] else [valid_g]
                    for g in subs:
                        if not g.is_empty:
                            results[(tile.z, tile.x, tile.y)][layer_name].append(
                                {'geometry': mapping(g), 'properties': tags_dict})
                            feature_added = True
        except Exception:
            continue
    return dict(results), stats


def error_callback(err):
    print(f"\n[КРИТИЧЕСКАЯ ОШИБКА В ПОТОКЕ]: {err}")
    traceback.print_exception(type(err), err, err.__traceback__)


class MapHandler(osmium.SimpleHandler):
    def __init__(self):
        super().__init__()
        self.wkbfab = osmium.geom.WKBFactory()
        self.batch = []
        self.BATCH_SIZE = 5000
        self.pool = mp.Pool(max(1, mp.cpu_count() - 1))
        self.async_results = []
        self.nodes_count = 0
        self.ways_count = 0
        self.lock = threading.Semaphore(mp.cpu_count() * 2)

    def node(self, n):
        self.nodes_count += 1
        if self.nodes_count % 500000 == 0:
            print(f"\r[OSM Index] Загружено узлов: {self.nodes_count}...", end="")

    def _flush(self):
        if self.batch:
            self.lock.acquire()

            def callback(result):
                self.async_results.append(result)
                self.lock.release()

            self.pool.apply_async(process_geometry_batch, (self.batch,), callback=callback,
                                  error_callback=error_callback)
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

    def finish(self):
        self._flush()
        aggregated_tiles = defaultdict(lambda: defaultdict(list))

        print(f"\n[1/3] Ожидание завершения потоков. Отправлено батчей: {len(self.async_results)}...")
        self.pool.close()
        self.pool.join()

        global_stats = {
            'input_objects': 0, 'wkb_parse_errors': 0,
            'filtered_by_tags': 0, 'projection_errors': 0,
            'out_of_bounds': 0, 'successful_features': 0
        }

        print("\n[2/3] Агрегация геометрии...")
        for res_dict, stats in tqdm(self.async_results):
            for k in global_stats: global_stats[k] += stats.get(k, 0)
            for tile_key, layers_dict in res_dict.items():
                for layer_name, features in layers_dict.items():
                    aggregated_tiles[tile_key][layer_name].extend(features)

        print("\n")
        print(f"Всего объектов вошло: {global_stats['input_objects']}")
        print(f"Ошибки геометрии: {global_stats['wkb_parse_errors'] + global_stats['projection_errors']}")
        print(f"Исчезло за границами: {global_stats['out_of_bounds']}")
        print(f"Успешно дошло до MVT: {global_stats['successful_features']}")
        print("\n")

        return aggregated_tiles


def process_mvt_worker(args):
    tile_key, layers_dict = args
    z, x, y = tile_key
    mvt_layers = [{"name": name, "features": feats} for name, feats in layers_dict.items()]
    try:
        mvt_data = mapbox_vector_tile.encode(mvt_layers, default_options={"extents": MVT_EXTENT})
        return z, x, y, mvt_data
    except Exception as e:
        print(f"\n[MVT ENCODE ERROR] Тайл {z}/{x}/{y}: {e}")
        return None


def write_mbtiles(db_path, mvt_generator, total_tiles):
    if os.path.exists(db_path): os.remove(db_path)
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("PRAGMA synchronous = OFF;")
    cursor.execute("PRAGMA journal_mode = WAL;")
    cursor.execute("CREATE TABLE metadata (name text, value text);")
    cursor.execute("CREATE TABLE tiles (zoom_level integer, tile_column integer, tile_row integer, tile_data blob);")
    cursor.execute("CREATE UNIQUE INDEX tile_index ON tiles (zoom_level, tile_column, tile_row);")

    batch = []
    print(f"\n[3/3] Сериализация {total_tiles} MVT тайлов в БД...")
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
    handler = MapHandler()
    print("Чтение файла pbf")
    handler.apply_file(map_path, locations=True, idx='dense_file_array') # dense_file_array | flex_mem

    tiles_dict = handler.finish()

    if not tiles_dict:
        print("\nСловарь тайлов пуст. Отменена записи в бд.")
        return

    with mp.Pool(max(1, mp.cpu_count() - 1)) as pool:
        mvt_generator = pool.imap_unordered(process_mvt_worker, tiles_dict.items())
        write_mbtiles(output_path, mvt_generator, len(tiles_dict))


if __name__ == "__main__":
    # TODO Исправить ошибку перезаписи открытого файла
    # TODO добавить возможность переключать flex_mem
    # run("mapfiles/cyprus.osm.pbf", "cyprus_fast.mbtiles")
    print("Hello world!")
