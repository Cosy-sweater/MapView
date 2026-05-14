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

from backup import write_to_mbtiles

MVT_EXTENT = 4096

with open("config.yaml", 'r', encoding='utf-8') as f:
    CONFIG = yaml.safe_load(f)

ZOOMS = CONFIG.get('zooms', [6, 10, 15])
MAX_ZOOM = max(ZOOMS) if ZOOMS else 15

TOLERANCES_LINES = CONFIG.get('tolerances', {}).get('lines', {})
TOLERANCES_POLYGONS = CONFIG.get('tolerances', {}).get('polygons', {})


def get_layer_and_zoom(tags):
    if 'highway' in tags:
        hw = tags['highway']
        if hw in ['motorway', 'trunk']: return f"highway_{hw}", 6
        if hw in ['primary', 'secondary']: return f"highway_{hw}", 8
        if hw in ['tertiary', 'unclassified', 'residential']: return f"highway_{hw}", 11
        if hw in ['service', 'pedestrian', 'path', 'footway', 'cycleway']: return "highway_minor", 13

    if 'railway' in tags and tags['railway'] == 'rail':
        return "railway", 10

    if 'waterway' in tags:
        return "waterway", 10
    if tags.get('natural') in ['water', 'bay'] or tags.get('landuse') == 'reservoir':
        return "water_poly", 6

    if 'building' in tags:
        return "building", 13

    if tags.get('natural') in ['wood', 'scrub', 'heath', 'grassland'] or tags.get('landuse') in ['forest', 'grass',
                                                                                                 'meadow']:
        return "greenery", 8
    if tags.get('leisure') in ['park', 'garden', 'pitch', 'nature_reserve']:
        return "greenery", 10

    if 'landuse' in tags:
        lu = tags['landuse']
        if lu in ['residential', 'commercial', 'industrial', 'farmland', 'cemetery']:
            return f"landuse_{lu}", 10

    if tags.get('amenity') in ['parking', 'university', 'school', 'hospital']:
        return "amenity_area", 13

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


def process_geometry_batch(batch):
    results = defaultdict(lambda: defaultdict(list))
    mvt_bbox = box(-256, -256, MVT_EXTENT + 256, MVT_EXTENT + 256)

    for wkb_hex, tags_dict in batch:
        try:
            layer_name, min_zoom = get_layer_and_zoom(tags_dict)
            if not layer_name: continue

            geom = load_wkb(wkb_hex, hex=True)
            if not geom.is_valid: geom = make_valid(geom)
            if geom.is_empty: continue

            bounds = geom.bounds
            is_poly = geom.geom_type in ['Polygon', 'MultiPolygon']
            effective_min_zoom = min(min_zoom, MAX_ZOOM)

            for zoom in ZOOMS:
                if zoom < effective_min_zoom: continue

                pixel_size_deg = 360.0 / (256.0 * (2 ** zoom))

                tol_multiplier = 1.0
                if layer_name.startswith('highway'):
                    tol_multiplier = 0.5
                elif layer_name == 'building':
                    tol_multiplier = 0.2
                elif layer_name == 'waterway':
                    tol_multiplier = 1.2

                base_tol = TOLERANCES_POLYGONS.get(zoom, 1.0) if is_poly else TOLERANCES_LINES.get(zoom, 1.0)
                tolerance = pixel_size_deg * base_tol * tol_multiplier

                preserve = True if is_poly or zoom >= 11 else False
                simplified_geom = geom.simplify(tolerance, preserve_topology=preserve)

                if simplified_geom.is_empty: continue
                if not simplified_geom.is_valid: simplified_geom = make_valid(simplified_geom)

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
        except BaseException:
            pass

    return {k: dict(v) for k, v in results.items()}


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
            print(f"\r[OSM Index] Загружено координат: {self.nodes_count}...", end="")

    def _flush(self):
        if self.batch:
            self.lock.acquire()

            def callback(result):
                self.async_results.append(result)
                self.lock.release()

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


def process_mvt_worker(args):
    tile_key, layers_dict = args
    z, x, y = tile_key
    mvt_layers = [{"name": name, "features": feats} for name, feats in layers_dict.items()]
    try:
        mvt_data = mapbox_vector_tile.encode(mvt_layers, default_options={"extents": MVT_EXTENT})
        return z, x, y, mvt_data
    except BaseException:
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
    handler = MapHandler()
    print("Инициализация библиотеки Osmium (чтение .pbf)...")
    handler.apply_file(map_path, locations=True, idx='flex_mem')

    tiles_dict = handler.finish_and_aggregate()

    with mp.Pool(max(1, mp.cpu_count() - 1)) as pool:
        mvt_generator = pool.imap_unordered(process_mvt_worker, tiles_dict.items())
        write_mbtiles(output_path, mvt_generator, len(tiles_dict))


if __name__ == "__main__":
    run("mapfiles/cyprus.osm.pbf", "cyprus_fast.mbtiles")
