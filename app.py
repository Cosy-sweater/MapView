import sys
import sqlite3
import json
import math
from collections import OrderedDict
from PyQt5.QtWidgets import QApplication, QWidget, QMainWindow
from PyQt5.QtGui import QPainter, QPainterPath, QPen, QColor, QPixmap, QImage
from PyQt5.QtCore import Qt, QObject, pyqtSignal, QRunnable, QThreadPool, pyqtSlot, QRectF
from shapely.wkb import loads as load_wkb
import mercantile

# Импорт уровней из вашего конфига
from constants import ZOOM_LEVELS

# --- Настройки ---
DB_ZOOMS = ZOOM_LEVELS

STYLES = {
    'highway': {
        'motorway': {'color': '#e990a0', 'width': 4},
        'trunk': {'color': '#f9b29c', 'width': 3},
        'primary': {'color': '#fcd6a4', 'width': 3},
        'secondary': {'color': '#f7fabf', 'width': 2},
        'default': {'color': '#ffffff', 'width': 1.5}
    },
    'building': {
        'default': {'color': '#d9d0c9', 'outline': '#c0b8b2'}
    },
    'water': {
        'default': {'color': '#a5c0df'}
    },
    'background': '#f2efe9'
}


# --- Вспомогательная математика ---
def lonlat_to_pixel(lon, lat, zoom):
    # Защита от Math Domain Error на полюсах
    lat = max(min(lat, 85.0511), -85.0511)
    n = 2.0 ** zoom
    x = (lon + 180.0) / 360.0 * n * 256.0
    lat_rad = math.radians(lat)
    y = (1.0 - math.asinh(math.tan(lat_rad)) / math.pi) / 2.0 * n * 256.0
    return x, y


def pixel_to_lonlat(px, py, zoom):
    n = 2.0 ** zoom
    lon = px / (n * 256.0) * 360.0 - 180.0
    lat_rad = math.atan(math.sinh(math.pi * (1 - 2 * py / (n * 256.0))))
    lat = math.degrees(lat_rad)
    return lon, lat


# --- Фоновая загрузка с поддержкой ОТМЕНЫ ---
class WorkerSignals(QObject):
    finished = pyqtSignal(tuple, QImage)


class TileWorker(QRunnable):
    def __init__(self, z, x, y, db_path):
        super().__init__()
        self.z, self.x, self.y = z, x, y
        self.db_path = db_path
        self.signals = WorkerSignals()
        self.cancelled = False  # Флаг отмены задачи

    @pyqtSlot()
    def run(self):
        # Если задача устарела еще до старта потока — отменяем
        if self.cancelled:
            return

        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()

            query = """
                SELECT g.geometry_wkb, tg.tags_json
                FROM tiles t
                JOIN geometries g ON t.geom_id = g.id
                JOIN tags tg ON t.tag_id = tg.id
                WHERE t.zoom = ? AND t.tile_x = ? AND t.tile_y = ?
            """
            cursor.execute(query, (self.z, self.x, self.y))
            rows = cursor.fetchall()

            # Вторая проверка после SQL-запроса
            if self.cancelled:
                conn.close()
                return

            image = QImage(256, 256, QImage.Format_ARGB32)
            image.fill(Qt.transparent)

            if rows:
                painter = QPainter(image)
                painter.setRenderHint(QPainter.Antialiasing, True)
                try:
                    self._draw_rows(painter, rows)
                finally:
                    painter.end()

            conn.close()

            # Финальная проверка перед отправкой в UI
            if not self.cancelled:
                self.signals.finished.emit((self.z, self.x, self.y), image.copy())

        except Exception as e:
            print(f"Worker Error [{self.z}/{self.x}/{self.y}]: {e}")

    def _draw_rows(self, painter, rows):
        # РАЗДЕЛЯЕМ ВОДУ НА ПОЛИГОНЫ И ЛИНИИ
        layer_water_poly = []
        layer_water_line = []
        layer_buildings = []
        layer_roads = []
        tx, ty = self.x * 256, self.y * 256

        for wkb_data, tags_json in rows:
            try:
                tags = json.loads(tags_json)
                geom = load_wkb(wkb_data)

                # Фильтруем объекты по типу геометрии
                if 'natural' in tags or 'waterway' in tags:
                    if geom.geom_type in ['Polygon', 'MultiPolygon']:
                        layer_water_poly.append(geom)
                    else:
                        layer_water_line.append(geom)
                elif 'building' in tags and self.z >= 13:
                    layer_buildings.append(geom)
                elif 'highway' in tags:
                    layer_roads.append((geom, tags.get('highway', 'default')))
            except:
                pass

        # 1. Отрисовка: Вода (Озера) - Заливка без обводки
        if layer_water_poly:
            painter.setPen(Qt.NoPen)
            painter.setBrush(QColor(STYLES['water']['default']['color']))
            for geom in layer_water_poly:
                self._draw_geom(geom, painter, tx, ty)

        # 2. Отрисовка: Вода (Реки) - Линии без заливки
        if layer_water_line:
            painter.setPen(
                QPen(QColor(STYLES['water']['default']['color']), 2, Qt.SolidLine, Qt.RoundCap, Qt.RoundJoin))
            painter.setBrush(Qt.NoBrush)  # КРИТИЧЕСКИ ВАЖНО: Никакой заливки для рек!
            for geom in layer_water_line:
                self._draw_geom(geom, painter, tx, ty)

        # 3. Отрисовка: Здания - Заливка с обводкой
        if layer_buildings:
            b_style = STYLES['building']['default']
            painter.setPen(QPen(QColor(b_style['outline']), 1))
            painter.setBrush(QColor(b_style['color']))
            for geom in layer_buildings:
                self._draw_geom(geom, painter, tx, ty)

        # 4. Отрисовка: Дороги - Линии без заливки
        for geom, hw_type in layer_roads:
            style = STYLES['highway'].get(hw_type, STYLES['highway']['default'])
            pen = QPen(QColor(style['color']))
            pen.setWidthF(style['width'])
            pen.setCapStyle(Qt.RoundCap)
            pen.setJoinStyle(Qt.RoundJoin)
            painter.setPen(pen)
            painter.setBrush(Qt.NoBrush)
            self._draw_geom(geom, painter, tx, ty)

    def _draw_geom(self, geom, painter, tx, ty):
        if geom.geom_type in ('LineString', 'LinearRing'):
            self._draw_path(geom.coords, painter, tx, ty)
        elif geom.geom_type == 'MultiLineString':
            for line in geom.geoms: self._draw_path(line.coords, painter, tx, ty)
        elif geom.geom_type == 'Polygon':
            self._draw_poly(geom, painter, tx, ty)
        elif geom.geom_type == 'MultiPolygon':
            for poly in geom.geoms: self._draw_poly(poly, painter, tx, ty)

    def _draw_path(self, coords, painter, tx, ty):
        path = QPainterPath()
        first = True
        for lon, lat in coords:
            px, py = lonlat_to_pixel(lon, lat, self.z)
            if first:
                path.moveTo(px - tx, py - ty)
                first = False
            else:
                path.lineTo(px - tx, py - ty)
        painter.drawPath(path)

    def _draw_poly(self, poly, painter, tx, ty):
        path = QPainterPath()
        self._add_ring(path, poly.exterior.coords, tx, ty)
        for interior in poly.interiors:
            self._add_ring(path, interior.coords, tx, ty)
        painter.drawPath(path)

    def _add_ring(self, path, coords, tx, ty):
        first = True
        for lon, lat in coords:
            px, py = lonlat_to_pixel(lon, lat, self.z)
            if first:
                path.moveTo(px - tx, py - ty)
                first = False
            else:
                path.lineTo(px - tx, py - ty)
        path.closeSubpath()


# --- Главный виджет карты ---
class MapWidget(QWidget):
    def __init__(self, db_path):
        super().__init__()
        self.db_path = db_path
        self.zoom = 13.0
        self.center_lon, self.center_lat = 33.38, 35.14

        self.is_panning = False
        self.last_mouse_pos = None

        self.pixmap_cache = OrderedDict()
        self.MAX_CACHE_TILES = 150  # Лимит кэша в оперативной памяти

        self.active_workers = OrderedDict()
        self.MAX_QUEUE_SIZE = 64  # Лимит очереди для защиты CPU

        self.thread_pool = QThreadPool()
        self.thread_pool.setMaxThreadCount(6)

    def get_db_zoom(self, current_zoom):
        available_zooms = [z for z in DB_ZOOMS if z <= current_zoom]
        return max(available_zooms) if available_zooms else min(DB_ZOOMS)

    @pyqtSlot(tuple, QImage)
    def on_tile_loaded(self, key, image):
        self.pixmap_cache[key] = QPixmap.fromImage(image)

        if key in self.active_workers:
            del self.active_workers[key]

        while len(self.pixmap_cache) > self.MAX_CACHE_TILES:
            self.pixmap_cache.popitem(last=False)

        self.update()

    def paintEvent(self, event):
        painter = QPainter(self)
        painter.fillRect(self.rect(), QColor(STYLES['background']))
        # Включаем плавное сглаживание при масштабировании тайлов
        painter.setRenderHint(QPainter.SmoothPixmapTransform, True)

        cx, cy = self.width() / 2, self.height() / 2
        db_zoom = self.get_db_zoom(self.zoom)
        scale_factor = 2.0 ** (self.zoom - db_zoom)

        c_px_x, c_px_y = lonlat_to_pixel(self.center_lon, self.center_lat, self.zoom)

        west, north = pixel_to_lonlat(c_px_x - cx, c_px_y - cy, self.zoom)
        east, south = pixel_to_lonlat(c_px_x + cx, c_px_y + cy, self.zoom)

        raw_tiles = list(mercantile.tiles(west, south, east, north, [db_zoom]))

        # --- 1. АГРЕССИВНАЯ ОЧИСТКА ОЧЕРЕДИ ---
        # Удаляем запросы старых зумов
        for key, worker in list(self.active_workers.items()):
            if key[0] != db_zoom:
                worker.cancelled = True
                del self.active_workers[key]

        # Если очередь переполнена, удаляем самые старые запросы
        while len(self.active_workers) >= self.MAX_QUEUE_SIZE:
            key, worker = self.active_workers.popitem(last=False)
            worker.cancelled = True

        # --- 2. СОРТИРОВКА ТАЙЛОВ ПО УДАЛЕННОСТИ ОТ ЦЕНТРА ---
        prioritized_tiles = []
        for tile in raw_tiles:
            tile_px_x = (tile.x * 256) * scale_factor
            tile_px_y = (tile.y * 256) * scale_factor
            screen_x = tile_px_x - c_px_x + cx
            screen_y = tile_px_y - c_px_y + cy

            tile_center_x = screen_x + (128 * scale_factor)
            tile_center_y = screen_y + (128 * scale_factor)

            dist = (tile_center_x - cx) ** 2 + (tile_center_y - cy) ** 2
            prioritized_tiles.append((dist, tile, screen_x, screen_y))

        prioritized_tiles.sort(key=lambda t: t[0])

        # --- 3. ОТРИСОВКА И ЗАПРОСЫ ---
        for dist, tile, screen_x, screen_y in prioritized_tiles:
            key = (tile.z, tile.x, tile.y)
            # QRectF предотвращает появление щелей в 1 пиксель между тайлами
            dest_rect = QRectF(screen_x, screen_y, 256 * scale_factor, 256 * scale_factor)

            if key in self.pixmap_cache:
                # Отрисовка идеального тайла
                pix = self.pixmap_cache[key]
                painter.drawPixmap(dest_rect, pix, QRectF(0, 0, 256, 256))
            else:
                # Запуск фонового воркера
                if key not in self.active_workers:
                    worker = TileWorker(tile.z, tile.x, tile.y, self.db_path)
                    self.active_workers[key] = worker
                    worker.signals.finished.connect(self.on_tile_loaded)
                    self.thread_pool.start(worker)

                # --- ЛОГИКА OVER-ZOOMING (ОТКАТ К РОДИТЕЛЮ) ---
                fallback_found = False
                parent_z, parent_x, parent_y = tile.z, tile.x, tile.y
                dz = 0

                while parent_z > min(DB_ZOOMS):
                    parent_z -= 1
                    parent_x //= 2
                    parent_y //= 2
                    dz += 1

                    if (parent_z, parent_x, parent_y) in self.pixmap_cache:
                        parent_pix = self.pixmap_cache[(parent_z, parent_x, parent_y)]
                        scale_diff = 2 ** dz
                        src_w = 256 / scale_diff
                        src_h = 256 / scale_diff
                        src_x = (tile.x % scale_diff) * src_w
                        src_y = (tile.y % scale_diff) * src_h

                        # Растягиваем нужный фрагмент старого тайла на весь квадрат
                        painter.drawPixmap(dest_rect, parent_pix, QRectF(src_x, src_y, src_w, src_h))
                        fallback_found = True
                        break

                if not fallback_found:
                    painter.setPen(QPen(Qt.gray, 1, Qt.DashLine))
                    painter.drawRect(dest_rect.toRect())

        # --- ИНФО-ПАНЕЛЬ ---
        painter.setPen(Qt.NoPen)
        painter.setBrush(QColor(255, 255, 255, 220))
        painter.drawRect(10, 10, 280, 110)

        painter.setPen(Qt.black)
        painter.drawText(20, 30, f"Zoom: {self.zoom:.2f} (DB Layer: Z{db_zoom})")
        painter.drawText(20, 50, f"Queue / Active: {len(self.active_workers)} / {self.MAX_QUEUE_SIZE}")
        painter.drawText(20, 70, f"RAM Cache: {len(self.pixmap_cache)} / {self.MAX_CACHE_TILES} tiles")

        cache_mb = len(self.pixmap_cache) * 0.25
        if cache_mb > (self.MAX_CACHE_TILES * 0.25) * 0.85:
            painter.setPen(Qt.red)
        else:
            painter.setPen(Qt.darkGreen)
        painter.drawText(20, 90, f"Cache Size: ~{cache_mb:.1f} MB")

        painter.setPen(Qt.blue)
        painter.drawText(20, 110, f"Center: Lat {self.center_lat:.5f}, Lon {self.center_lon:.5f}")

    # --- Обработка мыши ---
    def mousePressEvent(self, event):
        if event.button() == Qt.LeftButton:
            self.is_panning = True
            self.last_mouse_pos = event.pos()

    def mouseMoveEvent(self, event):
        if self.is_panning:
            dx = event.x() - self.last_mouse_pos.x()
            dy = event.y() - self.last_mouse_pos.y()
            c_px, c_py = lonlat_to_pixel(self.center_lon, self.center_lat, self.zoom)
            self.center_lon, self.center_lat = pixel_to_lonlat(c_px - dx, c_py - dy, self.zoom)
            self.last_mouse_pos = event.pos()
            self.update()

    def mouseReleaseEvent(self, event):
        if event.button() == Qt.LeftButton:
            self.is_panning = False

    def wheelEvent(self, event):
        zoom_step = 0.5 if event.angleDelta().y() > 0 else -0.5
        self.zoom = max(4.0, min(18.0, self.zoom + zoom_step))
        self.update()


if __name__ == '__main__':
    app = QApplication(sys.argv)
    window = QMainWindow()
    window.setWindowTitle("Vector Tile Viewer (Pro Edition)")
    window.resize(1024, 768)

    # Укажите путь к вашей SQLite базе данных
    map_widget = MapWidget("vector_tiles.sqlite")
    window.setCentralWidget(map_widget)
    window.show()
    sys.exit(app.exec_())