import sys
import os
import math
import time
import zlib
import sqlite3
import yaml
import queue
import threading
import mercantile
import mapbox_vector_tile
from cachetools import LRUCache

from PyQt5.QtWidgets import QApplication, QMainWindow, QOpenGLWidget, QVBoxLayout, QWidget, QPushButton
from PyQt5.QtGui import QPainter, QPainterPath, QColor, QPen, QBrush, QFont
from PyQt5.QtCore import Qt, QPointF, QRectF, pyqtSignal, QObject

# ==========================================
# Настройки
# ==========================================
TILE_SIZE = 256
MVT_EXTENT = 4096
MVT_SCALE = TILE_SIZE / MVT_EXTENT

# Доступные слои в базе данных конвертера
MIN_DB_ZOOM = 6
MAX_DB_ZOOM = 14

LAYER_PRIORITY = {
    'greenery': 10,
    'water_poly': 20,
    'waterway': 30,
    'highway_service': 40,
    'highway_residential': 41,
    'highway_unclassified': 42,
    'highway_tertiary': 43,
    'highway_secondary': 44,
    'highway_primary': 45,
    'highway_trunk': 46,
    'highway_motorway': 47,
    'building': 50
}


class StyleManager:
    def __init__(self, config_path="style.yaml"):
        self.config_path = config_path
        self.rules = {}
        self.load_styles()

    def load_styles(self):
        try:
            with open(self.config_path, 'r', encoding='utf-8') as f:
                self.rules = yaml.safe_load(f).get('layers', {})
        except Exception:
            self.rules = {}

    def get_style(self, layer_name, zoom):
        """Теперь мы просто ищем правило по имени MVT-слоя"""
        rule = self.rules.get(layer_name)
        if rule and zoom >= rule.get('z_min', 0):
            return rule
        return None


class WorkerSignals(QObject):
    tile_decoded = pyqtSignal(tuple, list)


# ==========================================
# Асинхронный Загрузчик
# ==========================================
class TileLoader:
    def __init__(self, db_path, cache_ref):
        self.db_path = db_path
        self.cache = cache_ref
        self.task_queue = queue.PriorityQueue()
        self.signals = WorkerSignals()
        self.active_workers = True
        self.visible_tiles = set()

        # Защита от спама в очередь (Проблема №2)
        self.loading_tiles = set()
        self.local = threading.local()

        for _ in range(4):
            t = threading.Thread(target=self._worker_loop, daemon=True)
            t.start()

    def get_db(self):
        if not hasattr(self.local, 'conn'):
            self.local.conn = sqlite3.connect(self.db_path, check_same_thread=False)
            self.local.cursor = self.local.conn.cursor()
        return self.local.cursor

    def request_tile(self, z, x, y, center_tx, center_ty):
        tile_key = (z, x, y)

        # Если тайл уже в кэше ИЛИ уже качается — игнорируем!
        if tile_key in self.cache or tile_key in self.loading_tiles:
            return

        self.loading_tiles.add(tile_key)

        dist_to_center = math.hypot(x - center_tx, y - center_ty)
        priority = (dist_to_center, -time.time())
        self.task_queue.put((priority, tile_key))

    def _worker_loop(self):
        while self.active_workers:
            try:
                priority, tile_key = self.task_queue.get(timeout=1)
                z, x, y = tile_key

                # Если мы улетели камерой, отменяем задачу (сборка мусора)
                if tile_key not in self.visible_tiles:
                    self.loading_tiles.discard(tile_key)
                    self.task_queue.task_done()
                    continue

                cursor = self.get_db()
                tms_y = (1 << z) - 1 - y
                cursor.execute("SELECT tile_data FROM tiles WHERE zoom_level=? AND tile_column=? AND tile_row=?",
                               (z, x, tms_y))
                row = cursor.fetchone()

                compiled_features = []
                if row:
                    raw_data = zlib.decompress(row[0])
                    decoded = mapbox_vector_tile.decode(raw_data)
                    # Проблема №1: Вся математика геометрии теперь в фоновом потоке!
                    compiled_features = self._build_hardware_paths(decoded)

                self.signals.tile_decoded.emit(tile_key, compiled_features)
                self.loading_tiles.discard(tile_key)  # Снимаем блокировку
                self.task_queue.task_done()

            except queue.Empty:
                continue
            except Exception as e:
                self.loading_tiles.discard(tile_key)
                self.task_queue.task_done()

    def _build_hardware_paths(self, mvt_data):
        """Создает QPainterPath прямо в фоне, чтобы не тормозить UI"""
        features_data = []
        for layer_name, layer_data in mvt_data.items():
            for feat in layer_data['features']:
                geom_type = feat['geometry']['type']
                coords = feat['geometry']['coordinates']

                path = QPainterPath()
                if geom_type == 'LineString':
                    path.moveTo(coords[0][0] * MVT_SCALE, coords[0][1] * MVT_SCALE)
                    for pt in coords[1:]:
                        path.lineTo(pt[0] * MVT_SCALE, pt[1] * MVT_SCALE)
                elif geom_type == 'Polygon':
                    for ring in coords:
                        path.moveTo(ring[0][0] * MVT_SCALE, ring[0][1] * MVT_SCALE)
                        for pt in ring[1:]:
                            path.lineTo(pt[0] * MVT_SCALE, pt[1] * MVT_SCALE)
                        path.closeSubpath()

                features_data.append({
                    'path': path,
                    'tags': feat['properties'],
                    'type': geom_type,
                    'layer_name': layer_name
                })
        return features_data


# ==========================================
# Главный OpenGL Холст Карты
# ==========================================
class MapCanvas(QOpenGLWidget):
    def __init__(self, db_path):
        super().__init__()
        self.setMouseTracking(True)
        self.setUpdateBehavior(QOpenGLWidget.PartialUpdate)
        self.style_manager = StyleManager()
        self.tile_cache = LRUCache(maxsize=1500)

        self.loader = TileLoader(db_path, self.tile_cache)
        self.loader.signals.tile_decoded.connect(self.on_tile_decoded)

        self.center_lon = 33.3823
        self.center_lat = 35.1856
        self.zoom = 10.0
        self.dragging = False
        self.last_mouse_pos = None

    def on_tile_decoded(self, tile_key, compiled_features):
        # UI поток теперь просто сохраняет готовые контуры и просит отрисовать
        self.tile_cache[tile_key] = compiled_features
        self.update()

    # --- НАВИГАЦИЯ ---
    def mousePressEvent(self, event):
        if event.button() == Qt.LeftButton:
            self.dragging = True
            self.last_mouse_pos = event.pos()

    def mouseReleaseEvent(self, event):
        if event.button() == Qt.LeftButton:
            self.dragging = False

    def mouseMoveEvent(self, event):
        if self.dragging:
            delta = event.pos() - self.last_mouse_pos
            pixels_per_lon = (TILE_SIZE * (2 ** self.zoom)) / 360.0
            pixels_per_lat = (TILE_SIZE * (2 ** self.zoom)) / 180.0
            self.center_lon -= delta.x() / pixels_per_lon
            self.center_lat += delta.y() / pixels_per_lat
            self.last_mouse_pos = event.pos()
            self.update()

    def wheelEvent(self, event):
        zoom_delta = event.angleDelta().y() / 1200.0
        # Разрешаем зумить камеру от 2 (весь мир) до 20 (домики)
        self.zoom = max(8.0, min(17.0, self.zoom + zoom_delta))
        self.update()

    # --- ОТРИСОВКА ---
    def paintEvent(self, event):
        painter = QPainter(self)
        painter.setRenderHint(QPainter.Antialiasing)
        painter.fillRect(self.rect(), QColor("#AADAFF"))

        w, h = self.width(), self.height()

        # Проблема №3: Ограничиваем запрос к БД пределами MIN_DB_ZOOM и MAX_DB_ZOOM
        target_z = int(math.floor(self.zoom))
        z_int = max(MIN_DB_ZOOM, min(MAX_DB_ZOOM, target_z))

        # Если self.zoom = 16, а z_int = 14, scale_fraction будет 4.0 (Overzoom)
        # Если self.zoom = 4, а z_int = 6, scale_fraction будет 0.25 (Underzoom)
        scale_fraction = 2 ** (self.zoom - z_int)

        center_tx = (self.center_lon + 180.0) / 360.0 * (2 ** z_int)
        lat_rad = math.radians(self.center_lat)
        center_ty = (1.0 - math.asinh(math.tan(lat_rad)) / math.pi) / 2.0 * (2 ** z_int)

        tiles_w = (w / TILE_SIZE) / scale_fraction
        tiles_h = (h / TILE_SIZE) / scale_fraction

        min_tx = int(math.floor(center_tx - tiles_w / 2))
        max_tx = int(math.ceil(center_tx + tiles_w / 2))
        min_ty = int(math.floor(center_ty - tiles_h / 2))
        max_ty = int(math.ceil(center_ty + tiles_h / 2))

        # Обновляем видимые тайлы для Background Worker
        visible_keys = set((z_int, x, y) for x in range(min_tx, max_tx + 1) for y in range(min_ty, max_ty + 1))
        self.loader.visible_tiles = visible_keys

        painter.save()
        painter.translate(w / 2, h / 2)
        painter.scale(scale_fraction, scale_fraction)

        for x in range(min_tx, max_tx + 1):
            for y in range(min_ty, max_ty + 1):
                tile_key = (z_int, x, y)
                px = (x - center_tx) * TILE_SIZE
                py = (y - center_ty) * TILE_SIZE

                painter.save()
                painter.translate(px, py)

                if tile_key in self.tile_cache:
                    self.draw_vector_features(painter, self.tile_cache[tile_key], z_int, scale_fraction)
                else:
                    self.loader.request_tile(z_int, x, y, center_tx, center_ty)

                # Дебаг сетки тайлов (можно закомментировать)
                # painter.setPen(QPen(QColor(200, 200, 200, 100), 1))
                # painter.drawRect(0, 0, TILE_SIZE, TILE_SIZE)
                painter.restore()

        painter.restore()
        self.draw_overlays(painter, w, h, z_int)
        painter.end()

    def draw_vector_features(self, painter, features, zoom, scale_fraction):
        # 1. Сортируем все объекты по Z-Index (снизу вверх)
        sorted_features = sorted(features, key=lambda f: LAYER_PRIORITY.get(f.get('layer_name', ''), 0))

        for feat in sorted_features:
            layer_name = feat.get('layer_name', '')

            # Оптимизация (Culling) для мелких зданий
            if feat['type'] == 'Polygon' and layer_name == 'building':
                rect = feat['path'].boundingRect()
                if (rect.width() * scale_fraction < 2.5) and (rect.height() * scale_fraction < 2.5):
                    continue

            # 2. Получаем стиль, передавая ТОЛЬКО имя слоя (например, 'highway_primary')
            rule = self.style_manager.get_style(layer_name, zoom)
            if not rule: continue

            pen = QPen(Qt.NoPen)
            brush = QBrush(Qt.NoBrush)

            if 'color' in rule:
                pen = QPen(QColor(rule['color']), rule.get('width', 1.0))
                pen.setJoinStyle(Qt.RoundJoin)
                pen.setCapStyle(Qt.RoundCap)
            if 'fill' in rule and feat['type'] == 'Polygon':
                brush = QBrush(QColor(rule['fill']))

            painter.setPen(pen)
            painter.setBrush(brush)
            painter.drawPath(feat['path'])

    def draw_overlays(self, painter, w, h, z_int):
        painter.setPen(Qt.black)
        painter.setFont(QFont("Consolas", 10))
        info = [
            f"Zoom: {self.zoom:.2f} (DB Layer: {z_int})",
            f"Cache: {len(self.tile_cache)} | Queue: {self.loader.task_queue.qsize()}",
            f"Active Load: {len(self.loader.loading_tiles)}"
        ]
        y_offset = 20
        for text in info:
            painter.setPen(Qt.white);
            painter.drawText(11, y_offset + 1, text)
            painter.setPen(Qt.black);
            painter.drawText(10, y_offset, text)
            y_offset += 15

        # --- МАСШТАБНАЯ ЛИНЕЙКА ---
        meters_per_pixel = 156543.03392 * math.cos(math.radians(self.center_lat)) / (2 ** self.zoom)
        target_meters = meters_per_pixel * 150  # Идеальная ширина ~ 150px

        if target_meters > 10000:
            bar_dist = 10000; label = "10 km"
        elif target_meters > 5000:
            bar_dist = 5000; label = "5 km"
        elif target_meters > 1000:
            bar_dist = 1000; label = "1 km"
        elif target_meters > 500:
            bar_dist = 500; label = "500 m"
        else:
            bar_dist = 100; label = "100 m"

        bar_px = bar_dist / meters_per_pixel
        bottom_y = h - 25
        painter.setPen(QPen(Qt.black, 3))
        painter.drawLine(15, bottom_y, int(15 + bar_px), bottom_y)
        painter.drawLine(15, bottom_y - 5, 15, bottom_y + 5)
        painter.drawLine(int(15 + bar_px), bottom_y - 5, int(15 + bar_px), bottom_y + 5)
        painter.drawText(15, bottom_y - 10, label)


# ==========================================
# Главное окно
# ==========================================
class MainWindow(QMainWindow):
    def __init__(self, db_path):
        super().__init__()
        self.setWindowTitle("Pro Vector Tile Viewer")
        self.resize(1200, 800)

        central_widget = QWidget()
        layout = QVBoxLayout(central_widget)
        layout.setContentsMargins(0, 0, 0, 0)

        self.map_canvas = MapCanvas(db_path)

        btn_reload = QPushButton("🔄 Перезагрузить стили (style.yaml)")
        btn_reload.clicked.connect(self.reload_styles)
        btn_reload.setStyleSheet("background-color: #333; color: white; padding: 5px;")

        layout.addWidget(btn_reload)
        layout.addWidget(self.map_canvas)
        self.setCentralWidget(central_widget)

    def reload_styles(self):
        self.map_canvas.style_manager.load_styles()
        self.map_canvas.update()


if __name__ == "__main__":
    app = QApplication(sys.argv)
    db_file = "cyprus_fast.mbtiles"
    window = MainWindow(db_file)
    window.show()
    sys.exit(app.exec_())