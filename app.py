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
from PyQt5.QtGui import QPainter, QPainterPath, QColor, QPen, QBrush, QFont, QPolygonF
from PyQt5.QtCore import Qt, QPointF, QRectF, pyqtSignal, QObject

# ==========================================
# 1. Настройки и Глобальные константы
# ==========================================
TILE_SIZE = 256
MVT_EXTENT = 4096
MVT_SCALE = TILE_SIZE / MVT_EXTENT  # Перевод MVT координат в 256px тайл


# ==========================================
# 2. Менеджер Стилизации (Горячая замена)
# ==========================================
class StyleManager:
    def __init__(self, config_path="style.yaml"):
        self.config_path = config_path
        self.rules = {}
        self.load_styles()

    def load_styles(self):
        try:
            with open(self.config_path, 'r', encoding='utf-8') as f:
                self.rules = yaml.safe_load(f).get('layers', {}).get('osm', {})
            print("Стили успешно загружены.")
        except Exception as e:
            print(f"Ошибка загрузки стилей: {e}")
            self.rules = {}

    def get_style(self, tags, zoom):
        for key in ['highway', 'waterway', 'building', 'natural']:
            if key in tags:
                rule = self.rules.get(key)
                if rule and zoom >= rule.get('z_min', 0):
                    return rule, key
        return None, None


# ==========================================
# 3. Асинхронный Загрузчик (Worker & Queue)
# ==========================================
class WorkerSignals(QObject):
    # Сигнал для передачи готовых путей в UI-поток
    tile_decoded = pyqtSignal(tuple, list)


class TileLoader:
    def __init__(self, db_path, cache_ref):
        self.db_path = db_path
        self.cache = cache_ref
        self.task_queue = queue.PriorityQueue()
        self.signals = WorkerSignals()
        self.active_workers = True
        self.visible_tiles = set()  # Для инвалидации старых задач

        # Thread Local Storage для SQLite (SQLite не любит шаринг между потоками)
        self.local = threading.local()

        for _ in range(4):  # 4 потока декодирования
            t = threading.Thread(target=self._worker_loop, daemon=True)
            t.start()

    def get_db(self):
        if not hasattr(self.local, 'conn'):
            self.local.conn = sqlite3.connect(self.db_path, check_same_thread=False)
            self.local.cursor = self.local.conn.cursor()
        return self.local.cursor

    def request_tile(self, z, x, y, center_tx, center_ty):
        if (z, x, y) in self.cache:
            return

        # ТЗ: Приоритет по центру + LIFO для новых
        dist_to_center = math.hypot(x - center_tx, y - center_ty)
        lifo_priority = -time.time()
        priority = (dist_to_center, lifo_priority)

        self.task_queue.put((priority, (z, x, y)))

    def _worker_loop(self):
        while self.active_workers:
            try:
                priority, tile_key = self.task_queue.get(timeout=1)
                z, x, y = tile_key

                # ТЗ: Garbage Collection (Инвалидация)
                if tile_key not in self.visible_tiles:
                    self.task_queue.task_done()
                    continue

                cursor = self.get_db()
                tms_y = (1 << z) - 1 - y  # Конвертация Web Mercator -> TMS (MBTiles)
                cursor.execute("SELECT tile_data FROM tiles WHERE zoom_level=? AND tile_column=? AND tile_row=?",
                               (z, x, tms_y))
                row = cursor.fetchone()

                if row:
                    raw_data = zlib.decompress(row[0])
                    decoded = mapbox_vector_tile.decode(raw_data)
                    paths = self._build_paths(decoded)
                    self.signals.tile_decoded.emit(tile_key, paths)
                else:
                    # Пустой тайл (нет данных)
                    self.signals.tile_decoded.emit(tile_key, [])

                self.task_queue.task_done()
            except queue.Empty:
                continue
            except Exception as e:
                print(f"Ошибка декодирования тайла {tile_key}: {e}")
                self.task_queue.task_done()

    def _build_paths(self, mvt_data):
        """Парсинг MVT геометрии в массивы данных (БЕЗ QPainterPath, т.к. мы в фоне)"""
        features_data = []
        if 'osm' not in mvt_data: return features_data

        for feat in mvt_data['osm']['features']:
            geom_type = feat['geometry']['type']
            coords = feat['geometry']['coordinates']
            tags = feat['properties']

            features_data.append({
                'type': geom_type,
                'coords': coords,
                'tags': tags
            })
        return features_data


# ==========================================
# 4. Главный OpenGL Холст Карты
# ==========================================
class MapCanvas(QOpenGLWidget):
    def __init__(self, db_path):
        super().__init__()
        self.setMouseTracking(True)
        self.setUpdateBehavior(QOpenGLWidget.PartialUpdate)

        self.style_manager = StyleManager()

        # ТЗ: LRU Кэш для QPainterPath (память ограничиваем 1000 тайлами)
        self.tile_cache = LRUCache(maxsize=1000)
        self.loader = TileLoader(db_path, self.tile_cache)
        self.loader.signals.tile_decoded.connect(self.on_tile_decoded)

        # Навигация (Кипр по умолчанию)
        self.center_lon = 33.3823
        self.center_lat = 35.1856
        self.zoom = 10.0

        # Взаимодействие
        self.dragging = False
        self.last_mouse_pos = None

    def on_tile_decoded(self, tile_key, features_data):
        """Вызывается в Главном потоке. Конвертирует сырые данные в аппаратные QPainterPath"""
        compiled_features = []
        for feat in features_data:
            path = QPainterPath()
            if feat['type'] == 'LineString':
                path.moveTo(feat['coords'][0][0] * MVT_SCALE, feat['coords'][0][1] * MVT_SCALE)
                for pt in feat['coords'][1:]:
                    path.lineTo(pt[0] * MVT_SCALE, pt[1] * MVT_SCALE)
            elif feat['type'] == 'Polygon':
                for ring in feat['coords']:
                    path.moveTo(ring[0][0] * MVT_SCALE, ring[0][1] * MVT_SCALE)
                    for pt in ring[1:]:
                        path.lineTo(pt[0] * MVT_SCALE, pt[1] * MVT_SCALE)
                    path.closeSubpath()

            compiled_features.append({
                'path': path,
                'tags': feat['tags'],
                'type': feat['type']
            })

        self.tile_cache[tile_key] = compiled_features
        self.update()  # Запрос перерисовки экрана

    # --- МАТЕМАТИКА НАВИГАЦИИ ---
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
            # Перевод смещения пикселей экрана в градусы
            pixels_per_lon = (TILE_SIZE * (2 ** self.zoom)) / 360.0
            pixels_per_lat = (TILE_SIZE * (2 ** self.zoom)) / 180.0  # Упрощенно для панорамирования

            self.center_lon -= delta.x() / pixels_per_lon
            self.center_lat += delta.y() / pixels_per_lat
            self.last_mouse_pos = event.pos()
            self.update()

    def wheelEvent(self, event):
        zoom_delta = event.angleDelta().y() / 1200.0
        self.zoom = max(6.0, min(14.0, self.zoom + zoom_delta))
        self.update()

    # --- ОТРИСОВКА ---
    def paintEvent(self, event):
        painter = QPainter(self)
        painter.setRenderHint(QPainter.Antialiasing)
        painter.fillRect(self.rect(), QColor("#F4F4F4"))  # Фон карты

        # 1. Расчет видимой сетки тайлов
        w, h = self.width(), self.height()
        cx, cy = mercantile.xy(self.center_lon, self.center_lat)

        z_int = int(self.zoom)
        scale_fraction = 2 ** (self.zoom - z_int)

        # Вычисляем дробный тайл в центре экрана
        center_tx = (self.center_lon + 180.0) / 360.0 * (2 ** z_int)
        lat_rad = math.radians(self.center_lat)
        center_ty = (1.0 - math.asinh(math.tan(lat_rad)) / math.pi) / 2.0 * (2 ** z_int)

        # Определяем границы экрана в тайлах
        tiles_w = (w / TILE_SIZE) / scale_fraction
        tiles_h = (h / TILE_SIZE) / scale_fraction

        min_tx = int(math.floor(center_tx - tiles_w / 2))
        max_tx = int(math.ceil(center_tx + tiles_w / 2))
        min_ty = int(math.floor(center_ty - tiles_h / 2))
        max_ty = int(math.ceil(center_ty + tiles_h / 2))

        # Обновляем видимые тайлы для Garbage Collector'а
        visible_keys = set((z_int, x, y) for x in range(min_tx, max_tx + 1) for y in range(min_ty, max_ty + 1))
        self.loader.visible_tiles = visible_keys

        # 2. Рендеринг тайлов
        painter.save()
        # Смещаем центр координат painter'а в центр экрана
        painter.translate(w / 2, h / 2)
        painter.scale(scale_fraction, scale_fraction)

        for x in range(min_tx, max_tx + 1):
            for y in range(min_ty, max_ty + 1):
                tile_key = (z_int, x, y)

                # Пиксельные координаты левого верхнего угла тайла относительно центра
                px = (x - center_tx) * TILE_SIZE
                py = (y - center_ty) * TILE_SIZE

                painter.save()
                painter.translate(px, py)

                if tile_key in self.tile_cache:
                    self.draw_vector_features(painter, self.tile_cache[tile_key], z_int)
                else:
                    self.loader.request_tile(z_int, x, y, center_tx, center_ty)
                    # ТЗ: Overzooming (ищем тайл меньшего зума в кэше)
                    self.draw_overzoom(painter, z_int, x, y)

                # Отрисовка границ тайла (для дебага)
                painter.setPen(QPen(QColor(200, 200, 200, 100), 1))
                painter.drawRect(0, 0, TILE_SIZE, TILE_SIZE)
                painter.restore()

        painter.restore()

        # 3. Дебаг Overlay и Масштабная линейка
        self.draw_overlays(painter, w, h)
        painter.end()

    def draw_vector_features(self, painter, features, zoom):
        """Отрисовка массива QPainterPath с применением текущих стилей"""
        for feat in features:
            rule, layer_name = self.style_manager.get_style(feat['tags'], zoom)
            if not rule: continue

            path = feat['path']
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
            painter.drawPath(path)

    def draw_overzoom(self, painter, z, x, y):
        """ТЗ: Если тайла нет, берем родительский тайл, растягиваем и смещаем"""
        for z_diff in range(1, 4):
            parent_z = z - z_diff
            parent_x = x >> z_diff
            parent_y = y >> z_diff

            if (parent_z, parent_x, parent_y) in self.tile_cache:
                # Нашли! Рассчитываем смещение внутри родителя
                scale = 2 ** z_diff
                offset_x = (x - (parent_x << z_diff)) * TILE_SIZE
                offset_y = (y - (parent_y << z_diff)) * TILE_SIZE

                painter.save()
                # Клиппируем, чтобы куски родителя не вылезали за границы текущего тайла
                painter.setClipRect(0, 0, TILE_SIZE, TILE_SIZE)

                # Масштабируем и сдвигаем родительский тайл
                painter.translate(-offset_x, -offset_y)
                painter.scale(scale, scale)

                self.draw_vector_features(painter, self.tile_cache[(parent_z, parent_x, parent_y)], parent_z)
                painter.restore()
                return  # Нарисовали самый свежий родитель, выходим

    def draw_overlays(self, painter, w, h):
        # Отладочная информация в левом верхнем углу
        painter.setPen(Qt.black)
        painter.setFont(QFont("Consolas", 10))

        info = [
            f"Coords: {self.center_lon:.4f}, {self.center_lat:.4f}",
            f"Viewport Zoom: {self.zoom:.2f}",
            f"DB Level (z_int): {int(self.zoom)}",
            f"Cache Memory: {len(self.tile_cache)} / 1000 tiles",
            f"Queue Size: {self.loader.task_queue.qsize()}"
        ]

        y_offset = 20
        for text in info:
            # Тень для читаемости
            painter.setPen(Qt.white)
            painter.drawText(11, y_offset + 1, text)
            painter.setPen(Qt.black)
            painter.drawText(10, y_offset, text)
            y_offset += 15

        # Масштабная линейка в левом нижнем углу
        # Метров в одном пикселе на экваторе = 156543.03 / 2^zoom
        meters_per_pixel = 156543.03392 * math.cos(math.radians(self.center_lat)) / (2 ** self.zoom)

        # Выбираем красивый шаг (например 5 км, 1 км, 500 м)
        target_width_px = 150
        target_meters = meters_per_pixel * target_width_px

        if target_meters > 5000:
            bar_dist = 5000; label = "5 km"
        elif target_meters > 1000:
            bar_dist = 1000; label = "1 km"
        elif target_meters > 500:
            bar_dist = 500; label = "500 m"
        else:
            bar_dist = 100; label = "100 m"

        bar_px = bar_dist / meters_per_pixel

        bottom_y = h - 20
        painter.setPen(QPen(Qt.black, 3))
        painter.drawLine(10, bottom_y, int(10 + bar_px), bottom_y)
        painter.drawLine(10, bottom_y - 5, 10, bottom_y + 5)
        painter.drawLine(int(10 + bar_px), bottom_y - 5, int(10 + bar_px), bottom_y + 5)

        painter.drawText(10, bottom_y - 10, label)


# ==========================================
# 5. Главное окно приложения
# ==========================================
class MainWindow(QMainWindow):
    def __init__(self, db_path):
        super().__init__()
        self.setWindowTitle("OSM Vector Tile Viewer")
        self.resize(1024, 768)

        central_widget = QWidget()
        layout = QVBoxLayout(central_widget)
        layout.setContentsMargins(0, 0, 0, 0)

        self.map_canvas = MapCanvas(db_path)

        # Кнопка горячей перезагрузки стилей
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

    # Имя файла базы, который мы сгенерировали скриптом конвертера
    db_file = "cyprus_fast.mbtiles"

    if not os.path.exists(db_file):
        print(f"ВНИМАНИЕ: Файл {db_file} не найден. Карта будет пустой.")

    window = MainWindow(db_file)
    window.show()
    sys.exit(app.exec_())