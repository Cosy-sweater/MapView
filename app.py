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

from PyQt5.QtWidgets import (QApplication, QMainWindow, QOpenGLWidget, QVBoxLayout,
                             QWidget, QPushButton, QHBoxLayout, QFileDialog,
                             QTextEdit, QMessageBox, QLineEdit, QLabel, QDialog,
                             QScrollArea, QFormLayout, QColorDialog, QDoubleSpinBox)
from PyQt5.QtGui import QPainter, QPainterPath, QColor, QPen, QBrush, QFont, QTextCursor
from PyQt5.QtCore import Qt, pyqtSignal, QObject, QThread

# ==========================================
# Настройки и Файловая система
# ==========================================
TILE_SIZE = 256
MVT_EXTENT = 4096
MVT_SCALE = TILE_SIZE / MVT_EXTENT

MIN_DB_ZOOM = 6
MAX_DB_ZOOM = 14

MAX_CACHE_TILES = 600
TARGET_CACHE_TILES = 500

MAPFILES_DIR = os.path.join(os.getcwd(), ".mapfiles")
os.makedirs(MAPFILES_DIR, exist_ok=True)
DOWNLOADS_DIR = os.path.join(os.path.expanduser("~"), "Downloads")

LAYER_PRIORITY = {
    'landmass': 5,
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
    'building_large': 50,
    'building_small': 51
}

DEFAULT_STYLES = {
    'layers': {
        'landmass': {'fill': '#F2EFE9', 'z_min': 6},
        'greenery': {'fill': '#C2DCA8', 'z_min': 6},
        'water_poly': {'fill': '#74A0C2', 'z_min': 6},
        'waterway': {'color': '#74A0C2', 'width': 1.5, 'z_min': 7},
        'building_large': {'fill': '#D9D8D6', 'color': '#B4B3B1', 'width': 0.5, 'z_min': 13},
        'building_small': {'fill': '#D9D8D6', 'color': '#C4C3C1', 'width': 0.5, 'z_min': 14},
        'highway_service': {'color': '#FFFFFF', 'width': 1.0, 'z_min': 14},
        'highway_residential': {'color': '#FFFFFF', 'width': 1.5, 'z_min': 12},
        'highway_unclassified': {'color': '#FFFFFF', 'width': 1.5, 'z_min': 11},
        'highway_tertiary': {'color': '#FFFFB3', 'width': 2.0, 'z_min': 10},
        'highway_secondary': {'color': '#F6CFA6', 'width': 2.5, 'z_min': 9},
        'highway_primary': {'color': '#FCD6A4', 'width': 3.0, 'z_min': 7},
        'highway_trunk': {'color': '#F9B29C', 'width': 3.5, 'z_min': 6},
        'highway_motorway': {'color': '#E892A2', 'width': 4.0, 'z_min': 6}
    }
}


# ==========================================
# Утилиты
# ==========================================
class EmittingStream(QObject):
    textWritten = pyqtSignal(str)

    def write(self, text):
        self.textWritten.emit(str(text))

    def flush(self):
        pass


class ConverterThread(QThread):
    finished = pyqtSignal(str)
    error = pyqtSignal(str)

    def __init__(self, pbf_path, mbtiles_path):
        super().__init__()
        self.pbf_path = pbf_path
        self.mbtiles_path = mbtiles_path

    def run(self):
        try:
            import converter
            converter.run(self.pbf_path, self.mbtiles_path)
            self.finished.emit(self.mbtiles_path)
        except Exception as e:
            self.error.emit(str(e))


class StyleManager:
    def __init__(self, config_path="style.yaml"):
        self.config_path = config_path
        self.rules = {}
        self.load_styles()

    def load_styles(self):
        if not os.path.exists(self.config_path):
            self.reset_to_defaults()

        try:
            with open(self.config_path, 'r', encoding='utf-8') as f:
                self.rules = yaml.safe_load(f).get('layers', {})
        except Exception:
            self.rules = {}

    def save_styles(self, new_rules):
        self.rules = new_rules
        with open(self.config_path, 'w', encoding='utf-8') as f:
            yaml.dump({'layers': self.rules}, f, default_flow_style=False, allow_unicode=True)

    def reset_to_defaults(self):
        with open(self.config_path, 'w', encoding='utf-8') as f:
            yaml.dump(DEFAULT_STYLES, f, default_flow_style=False, allow_unicode=True)
        self.rules = DEFAULT_STYLES['layers']

    def get_style(self, layer_name, zoom):
        rule = self.rules.get(layer_name)
        if rule and zoom >= rule.get('z_min', 0):
            return rule
        return None


# ==========================================
# Окно Редактора Стилей
# ==========================================
class StyleEditorDialog(QDialog):
    def __init__(self, style_manager, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Редактор стилей (style.yaml)")
        self.setWindowFlags(self.windowFlags() & ~Qt.WindowContextHelpButtonHint)
        self.resize(500, 600)
        self.style_manager = style_manager

        self.inputs = {}
        layout = QVBoxLayout(self)

        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        form_widget = QWidget()
        self.form_layout = QFormLayout(form_widget)

        self.populate_form()

        scroll.setWidget(form_widget)
        layout.addWidget(scroll)

        btn_layout = QHBoxLayout()
        btn_save = QPushButton("💾 Сохранить стили")
        btn_save.clicked.connect(self.save_and_close)

        btn_reset = QPushButton("⚠️ Сбросить по умолчанию")
        btn_reset.clicked.connect(self.reset_styles)
        btn_reset.setStyleSheet("background-color: #A00; color: white;")

        btn_layout.addWidget(btn_save)
        btn_layout.addWidget(btn_reset)
        layout.addLayout(btn_layout)

    def populate_form(self):
        while self.form_layout.count():
            item = self.form_layout.takeAt(0)
            widget = item.widget()
            if widget is not None: widget.deleteLater()

        self.inputs.clear()

        for layer_name, props in self.style_manager.rules.items():
            row_widget = QWidget()
            row_layout = QHBoxLayout(row_widget)
            row_layout.setContentsMargins(0, 0, 0, 0)

            self.inputs[layer_name] = {}

            if 'fill' in props:
                btn_fill = self.create_color_button(props['fill'])
                row_layout.addWidget(QLabel("Заливка:"))
                row_layout.addWidget(btn_fill)
                self.inputs[layer_name]['fill'] = btn_fill

            if 'color' in props:
                btn_color = self.create_color_button(props['color'])
                row_layout.addWidget(QLabel("Линия:"))
                row_layout.addWidget(btn_color)
                self.inputs[layer_name]['color'] = btn_color

            if 'width' in props:
                spin_w = QDoubleSpinBox()
                spin_w.setRange(0.1, 10.0)
                spin_w.setSingleStep(0.5)
                spin_w.setValue(props['width'])
                row_layout.addWidget(QLabel("Толщина:"))
                row_layout.addWidget(spin_w)
                self.inputs[layer_name]['width'] = spin_w

            row_layout.addStretch()
            self.form_layout.addRow(f"<b>{layer_name}</b>", row_widget)

    def create_color_button(self, hex_color):
        btn = QPushButton()
        btn.setFixedSize(30, 20)
        btn.setStyleSheet(f"background-color: {hex_color}; border: 1px solid black;")
        btn.color_val = hex_color

        def pick_color():
            color = QColorDialog.getColor(QColor(btn.color_val), self, "Выберите цвет",
                                          QColorDialog.DontUseNativeDialog)
            if color.isValid():
                btn.color_val = color.name()
                btn.setStyleSheet(f"background-color: {btn.color_val}; border: 1px solid black;")

        btn.clicked.connect(pick_color)
        return btn

    def save_and_close(self):
        new_rules = {}
        for layer_name, old_props in self.style_manager.rules.items():
            new_rules[layer_name] = old_props.copy()
            if 'fill' in self.inputs[layer_name]: new_rules[layer_name]['fill'] = self.inputs[layer_name][
                'fill'].color_val
            if 'color' in self.inputs[layer_name]: new_rules[layer_name]['color'] = self.inputs[layer_name][
                'color'].color_val
            if 'width' in self.inputs[layer_name]: new_rules[layer_name]['width'] = self.inputs[layer_name][
                'width'].value()

        self.style_manager.save_styles(new_rules)
        self.accept()

    def reset_styles(self):
        reply = QMessageBox.question(self, 'Сброс', 'Точно сбросить все стили к стандартным?',
                                     QMessageBox.Yes | QMessageBox.No)
        if reply == QMessageBox.Yes:
            self.style_manager.reset_to_defaults()
            self.populate_form()


# ==========================================
# Окно Конвертера
# ==========================================
class ConverterWindow(QWidget):
    conversion_finished = pyqtSignal(str)

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Создание новой карты (Конвертер)")
        self.resize(700, 500)

        self.selected_pbf = None
        self.target_mbtiles = None

        layout = QVBoxLayout(self)

        settings_layout = QFormLayout()
        self.btn_select_pbf = QPushButton("Выбрать файл .pbf")
        self.btn_select_pbf.clicked.connect(self.select_pbf)
        self.lbl_pbf = QLabel("Файл не выбран")

        self.input_name = QLineEdit("Новая_Карта")
        self.input_name.setPlaceholderText("Введите имя карты без расширения...")

        settings_layout.addRow(self.btn_select_pbf, self.lbl_pbf)
        settings_layout.addRow("Имя карты:", self.input_name)
        layout.addLayout(settings_layout)

        self.btn_start = QPushButton("▶ НАЧАТЬ КОНВЕРТАЦИЮ")
        self.btn_start.setStyleSheet("background-color: #2e7d32; color: white; font-weight: bold; padding: 10px;")
        self.btn_start.clicked.connect(self.start_conversion)
        layout.addWidget(self.btn_start)

        self.log_console = QTextEdit()
        self.log_console.setReadOnly(True)
        self.log_console.setTextInteractionFlags(Qt.NoTextInteraction)  # ЗАЩИТА ОТ КЛИКОВ (Логи не сбиваются)
        self.log_console.setStyleSheet("background-color: #1e1e1e; color: #00ff00; font-family: Consolas;")
        layout.addWidget(self.log_console)

        self.original_stdout = sys.stdout
        self.original_stderr = sys.stderr
        self.stream = EmittingStream()
        self.stream.textWritten.connect(self.write_log)

    def select_pbf(self):
        path, _ = QFileDialog.getOpenFileName(self, "Выберите PBF", DOWNLOADS_DIR, "OSM PBF (*.osm.pbf)")
        if path:
            self.selected_pbf = path
            self.lbl_pbf.setText(os.path.basename(path))

    def write_log(self, text):
        cursor = self.log_console.textCursor()
        if text.startswith('\r'):
            cursor.movePosition(QTextCursor.StartOfLine)
            cursor.movePosition(QTextCursor.EndOfLine, QTextCursor.KeepAnchor)
            cursor.removeSelectedText()
            cursor.insertText(text[1:])
        else:
            cursor.movePosition(QTextCursor.End)
            cursor.insertText(text)
        self.log_console.setTextCursor(cursor)
        self.log_console.ensureCursorVisible()

    def start_conversion(self):
        if not self.selected_pbf:
            QMessageBox.warning(self, "Ошибка", "Сначала выберите файл PBF!")
            return

        map_name = self.input_name.text().strip()
        if not map_name:
            QMessageBox.warning(self, "Ошибка", "Имя карты не может быть пустым!")
            return

        self.target_mbtiles = os.path.join(MAPFILES_DIR, f"{map_name}.mbtiles")

        if os.path.exists(self.target_mbtiles):
            reply = QMessageBox.question(self, 'Перезапись', 'Карта с таким именем уже существует. Перезаписать?',
                                         QMessageBox.Yes | QMessageBox.No)
            if reply == QMessageBox.No: return

        self.btn_start.setEnabled(False)
        self.btn_select_pbf.setEnabled(False)
        self.input_name.setEnabled(False)

        sys.stdout = self.stream
        sys.stderr = self.stream
        print(f"--- НАЧАЛО: {map_name} ---")

        self.thread = ConverterThread(self.selected_pbf, self.target_mbtiles)
        self.thread.finished.connect(self.on_finished)
        self.thread.error.connect(self.on_error)
        self.thread.start()

    def on_finished(self, mbtiles_path):
        self.restore_streams()
        self.conversion_finished.emit(mbtiles_path)
        QMessageBox.information(self, "Успех", f"Карта '{self.input_name.text()}' успешно создана!")
        self.close()

    def on_error(self, err_msg):
        self.restore_streams()
        self.btn_start.setEnabled(True)
        QMessageBox.critical(self, "Ошибка", f"Сбой конвертации:\n{err_msg}")

    def restore_streams(self):
        sys.stdout = self.original_stdout
        sys.stderr = self.original_stderr

    def closeEvent(self, event):
        self.restore_streams()
        event.accept()


# ==========================================
# Асинхронный Загрузчик (WorkerSignals & TileLoader)
# ==========================================
class WorkerSignals(QObject):
    tile_decoded = pyqtSignal(tuple, list)


class TileLoader:
    def __init__(self, db_path, cache_ref):
        self.db_path = db_path
        self.cache = cache_ref
        self.task_queue = queue.PriorityQueue()
        self.signals = WorkerSignals()
        self.active_workers = True
        self.visible_tiles = set()
        self.loading_tiles = set()
        self.local = threading.local()

        self.threads = []
        for _ in range(4):
            t = threading.Thread(target=self._worker_loop, daemon=True)
            t.start()
            self.threads.append(t)

    def stop(self):
        self.active_workers = False
        while not self.task_queue.empty():
            try:
                self.task_queue.get_nowait()
            except:
                pass

        # Надежное закрытие потоков и SQLite базы
        for t in self.threads:
            if t.is_alive():
                t.join(timeout=1.0)

    def get_db(self):
        if not hasattr(self.local, 'conn'):
            self.local.conn = sqlite3.connect(self.db_path, check_same_thread=False)
            self.local.cursor = self.local.conn.cursor()
        return self.local.cursor

    def request_tile(self, z, x, y, center_tx, center_ty):
        tile_key = (z, x, y)
        if tile_key in self.cache or tile_key in self.loading_tiles: return
        self.loading_tiles.add(tile_key)
        dist_to_center = math.hypot(x - center_tx, y - center_ty)
        self.task_queue.put(((dist_to_center, -time.time()), tile_key))

    def _worker_loop(self):
        while self.active_workers:
            try:
                priority, tile_key = self.task_queue.get(timeout=0.5)
                z, x, y = tile_key

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
                    compiled_features = self._build_hardware_paths(decoded)

                self.signals.tile_decoded.emit(tile_key, compiled_features)
                self.loading_tiles.discard(tile_key)
                self.task_queue.task_done()
            except queue.Empty:
                continue
            except Exception:
                self.loading_tiles.discard(tile_key)
                self.task_queue.task_done()

        if hasattr(self.local, 'conn'): self.local.conn.close()

    def _build_hardware_paths(self, mvt_data):
        features_data = []
        for layer_name, layer_data in mvt_data.items():
            for feat in layer_data['features']:
                geom_type = feat['geometry']['type']
                coords = feat['geometry']['coordinates']

                path = QPainterPath()
                if geom_type == 'LineString':
                    path.moveTo(coords[0][0] * MVT_SCALE, coords[0][1] * MVT_SCALE)
                    for pt in coords[1:]: path.lineTo(pt[0] * MVT_SCALE, pt[1] * MVT_SCALE)
                elif geom_type == 'Polygon':
                    for ring in coords:
                        path.moveTo(ring[0][0] * MVT_SCALE, ring[0][1] * MVT_SCALE)
                        for pt in ring[1:]: path.lineTo(pt[0] * MVT_SCALE, pt[1] * MVT_SCALE)
                        path.closeSubpath()

                features_data.append(
                    {'path': path, 'tags': feat['properties'], 'type': geom_type, 'layer_name': layer_name})
        return features_data


# ==========================================
# Отрисовка Карты
# ==========================================
class MapCanvas(QOpenGLWidget):
    def __init__(self):
        super().__init__()
        self.setMouseTracking(True)
        self.setUpdateBehavior(QOpenGLWidget.PartialUpdate)
        self.style_manager = StyleManager()

        # Замена LRUCache на стандартный dict (Свой механизм сборки мусора)
        self.tile_cache = {}
        self.loader = None
        self.db_path = None

        self.center_lon = 33.3823
        self.center_lat = 35.1856
        self.zoom = 10.0
        self.dragging = False
        self.last_mouse_pos = None

    def _auto_center(self, db_path):
        """Математически вычисляет центр региона по данным из БД"""
        try:
            conn = sqlite3.connect(db_path)
            c = conn.cursor()

            c.execute("SELECT MIN(zoom_level) FROM tiles")
            z_row = c.fetchone()
            if not z_row or z_row[0] is None: return
            z = z_row[0]

            c.execute(
                "SELECT MIN(tile_column), MAX(tile_column), MIN(tile_row), MAX(tile_row) FROM tiles WHERE zoom_level=?",
                (z,))
            row = c.fetchone()
            conn.close()

            if row and row[0] is not None:
                min_x, max_x, min_y, max_y = row
                center_x = (min_x + max_x) / 2.0
                center_y_tms = (min_y + max_y) / 2.0
                center_y = (1 << z) - 1 - center_y_tms

                n = 2.0 ** z
                self.center_lon = center_x / n * 360.0 - 180.0
                lat_rad = math.atan(math.sinh(math.pi * (1 - 2 * center_y / n)))
                self.center_lat = math.degrees(lat_rad)
                self.zoom = float(z)
                print(f"Карта отцентрирована: Lat {self.center_lat:.4f}, Lon {self.center_lon:.4f}")
        except Exception as e:
            print(f"Ошибка авто-центрирования: {e}")

    def load_database(self, db_path):
        if self.loader: self.loader.stop()
        self.tile_cache.clear()

        if os.path.exists(db_path):
            self.db_path = db_path
            self._auto_center(db_path)
            self.loader = TileLoader(db_path, self.tile_cache)
            self.loader.signals.tile_decoded.connect(self.on_tile_decoded)
            self.update()

    def unload_database(self):
        if self.loader:
            self.loader.stop()
            self.loader = None
        self.db_path = None
        self.tile_cache.clear()
        self.update()

    def clean_cache(self):
        """Умная очистка кэша (Spatial Eviction Policy)"""
        if len(self.tile_cache) <= MAX_CACHE_TILES:
            return

        current_z = int(math.floor(self.zoom))

        # Оценка бесполезности тайла
        def get_score(key):
            z, x, y = key
            z_diff = abs(z - current_z)

            # Перевод тайла в координаты для расчета дистанции до центра
            n = 2.0 ** z
            tile_lon = (x + 0.5) / n * 360.0 - 180.0
            lat_rad = math.atan(math.sinh(math.pi * (1 - 2 * (y + 0.5) / n)))
            tile_lat = math.degrees(lat_rad)

            dist_sq = (tile_lon - self.center_lon) ** 2 + (tile_lat - self.center_lat) ** 2
            return (z_diff, dist_sq)

        # Сортируем тайлы: сначала удаляем другие зумы, затем дальние по дистанции
        sorted_keys = sorted(self.tile_cache.keys(), key=get_score, reverse=True)

        tiles_to_delete = len(self.tile_cache) - TARGET_CACHE_TILES
        deleted = 0

        for k in sorted_keys:
            if deleted >= tiles_to_delete: break
            if self.loader and k in self.loader.visible_tiles: continue

            del self.tile_cache[k]
            deleted += 1

    def on_tile_decoded(self, tile_key, compiled_features):
        self.tile_cache[tile_key] = compiled_features
        # Запускаем сборку мусора при добавлении нового тайла
        self.clean_cache()
        self.update()

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
            self.center_lon -= delta.x() / ((TILE_SIZE * (2 ** self.zoom)) / 360.0)
            self.center_lat += delta.y() / ((TILE_SIZE * (2 ** self.zoom)) / 180.0)
            self.last_mouse_pos = event.pos()
            self.update()

    def wheelEvent(self, event):
        self.zoom = max(6.0, min(17.0, self.zoom + event.angleDelta().y() / 1200.0))
        self.update()

    def paintEvent(self, event):
        painter = QPainter(self)
        painter.setRenderHint(QPainter.Antialiasing)
        painter.fillRect(self.rect(), QColor("#AADAFF"))

        if not self.loader:
            painter.setPen(Qt.black)
            painter.setFont(QFont("Arial", 14))
            painter.drawText(self.rect(), Qt.AlignCenter, "Карта не загружена. Откройте или создайте MBTiles.")
            painter.end()
            return

        w, h = self.width(), self.height()
        z_int = max(MIN_DB_ZOOM, min(MAX_DB_ZOOM, int(math.floor(self.zoom))))
        scale = 2 ** (self.zoom - z_int)

        center_tx = (self.center_lon + 180.0) / 360.0 * (2 ** z_int)
        center_ty = (1.0 - math.asinh(math.tan(math.radians(self.center_lat))) / math.pi) / 2.0 * (2 ** z_int)

        tiles_w, tiles_h = (w / TILE_SIZE) / scale, (h / TILE_SIZE) / scale
        min_tx, max_tx = int(math.floor(center_tx - tiles_w / 2)), int(math.ceil(center_tx + tiles_w / 2))
        min_ty, max_ty = int(math.floor(center_ty - tiles_h / 2)), int(math.ceil(center_ty + tiles_h / 2))

        self.loader.visible_tiles = set(
            (z_int, x, y) for x in range(min_tx, max_tx + 1) for y in range(min_ty, max_ty + 1))

        painter.save()
        painter.translate(w / 2, h / 2)
        painter.scale(scale, scale)

        for x in range(min_tx, max_tx + 1):
            for y in range(min_ty, max_ty + 1):
                tile_key = (z_int, x, y)
                painter.save()
                painter.translate((x - center_tx) * TILE_SIZE, (y - center_ty) * TILE_SIZE)

                if tile_key in self.tile_cache:
                    self.draw_vector_features(painter, self.tile_cache[tile_key], z_int, scale)
                else:
                    self.loader.request_tile(z_int, x, y, center_tx, center_ty)
                painter.restore()

        painter.restore()

        # Информационный Overlay с Координатами
        painter.setPen(Qt.black)
        painter.setFont(QFont("Consolas", 10))
        y_offset = 20
        info = [
            f"Center: {self.center_lat:.4f}, {self.center_lon:.4f}",
            f"Zoom: {self.zoom:.2f}",
            f"Cache: {len(self.tile_cache)} / {MAX_CACHE_TILES}"
        ]

        for text in info:
            painter.setPen(Qt.white);
            painter.drawText(11, y_offset + 1, text)
            painter.setPen(Qt.black);
            painter.drawText(10, y_offset, text)
            y_offset += 15
        painter.end()

    def draw_vector_features(self, painter, features, zoom, scale_fraction):
        sorted_features = sorted(features, key=lambda f: LAYER_PRIORITY.get(f.get('layer_name', ''), 0))

        for feat in sorted_features:
            l_name = feat.get('layer_name', '')
            if feat['type'] == 'Polygon' and l_name.startswith('building'):
                rect = feat['path'].boundingRect()
                if (rect.width() * scale_fraction < 2.5) and (rect.height() * scale_fraction < 2.5): continue

            rule = self.style_manager.get_style(l_name, zoom)
            if not rule: continue

            pen, brush = QPen(Qt.NoPen), QBrush(Qt.NoBrush)
            if 'color' in rule:
                pen = QPen(QColor(rule['color']), rule.get('width', 1.0))
                pen.setJoinStyle(Qt.RoundJoin);
                pen.setCapStyle(Qt.RoundCap)
            if 'fill' in rule and feat['type'] == 'Polygon':
                brush = QBrush(QColor(rule['fill']))

            painter.setPen(pen);
            painter.setBrush(brush)
            painter.drawPath(feat['path'])


# ==========================================
# Главное Окно
# ==========================================
class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Главное окно карты")
        self.resize(1200, 800)

        toolbar = QHBoxLayout()

        btn_open = QPushButton("Открыть Карту")
        btn_open.clicked.connect(self.open_map)

        btn_convert = QPushButton("⚙Конвертер (PBF -> MBTiles)")
        btn_convert.clicked.connect(self.open_converter_dialog)

        btn_style = QPushButton("Редактор Стилей")
        btn_style.clicked.connect(self.open_style_editor)

        btn_delete = QPushButton("Удалить Карту")
        btn_delete.setStyleSheet("background-color: #A00; color: white;")
        btn_delete.clicked.connect(self.delete_map)

        toolbar.addWidget(btn_open)
        toolbar.addWidget(btn_convert)
        toolbar.addWidget(btn_style)
        toolbar.addStretch()
        toolbar.addWidget(btn_delete)

        self.map_canvas = MapCanvas()

        main_layout = QVBoxLayout()
        main_layout.addLayout(toolbar)
        main_layout.addWidget(self.map_canvas)

        central_widget = QWidget()
        central_widget.setLayout(main_layout)
        self.setCentralWidget(central_widget)

        self.converter_window = None

    def open_map(self):
        file_name, _ = QFileDialog.getOpenFileName(self, "Открыть MBTiles", MAPFILES_DIR, "MBTiles (*.mbtiles)")
        if file_name:
            self.map_canvas.load_database(file_name)
            self.setWindowTitle(f"Карта - {os.path.basename(file_name)}")

    def delete_map(self):
        file_name, _ = QFileDialog.getOpenFileName(self, "Выберите карту для УДАЛЕНИЯ", MAPFILES_DIR,
                                                   "MBTiles (*.mbtiles)")
        if not file_name: return

        reply = QMessageBox.warning(self, 'Удаление', f"Удалить файл навсегда?\n{os.path.basename(file_name)}",
                                    QMessageBox.Yes | QMessageBox.No)
        if reply == QMessageBox.Yes:
            if self.map_canvas.db_path == file_name:
                self.map_canvas.unload_database()
                self.setWindowTitle("Главное окно карты")

            try:
                os.remove(file_name)
                QMessageBox.information(self, "Успех", "Карта удалена.")
            except Exception as e:
                QMessageBox.critical(self, "Ошибка", f"Не удалось удалить файл:\n{e}")

    def open_converter_dialog(self):
        self.converter_window = ConverterWindow()
        self.converter_window.conversion_finished.connect(self.load_new_map)
        self.converter_window.show()

    def open_style_editor(self):
        editor = StyleEditorDialog(self.map_canvas.style_manager, self)
        if editor.exec_():
            self.map_canvas.update()

    def load_new_map(self, file_path):
        self.map_canvas.load_database(file_path)
        self.setWindowTitle(f"Карта - {os.path.basename(file_path)}")


if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = MainWindow()
    window.show()
    sys.exit(app.exec_())