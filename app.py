import os
import sys
import math
import time
import zlib
import sqlite3
import queue
import threading

import yaml
import mapbox_vector_tile

from PyQt5.QtWidgets import (
    QApplication, QMainWindow, QOpenGLWidget, QVBoxLayout, QWidget,
    QPushButton, QHBoxLayout, QFileDialog, QTextEdit, QMessageBox,
    QLineEdit, QLabel, QDialog, QScrollArea, QFormLayout, QColorDialog,
    QDoubleSpinBox, QCheckBox, QSpinBox, QGridLayout, QComboBox
)
from PyQt5.QtGui import (
    QPainter, QPainterPath, QColor, QPen, QBrush, QFont,
    QTextCursor, QImage, QPixmap
)
from PyQt5.QtCore import Qt, pyqtSignal, QObject, QThread, QTimer, QRectF

TILE_SIZE = 256
MVT_EXTENT = 4096
MVT_SCALE = TILE_SIZE / MVT_EXTENT
MIN_DB_ZOOM, MAX_DB_ZOOM = 6, 14
MAX_CACHE_TILES, TARGET_CACHE_TILES = 600, 500

MAPFILES_DIR = os.path.join(os.getcwd(), "mapfiles")
os.makedirs(MAPFILES_DIR, exist_ok=True)

DEFAULT_STYLES = {
    'layers': {
        'landmass': {'fill': '#F2EFE9', 'z_min': 6, 'z_index': 5, 'visible': True},
        'water_poly': {'fill': '#74A0C2', 'z_min': 6, 'z_index': 20, 'visible': True},
        'building_large': {'fill': '#D9D8D6', 'color': '#B4B3B1', 'width': 0.5, 'z_min': 13, 'z_index': 50,
                           'visible': True},
        'highway_primary': {'color': '#FCD6A4', 'width': 3.0, 'z_min': 7, 'z_index': 45, 'visible': True},
    }
}


class EmittingStream(QObject):
    textWritten = pyqtSignal(str)

    def __init__(self):
        super().__init__()
        self.buffer = ""
        self.last_emit_time = time.time()

    def write(self, text):
        self.buffer += str(text)
        # Отправляем в UI не чаще, чем раз в 50 миллисекунд
        if time.time() - self.last_emit_time > 0.05:
            self.textWritten.emit(self.buffer)
            self.buffer = ""
            self.last_emit_time = time.time()

    def flush(self):
        if self.buffer:
            self.textWritten.emit(self.buffer)
            self.buffer = ""


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
            self.save_styles(DEFAULT_STYLES['layers'])
        try:
            with open(self.config_path, 'r', encoding='utf-8') as f:
                self.rules = yaml.safe_load(f).get('layers', {})
        except Exception:
            self.rules = {}

    def save_styles(self, new_rules):
        self.rules = new_rules
        with open(self.config_path, 'w', encoding='utf-8') as f:
            yaml.dump({'layers': self.rules}, f, default_flow_style=False)

    def get_style(self, layer_name, zoom):
        rule = self.rules.get(layer_name)
        if rule and rule.get('visible', True) and zoom >= rule.get('z_min', 0):
            return rule
        return None


class StyleEditorDialog(QDialog):
    def __init__(self, style_manager, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Редактор стилей")
        self.resize(850, 500)
        self.sm = style_manager
        self.inputs = {}

        layout = QVBoxLayout(self)
        top_layout = QHBoxLayout()

        self.combo = QComboBox()
        self.combo.setEditable(True)
        self.combo.addItems([
            "landmass", "water_poly", "waterway", "greenery",
            "building_large", "building_small", "highway_motorway",
            "highway_trunk", "highway_primary", "highway_secondary",
            "highway_tertiary", "highway_residential"
        ])

        btn_add = QPushButton("Добавить слой")
        btn_add.clicked.connect(self.add_layer)
        top_layout.addWidget(self.combo)
        top_layout.addWidget(btn_add)
        layout.addLayout(top_layout)

        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        widget = QWidget()
        self.grid = QGridLayout(widget)
        self.grid.setAlignment(Qt.AlignTop)
        scroll.setWidget(widget)
        layout.addWidget(scroll)

        btn_save = QPushButton("Сохранить и Применить")
        btn_save.setStyleSheet("background-color: #2e7d32; color: white; padding: 8px;")
        btn_save.clicked.connect(self.save)
        layout.addWidget(btn_save)

        self.refresh()

    def refresh(self):
        for i in reversed(range(self.grid.count())):
            widget = self.grid.itemAt(i).widget()
            if widget:
                widget.deleteLater()
        self.inputs.clear()

        headers = ["Вкл", "Слой", "Z-Index", "Min Z", "Заливка", "Линия", "Толщ.", ""]
        for col, title in enumerate(headers):
            self.grid.addWidget(QLabel(f"<b>{title}</b>"), 0, col)

        layers = sorted(self.sm.rules.items(), key=lambda x: x[1].get('z_index', 0), reverse=True)
        for row, (name, props) in enumerate(layers, start=1):
            self.inputs[name] = {}

            def add_w(col, w, key=None):
                self.grid.addWidget(w, row, col)
                if key:
                    self.inputs[name][key] = w

            chk = QCheckBox()
            chk.setChecked(props.get('visible', True))

            sz = QSpinBox()
            sz.setRange(0, 999)
            sz.setValue(props.get('z_index', 10))

            smz = QSpinBox()
            smz.setRange(0, 22)
            smz.setValue(props.get('z_min', 0))

            sw = QDoubleSpinBox()
            sw.setSingleStep(0.5)
            sw.setValue(props.get('width', 0.0))

            btn_del = QPushButton("Х")
            btn_del.clicked.connect(lambda _, n=name: self.delete_layer(n))

            add_w(0, chk, 'visible')
            add_w(1, QLabel(name))
            add_w(2, sz, 'z_index')
            add_w(3, smz, 'z_min')
            add_w(4, self.color_btn(props.get('fill', '')), 'fill')
            add_w(5, self.color_btn(props.get('color', '')), 'color')
            add_w(6, sw, 'width')
            add_w(7, btn_del)

    def color_btn(self, hex_c):
        btn = QPushButton()
        btn.c_val = hex_c
        btn.setFixedSize(40, 20)
        bg = hex_c if hex_c else 'transparent'
        btn.setStyleSheet(f"background: {bg}; border: 1px solid gray;")
        btn.clicked.connect(lambda: self.pick_color(btn))
        return btn

    def pick_color(self, btn):
        initial = QColor(btn.c_val) if btn.c_val else Qt.white
        color = QColorDialog.getColor(initial, self)
        if color.isValid():
            btn.c_val = color.name()
            btn.setStyleSheet(f"background: {btn.c_val}; border: 1px solid black;")

    def add_layer(self):
        name = self.combo.currentText().strip()
        if name and name not in self.sm.rules:
            self.sm.rules[name] = {'visible': True, 'z_index': 10, 'z_min': 0}
            self.refresh()

    def delete_layer(self, name):
        if name in self.sm.rules:
            del self.sm.rules[name]
            self.refresh()

    def save(self):
        for name, w in self.inputs.items():
            self.sm.rules[name].update({
                'visible': w['visible'].isChecked(),
                'z_index': w['z_index'].value(),
                'z_min': w['z_min'].value(),
                'width': w['width'].value()
            })
            for c_type in ('fill', 'color'):
                val = w[c_type].c_val
                if val:
                    self.sm.rules[name][c_type] = val
                elif c_type in self.sm.rules[name]:
                    del self.sm.rules[name][c_type]

        self.sm.save_styles(self.sm.rules)
        self.accept()


class ConverterWindow(QWidget):
    conversion_finished = pyqtSignal(str)

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Создание новой карты")
        self.resize(700, 500)

        self.selected_pbf = None
        self.target_mbtiles = None

        layout = QVBoxLayout(self)
        form = QFormLayout()

        self.btn_select = QPushButton("Выбрать файл .pbf")
        self.btn_select.clicked.connect(self.select_pbf)
        self.lbl_pbf = QLabel("Файл не выбран")

        self.input_name = QLineEdit("Новая_Карта")

        form.addRow(self.btn_select, self.lbl_pbf)
        form.addRow("Имя карты:", self.input_name)
        layout.addLayout(form)

        self.btn_start = QPushButton("НАЧАТЬ КОНВЕРТАЦИЮ")
        self.btn_start.setStyleSheet("background-color: #2e7d32; color: white; font-weight: bold; padding: 10px;")
        self.btn_start.clicked.connect(self.start_conversion)
        layout.addWidget(self.btn_start)

        self.console = QTextEdit()
        self.console.setReadOnly(True)
        self.console.setTextInteractionFlags(Qt.NoTextInteraction)
        self.console.setStyleSheet("background-color: #1e1e1e; color: #00ff00; font-family: Consolas;")
        layout.addWidget(self.console)

        self.stream = EmittingStream()
        self.stream.textWritten.connect(self.write_log)
        self.orig_stdout = sys.stdout
        self.orig_stderr = sys.stderr

    def select_pbf(self):
        path, _ = QFileDialog.getOpenFileName(self, "Выберите PBF", os.path.expanduser("~/Downloads"), "OSM PBF (*.osm.pbf)")
        if path:
            self.selected_pbf = path
            self.lbl_pbf.setText(os.path.basename(path))

    def write_log(self, text):
        cursor = self.console.textCursor()
        if text.startswith('\r'):
            cursor.movePosition(QTextCursor.StartOfLine)
            cursor.movePosition(QTextCursor.EndOfLine, QTextCursor.KeepAnchor)
            cursor.removeSelectedText()
            cursor.insertText(text[1:])
        else:
            cursor.movePosition(QTextCursor.End)
            cursor.insertText(text)
        self.console.setTextCursor(cursor)
        self.console.ensureCursorVisible()

    def start_conversion(self):
        map_name = self.input_name.text().strip()
        if not self.selected_pbf or not map_name:
            QMessageBox.warning(self, "Ошибка", "Заполните все поля!")
            return

        self.target_mbtiles = os.path.join(MAPFILES_DIR, f"{map_name}.mbtiles")

        if os.path.exists(self.target_mbtiles):
            reply = QMessageBox.question(self, 'Перезапись', 'Перезаписать существующий файл?',
                                         QMessageBox.Yes | QMessageBox.No)
            if reply == QMessageBox.No:
                return

        self.btn_start.setEnabled(False)
        self.btn_select.setEnabled(False)
        self.input_name.setEnabled(False)

        sys.stdout = self.stream
        sys.stderr = self.stream

        self.thread = ConverterThread(self.selected_pbf, self.target_mbtiles)
        self.thread.finished.connect(self.on_finished)
        self.thread.error.connect(self.on_error)
        self.thread.start()

    def on_finished(self, path):
        self.restore_streams()
        QMessageBox.information(self, "Успех", f"Конвертация успешно завершена!")
        self.conversion_finished.emit(path)
        self.close()

    def on_error(self, err_msg):
        self.restore_streams()
        self.btn_start.setEnabled(True)
        QMessageBox.critical(self, "Ошибка", f"Сбой:\n{err_msg}")

    def restore_streams(self):
        sys.stdout = self.orig_stdout
        sys.stderr = self.orig_stderr

    def closeEvent(self, event):
        self.restore_streams()
        event.accept()


class GoToDialog(QDialog):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Перейти к координатам")
        self.setFixedSize(260, 160)

        layout = QFormLayout(self)

        self.lat_input = QDoubleSpinBox()
        self.lat_input.setRange(-85.0, 85.0)
        self.lat_input.setDecimals(6)

        self.lon_input = QDoubleSpinBox()
        self.lon_input.setRange(-180.0, 180.0)
        self.lon_input.setDecimals(6)

        self.zoom_input = QDoubleSpinBox()
        self.zoom_input.setRange(6.0, 17.0)
        self.zoom_input.setValue(12.0)

        layout.addRow("Широта (Lat):", self.lat_input)
        layout.addRow("Долгота (Lon):", self.lon_input)
        layout.addRow("Масштаб (Zoom):", self.zoom_input)

        btn = QPushButton("Перейти")
        btn.clicked.connect(self.accept)
        layout.addWidget(btn)

    def get_values(self):
        return self.lat_input.value(), self.lon_input.value(), self.zoom_input.value()


class WorkerSignals(QObject):
    tile_decoded = pyqtSignal(tuple, QImage)


class TileLoader:
    def __init__(self, db_path, cache_ref, style_manager):
        self.db_path = db_path
        self.cache = cache_ref
        self.style_manager = style_manager

        self.task_queue = queue.PriorityQueue()
        self.signals = WorkerSignals()
        self.active_workers = True
        self.visible_tiles = set()
        self.loading_tiles = set()
        self.local = threading.local()

        self.threads = [threading.Thread(target=self._worker_loop, daemon=True) for _ in range(4)]
        for t in self.threads:
            t.start()

    def stop(self):
        self.active_workers = False
        while not self.task_queue.empty():
            try:
                self.task_queue.get_nowait()
            except queue.Empty:
                break
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
        if tile_key in self.cache or tile_key in self.loading_tiles:
            return
        self.loading_tiles.add(tile_key)
        dist = math.hypot(x - center_tx, y - center_ty)
        self.task_queue.put(((dist, -time.time()), tile_key))

    def _worker_loop(self):
        while self.active_workers:
            try:
                _, tile_key = self.task_queue.get(timeout=0.5)
                z, x, y = tile_key

                if tile_key not in self.visible_tiles:
                    self.loading_tiles.discard(tile_key)
                    self.task_queue.task_done()
                    continue

                # Ищем данные: либо текущий тайл, либо его родителя
                raw_data, pz, px, py = self._get_tile_data(z, x, y)

                hi_res_factor = 2.0
                img_size = int(TILE_SIZE * hi_res_factor)
                image = QImage(img_size, img_size, QImage.Format_ARGB32_Premultiplied)
                image.fill(Qt.transparent)

                if raw_data:
                    decoded = mapbox_vector_tile.decode(zlib.decompress(raw_data))
                    compiled_features = self._build_hardware_paths(decoded)

                    painter = QPainter(image)
                    painter.setRenderHint(QPainter.Antialiasing)

                    # 1. Масштабирование для Hi-Res мониторов
                    painter.scale(hi_res_factor, hi_res_factor)

                    # 2. Математика рендеринга заново (Векторный Оверзуминг)
                    scale_ovz = 2 ** (z - pz)
                    dx = x - (px * scale_ovz)
                    dy = y - (py * scale_ovz)

                    # Сдвигаем холст так, чтобы в кадр попала только нужная четверть родителя
                    painter.translate(-dx * TILE_SIZE, -dy * TILE_SIZE)
                    painter.scale(scale_ovz, scale_ovz)

                    valid_features = []
                    for feat in compiled_features:
                        rule = self.style_manager.get_style(feat.get('layer_name', ''), z)
                        if rule:
                            valid_features.append((feat, rule))

                    valid_features.sort(key=lambda item: item[1].get('z_index', 0))

                    for feat, rule in valid_features:
                        l_name = feat.get('layer_name', '')
                        if feat['type'] == 'Polygon' and l_name.startswith('building'):
                            rect = feat['path'].boundingRect()
                            # Порог отрисовки мелких зданий масштабируется вместе с холстом
                            if rect.width() * scale_ovz < 2.5 and rect.height() * scale_ovz < 2.5:
                                continue

                        pen = QPen(Qt.NoPen)
                        brush = QBrush(Qt.NoBrush)

                        if 'color' in rule:
                            # Ширина пера фиксированная (width), а setCosmetic(True) не даст ей размыться при скейле
                            pen = QPen(QColor(rule['color']), rule.get('width', 1.0))
                            pen.setJoinStyle(Qt.RoundJoin)
                            pen.setCapStyle(Qt.RoundCap)
                            pen.setCosmetic(True)

                        if 'fill' in rule and feat['type'] == 'Polygon':
                            brush = QBrush(QColor(rule['fill']))

                        painter.setPen(pen)
                        painter.setBrush(brush)
                        painter.drawPath(feat['path'])

                    painter.end()

                self.signals.tile_decoded.emit(tile_key, image.copy())
                self.loading_tiles.discard(tile_key)
                self.task_queue.task_done()

            except queue.Empty:
                continue
            except Exception as e:
                print(f"Ошибка рендеринга тайла: {e}")
                self.loading_tiles.discard(tile_key)
                self.task_queue.task_done()

        if hasattr(self.local, 'conn'):
            self.local.conn.close()

    def _get_tile_data(self, z, x, y):
        cursor = self.get_db()
        cz, cx, cy = z, x, y

        # Рекурсивно спускаемся до минимального зума в поисках ближайшего родителя
        while cz >= MIN_DB_ZOOM:
            tms_y = (1 << cz) - 1 - cy
            cursor.execute(
                "SELECT tile_data FROM tiles WHERE zoom_level=? AND tile_column=? AND tile_row=?",
                (cz, cx, tms_y)
            )
            row = cursor.fetchone()
            if row:
                return row[0], cz, cx, cy

            # Поднимаемся на уровень выше (к родительскому тайлу)
            cz -= 1
            cx //= 2
            cy //= 2

        return None, None, None, None

    def _build_hardware_paths(self, mvt_data):
        features_data = []
        for layer_name, layer_data in mvt_data.items():
            for feat in layer_data['features']:
                geom_type = feat['geometry']['type']
                coords = feat['geometry']['coordinates']

                path = QPainterPath()
                if geom_type == 'LineString':
                    path.moveTo(coords[0][0] * MVT_SCALE, (MVT_EXTENT - coords[0][1]) * MVT_SCALE)
                    for pt in coords[1:]:
                        path.lineTo(pt[0] * MVT_SCALE, (MVT_EXTENT - pt[1]) * MVT_SCALE)
                elif geom_type == 'Polygon':
                    path.setFillRule(Qt.OddEvenFill)
                    for ring in coords:
                        path.moveTo(ring[0][0] * MVT_SCALE, (MVT_EXTENT - ring[0][1]) * MVT_SCALE)
                        for pt in ring[1:]:
                            path.lineTo(pt[0] * MVT_SCALE, (MVT_EXTENT - pt[1]) * MVT_SCALE)
                        path.closeSubpath()
                elif geom_type == 'MultiPolygon':
                    path.setFillRule(Qt.OddEvenFill)
                    for poly in coords:
                        for ring in poly:
                            path.moveTo(ring[0][0] * MVT_SCALE, (MVT_EXTENT - ring[0][1]) * MVT_SCALE)
                            for pt in ring[1:]:
                                path.lineTo(pt[0] * MVT_SCALE, (MVT_EXTENT - pt[1]) * MVT_SCALE)
                            path.closeSubpath()

                features_data.append({
                    'path': path, 'tags': feat['properties'],
                    'type': geom_type, 'layer_name': layer_name
                })
        return features_data


class MapCanvas(QOpenGLWidget):
    def __init__(self):
        super().__init__()
        self.setMouseTracking(True)
        self.setUpdateBehavior(QOpenGLWidget.PartialUpdate)

        self.style_manager = StyleManager()
        self.tile_cache = {}
        self.loader = None
        self.db_path = None
        self.available_zooms = []

        self.center_lon = 33.3823
        self.center_lat = 35.1856
        self.zoom = 10.0
        self.dragging = False
        self.last_mouse_pos = None

        self.request_timer = QTimer(self)
        self.request_timer.setSingleShot(True)
        self.request_timer.setInterval(200)
        self.request_timer.timeout.connect(self._queue_visible_tiles)

    def _auto_center(self, db_path):
        try:
            conn = sqlite3.connect(db_path)
            c = conn.cursor()
            c.execute("SELECT MAX(zoom_level) FROM tiles")
            z_row = c.fetchone()
            if not z_row or z_row[0] is None:
                return
            z = z_row[0]

            c.execute("SELECT AVG(tile_column), AVG(tile_row) FROM tiles WHERE zoom_level=?", (z,))
            row = c.fetchone()
            conn.close()

            if row and row[0] is not None:
                avg_x, avg_y_tms = row

                avg_y = (1 << z) - 1 - avg_y_tms

                n = 2.0 ** z
                self.center_lon = avg_x / n * 360.0 - 180.0
                lat_rad = math.atan(math.sinh(math.pi * (1 - 2 * avg_y / n)))
                self.center_lat = math.degrees(lat_rad)

                self.zoom = max(6.0, z - 1.0)
        except Exception as e:
            print(f"Ошибка автоцентрирования: {e}")

    def load_database(self, db_path):
        if self.loader:
            self.loader.stop()
        self.tile_cache.clear()

        if os.path.exists(db_path):
            try:
                conn = sqlite3.connect(db_path)
                c = conn.cursor()
                c.execute("SELECT DISTINCT zoom_level FROM tiles ORDER BY zoom_level")
                self.available_zooms = [row[0] for row in c.fetchall()]
                conn.close()
            except Exception as e:
                print(f"Ошибка чтения зумов: {e}")

            self.db_path = db_path
            self._auto_center(db_path)
            self.loader = TileLoader(db_path, self.tile_cache, self.style_manager)
            self.loader.signals.tile_decoded.connect(self.on_tile_decoded)
            self.update()
            self.request_timer.start(0)

    def unload_database(self):
        if self.loader:
            self.loader.stop()
            self.loader = None
        self.db_path = None
        self.tile_cache.clear()
        self.update()

    def clean_cache(self):
        if len(self.tile_cache) <= MAX_CACHE_TILES:
            return

        current_z = int(math.floor(self.zoom))

        def get_score(key):
            z, x, y = key
            z_diff = abs(z - current_z)
            n = 2.0 ** z
            tile_lon = (x + 0.5) / n * 360.0 - 180.0
            lat_rad = math.atan(math.sinh(math.pi * (1 - 2 * (y + 0.5) / n)))
            tile_lat = math.degrees(lat_rad)
            dist_sq = (tile_lon - self.center_lon) ** 2 + (tile_lat - self.center_lat) ** 2
            return z_diff, dist_sq

        sorted_keys = sorted(self.tile_cache.keys(), key=get_score, reverse=True)
        tiles_to_delete = len(self.tile_cache) - TARGET_CACHE_TILES
        deleted = 0

        for k in sorted_keys:
            if deleted >= tiles_to_delete:
                break
            if self.loader and k in self.loader.visible_tiles:
                continue
            del self.tile_cache[k]
            deleted += 1

    def on_tile_decoded(self, tile_key, image):
        self.tile_cache[tile_key] = QPixmap.fromImage(image)
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
            self.request_timer.start()

    def wheelEvent(self, event):
        self.zoom = max(6.0, min(17.0, self.zoom + event.angleDelta().y() / 1200.0))
        self.update()
        self.request_timer.start()

    def _calc_viewport(self):
        w, h = self.width(), self.height()
        z_int = max(MIN_DB_ZOOM, min(MAX_DB_ZOOM, int(math.floor(self.zoom))))
        scale = 2 ** (self.zoom - z_int)

        center_tx = (self.center_lon + 180.0) / 360.0 * (2 ** z_int)
        center_ty = (1.0 - math.asinh(math.tan(math.radians(self.center_lat))) / math.pi) / 2.0 * (2 ** z_int)

        tiles_w, tiles_h = (w / TILE_SIZE) / scale, (h / TILE_SIZE) / scale
        min_tx, max_tx = int(math.floor(center_tx - tiles_w / 2)), int(math.ceil(center_tx + tiles_w / 2))
        min_ty, max_ty = int(math.floor(center_ty - tiles_h / 2)), int(math.ceil(center_ty + tiles_h / 2))

        return z_int, center_tx, center_ty, min_tx, max_tx, min_ty, max_ty, scale

    def _queue_visible_tiles(self):
        if not self.loader:
            return

        z_int, ctx, cty, min_tx, max_tx, min_ty, max_ty, _ = self._calc_viewport()

        self.loader.visible_tiles = set(
            (z_int, x, y) for x in range(min_tx, max_tx + 1) for y in range(min_ty, max_ty + 1)
        )

        for x in range(min_tx, max_tx + 1):
            for y in range(min_ty, max_ty + 1):
                self.loader.request_tile(z_int, x, y, ctx, cty)

    def draw_overlays(self, painter):
        # Scale bar
        meters_per_pixel = (
                156543.03392 * math.cos(math.radians(self.center_lat)) / (2 ** self.zoom)
        )
        values = [1 * (10 ** i) for i in range(6)] + \
                 [2 * (10 ** i) for i in range(6)] + \
                 [5 * (10 ** i) for i in range(6)]
        values.sort()

        chosen = 1
        for v in values:
            if meters_per_pixel * 140 >= v:
                chosen = v

        px_length = chosen / meters_per_pixel
        y = self.height() - 30
        x = 20

        painter.setPen(QPen(Qt.white, 5))
        painter.drawLine(x, y, int(x + px_length), y)
        painter.setPen(QPen(Qt.black, 2))
        painter.drawLine(x, y, int(x + px_length), y)
        painter.drawLine(x, y - 6, x, y + 6)
        painter.drawLine(int(x + px_length), y - 6, int(x + px_length), y + 6)

        label = f"{chosen / 1000:.0f} km" if chosen >= 1000 else f"{chosen:.0f} m"

        font = QFont("Segoe UI", 10, QFont.Bold)
        painter.setFont(font)
        path = QPainterPath()
        path.addText(x, y - 10, font, label)

        painter.setPen(QPen(Qt.white, 3))
        painter.drawPath(path)
        painter.fillPath(path, QColor("#111111"))

        # Coordinates & Zoom info
        font_info = QFont("Segoe UI", 12, QFont.Bold)
        painter.setFont(font_info)
        info_lines = [
            f"Lat: {self.center_lat:.5f}",
            f"Lon: {self.center_lon:.5f}",
            f"Zoom: {self.zoom:.2f}"
        ]

        y_offset = 28
        for text in info_lines:
            path_info = QPainterPath()
            path_info.addText(14, y_offset, font_info, text)
            painter.setPen(QPen(Qt.white, 3))
            painter.drawPath(path_info)
            painter.fillPath(path_info, QColor("#111111"))
            y_offset += 24

    def paintEvent(self, event):
        painter = QPainter(self)
        painter.setRenderHint(QPainter.Antialiasing)
        painter.setRenderHint(QPainter.SmoothPixmapTransform)
        painter.fillRect(self.rect(), QColor("#b6eda8"))

        if not self.loader:
            painter.setPen(Qt.black)
            painter.setFont(QFont("Arial", 14))
            painter.drawText(self.rect(), Qt.AlignCenter, "Карта не загружена. Откройте или создайте новую.")
            painter.end()
            return

        z_int, ctx, cty, min_tx, max_tx, min_ty, max_ty, scale = self._calc_viewport()

        painter.save()
        painter.translate(self.width() / 2, self.height() / 2)
        painter.scale(scale, scale)

        for x in range(min_tx, max_tx + 1):
            for y in range(min_ty, max_ty + 1):
                tile_key = (z_int, x, y)
                if tile_key in self.tile_cache:
                    rect = QRectF((x - ctx) * TILE_SIZE, (y - cty) * TILE_SIZE, TILE_SIZE, TILE_SIZE)
                    pix = self.tile_cache[tile_key]
                    painter.drawPixmap(rect, pix, QRectF(pix.rect()))

        painter.restore()
        self.draw_overlays(painter)
        painter.end()


class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Главное окно карты")
        self.resize(1200, 800)

        toolbar = QHBoxLayout()

        btn_open = QPushButton("Открыть")
        btn_open.clicked.connect(self.open_map)

        btn_convert = QPushButton("Конвертер")
        btn_convert.clicked.connect(self.open_converter_dialog)

        btn_style = QPushButton("Стили")
        btn_style.clicked.connect(self.open_style_editor)

        btn_zoom_in = QPushButton("+")
        btn_zoom_in.setFixedWidth(40)
        btn_zoom_in.clicked.connect(lambda: self.adjust_zoom(0.5))

        btn_zoom_out = QPushButton("-")
        btn_zoom_out.setFixedWidth(40)
        btn_zoom_out.clicked.connect(lambda: self.adjust_zoom(-0.5))

        btn_goto = QPushButton("Перейти")
        btn_goto.clicked.connect(self.go_to_coordinates)

        btn_delete = QPushButton("Удалить")
        btn_delete.setStyleSheet("background-color: #A00; color: white;")
        btn_delete.clicked.connect(self.delete_map)

        toolbar.addWidget(btn_open)
        toolbar.addWidget(btn_convert)
        toolbar.addWidget(btn_zoom_in)
        toolbar.addWidget(btn_zoom_out)
        toolbar.addWidget(btn_goto)
        toolbar.addStretch()
        toolbar.addWidget(btn_style)
        toolbar.addWidget(btn_delete)

        self.map_canvas = MapCanvas()

        main_layout = QVBoxLayout()
        main_layout.addLayout(toolbar)
        main_layout.addWidget(self.map_canvas)

        central_widget = QWidget()
        central_widget.setLayout(main_layout)
        self.setCentralWidget(central_widget)

    def open_map(self):
        file_name, _ = QFileDialog.getOpenFileName(self, "Открыть MBTiles", MAPFILES_DIR, "MBTiles (*.mbtiles)")
        if file_name:
            self.map_canvas.load_database(file_name)
            self.setWindowTitle(f"Карта - {os.path.basename(file_name)}")

    def delete_map(self):
        file_name, _ = QFileDialog.getOpenFileName(self, "Удалить карту", MAPFILES_DIR, "MBTiles (*.mbtiles)")
        if not file_name:
            return

        reply = QMessageBox.warning(
            self, 'Удаление', f"Удалить файл навсегда?\n{os.path.basename(file_name)}",
            QMessageBox.Yes | QMessageBox.No
        )

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

        def on_finished(path):
            self.map_canvas.load_database(path)
            self.setWindowTitle(f"Карта - {os.path.basename(path)}")

        self.converter_window.conversion_finished.connect(on_finished)
        self.converter_window.show()

    def open_style_editor(self):
        editor = StyleEditorDialog(self.map_canvas.style_manager, self)
        if editor.exec_():
            self.map_canvas.tile_cache.clear()
            self.map_canvas._queue_visible_tiles()
            self.map_canvas.update()

    def adjust_zoom(self, delta):
        self.map_canvas.zoom = max(6.0, min(17.0, self.map_canvas.zoom + delta))
        self.map_canvas.update()
        self.map_canvas.request_timer.start(200)

    def go_to_coordinates(self):
        dlg = GoToDialog(self)
        if dlg.exec_():
            lat, lon, zoom = dlg.get_values()
            self.map_canvas.center_lat = lat
            self.map_canvas.center_lon = lon
            self.map_canvas.zoom = zoom
            self.map_canvas.update()
            self.map_canvas.request_timer.start(200)


if __name__ == "__main__":
    app = QApplication(sys.argv)
    app.setAttribute(Qt.AA_UseOpenGLES)
    window = MainWindow()
    window.show()
    sys.exit(app.exec_())