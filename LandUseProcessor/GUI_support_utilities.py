from PyQt6.QtWidgets import (
    QApplication, QMessageBox, QMenu, QStatusBar, QLabel, QTableWidget, QTableWidgetItem,
    QSizePolicy, QWidget, QVBoxLayout, QListWidget, QPushButton, QDialog, QTabWidget, QHBoxLayout, QFileDialog, 
    )

from PyQt6.QtGui import QAction 
from PyQt6.QtCore import Qt
import pandas as pd
import logging

from utility import ThreadWrapper, dialog_level, IndentAdapter
from LandUseUtilities.Parcels import Parcels
from LandUseUtilities.parcel_interpolation import LinearParcelInterpolator


class Shared_GUI_Widgets:
    def enableAllButtons(self, btns: list = None):
        if btns == None:
            btns = self.findChildren(QPushButton)
        for btn in btns:
            btn.setEnabled(True)

    def disableAllButtons(self, btns: list = None):
        if btns == None:
            btns = self.findChildren(QPushButton)
        for btn in btns:
            btn.setEnabled(False)

    def _on_process_thread_error(self, status_bar_section, e):
        # called when the thread encounters an error
        self.enableAllButtons()
        status_bar_section.setText("Error")
        QMessageBox.critical(self, "Error", str(e))

    def _on_process_thread_finished(self, statusbar_section, ret):
        # called when the thread is finished
        self.enableAllButtons()        
        statusbar_section.setText("Done")        

    def make_list_panel(self, title, items, v_policy=QSizePolicy.Policy.Expanding):
        container = QWidget()
        layout = QVBoxLayout(container)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(5)

        label = QLabel(title)
        listbox = QListWidget()
        listbox.addItems(items)
        listbox.setSizePolicy(QSizePolicy.Policy.Preferred, v_policy)
        listbox.setStyleSheet("""
            QListWidget::item:selected {
                background: palette(highlight);
                color: palette(highlighted-text);
            }
            """)

        layout.addWidget(label)
        layout.addWidget(listbox)

        return container, listbox

    def create_context_menu(self, table, pos):
        menu = QMenu(table)
        copy_action = QAction("Copy All to Clipboard", table)
        delete_action = QAction("Delete Selected", table)
        
        copy_action.triggered.connect(lambda: self.copy_result_to_clipboard(table))
        delete_action.triggered.connect(lambda: self.delete_selected(table))

        menu.addAction(copy_action)
        menu.addAction(delete_action)
        menu.exec(table.viewport().mapToGlobal(pos))
    
    def copy_result_to_clipboard(self, table):
        rows = table.rowCount()
        cols = table.columnCount()

        headers = [table.horizontalHeaderItem(c).text() for c in range(cols)]
        text = "\t".join(headers) + "\n"
        for row in range(rows):
            row_data = []
            for col in range(cols):
                item = table.item(row, col)
                row_data.append(item.text() if item else "")
            text += "\t".join(row_data) + "\n"
        
        clipboard = QApplication.clipboard()
        clipboard.setText(text)
        QMessageBox.information(table, "Copied", "All data copied to clipboard including headers.")

    def delete_selected(self, table):
        selected_rows = sorted({item.row() for item in table.selectedItems()}, reverse = True)

        for row in selected_rows:
            table.removeRow(row)

    def create_status_bar(self, parent, num_sections):
        """Initialize status bar with num_sections sections."""
        parent.status_bar = QStatusBar()
        parent.status_sections = []
        for i in range(num_sections):
            parent.status_sections.append(QLabel(""))
            parent.status_bar.addPermanentWidget(parent.status_sections[i], 1)

        parent.main_layout.addWidget(parent.status_bar)


    def on_table_selection_changed(self, table):
        """compute sum of selected numeric cells from  a table"""
        items = table.selectedItems()
        total = 0.0
        found = False

        for it in items:
            txt = (it.text() or '').strip()
            if txt == '':
                continue
            #remove thousnad separator ","
            txt2 = txt.replace(',',  '')
            try:
                val = float(txt2)
                total += val
                found = True
            except Exception: # ignore all non_numeric cells
                continue

        if found:
            self.status_sections[1].setText(f"Sum: {total}")
            self.status_sections[2].setText(f"{len(items)} selected")
        else:
            self.status_sections[1].setText("")
            self.status_sections[2].setText("")


    def table_to_list_of_dicts(self, table: QTableWidget) -> list[dict]:
        '''read table into a list of dict'''
        rows = table.rowCount()
        cols = table.columnCount()

        # Get column headers
        headers = [
            table.horizontalHeaderItem(c).text()
            for c in range(cols)
        ]

        data = []

        for row in range(rows):
            row_dict = {}

            for col, header in enumerate(headers):
                item = table.item(row, col)
                row_dict[header] = item.text() if item else ""

            data.append(row_dict)

        return data

class NumericTableWidgetItem(QTableWidgetItem):
    """Custom QTableWidgetItem that treats numbers correctly for sorting."""
    def __init__(self, text):
        text = "" if text is None else str(text)
        super().__init__(text)
        try:
            self.numeric_value = float(text)
            self.is_numeric = True
        except ValueError:
            self.numeric_value = text
            self.is_numeric = False

    def __lt__(self, other):
        if isinstance(other, NumericTableWidgetItem):
            if self.is_numeric and other.is_numeric:
                return self.numeric_value < other.numeric_value
            
            if self.is_numeric != other.is_numeric:
                return self.is_numeric
        return super().__lt__(other)
    

class ValidationAndSummary(QDialog, Shared_GUI_Widgets):
    def __init__(self, parent=None, msg=None, data_dict=None):
        # data_dict: dictionary containing data to be displayed in the tables
        super().__init__(parent)
        self.setWindowTitle("Validation and Summary")
        self.setMinimumWidth(600)
        self.data_dict = data_dict
        self.msg = msg
        self._init_ui()

        # status bar is created from Shared_GUI_Widgets
        self.create_status_bar(self, 4)

    def _init_ui(self):
        """Initialize the user interface."""
        self.main_layout = QVBoxLayout()
        self.setLayout(self.main_layout)

        info_label = QLabel(self.msg)
        self.main_layout.addWidget(info_label)
        self.tab_pages = {}

        self.tabs = QTabWidget()
        
        for key, value in self.data_dict.items():
            self.tab_pages[key] = QTableWidget() 
            self.tab_pages[key].setSortingEnabled(False)
            self.tab_pages[key].setRowCount(value.shape[0])
            self.tab_pages[key].setColumnCount(len(value.columns))
            self.tab_pages[key].setHorizontalHeaderLabels(value.columns)
            self.tab_pages[key].setContextMenuPolicy(Qt.ContextMenuPolicy.CustomContextMenu)
            self.tab_pages[key].customContextMenuRequested.connect( 
                lambda pos, t=self.tab_pages[key]: self.create_context_menu(t, pos)
            )
            self.tab_pages[key].selectionModel().selectionChanged.connect(
                lambda sel, des, t=self.tab_pages[key]: self.on_table_selection_changed(t)
            )

            # load data into tab. value is a dataframe
            for row in range(value.shape[0]):
                for col in range(len(value.columns)):
                    val = value.iat[row, col]
                    item = NumericTableWidgetItem(val)
                    self.tab_pages[key].setItem(row, col, item)

            self.tab_pages[key].setSortingEnabled(True)
            self.tabs.addTab(self.tab_pages[key], key)

        self.main_layout.addWidget(self.tabs)
        close_button = QPushButton("Close")
        close_button.clicked.connect(self.close)
        self.main_layout.addWidget(close_button)

class BaseDataGenerator(QDialog, Shared_GUI_Widgets):
    def __init__(self, parent = None, message = None):
        super().__init__(parent)
        self.__init_ui__(message)
        self.create_status_bar(self, 4)
        
        self.base_file = ""
        self.lower_boundary_file = ""
        self.upper_boundary_file = ""
        self.base_parcel : Parcels = None
        base_logger = logging.getLogger(__name__)
        indent = dialog_level(self)
        self.logger = IndentAdapter(base_logger, indent)

        self.logger.info("Base Parcel Data Generator initialized.")

    def __init_ui__(self, msg):
        """Initialize the user interface."""
        self.setWindowTitle("Base Parcel Process")
        self.main_layout = QVBoxLayout()
        self.setLayout(self.main_layout)

        info_label = QLabel(msg)
        self.main_layout.addWidget(info_label)
        hbox = QHBoxLayout()
        op1_label = QLabel("Select a Parcel File as the Base")
        hbox.addWidget(op1_label)
        select_btn = QPushButton("Select a Base File")
        select_btn.clicked.connect(lambda: self.select_file("Select a Base Parcel File", op1_label))
        hbox.addWidget(select_btn)
        self.main_layout.addLayout(hbox)

        self.base_filename_label = QLabel("No File is Selected")
        self.main_layout.addWidget(self.base_filename_label)

        groupbox_layout =  QVBoxLayout()
        groupbox_layout.addWidget(QLabel("Interpolate from Two Parcel Files"))
        hbox = QHBoxLayout()
        self.sel1_btn = QPushButton("Select the Parcel File for the Lower Boundary")
        self.sel1_btn.clicked.connect(lambda: self.select_file_for_interpolation("lower"))
        self.sel2_btn = QPushButton("Select the Parcel File for the Upper Boundary")
        self.sel2_btn.clicked.connect(lambda: self.select_file_for_interpolation("upper"))
        hbox.addWidget(self.sel1_btn)
        hbox.addWidget(self.sel2_btn)
        groupbox_layout.addLayout(hbox)

        self.table = QTableWidget()
        self.table.setColumnCount(3)
        self.table.setHorizontalHeaderLabels(["Side", "Year", "File"])
        self.table.setContextMenuPolicy(Qt.ContextMenuPolicy.CustomContextMenu)
        self.table.customContextMenuRequested.connect(lambda pos: self.create_context_menu(self.table, pos))
        groupbox_layout.addWidget(self.table)

        self.interpolate_btn = QPushButton("Interpolate")
        self.interpolate_btn.clicked.connect(self.interpolation_btn_clicked)
        groupbox_layout.addWidget(self.interpolate_btn)
        self.main_layout.addLayout(groupbox_layout)

        hbox = QHBoxLayout()
        self.valid_btn = QPushButton("Validate")
        self.valid_btn.clicked.connect(self.validate_btn_clicked)
        self.valid_btn.setEnabled(False)
        hbox.addWidget(self.valid_btn)

        self.summarize_btn = QPushButton("Summarize")
        self.summarize_btn.clicked.connect(self.summarize_btn_clicked) 
        self.summarize_btn.setEnabled(False)
        hbox.addWidget(self.summarize_btn)
        self.main_layout.addLayout(hbox)

    def select_file_for_interpolation(self, side):
        path, _ = QFileDialog.getOpenFileName(
                    self, f"Please select a parcel file for {side} boundary", "",
                    "Data Files (*.csv *.txt *.*)"
                )
        rowCount = self.table.rowCount()
        self.table.insertRow(rowCount)
        self.table.setItem(rowCount, 0, QTableWidgetItem(side))
        year_item = QTableWidgetItem(0)
        self.table.setItem(rowCount, 1, year_item)
        self.table.setItem(rowCount, 2, QTableWidgetItem(path))

    def select_file(self, message, label = None):
        path, _ = QFileDialog.getOpenFileName(
                    self, message, "",
                    "Data Files (*.csv *.txt *.*)"
                )
        if path and label is not None:
            label.setText(path)
            self.sel1_btn.setEnabled(False)
            self.sel2_btn.setEnabled(False)
            self.interpolate_btn.setEnabled(False)
            indent = dialog_level(self)
            self.base_parcel = Parcels(self.parent().project_settings['subarea_file'], self.parent().project_settings['lookup_file'], path, self.parent().horizon_year, indent + 1)
            self.status_sections[0].setText("Base parcel selected.")
            self.valid_btn.setEnabled(True)
            self.summarize_btn.setEnabled(True)
            self.base_file = path

            self.logger.info(f"Selected base parcel file: {path}")
        return
    
    def interpolation_btn_clicked(self):
        self.status_sections[0].setText("interpolating")
        self.disableAllButtons()

        if (self.table.item(0,1).text().strip().isdigit() == False) or (self.table.item(1,1).text().strip().isdigit() == False):
            QMessageBox.critical(self, "Error", "Check horizon years for interpolation.")
            return
        
        base_year_dict = {}
        num_row = self.table.rowCount()

        if num_row > 2 or num_row < 2:
            QMessageBox.critical(self, "Error", "Too many files for interpolation.")
            return
        
        for row in range(num_row):
            base_year_dict[self.table.item(row, 0).text()] = {"year": int(self.table.item(row, 1).text()), "path": self.table.item(row, 2).text()}
        
        lower = int(base_year_dict['lower']['year'])
        lower_path = base_year_dict['lower']['path']
        upper = int(base_year_dict['upper']['year'])
        upper_path = base_year_dict['upper']['path']

        self.worker = ThreadWrapper(self.interpolate_two_parcel_files, lower_path, upper_path, lower, upper, self.parent().horizon_year)
        self.worker.finished.connect(lambda interpolation_parcel: self._on_interpolation_finished(interpolation_parcel))
        self.worker.error.connect(lambda eobj: self._on_interpolation_error(eobj))
        self.worker.start()

    def interpolate_two_parcel_files(self, lower_path, upper_path, lower_year, upper_year, horizon_year) -> Parcels:
        '''
        create a parcel data by interpolating two parcels.
    
        :param lower_path: parcel file for the left bookend
        :param upper_path: parcel file for the right bookend
        :param lower_year: horizon year for the left bookend
        :param upper_year: horizon year for the right bookend
        :param horizon_year: horizon for the interpolated parcel
        :return: interpolated parcel data
        :rtype: Parcels
        '''
        import debugpy
        debugpy.breakpoint()
        indent = dialog_level(self)
        left_parcels = Parcels(self.parent().project_settings['subarea_file'], self.parent().project_settings['lookup_file'], lower_path, lower_year, indent + 1)
        right_parcels = Parcels(self.parent().project_settings['subarea_file'], self.parent().project_settings['lookup_file'], upper_path, upper_year, indent + 1)

        interpolation = LinearParcelInterpolator(self.parent().output_dir, indent)

        self.logger.info(f"Interpolating parcel data between {lower_year} and {upper_year} for horizon year {horizon_year}")
        self.logger.info(f"Lower boundary file: {lower_path}")
        self.logger.info(f"Upper boundary file: {upper_path}")

        # Parcels DataFrame after interpolation
        interpolated_parcels = interpolation.interpolate(left_parcels, right_parcels, horizon_year)
        return interpolated_parcels

    def _on_interpolation_finished(self, parcels : Parcels):
        self.base_parcel = parcels
        self.enableAllButtons()
        self.status_sections[0].setText('Done')
        self.base_filename_label.setText(self.base_parcel.filename)
        self.base_file = self.base_parcel.filename

    def _on_interpolation_error(self, exception_obj):
        # called when the thread encounters an error
        self.enableAllButtons()
        self.status_sections[0].setText("interpolation failed")
        QMessageBox.critical(self, "Error", str(exception_obj))
                             
    def validate_btn_clicked(self):
        self.status_sections[0].setText("running")
        self.disableAllButtons()

        self.worker = ThreadWrapper(self.base_parcel.validate_parcel_file)
        self.worker.finished.connect(lambda validate_dict: self._on_validation_finished(validate_dict))
        self.worker.error.connect(lambda message: self._on_validation_error(self.status_sections[0], message))
        self.worker.start()
        
    def _on_validation_finished(self, validate_dict):
        # called when the thread is finished
        self.enableAllButtons()
        
        self.status_sections[0].setText("Done")
        # Add validation logic here
        valid_dialog = ValidationAndSummary(self, "Validation and Summary of the base parcel data", validate_dict)
        valid_dialog.exec()
        self.status_sections[0].setText("")

    def _on_validation_error(self, status_bar_section, message):
        # called when the thread encounters an error
        self.enableAllButtons()
        status_bar_section.setText("Error")
        QMessageBox.critical(self, "Error", message)

    def summarize_btn_clicked(self):
        summary_dict = self.base_parcel.summarize_parcel_data(self.parent().output_dir, 'base')
        summary_dialog = ValidationAndSummary(self, "Base Parcel File Summary", summary_dict)
        summary_dialog.exec()    

    def closeEvent(self, event):
        self.logger.info("Base Parcel Data Generator is closed.")
        self.accept()
        event.accept()   
