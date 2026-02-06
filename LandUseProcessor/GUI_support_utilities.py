from PyQt6.QtWidgets import (
    QApplication, QMessageBox, QMenu, QStatusBar, QLabel, QTableWidget, QTableWidgetItem,
    QSizePolicy, QWidget, QVBoxLayout, QListWidget,
    )

from PyQt6.QtGui import QAction 

import pandas as pd

class Shared_GUI_Widgets:
    def _on_process_thread_error(self, btns, status_bar_section, e):
        # called when the thread encounters an error
        for btn in btns:
            btn.setEnabled(True)
        status_bar_section.setText("Error")
        QMessageBox.critical(self, "Error", str(e))

    def _on_process_thread_finished(self, btns, statusbar_section, ret):
        # called when the thread is finished
        for btn in btns:
            btn.setEnabled(True)
        
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