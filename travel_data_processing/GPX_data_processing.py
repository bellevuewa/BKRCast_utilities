import sys
import os
import gpxpy
import geopandas as gpd
from shapely.geometry import Point
from PyQt6.QtWidgets import (
    QApplication, QWidget, QVBoxLayout, QHBoxLayout, QPushButton,
    QTableWidget, QTableWidgetItem, QFileDialog, QLabel, QMessageBox, QLineEdit, QSizePolicy
)
from PyQt6.QtCore import Qt

# === USER SETTINGS ===
projected_crs = "EPSG:2285"  # NAD83 / Washington North (ftUS)

class GPXConverterGUI(QWidget):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("GPX to Shapefile Converter")
        self.resize(800, 400)
        self.gpx_files = []
        self.output_folder = ""
        self.min_distance_ft = 10  # minimum distance in feet
        self.init_ui()

    def init_ui(self):
        layout = QVBoxLayout()

        # Buttons
        self.btn_select_files = QPushButton("Select GPX Files")
        self.btn_select_folder = QPushButton("Select Output Folder")
        self.btn_process = QPushButton("Process Files")

        # Minimum distance (label and textbox on same row)
        self.min_distance_label = QLabel('Minimum Distance (ft):')
        self.min_distance_textbox = QLineEdit(str(self.min_distance_ft))
        self.min_distance_textbox.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Fixed)

        # put label and textbox on a single horizontal row
        h_layout = QHBoxLayout()
        h_layout.addWidget(self.min_distance_label)
        h_layout.addWidget(self.min_distance_textbox)

        # Table to show file info
        self.table = QTableWidget()
        self.table.setColumnCount(3)
        self.table.setHorizontalHeaderLabels(["File Name", "Total Points", "Exported Points"])
        self.table.setSortingEnabled(True)

        # Labels
        self.lbl_output = QLabel("Output folder: Not selected")
        self.lbl_summary = QLabel("")

        # Connect buttons
        self.btn_select_files.clicked.connect(self.select_files)
        self.btn_select_folder.clicked.connect(self.select_folder)
        self.btn_process.clicked.connect(self.process_files)

        # Layout
        layout.addWidget(self.btn_select_files)
        layout.addWidget(self.btn_select_folder)
        layout.addLayout(h_layout)
        layout.addWidget(self.table)
        layout.addWidget(self.lbl_output)
        layout.addWidget(self.btn_process)
        layout.addWidget(self.lbl_summary)


        self.setLayout(layout)

    def select_files(self):
        files, _ = QFileDialog.getOpenFileNames(self, "Select GPX Files", "", "GPX Files (*.gpx)")
        if files:
            self.gpx_files = files
            self.table.setRowCount(0)
            for f in self.gpx_files:
                total = self.count_points(f)
                row = self.table.rowCount()
                self.table.insertRow(row)
                self.table.setItem(row, 0, QTableWidgetItem(os.path.basename(f)))
                self.table.setItem(row, 1, QTableWidgetItem(str(total)))
  

    def select_folder(self):
        folder = QFileDialog.getExistingDirectory(self, "Select Output Folder")
        if folder:
            self.output_folder = folder
            self.lbl_output.setText(f"Output folder: {folder}")

    def count_points(self, gpx_path):
        """Return total points count"""
        points = []
        with open(gpx_path, 'r', encoding='utf-8') as f:
            gpx = gpxpy.parse(f)
            for track in gpx.tracks:
                for segment in track.segments:
                    for pt in segment.points:
                        points.append(Point(pt.longitude, pt.latitude))
        total = len(points)
        return total

    def filter_points(self, gdf, min_dist_ft):
        """Filter points closer than min_dist_ft"""
        if gdf.empty:
            return gdf
        filtered = [gdf.iloc[0]]
        last_geom = gdf.iloc[0].geometry
        for idx, row in gdf.iloc[1:].iterrows():
            if last_geom.distance(row.geometry) >= min_dist_ft:
                filtered.append(row)
                last_geom = row.geometry
        return gpd.GeoDataFrame(filtered, crs=gdf.crs)

    def compute_velocity(self, gdf):
        """Compute velocity (mph) between consecutive points"""
        gdf = gdf.sort_values("time").reset_index(drop=True)
        gdf["vel_mph"] = 0.0
        for i in range(1, len(gdf)):
            p1, p2 = gdf.loc[i-1, "geometry"], gdf.loc[i, "geometry"]
            t1, t2 = gdf.loc[i-1, "time"], gdf.loc[i, "time"]
            if p1 and p2 and t1 and t2:
                dt = (t2 - t1).total_seconds()
                if dt > 0:
                    dist_ft = p1.distance(p2)
                    gdf.at[i, "vel_mph"] = dist_ft / dt * 0.681818
        return gdf

    def process_files(self):
        if not self.gpx_files:
            QMessageBox.warning(self, "No Files", "Please select GPX files first.")
            return
        if not self.output_folder:
            QMessageBox.warning(self, "No Folder", "Please select output folder.")
            return

        os.makedirs(self.output_folder, exist_ok=True)
        processed_count = 0

        # Get minimum distance from textbox
        try:
            self.min_distance_ft = float(self.min_distance_textbox.text())
        except ValueError:
            QMessageBox.warning(self, "Invalid Distance", "Please enter a valid number for minimum distance.")
            return

        for row, gpx_path in enumerate(self.gpx_files):
            points_data = []
            base_name = os.path.splitext(os.path.basename(gpx_path))[0]
            with open(gpx_path, 'r', encoding='utf-8') as f:
                gpx = gpxpy.parse(f)
                for track in gpx.tracks:
                    for segment in track.segments:
                        for pt in segment.points:
                            points_data.append({
                                "time": pt.time,
                                "geometry": Point(pt.longitude, pt.latitude)
                            })
            if not points_data:
                continue

            gdf = gpd.GeoDataFrame(points_data, crs="EPSG:4326").to_crs(projected_crs)
            gdf_filtered = self.filter_points(gdf, self.min_distance_ft)
            gdf_filtered = self.compute_velocity(gdf_filtered)

            # Save shapefile
            gdf_filtered["time_str"] = gdf_filtered["time"].dt.strftime("%Y-%m-%d %H:%M:%S")
            out_shp = os.path.join(self.output_folder, f"{base_name}.shp")
            gdf_filtered.to_file(out_shp, driver="ESRI Shapefile")

            # Update table filtered count
            self.table.setItem(row, 2, QTableWidgetItem(str(len(gdf_filtered))))
            processed_count += 1

        QMessageBox.information(self, "Done", f"Processed {processed_count} GPX file(s).")
        self.lbl_summary.setText(f"✅ {processed_count} files processed successfully.")

def main():
    app = QApplication(sys.argv)
    window = GPXConverterGUI()
    window.show()
    sys.exit(app.exec())

if __name__ == "__main__":
    main()
