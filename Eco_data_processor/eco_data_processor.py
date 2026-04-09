import sys
import pandas as pd
import datetime
from PyQt6.QtWidgets import (
    QApplication, QWidget, QVBoxLayout, QPushButton, QLabel,
    QFileDialog, QMessageBox
)

class DataAggregator(QWidget):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("EastRail Aggregator (Monthly Averages)")
        self.resize(400, 200)

        self.layout = QVBoxLayout()

        # File selection
        self.file_label = QLabel("No file selected")
        self.select_button = QPushButton("Select CSV File")
        self.select_button.clicked.connect(self.load_file)

        # Run button
        self.run_button = QPushButton("Run Aggregation")
        self.run_button.clicked.connect(self.run_aggregation)

        self.layout.addWidget(self.file_label)
        self.layout.addWidget(self.select_button)
        self.layout.addWidget(self.run_button)

        self.setLayout(self.layout)

        self.df = None

    def load_file(self):
        file_path, _ = QFileDialog.getOpenFileName(self, "Open CSV", "", "CSV Files (*.csv)")
        if file_path:
            self.file_label.setText(file_path)
            self.df = pd.read_csv(file_path)

            # --- Parse mixed datetime ---
            dt_series = pd.to_datetime(self.df['Time'], errors='coerce')
            mask = dt_series.isna()
            if mask.any():
                dt_series.loc[mask] = pd.to_datetime(self.df.loc[mask, 'Time'], dayfirst=True, errors='coerce')

            self.df['Time'] = dt_series
            self.df = self.df.dropna(subset=['Time'])

            # --- Time fields ---
            self.df['Month'] = self.df['Time'].dt.strftime('%b')
            self.df['Date'] = self.df['Time'].dt.date
            self.df['Hour'] = self.df['Time'].dt.hour + self.df['Time'].dt.minute / 60

    def get_time_period(self, hour):
        if 6 <= hour < 9:
            return "AM"
        elif 9 <= hour < 15.5:
            return "MD"
        elif 15.5 <= hour < 18.5:
            return "PM"
        else:
            return "NI"

    def run_aggregation(self):
        if self.df is None:
            QMessageBox.warning(self, "Error", "Please load a file first")
            return

        mid_weekdays = ['Tuesday', 'Wednesday', 'Thursday']

        month_order = ['Jan','Feb','Mar','Apr','May','Jun', 'Jul','Aug','Sep','Oct','Nov','Dec'
]

        df = self.df.copy()
        df['TimePeriod'] = df['Hour'].apply(self.get_time_period)
        df['Weekday'] = df['Time'].dt.strftime('%A')

        numeric_cols = df.select_dtypes(include='number').columns

        # --- 1. Average by Time of Day by Month ---
        tod_month = (
            df.groupby(['Month', 'Date',  'Weekday','TimePeriod'])[numeric_cols]
            .sum()
            .reset_index()
        )

        # --- 2. Average Daily Total by Month ---
        daily = (
            df.groupby(['Date', 'Weekday'])[numeric_cols]
            .sum()
            .reset_index()
        )
        daily['Month'] = pd.to_datetime(daily['Date']).dt.strftime('%b')

        monthly_daily_avg = (
            daily.groupby('Month')[numeric_cols]
            .mean()
            .reset_index()
        )

        monthly_daily_avg['Month'] = pd.Categorical(monthly_daily_avg['Month'], categories=month_order, ordered=True)
        monthly_daily_avg = monthly_daily_avg.sort_values('Month')
        for col in numeric_cols:
            monthly_daily_avg[col] = monthly_daily_avg[col].fillna(0).astype('int')

        midweekday_avg = ( 
            daily.loc[daily['Weekday'].isin(mid_weekdays)].groupby('Month')[numeric_cols]
            .mean()
            .reset_index()
        )

        midweekday_avg['Month'] = pd.Categorical(midweekday_avg['Month'], categories=month_order, ordered=True)
        midweekday_avg = midweekday_avg.sort_values('Month')
        for col in numeric_cols:
            midweekday_avg[col] = midweekday_avg[col].fillna(0).astype('int')

        # --- Save to Excel ---
        save_path, _ = QFileDialog.getSaveFileName(self, "Save Excel", "output.xlsx", "Excel Files (*.xlsx)")
        if save_path:
            with pd.ExcelWriter(save_path) as writer:
                readme = writer.book.add_worksheet('README')
                readme.write(0, 0, datetime.datetime.now().strftime("Generated on %Y-%m-%d %H:%M:%S"))
                readme.write(2, 0, save_path)
                readme.write(4, 0, "Description")
                readme.write(5, 0, "Average values by Time of Day (AM/MD/PM/NI) by Month")
                readme.write(6, 0, "Average daily totals aggregated by Month")  
                readme.write(8, 0, "Notes")
                readme.write(9, 0, "Time of Day is categorized as: AM (6-9), MD (9-15:30), PM (15:30-18:30), NI (18:30-6)")
                readme.write(10, 0, "Daily totals are calculated by summing all records for each date, then averaged by month")
                readme.write(11, 0, f'Midweek average is calculated by filtering for {mid_weekdays} and then averaging daily totals by month')


                tod_month.to_excel(writer, sheet_name='Avg_by_TOD_Month', index=False)
                monthly_daily_avg.to_excel(writer, sheet_name='Avg_Daily_by_Month', index=False)
                daily.to_excel(writer, sheet_name='Daily_Totals', index=False)
                midweekday_avg.to_excel(writer, sheet_name='Midweek_Avg_Daily', index=False)

            QMessageBox.information(self, "Success", "File saved successfully")


if __name__ == '__main__':
    app = QApplication(sys.argv)
    window = DataAggregator()
    window.show()
    sys.exit(app.exec())