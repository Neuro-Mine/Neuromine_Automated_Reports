import schedule
import time
from PullData import DataPull
from datetime import datetime, timedelta

import os
os.chdir(os.path.dirname(os.path.abspath(__file__)))


# API Info
APi_Info = {
    "api_url": "https://efnminocore.minopex.com/DataExtract/DataPoints_Elandsfontein/Fetch",
    "Authorization": "a6ez@$m$uvyxJTzdcy3SjvTus^L7dkd7"
}


def fetch_and_send_report():
    print("Running scheduled data fetch and alert...")

    # Get current date and time
    now = datetime.now()

    # Determine today's 6 AM
    today_6am = now.replace(hour=6, minute=0, second=0, microsecond=0)

    # Determine yesterday's 6 AM
    yesterday_6am = today_6am - timedelta(days=1)

    # Format the start and end dates
    start_date = yesterday_6am.strftime("%Y-%m-%d %H:%M")
    end_date = today_6am.strftime("%Y-%m-%d %H:%M")
    
    print("Start time: ", start_date,"\n","End Date:",end_date)

    # Create an instance of DataPull
    datapull = DataPull(APi_Info)

    # Targets
    targets = {
        "F04158B1-5DBB-4007-A270-AF6A00DAFDA0": {"name": "Yield (Target)", "lower_bound": 1, "upper_bound": None, "multiplier": 1},
        "269FDADE-EC6D-4BBD-85C4-AEC200D370C5": {"name": "P2O5 Recovery (Target)", "lower_bound": 1, "upper_bound": None, "multiplier": 100},
        "A7609881-0F51-4218-8B25-AF6B009B4F05": {"name": "Final Concentrate Grade (Target)", "lower_bound": 1, "upper_bound": None, "multiplier": 1},
        "14B37EAA-D012-4D57-832A-AEB700D96118": {"name": "Dryer Product (Target)", "lower_bound": 1, "upper_bound": None, "multiplier": 1},
        "3B053B88-A0E2-45BA-AD46-AEC100B3A262": {"name": "Plant Throughput (Target)", "lower_bound": 1, "upper_bound": None, "multiplier": 1},
        "F8A4F8B6-7A2F-4704-8840-AEC100AD06DC": {"name": "Plant Run Time (Target)", "lower_bound": 1, "upper_bound": None, "multiplier": 1},
        "40B019BE-E936-4D53-9497-AF6B00928185": {"name": "Head Grade (Target)", "lower_bound": 1, "upper_bound": None, "multiplier": 1},
        "52B34F88-171C-47AA-BDE0-AEC100A5C574": {"name": "Primary Screen Undersize Feed (Target)", "lower_bound": 1, "upper_bound": None, "multiplier": 1},
        "B56C997E-0666-45A3-B902-AF6B00928186": {"name": "Primary Screen Oversize Feed (Target)", "lower_bound": None, "upper_bound": 1, "multiplier": 1},
        "AFA2A481-8115-4FD7-B3EB-AEB700D5E520": {"name": "Primary Screen Feed (Target)", "lower_bound": 1, "upper_bound": None, "multiplier": 1},
        "1456A2B4-E699-4865-B737-AEBF00DC94A6": {"name": "ROM Stockpile Feed (Target)", "lower_bound": 1, "upper_bound": None, "multiplier": 1}
    }

    targets = datapull.get_SRI_data(targets, start_date, end_date)

    # Entity Configuration
    entity_config = {
        "60FAB993-F296-44EA-B8FA-AEC200D537C1": {"name": "Yield",
            "lower_bound": targets.iloc[0, targets.columns.get_loc("Yield (Target)")] if "Yield (Target)" in targets.columns else None,
            "upper_bound": None, "multiplier": 100, "unit": "%"},

        "932794FC-5B81-4BC5-8CD8-AFC7009B78F2": {"name": "Declared Concentrate On Spec - Tons",
            "lower_bound": None, "upper_bound": None, "multiplier": 1, "unit": "Ton"},

        "B0870EB3-970D-4BB9-A6A8-AFC700B30704": {"name": "Declared Concentrate On Spec - Grade",
            "lower_bound": None, "upper_bound": None, "multiplier": 100, "unit": "%"},

        "133E59F3-457A-4360-92E9-AEC200CE5E33": {"name": "P2O5 Recovery",
            "lower_bound": targets.iloc[0, targets.columns.get_loc("P2O5 Recovery (Target)")] if "P2O5 Recovery (Target)" in targets.columns else None,
            "upper_bound": None, "multiplier": 100, "unit": "%"},

        "B6094B17-11E0-41C1-9B04-AF2D00A7128C": {"name": "Final Concentrate Grade",
            "lower_bound": targets.iloc[0, targets.columns.get_loc("Final Concentrate Grade (Target)")] if "Final Concentrate Grade (Target)" in targets.columns else None,
            "upper_bound": 100, "multiplier": 100, "unit": "%"},

        "D02E4047-294F-4B00-AEA4-AEC101151C52": {"name": "Dryer Product",
            "lower_bound": targets.iloc[0, targets.columns.get_loc("Dryer Product (Target)")] if "Dryer Product (Target)" in targets.columns else None,
            "upper_bound": None, "multiplier": 1, "unit": ""},

        "3599BC39-65F3-4C48-A2F6-AEC100B2CCB2": {"name": "Plant Throughput",
            "lower_bound": targets.iloc[0, targets.columns.get_loc("Plant Throughput (Target)")] if "Plant Throughput (Target)" in targets.columns else None,
            "upper_bound": None, "multiplier": 1, "unit": "t/h"},

        "483789F3-B1DA-4C8C-BF02-AEC100AD7C37": {"name": "Plant Run Time",
            "lower_bound": targets.iloc[0, targets.columns.get_loc("Plant Run Time (Target)")] if "Plant Run Time (Target)" in targets.columns else None,
            "upper_bound": None, "multiplier": 1, "unit": "hrs"},

        "F93DEC12-A392-4424-B1AE-AF6B00869158": {"name": "Head Grade",
            "lower_bound": targets.iloc[0, targets.columns.get_loc("Head Grade (Target)")] if "Head Grade (Target)" in targets.columns else None,
            "upper_bound": None, "multiplier": 100, "unit": "%"},

        "20C01185-E071-404B-A393-AEC100A4FDE9": {"name": "Primary Screen Undersize Feed",
            "lower_bound": targets.iloc[0, targets.columns.get_loc("Primary Screen Undersize Feed (Target)")] if "Primary Screen Undersize Feed (Target)" in targets.columns else None,
            "upper_bound": None, "multiplier": 1, "unit": ""},

        "E36FC62A-1A1F-4502-BEC4-AEBF00E6FC5E": {"name": "Primary Screen Oversize Feed",
            "lower_bound": None,
            "upper_bound": targets.iloc[0, targets.columns.get_loc("Primary Screen Oversize Feed (Target)")] if "Primary Screen Oversize Feed (Target)" in targets.columns else None,
            "multiplier": 1, "unit": ""},

        "F20BE198-30D0-47B5-8295-AEB6007A5030": {"name": "Primary Screen Feed",
            "lower_bound": targets.iloc[0, targets.columns.get_loc("Primary Screen Feed (Target)")] if "Primary Screen Feed (Target)" in targets.columns else None,
            "upper_bound": None, "multiplier": 1, "unit": ""},

        "76F28C04-49CB-4283-8242-AEBF00D81D9C": {"name": "ROM Stockpile Feed",
            "lower_bound": targets.iloc[0, targets.columns.get_loc("ROM Stockpile Feed (Target)")] if "ROM Stockpile Feed (Target)" in targets.columns else None,
            "upper_bound": None, "multiplier": 1, "unit": ""}
    }


    # Fetch the data
    df = datapull.get_SRI_data(entity_config, start_date, end_date)
    df = df.astype(float).round(3)

    # Send alert to Telegram
    print("Sending Telegram Notification")
    datapull.send_telegram(df, entity_config)
    
    return df, targets

# Schedule the job to run every day at 8 AM
schedule.every().day.at("07:00").do(fetch_and_send_report)

# Keep the script running
while True:
    schedule.run_pending()
    time.sleep(30)
