import schedule
import time
from PullData import DataPull
from datetime import datetime, timedelta

import os
os.chdir(os.path.dirname(os.path.abspath(__file__)))


# API Info
APi_Info = {
    "api_url": "https://cmlminocore.minopex.com:443/DataExtract/Timescale_Carmichael/Fetch",
    "Authorization": "a6ez@$m$uvyxJTzdcy3SjvTus^L7dkd7"
}


def fetch_and_send_report(entity_ids = None):
    print("Running scheduled data fetch and alert...")

    # Date range (fetch yesterday's data up to today)
    # Get current date and time
    now = datetime.now()
    
    # Calculate start_date as 24 hours before now
    start_date = '2025-03-01 11:49:00+00:00' #(now - timedelta(days=20)).strftime("%Y-%m-%d %H:%M")

    # Set end_date as current time (when the script runs, 8 AM)
    end_date = '2025-03-19 12:28:00+00:00' # now.strftime("%Y-%m-%d %H:%M")
    
    print("Start time: ", start_date,"\n","End Date:",end_date)

    # Create an instance of DataPull
    datapull = DataPull(APi_Info)

    # Targets
    # targets = {
    #     "5a1a0996-9968-44f4-98d7-ad41008b795f": {"name": "Process Thickener Torque", "lower_bound": 0, "upper_bound": 4.58, "multiplier": 1},
    #     "6f241988-e719-4296-819c-acfd00c28ee6": {"name": "DMS Feed Rate", "lower_bound": 0, "upper_bound": 560, "multiplier": 1}
    # }

    # targets = datapull.get_SRI_data(targets, start_date, end_date)

    # # Entity Configuration
    entity_config = {
        "db970f5e-e8b7-4fea-a0d2-b29500ec133a": {"name": "db970f5e-e8b7-4fea-a0d2-b29500ec133a", "lower_bound": None, "upper_bound": 4.58, "multiplier": 1, "unit": "bar"}
        # "6f241988-e719-4296-819c-acfd00c28ee6": {"name": "DMS Feed Rate", "lower_bound": 560, "upper_bound": None, "multiplier": 1, "unit": "t/h"},
    }
    
    # entity_config = ["db970f5e-e8b7-4fea-a0d2-b29500ec133a"]

    # Fetch the data
    # df = datapull.get_SRI_data(entity_ids, start_date, end_date)
    # df =  datapull.get_SQL_data(site='Carmichael', date='2025-04-09')
    datapull.connect_mssql_db()
    df = datapull.get_SQL_data(site='Carmichael')
    df["%_time_in_state"] = 100 - df["Avg Limits Exceeded"]
    datapull.release_mssql_connection()
    
    datapull.send_telegram(df)
    
    # return df, targets
    return df

# Schedule the job to run every day at 8 AM
schedule.every().day.at("08:00").do(fetch_and_send_report)

# Keep the script running
while True:
    schedule.run_pending()
    time.sleep(30)