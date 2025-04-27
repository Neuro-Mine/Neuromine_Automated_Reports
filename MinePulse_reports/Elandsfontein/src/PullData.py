import requests
import pandas as pd
import logging
from datetime import datetime
import numpy as np


import os
os.chdir(os.path.dirname(os.path.abspath(__file__)))


# Configure logging
logging.basicConfig(
    filename='../etc/sri_data.log', level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class DataPull:
    def __init__(self, api_info):
        """Initialize DataPull with API information."""
        self.api_url = api_info["api_url"]
        self.headers = {"Authorization": f"Bearer {api_info['Authorization']}"}

    def get_SRI_data(self, entity_config, start_date, end_date, granularity=300):
        """
        Fetch SRI data for given EntityIDs and return as a DataFrame.
        
        Args:
            entity_config (dict): Dictionary containing entity metadata.
            start_date (str): Start date in "YYYY-MM-DD HH:MM" format.
            end_date (str): End date in "YYYY-MM-DD HH:MM" format.
            granularity (int): Time interval in seconds (default is 300).

        Returns:
            pd.DataFrame: Data with Timestamp as index and renamed columns.
        """
        all_data = []  # List to store data for all entities
        entity_ids = entity_config.keys()

        for entity_id in entity_ids:
            params = {
                "EntityID": entity_id,
                "StartDate": start_date,
                "EndDate": end_date,
                "Granularity": granularity
            }

            try:
                response = requests.get(self.api_url, headers=self.headers, params=params, timeout=30)
                response.raise_for_status()
                data = response.json()

                if data:
                    # Convert to DataFrame
                    df = pd.DataFrame(data)
                    df["Timestamp"] = pd.to_datetime(df["Timestamp"])  # Convert to datetime
                    df.set_index("Timestamp", inplace=True)  # Set index
                    df = df[["ApprovedValue"]]  # Keep only ApprovedValue column
                    df.rename(columns={"ApprovedValue": entity_config[entity_id]["name"]}, inplace=True)  # Rename column
                    all_data.append(df)

                    logger.info(f"Data fetched successfully for EntityID {entity_id}")

                else:
                    logger.warning(f"No data found for EntityID {entity_id}")

            except requests.RequestException as e:
                logger.error(f"Request failed for EntityID {entity_id}: {e}")

        # Combine all entity data into a single DataFrame
        if all_data:
            final_df = pd.concat(all_data, axis=1)  # Merge by Timestamp
            return final_df
        else:
            return pd.DataFrame()  # Return empty DataFrame if no data found

    def send_telegram(self, df, entity_config):
        """
        Send the latest values from the DataFrame to Telegram, with alerts.

        Args:
            df (pd.DataFrame): The DataFrame containing data.
            entity_config (dict): Dictionary containing entity metadata.
        """

        # Telegram API Information
        telegram_api_url = "https://minopex.chat/api/telegram/messages"
        telegram_token = "rzSZtTRM8tKzNEFVCNSFUdfYYUfsQI8Z9GkMqDVO1d9411b2"
        telegram_telegram_ids = "1544960677,5675648641,6741170232,6728426799,6651869013,978298696,6865055905,6406512582,6315724673,6646299248,5373457257,5675648641"
        # telegram_telegram_ids = "1544960677"


        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "Authorization": f"Bearer {telegram_token}"
        }

        # Get the latest values from the DataFrame (last row)
        latest_values = df.iloc[0]

        features = ""

        # Loop through each column and compare values with bounds
        for entity_id, meta in entity_config.items():
            col_name = meta["name"]
            value = latest_values.get(col_name, None)
            multiplier = meta["multiplier"]
            unit = meta["unit"]

            if value is not None:
                value *= multiplier  # Apply multiplier correctly

                lower = round(float(meta["lower_bound"]), 2)*multiplier if meta["lower_bound"] is not None else None
                upper = round(float(meta["upper_bound"]), 2)*multiplier if meta["upper_bound"] is not None else None

                # Fix target display
                target_display = f"{lower :.1f}" if (lower is not None and lower > 0) else f"{upper:.1f}" if upper is not None else "Not Set"

                if target_display == 'Not Set':
                    status = "🤖"  # Below lower bound
                elif upper is not None and value > upper:
                    status = "🔴"  # Above upper bound
                elif lower is not None and value < lower: 
                    status = "🔴"  # Grey robot if no thresholds exist 
                else:
                    status = "🟢"  # Within normal range


                # Append to message
                features += f"\n{status} <b>{col_name}</b>: {value:.1f} {unit} (target: {target_display} {unit})\n"


        # Construct the message
        message = f"""
<b>[REPORT] Elandsfontein Daily Production Report</b>\n
(Average value for the last 24 hours)
---------------\n
{features}
\n Neuromine Services
"""

        data = {
            "message": message,
            "users": telegram_telegram_ids.split(',')
        }

        # Send the POST request
        try:
            response = requests.post(telegram_api_url, headers=headers, json=data)

            # Check the response
            if response.status_code == 200:
                print("Message sent successfully!")
            else:
                print(f"Failed to send message. Status code: {response.status_code}")
                print("Response:", response.text)

        except Exception as e:
            print(f"Error sending Telegram message: {e}")
