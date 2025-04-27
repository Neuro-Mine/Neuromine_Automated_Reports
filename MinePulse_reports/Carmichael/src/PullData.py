import requests
import pandas as pd
import logging
from datetime import datetime
import numpy as np
from sqlalchemy import create_engine, text
import time
import pymssql

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

        self.mssql_conn = None
        self.logger = logger  # you can configure your logger here
        
        
    def connect_mssql_db(self):
        """Connects directly to MSSQL using pymssql (no pooling)."""
        try:
            host = "10.4.15.5"
            database = "NEUROMINE00"
            user = "neuromine"
            password = "M1ner@l5"
            port = "1433"

            self.mssql_conn = pymssql.connect(
                server=host,
                user=user,
                password=password,
                database=database,
                port=int(port),
                login_timeout=10,
                timeout=30,
                charset='UTF-8'
            )
            self.logger.info("MSSQL connected successfully via pymssql")

        except Exception as e:
            self.logger.error(f"Error connecting to MSSQL: {e}", exc_info=True)
            self.mssql_conn = None

    def release_mssql_connection(self):
        """Closes the MSSQL connection if open."""
        if self.mssql_conn:
            self.mssql_conn.close()
            self.mssql_conn = None
            self.logger.info("MSSQL connection closed")
            
            
    def get_SQL_data(self, site=""):
        """
        Fetch controller-level averages from the SQL Server database for the latest date available.

        Args:
            site (str): Site name to filter on.

        Returns:
            pd.DataFrame: DataFrame with controller averages.
        """
        expected_columns = ['Controller', 'Avg Limits Exceeded', 'avg_shutdown_%']
        max_retries = 5
        retry_count = 0
        retry_delay = 5

        query = f"""
            SELECT [Controller], [Date], 
                   AVG([Avg_Limits_Exceeded_%]) AS "Avg Limits Exceeded",
                   AVG([avg_shutdown_%]) AS "avg_shutdown_%"
            FROM [NEUROMINE00].[dbo].[ProLET_Prod]
            WHERE [SITE] = '{site}'
              AND [Date] = (
                  SELECT MAX([Date])
                  FROM [NEUROMINE00].[dbo].[ProLET_Prod]
                  WHERE [SITE] = '{site}'
              )
            GROUP BY [Controller], [Date]
        """

        df = pd.DataFrame()

        def check_dataframe(df):
            if df.empty:
                return False
            return all(col in df.columns for col in expected_columns)

        while not check_dataframe(df):
            if retry_count >= max_retries:
                self.logger.error(f"Max retries reached while attempting SQL data fetch for site '{site}'.")
                break

            try:
                with self.mssql_conn.cursor(as_dict=True) as cursor:
                    cursor.execute(query)
                    result = cursor.fetchall()
                    df = pd.DataFrame(result)

                    if not df.empty:
                        self.logger.info(f"Successfully fetched SQL data for site '{site}' with {len(df)} rows.")

            except Exception as e:
                self.logger.error(f"SQL query failed for site '{site}': {e}", exc_info=True)
                df = pd.DataFrame()

            retry_count += 1
            if df.empty:
                time.sleep(retry_delay)

        return df

            
            
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
        entity_ids = entity_config.keys() if isinstance(entity_config, dict) else entity_config

        for entity_id in entity_ids:
            params = {
                "EntityID": entity_id,
                "StartDate": start_date,
                "EndDate": end_date,
                "Interval" : '1m',
                "Aggregate" : 'avg'
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
                    df = df[["Value_dbl"]]  # Keep only ApprovedValue column
                    df.rename(columns={"Value_dbl": entity_id}, inplace=True)  # Rename column
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
        
    
    def send_telegram(self, df):
        """
        Send the latest values from the DataFrame to Telegram, with alerts.

        Args:
            df (pd.DataFrame): The DataFrame containing data.
            entity_config (dict): Dictionary containing entity metadata.
        """

        # Telegram API Information
        telegram_api_url = "https://minopex.chat/api/telegram/messages"
        telegram_token = "rzSZtTRM8tKzNEFVCNSFUdfYYUfsQI8Z9GkMqDVO1d9411b2"
        telegram_telegram_ids = "1544960677,6741170232"

        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "Authorization": f"Bearer {telegram_token}"
        }

        # Get the latest values from the DataFrame (last row)
        latest_values = df.iloc[0]

        features = ""

        # Loop through each column and compare values with bounds
        for index, row in df.iterrows():
            target = 0.8
            controller = row['Controller']
            limit_exceeded = row['%_time_in_state']
            shutdown = row['avg_shutdown_%']
            
            if shutdown > 50:
                status = "🤖"
            elif limit_exceeded < target:
                status = "🔴"  # Above upper bound 
            else:
                status = "🟢"  # Within normal range

            # Append to message
            features += f"\n{status} <b>{controller}</b>: {limit_exceeded:.1f} (target: 80%)\n"
            
        # Construct the message
        message = f"""
<b>Carmichael Daily Controller Analysis </b>\n
🤖: Plant off for more that 50% \n
🔴: Bad Controller \n
🟢: Good Controller \n
(Performance for the last 24 hours)\n
Date: {df["Date"][0]}
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
