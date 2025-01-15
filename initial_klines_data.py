import pandas as pd
from binance.client import Client
from datetime import datetime
import mysql.connector
from mysql.connector import Error
from key import Key_api

api_key = Key_api.API_key
api_secret = Key_api.Secret_Key

client = Client(api_key, api_secret)

def get_klines(symbol, interval, start_str, end_str=None):
    klines = client.get_historical_klines(symbol, interval, start_str, end_str)
    return klines

def klines_to_dataframe(klines):
    df = pd.DataFrame(klines, columns=[
        "Open_Time", "Open", "High", "Low", "Close", "Volume",
        "Close_Time", "Quote_Asset_Volume", "Number_of_Trades",
        "Taker_Buy_Base_Asset_Volume", "Taker_Buy_Quote_Asset_Volume", "Unused_Ignore"
    ])
    df["Open_Time"] = pd.to_datetime(df["Open_Time"], unit='ms').dt.tz_localize('UTC').dt.tz_convert('Asia/Jakarta')
    df["Close_Time"] = pd.to_datetime(df["Close_Time"], unit='ms').dt.tz_localize('UTC').dt.tz_convert('Asia/Jakarta')
    df[["Open", "High", "Low", "Close", "Volume"]] = df[["Open", "High", "Low", "Close", "Volume"]].astype(float)
    return df

def main():
    symbol = 'TAOUSDT'
    interval = Client.KLINE_INTERVAL_1HOUR
    start_date = '2019-01-01'
    end_date = datetime.now().strftime('%Y-%m-%d')

    all_data = []
    current_start_date = start_date

    while True:
        klines = get_klines(symbol, interval, current_start_date, end_date)
        if not klines:
            break

        all_data.extend(klines)
        current_start_date = pd.to_datetime(klines[-1][0], unit='ms').strftime('%Y-%m-%d')

    df = klines_to_dataframe(all_data)
    df.drop(columns=['Unused_Ignore'], inplace=True)
    df = df.drop_duplicates(subset=['Open_Time'])
    df.reset_index(drop=True, inplace=True)
    return df

if __name__ == "__main__":
    df = main()



def save_to_mysql(df):
    try:
        connection = mysql.connector.connect(
            username = Key_api.DB_username,
            password = Key_api.DB_password,
            host = Key_api.DB_host,
            port = Key_api.DB_port,
            database_name = Key_api.DB_database_name,
        )
        if connection.is_connected():
            cursor = connection.cursor()
            create_table_query = '''
            CREATE TABLE IF NOT EXISTS bittensor_price (
                open_time DATETIME NOT NULL,
                open DECIMAL(18, 8) NOT NULL,
                high DECIMAL(18, 8) NOT NULL,
                low DECIMAL(18, 8) NOT NULL,
                close DECIMAL(18, 8) NOT NULL,
                volume DECIMAL(18, 8) NOT NULL,
                close_time DATETIME NOT NULL,
                quote_asset_volume DECIMAL(18, 8) NOT NULL,
                number_of_trades INT NOT NULL,
                taker_buy_base_asset_volume DECIMAL(18, 8) NOT NULL,
                taker_buy_quote_asset_volume DECIMAL(18, 8) NOT NULL,
                PRIMARY KEY (open_time)
            ) ENGINE=InnoDB;
            '''
            cursor.execute(create_table_query)

            df.columns = [col.lower() for col in df.columns]
            sql = '''
            INSERT INTO bittensor_price (
                open_time, open, high, low, close, volume, 
                close_time, quote_asset_volume, number_of_trades, 
                taker_buy_base_asset_volume, taker_buy_quote_asset_volume
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            '''
            data_to_insert = [tuple(row) for row in df.values]
            cursor.executemany(sql, data_to_insert)

            connection.commit()
    except Error as e:
        print("Error while connecting to MySQL", e)
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

save_to_mysql(df)
