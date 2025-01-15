import pandas as pd
import mysql.connector
import logging
from binance.client import Client
from datetime import datetime, timedelta
import pytz
from key import Key_api

logging.basicConfig(filename='data_insertion.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

username = Key_api.DB_username
password = Key_api.DB_password
host = Key_api.DB_host
port = Key_api.DB_port
database_name = Key_api.DB_database_name

api_key = Key_api.API_key
api_secret = Key_api.Secret_Key
client = Client(api_key, api_secret)

def get_klines(symbol, interval, limit=500):
    start_time = (datetime.now() - timedelta(days=15)).timestamp() * 1000
    klines = client.get_klines(symbol=symbol, interval=interval, limit=limit, startTime=int(start_time))
    return klines

def update_database(symbol, table_name, interval):
    klines_data = get_klines(symbol, interval)
    binance_data = pd.DataFrame(klines_data, columns=[
        'open_time', 'open', 'high', 'low', 'close', 'volume', 
        'close_time', 'quote_asset_volume', 'number_of_trades', 
        'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume', 'ignore'
    ])
    binance_data = binance_data[['open_time', 'open', 'high', 'low', 'close', 'volume', 'close_time', 'quote_asset_volume', 'number_of_trades', 'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume']]
    binance_data['open_time'] = pd.to_datetime(binance_data['open_time'], unit='ms')
    binance_data['close_time'] = pd.to_datetime(binance_data['close_time'], unit='ms')
    wib = pytz.timezone('Asia/Jakarta')
    binance_data['open_time'] = binance_data['open_time'].dt.tz_localize('UTC').dt.tz_convert(wib)
    binance_data['close_time'] = binance_data['close_time'].dt.tz_localize('UTC').dt.tz_convert(wib)
    binance_data = binance_data[binance_data['open_time'] != '0000-00-00 00:00:00']
    connection = mysql.connector.connect(
        user=username,
        password=password,
        host=host,
        port=port,
        database=database_name
    )
    cursor = connection.cursor()
    cursor.execute(f'SELECT open_time FROM {table_name}')
    existing_data = cursor.fetchall()
    existing_times = {row[0] for row in existing_data}
    new_data = binance_data[~binance_data['open_time'].isin(existing_times)]
    if not new_data.empty:
        inserted_times = []
        for index, row in new_data.iterrows():
            try:
                cursor.execute(
                    f'''
                    INSERT INTO {table_name} (
                        open_time, open, high, low, close, volume, 
                        close_time, quote_asset_volume, number_of_trades, 
                        taker_buy_base_asset_volume, taker_buy_quote_asset_volume
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ''',
                    (
                        row['open_time'], row['open'], row['high'], 
                        row['low'], row['close'], row['volume'], 
                        row['close_time'], row['quote_asset_volume'], 
                        row['number_of_trades'], row['taker_buy_base_asset_volume'], 
                        row['taker_buy_quote_asset_volume']
                    )
                )
                inserted_times.append(row['open_time'])
            except mysql.connector.Error as err:
                if err.errno == 1062:
                    logging.warning(f"Duplikat ditemukan untuk open_time: {row['open_time']}. Data ini diabaikan.")
                else:
                    logging.error(f"Kesalahan saat memasukkan data: {err}")
        connection.commit()
        logging.info(f"Data baru berhasil ditambahkan ke tabel {table_name}. Jumlah data yang ditambahkan: {len(new_data)}")
        logging.info(f"open_time yang berhasil dimasukkan: {inserted_times}")
    else:
        logging.info(f"Semua data di tabel {table_name} sudah paling update. Tidak ada data baru untuk ditambahkan.")
    cursor.close()
    connection.close()

if __name__ == "__main__":
    symbols_and_tables = {
        'SOLUSDT': 'solana_price',
        'ETHUSDT': 'ethereum_price',
        'BTCUSDT': 'bitcoin_price',
        'TAOUSDT': 'bittensor_price'
    }
    interval = Client.KLINE_INTERVAL_1HOUR
    for symbol, table_name in symbols_and_tables.items():
        update_database(symbol, table_name, interval)