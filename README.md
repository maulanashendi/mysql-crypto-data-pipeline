# Binance Klines Data Management

This project aims to retrieve, process, and store klines (candlestick) data from Binance using their API, as well as update existing data in a MySQL database. The system supports two primary functions: initial data retrieval and periodic data updates.

## Important Note
Some countries restrict access to the Binance API. It is recommended to use a **VPN with a Singapore server** or other supported regions to ensure seamless data retrieval from Binance.

## Files and Their Functions

### 1. `initial_klines_data.py`
- **Description**: Script to fetch initial klines data from Binance and store it in a MySQL database.
- **Features**:
  - Fetches historical klines data from a specified start date to the current date.
  - Processes raw data into a structured DataFrame.
  - Saves the processed data into a MySQL table.
- **Input**: Binance API parameters, trading symbol, and time interval.
- **Output**: Klines data stored in a MySQL database.

### 2. `update_klines_data.py`
- **Description**: Script to update existing klines data in the database with the latest data from Binance.
- **Features**:
  - Retrieves the latest klines data from Binance.
  - Checks for existing data in the database to avoid duplication.
  - Adds new data to the database if available.
- **Input**: Binance API parameters, trading symbol, and time interval.
- **Output**: Updated klines data stored in the MySQL database.

### 3. `sampel_btc_usdt_klines_daily.csv` and `sampel_btc_usdt_klines_hourly.csv`
- **Description**: Sample klines data files in CSV format for daily and hourly intervals.
- **Function**:
  - Provides examples of the data format processed by the system.

## Database Structure

The database tables follow the structure below:
- **Table Names**: `bittensor_price`, `solana_price`, `ethereum_price`, `bitcoin_price` (based on symbols).
- **Columns**:
  - `open_time`: Opening time of the candlestick (datetime format).
  - `open`: Opening price.
  - `high`: Highest price.
  - `low`: Lowest price.
  - `close`: Closing price.
  - `volume`: Trade volume.
  - `close_time`: Closing time of the candlestick.
  - `quote_asset_volume`: Asset volume traded in the quote currency.
  - `number_of_trades`: Number of trades.
  - `taker_buy_base_asset_volume`: Volume bought by takers in the base asset.
  - `taker_buy_quote_asset_volume`: Volume bought by takers in the quote currency.

## Installation

1. **Clone the repository**:
   ```bash
   git clone https://github.com/maulanashendi/mysql-crypto-data-pipeline.git
   cd mysql-crypto-data-pipeline
   ```

2. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

3. **Configure API and Database**:
   - Add your Binance API and MySQL credentials in the `key.py` file:
     ```python
     class Key_api:
         API_key = "YOUR_API_KEY"
         Secret_Key = "YOUR_API_SECRET"
         DB_username = "YOUR_DB_USERNAME"
         DB_password = "YOUR_DB_PASSWORD"
         DB_host = "YOUR_DB_HOST"
         DB_port = "YOUR_DB_PORT"
         DB_database_name = "YOUR_DATABASE_NAME"
     ```

4. **Run the scripts**:
   - **Initial data retrieval**:
     ```bash
     python initial_klines_data.py
     ```
   - **Data updates**:
     ```bash
     python update_klines_data.py
     ```

## Logging
The system logs information to the `data_insertion.log` file to monitor execution status, inserted data, and any errors encountered.
