# equity_market_analysis

The goal of this project is to build an end-to-end data pipeline to ingest and process daily stock market data from multiple stock exchanges. The pipeline maintains the source data in a structured format, organized by date. 
It also needs to produce analytical results that support business analysis.

### Overview:
- Spring Capital (hypothetical enterprise) collects data on trades and quotes from multiple exchanges every day. 
- Their data team creates data platforms and pipelines that provide the firm with insights through merging data points and y calculating key indicators. 
- Spring Capital’s business analysts want to better understand their raw quote data by referencing specific trade indicators which occur whenever their quote data is generated, including:
  - Latest trade price
  - Prior day closing price
  - 30-minute moving average trade price (Average price over the past 30 minutes,
  constantly updated. This is a common indicator which smooths the price trend and cuts down noise.)

As a data engineer, you are asked to build a data pipeline that produces a dataset including the above indicators for the business analysts.

High-level Architecture Diagram

![equity_market_data_architecture](https://github.com/meetapandit/equity_market_analysis/assets/15186489/4cf2d517-6f69-4166-ad59-c241e5b089ea)


### Technical Requirements
● This project is implemented using a data pipeline in PySpark
● Trade and quote data from each exchange contain billions of records. The pipeline needs to be scalable enough to handle that.
● Google Cloud Storage for maintaining partitions by days of updated datasets
● Preferred IDE of choice for coding, e.g. Intellij, PyCharm, Sublime

### Data Source
The source data used in this project is randomly generated stock exchange data.
● Trades: records that indicate transactions of stock shares between broker-dealers. See trade data below.
● Quotes: records of updates best bid/ask price for a stock symbol on a certain exchange. See quote data below.

Trade Schema
<img width="343" alt="Screenshot 2023-11-29 at 2 20 27 PM" src="https://github.com/meetapandit/equity_market_analysis/assets/15186489/fe5d5117-da81-462a-9b35-a8277a6216fe">

Quotes Schema
<img width="343" alt="Screenshot 2023-11-29 at 2 20 45 PM" src="https://github.com/meetapandit/equity_market_analysis/assets/15186489/57c14814-99dc-4829-8fd9-fafcb730490d">

### Data Pipeline steps
Step 1: Data Ingestion
- Exchanges share data in csv or json file formats. each file is mixed with both Trade and Quote records
- Ingestion step drops records that do not follow the schema

Step 2: EOD Batch load
- Loads all the progressively processed data for current day into daily tables for trade and quote.
- The Record Unique Identifier is the combination of columns: trade date, record type, symbol, event time, event sequence number.
- Exchanges may submit the records which are intended to correct errors in previous ones with updated information. Such updated records will have the same Record Unique Identifier, defined above.
- The process should discard the old record and load only the latest one to the table.
- Job should be scheduled to run at 5pm every day.

Step 3: Analytical ETL Load
- This steps addes 3 new columns for the business analysts to calculate trends in stock price changes
- The following 3 columns are added:
  - The latest trade price before the quote.
  - The latest 30 min moving average trade price before the quote.
  - The bid and ask price movement (difference) from the previous day's last trade price. For example, given the last trade price of $30, bid price of $30.45 has a movement of $0.45.
    
![Screenshot 2023-11-25 at 4 47 21 PM](https://github.com/meetapandit/equity_market_analysis/assets/15186489/665752cb-9d13-4274-b02a-dbe064da9a79)


Step 4: Pipeline Orchestration
- The pipeline is deployed to Google Cloud Platform VM instance for scheduling in Airflow

<img width="1083" alt="Screenshot 2023-11-25 at 4 37 21 PM" src="https://github.com/meetapandit/equity_market_analysis/assets/15186489/3d3895e7-533f-4acd-97b1-3febfc27dde9">

