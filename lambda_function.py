import logging
import json
import pandas as pd
import boto3
import io
import mysql.connector
import os
import time
import yfinance as yf
from datetime import datetime, timedelta
from typing import Any, Dict, Optional, Union

logger = logging.getLogger()
logger.setLevel('INFO')

def insert_data(cnx: mysql.connector.connection.MySQLConnection,
                merged_data: pd.DataFrame
                ) -> None:
    """
    Function to insert data into a MySQL table.

    Args:
        cnx (mysql.connector.connection.MySQLConnection): MySQL connection object.
        merged_data (pd.DataFrame): DataFrame containing the data to be inserted.
    """
    # Get the table name from environment variables
    table_name: str = os.environ['DB_TABLE_NAME']

    # SQL query for inserting data
    query: str = f'INSERT INTO {table_name} (fecha, moneda, Cotizacion, volumen) VALUES (%s, %s, %s, %s)'

    # Initialize cursor
    cursor = cnx.cursor()

    # Iterate over rows in DataFrame and insert data into the table
    for idx, row in merged_data.iterrows():
        data_moneda = (
            pd.Timestamp.to_pydatetime(row['fecha']),
            row['moneda'],
            row['cotizacion'],
            row['volumen'],
        )
        cursor.execute(query, data_moneda)

    # Commit changes
    cnx.commit()

    # Close cursor
    cursor.close()

def get_secret():

    secret_name = os.environ['SECRET_NAME']
    region_name = os.environ['REGION_NAME']

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )
    get_secret_value_response = client.get_secret_value(
        SecretId=secret_name
    )
    
    secret = get_secret_value_response['SecretString']
    return json.loads(secret)
    
def connect_to_mysql(attempts: int = 3,
                     delay: int = 2
                     ) -> Optional[mysql.connector.connection.MySQLConnection]:
    """
    Function to connect to MySQL database.

    Args:
        attempts (int): Number of connection attempts. Default is 3.
        delay (int): Delay between connection attempts in seconds. Default is 2.

    Returns:
        Optional[mysql.connector.connection.MySQLConnection]: MySQL connection object or None if connection fails.
    """
    secret: Dict = get_secret()
    # MySQL database configuration
    config = {
        'user': secret.get("username"),
        'password': secret.get("password"),
        'host': secret.get("host"),
        'database': secret.get("dbname"),
        'port': secret.get("port"),
        'raise_on_warnings': True
    }

    attempt = 1

    # Implement a reconnection routine
    while attempt < attempts + 1:
        try:
            # Try to establish the connection
            logger.info(f"Trying to connect to MySQL, attempt {attempt}")
            cnx = mysql.connector.connect(**config)
            logger.info(f"Connected to MySQL!")
            return cnx
        except (mysql.connector.Error, IOError) as err:
            if attempt == attempts:
                # Attempts to reconnect failed; returning None
                logger.info("Failed to connect, exiting without a connection: %s", err)
                return None
            logger.info(
                f"Connection failed: {err}. Retrying ({attempt}/{attemps})..."
            )
            # Progressive reconnect delay
            time.sleep(delay ** attempt)
            attempt += 1
    return None

def save_in_bucket(merged_data: pd.DataFrame) -> None:
    """
    Function to save DataFrame in an S3 bucket.

    Args:
        merged_data (pd.DataFrame): DataFrame to be saved in the bucket.
    """
    # Get the bucket name
    bucket_name: str = os.environ['BUCKET_DEST']

    # Export the DataFrame to a BytesIO object for S3 upload
    csv_buffer = io.StringIO()
    merged_data.to_csv(csv_buffer, index=False)
    csv_buffer.seek(0)

    # Initialize S3 client
    s3_client = boto3.client('s3')

    # Upload the CSV file to S3
    logger.info(f'Trying to upload .csv to {bucket_name}')
    s3_client.put_object(Body=csv_buffer.getvalue(), Bucket=bucket_name, Key=f"{(datetime.now()-timedelta(days=1)).strftime('%Y-%m-%d')}.csv")
    logger.info(f'CSV uploaded to {bucket_name} successfully')

def get_market_value() -> pd.DataFrame:
    """
    Function to get market values for a list of tickers.

    Returns:
        pd.DataFrame: DataFrame containing market values for all tickers.
    """
    # List of tickers
    tickers: List[str] = ['BTC-USD', 'ETH-USD', '^GSPC', '^IXIC', 'GC=F']

    # Dictionary to store DataFrames for each currency
    dataframes_por_moneda: Dict[str, pd.DataFrame] = {}

    # Getting data for each ticker
    for moneda in tickers:
        # Getting data for the ticker
        logger.info(f'Getting {moneda} data')
        data_moneda: pd.DataFrame = yf.download(moneda, period='1d', interval='1d')
        logger.info(f'Downloaded {moneda} data')

        # Selecting relevant columns
        data_moneda = data_moneda[['Adj Close', 'Volume']].copy()

        # Renaming columns
        data_moneda.columns = ['cotizacion', 'volumen']

        # Adding date column
        data_moneda['fecha'] = data_moneda.index

        # Adding currency name
        data_moneda['moneda'] = moneda

        # Reordering columns
        data_moneda = data_moneda[['fecha', 'cotizacion', 'volumen', 'moneda']]

        # Adding DataFrame to dictionary
        dataframes_por_moneda[moneda] = data_moneda

    dataframes_lista: List[pd.DataFrame] = list(dataframes_por_moneda.values())

    # Merging DataFrames into one
    merged_data: pd.DataFrame = pd.concat(dataframes_lista, ignore_index=True)

    return merged_data

def lambda_handler(event: Dict[str, Any],
                   context: Any
                   ) -> Dict[str, Union[int, str]]:
    """
    Lambda function handler. This function executes a series of actions, such as
    getting market data, saving it to a bucket, and then uploading it to a MySQL database.

    Args:
        event (Dict[str, Any]): The input event for the Lambda function.
        context (Any): The context for the Lambda function.

    Returns:
        Dict[str, Union[int, str]]: A dictionary containing the status code and a message.

    Raises:
        Exception: If any error occurs during the execution of the actions.
    """
    try:
        data = get_market_value()   # Get market data
        save_in_bucket(data)        # Save data to a bucket
        cnx = connect_to_mysql()    # Upload to the database
        if cnx is not None:
            insert_data(cnx, data)  # Insert data to MySQL
            cnx.close()             # Close the connection
            return {
                'statusCode': 200,
                'body': json.dumps('Data scrapped successfully!')
            }
    except Exception as e:
        raise e
