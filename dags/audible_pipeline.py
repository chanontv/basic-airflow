from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.utils.dates import days_ago
import pandas as pd
import requests

MYSQL_CONNECTION = "mysql_default"   #connection id name in Airflow
CONVERSION_RATE_URL = "https://r2de2-workshop-vmftiryt6q-ts.a.run.app/usd_thb_conversion_rate"

#path 
mysql_output_path = "/home/airflow/data/audible_data_merged.csv"
conversion_rate_output_path = "/home/airflow/data/conversion_rate.csv"
final_output_path = "/home/airflow/data/output.csv"

#For PythonOperator

def get_data_from_mysql(transaction_path):

    #use MySqlHook connect to MySQL from connection in Airflow
    mysqlserver = MySqlHook(MYSQL_CONNECTION)
    
    #query from database 
    audible_data = mysqlserver.get_pandas_df(sql="SELECT * FROM audible_data")
    audible_transaction = mysqlserver.get_pandas_df(sql="SELECT * FROM audible_transaction")

    #merge data 
    df = audible_transaction.merge(audible_data, how="left", left_on="book_id", right_on="Book_ID")

    #save to CSV 
    df.to_csv(transaction_path, index=False)
    print(f"Output to {transaction_path}")


def get_conversion_rate(conversion_rate_path):
    r = requests.get(CONVERSION_RATE_URL)
    result_conversion_rate = r.json()
    df = pd.DataFrame(result_conversion_rate)
    df = df.reset_index().rename(columns={"index": "date"})
    df.to_csv(conversion_rate_path, index=False)
    print(f"Output to {conversion_rate_path}")


def merge_data(transaction_path, conversion_rate_path, output_path):
    transaction = pd.read_csv(transaction_path)
    conversion_rate = pd.read_csv(conversion_rate_path)
    transaction['date'] = transaction['timestamp']
    transaction['date'] = pd.to_datetime(transaction['date']).dt.date
    conversion_rate['date'] = pd.to_datetime(conversion_rate['date']).dt.date

    #merge 2 DataFrame
    final_df = transaction.merge(conversion_rate, how="left", left_on="date", right_on="date")
    
    #remove "$" from column price & change type to float
    final_df["Price"] = final_df.apply(lambda x: x["Price"].replace("$",""), axis=1)
    final_df["Price"] = final_df["Price"].astype(float)
    
    final_df["THBPrice"] = final_df["Price"] * final_df["conversion_rate"]
    final_df = final_df.drop(["date", "book_id"], axis=1)

    #save to CSV
    final_df.to_csv(output_path, index=False)
    print(f"Output to {output_path}")

#create DAG

with DAG(
    "audible_pipeline",
    start_date=days_ago(1),
    description='Pipeline for ETL audible_pipeline',
    schedule_interval="@once",
    tags=["workshop"]
) as dag:

    #tasks

    t1 = PythonOperator(
        task_id="db_ingest",
        python_callable=get_data_from_mysql,
        op_kwargs={
            "transaction_path": mysql_output_path,
        },
    )

    t2 = PythonOperator(
        task_id="api_call",
        python_callable=get_conversion_rate,
        op_kwargs={
            "conversion_rate_path": conversion_rate_output_path,
        },
    )

    t3 = PythonOperator(
        task_id="merge_data",
        python_callable=merge_data,
        op_kwargs={
            "transaction_path": mysql_output_path,
            "conversion_rate_path": conversion_rate_output_path,
            "output_path" : final_output_path,
        },
    )

    #dependencie

    [t1, t2] >> t3
