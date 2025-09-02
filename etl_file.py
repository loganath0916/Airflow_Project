import os
import pymysql
import pandas as ps
from datetime import datetime

def fetch_data_from_mysql():

        mysql_config = {
                        'host' : 'localhost',
                        'user' : 'airflowuser',
                        'password' : 'airflowpass',
                        'database' : 'airflow_Pro'
                         }

        connection = pymysql.connect(**mysql_config)
        query = "select * from employees"
        df = ps.read_sql(query, connection)
        connection.close()
        return df

def ETL_data(df):
        ETL_df = df[df['AGE'] > 30]
        return ETL_df

def write_data_to_file(df):
        output_dir = "/home/loganath/extract"
        os.makedirs(output_dir, exist_ok=True)
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        file_name = f"etl_output_{timestamp}.csv"
        path_name = os.path.join(output_dir,file_name)
        df.to_csv(path_name, index=False)
        print(f"DATA WRITTEN TO {path_name}")

def etl_process():
        df =  fetch_data_from_mysql()
        ETL_df = ETL_data(df)
        write_data_to_file(ETL_df)

if __name__ == "__main__":
        etl_process()