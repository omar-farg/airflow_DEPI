from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import random
from airflow.operators.bash import BashOperator

output = "/opt/airflow/shared/tmp/random.txt"


def random_num():
    num = random.randint(1, 100)
    print(num)
    with open(output, "w") as f:
        f.write(str(num))


with DAG(
    "Airflow_Depi",
    start_date=datetime(2025, 10, 2),
    schedule_interval=timedelta(minutes=1),
    catchup=False,
) as dag:
    print_date = BashOperator(task_id="print_date", bash_command="date")

    welcome = PythonOperator(
        task_id="Welcome", python_callable=lambda: print("Welcome Omar")
    )

    random_number = PythonOperator(task_id="random_number", python_callable=random_num)

print_date >> welcome >> random_number
