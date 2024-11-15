from airflow import DAG
# import sys
# import os
# sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from airflow.operators.python_operator import PythonOperator
from utils.product_crawler import id_crawler, info_crawler
from utils.visualization import plot_price_hist, plot_discount

default_args = {
    "email": ['truongphat02092003@gmail.com'],
    "email_on_failure": True,
}

dag = DAG('tiki-pipeline', default_args=default_args, schedule_interval=None, catchup=False)

t1 = PythonOperator(
    task_id="product_id_crawling",
    python_callable=id_crawler,
    dag=dag,
)

t2 = PythonOperator(
    task_id="product_info_crawling",
    python_callable=info_crawler,
    # op_kwargs={"df_id": "{{ ti.xcom_pull(task_ids='product_id_crawling') }}"},
    dag=dag,
)

t3 = PythonOperator(
    task_id='plot_price_distribution',
    python_callable=plot_price_hist,
    # op_kwargs={"df_product": "{{ ti.xcom_pull(task_ids='product_info_crawling') }}"},
    dag=dag,
)

t4 = PythonOperator(
    task_id='find_top5_discount_product',
    python_callable=plot_discount,
    # op_kwargs={"df_product": "{{ ti.xcom_pull(task_ids='product_info_crawling') }}"},
    dag=dag,
)

# Set task dependencies
t1 >> t2 >> [t3, t4]