from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# Default arguments for the DAG
default_args = {
    'owner': 'DUC',
    'depends_on_past': False,
    'start_date': datetime(2026, 2, 26),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
with DAG(
    'lakehouse_daily_pipeline',
    default_args=default_args,
    description='A simple DAG to run the Lakehouse pipeline',
    schedule_interval='0 0 * * *', # Run daily at midnight
    catchup=False,
    tags=['lakehouse', 'bronze'],
) as dag:

    # Task to run the main pipeline script (Bronze Ingestion)
    run_bronze_ingestion = BashOperator(
        task_id='run_main_pipeline',
        bash_command='python /opt/airflow/app/main.py',
    )
    
    # Execute Spark job using docker exec. 
    # Since Airflow runs in its own container and 'docker.sock' is mounted, 
    # we can call docker exec to run spark-submit in the spark-master-2 container.
    SILVER_CMD = """
    docker exec -e HADOOP_USER_NAME=bitnami spark-master-2 spark-submit \
      --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,io.delta:delta-spark_2.12:3.1.0 \
      --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
      --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
      /opt/bitnami/spark/app/transform_stock.py
    """
    
    run_silver_transformation = BashOperator(
        task_id='run_silver_transformation',
        bash_command=SILVER_CMD,
    )
    
    GOLD_CMD = """
    docker exec -e HADOOP_USER_NAME=bitnami spark-master-2 spark-submit \
      --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,io.delta:delta-spark_2.12:3.1.0 \
      --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
      --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
      /opt/bitnami/spark/app/transform_gold.py
    """
    
    run_gold_transformation = BashOperator(
        task_id='run_gold_transformation',
        bash_command=GOLD_CMD,
    )

    # Set Dependencies
    run_bronze_ingestion >> run_silver_transformation >> run_gold_transformation

