from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

dag = DAG(
    "safebank_etl_pipeline",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    schedule="0 8 * * *",
    description="End-to-end Banking Data Pipeline.",
)


def create_spark_task(task_id, script_path):
    return BashOperator(
        task_id=task_id,
        bash_command=f"""
            docker exec sb-spark-master /opt/spark/bin/spark-submit \
            --master spark://sb-spark-master:7077 \
            --packages "io.delta:delta-spark_2.12:3.0.0,org.apache.hadoop:hadoop-aws:3.3.4,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,com.amazonaws:aws-java-sdk-bundle:1.12.262,org.postgresql:postgresql:42.6.0" \
            --driver-memory 2g \
            --executor-memory 2g \
            {script_path}
        """,
        dag=dag,
    )


register_connector = BashOperator(
    task_id="register_connector",
    bash_command="""
    if curl -s -f http://sb-debezium:8083/connectors/safebank-connector; then
        echo "Connector already exists. Skipping registration."
    else
        echo "Connector not found. Registering new one..."
        curl -i -X POST -H "Accept:application/json" -H "Content-Type:application/json" \
        http://sb-debezium:8083/connectors/ -d @/opt/airflow/scripts/connectors/safebank_connector.json
    fi
    """,
    dag=dag,
)

# bronze layer
ingest_bronze = create_spark_task(
    task_id="ingest_bronze",
    script_path="/opt/spark/scripts/spark/jobs/bronze/ingest_all.py",
)

# silver layer
process_silver_dim = create_spark_task(
    task_id="process_silver_dim",
    script_path="/opt/spark/scripts/spark/jobs/silver/process_dimensions.py",
)

process_silver_fact = create_spark_task(
    task_id="process_silver_fact",
    script_path="/opt/spark/scripts/spark/jobs/silver/process_facts.py",
)

# gold layer
gold_financial = create_spark_task(
    "gold_financial", "/opt/spark/scripts/spark/jobs/gold/daily_financial_report.py"
)
gold_loan = create_spark_task(
    "gold_loan", "/opt/spark/scripts/spark/jobs/gold/loan_risk_analysis.py"
)
gold_security = create_spark_task(
    "gold_security", "/opt/spark/scripts/spark/jobs/gold/security_risk_analysis.py"
)
gold_merchant = create_spark_task(
    "gold_merchant", "/opt/spark/scripts/spark/jobs/gold/merchant_spending_analysis.py"
)
gold_cust360 = create_spark_task(
    "gold_cust360", "/opt/spark/scripts/spark/jobs/gold/customer_360_view.py"
)

# dependencies
register_connector >> ingest_bronze >> process_silver_dim >> process_silver_fact
process_silver_fact >> [
    gold_financial,
    gold_loan,
    gold_security,
    gold_merchant,
    gold_cust360,
]
