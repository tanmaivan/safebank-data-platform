import os
import sys
import logging
from utils.spark_connector import create_spark_session

# add current directory to sys.path
curr_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(curr_dir)

# logging configuration
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


def main():
    logging.info(">>> [TEST] Starting Spark environment verification...")

    # initialize SparkSession
    try:
        spark = create_spark_session("check_env")
    except Exception as e:
        logging.info(f">>> [ERROR] Failed to create SparkSession: {e}")
        sys.exit(1)

    # create mock data
    data = [("Alice", 1000), ("Bob", 2000), ("Charlie", 3000)]
    columns = ["name", "amount"]
    df = spark.createDataFrame(data, columns)

    logging.info(">>> [TEST] Sample DataFrame created successfully.")
    df.show()

    # test write to minio
    output_path = "s3a://test-connection"

    logging.info(f">>> [TEST] Attempting to write to MinIO at: {output_path}")

    try:
        df.write.format("delta").mode("overwrite").save(output_path)
        logging.info(">>> [TEST] Write successful!")
    except Exception as e:
        logging.error(f"Failed to write to MinIO: {e}")
        spark.stop()
        sys.exit(1)

    # test read from minio
    logging.info(">>> [TEST] Attempting to read back from MinIO...")

    try:
        df_read = spark.read.format("delta").load(output_path)
        row_count = df_read.count()
        logging.info(f">>> [TEST] Read successful! Total rows: {row_count}")
    except Exception as e:
        logging.error(f">>> [ERROR] Failed to read from MinIO: {e}")
        spark.stop()
        sys.exit(1)

    logging.info(">>> [SUCCESS] Spark - MinIO - delta environment is ready.")
    spark.stop()


if __name__ == "__main__":
    main()
