import logging
from pyspark.sql import SparkSession

# Setup logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)


def create_spark_session(app_name: str) -> SparkSession:
    """
    Initializes a SparkSession with necessary JARs and Configurations
    for Kafka, Delta Lake, and MinIO (S3).
    """
    logging.info(f"Initializing SparkSession for: {app_name}")

    # define maven coordinates (jars)
    packages = [
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.7",  # Kafka Connector
        "io.delta:delta-spark_2.12:3.0.0",  # Delta Lake Core
        "org.apache.hadoop:hadoop-aws:3.3.4",  # AWS S3 Support
        "com.amazonaws:aws-java-sdk-bundle:1.12.262",  # AWS SDK
    ]

    # configure spark
    spark = (
        SparkSession.builder.appName(app_name)
        .master("spark://spark-master:7077")
        .config("spark.jars.packages", ",".join(packages))
        .config("spark.scheduler.mode", "FAIR")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.access.key", "admin")
        .config("spark.hadoop.fs.s3a.secret.key", "password")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")
    logging.info("SparkSession created successfully.")

    return spark
