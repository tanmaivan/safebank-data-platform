# =========================================================
# VARIABLES
# =========================================================
DB_CONTAINER = sb-postgres
DB_USER = safebank
DB_NAME = safebank
DEBEZIUM_URL = http://localhost:8083/connectors/
SPARK_MASTER = sb-spark-master

# Spark Packages: Delta Lake, Hadoop AWS (S3), Kafka
SPARK_PACKAGES = "io.delta:delta-spark_2.12:3.0.0,org.apache.hadoop:hadoop-aws:3.3.4,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.7,com.amazonaws:aws-java-sdk-bundle:1.12.262,org.postgresql:postgresql:42.6.0"

# Standard Spark Submit Command (DRY Principle)
SPARK_SUBMIT = docker exec -it $(SPARK_MASTER) /opt/spark/bin/spark-submit \
    --master spark://$(SPARK_MASTER):7077 \
    --driver-memory 2g \
    --executor-memory 2g \
    --executor-cores 1 \
    --packages $(SPARK_PACKAGES)

# =========================================================
# HELPERS
# =========================================================
help:
	@echo ""
	@echo "  THE PIPELINE CLI"
	@echo ""
	@echo "  +------------------------+------------------------------------------------------+"
	@printf "  | %-22s | %-52s |\n" "Command" "Description"
	@echo "  +------------------------+------------------------------------------------------+"
	@printf "  | %-22s | %-52s |\n" "INFRASTRUCTURE" ""
	@echo "  +------------------------+------------------------------------------------------+"
	@printf "  | %-22s | %-52s |\n" "make up" "Start all services (Docker Compose)"
	@printf "  | %-22s | %-52s |\n" "make down" "Stop and remove all services"
	@echo "  +------------------------+------------------------------------------------------+"
	@printf "  | %-22s | %-52s |\n" "DATABASE & KAFKA" ""
	@echo "  +------------------------+------------------------------------------------------+"
	@printf "  | %-22s | %-52s |\n" "make db-init" "Init Postgres Schema (LDBC Tables)"
	@printf "  | %-22s | %-52s |\n" "make db-ls" "List tables in Postgres"
	@printf "  | %-22s | %-52s |\n" "make conn-reg" "Register Debezium Connector"
	@printf "  | %-22s | %-52s |\n" "make conn-ls" "Check active connectors"
	@echo "  +------------------------+------------------------------------------------------+"
	@printf "  | %-22s | %-52s |\n" "ETL PIPELINES" ""
	@echo "  +------------------------+------------------------------------------------------+"
	@printf "  | %-22s | %-52s |\n" "make data-gen" "Run Python Data Generator"
	@printf "  | %-22s | %-52s |\n" "make spark-test" "Test Spark environment"
	@printf "  | %-22s | %-52s |\n" "make etl" "Run full pipeline (bronze -> silver -> gold)"
	@printf "  | %-22s | %-52s |\n" "make bronze" " 1. Ingest Kafka -> MinIO"
	@printf "  | %-22s | %-52s |\n" "make silver" " 2. Process Silver (Dims -> Facts)"
	@printf "  | %-22s | %-52s |\n" "make gold" " 3. Build All Gold Marts (a->e)"
	@printf "  | %-22s | %-52s |\n" "make gold-finance" "  - 3.a. Financial Mart"
	@printf "  | %-22s | %-52s |\n" "make gold-loan" "  - 3.b. Loan Risk Mart"
	@printf "  | %-22s | %-52s |\n" "make gold-security" "  - 3.c. Security Mart"
	@printf "  | %-22s | %-52s |\n" "make gold-merchant" "  - 3.d. Merchant Consumption Mart"
	@printf "  | %-22s | %-52s |\n" "make gold-cust360" "  - 3.e. Customer 360 Mart"
	@echo "  +------------------------+------------------------------------------------------+"
	@echo ""

# =========================================================
# INFRASTRUCTURE
# =========================================================
up:
	docker compose up -d

down:
	docker compose down

clean:
	docker compose down -v

# =========================================================
# DATABASE AND CONNECTORS
# =========================================================
db-init:
	@echo "Creating tables in Postgres..."
	cat scripts/sql/core_schema.sql | docker exec -i $(DB_CONTAINER) psql -U $(DB_USER) -d $(DB_NAME)
	@echo "Database initialized successfully!"

db-ls:
	@echo "Listing tables in Postgres..."
	docker exec -it $(DB_CONTAINER) psql -U $(DB_USER) -d $(DB_NAME) -c "\dt"

conn-reg:
	@echo "Registering Debezium connector..."
	curl -i -X POST -H "Accept:application/json" -H "Content-Type:application/json" \
	$(DEBEZIUM_URL) -d @scripts/connectors/safebank_connector.json
	@echo "\nConnector registered!"

conn-ls:
	curl -s $(DEBEZIUM_URL) | jq . || curl -s $(DEBEZIUM_URL)

# =========================================================
# DATA GENERATION
# =========================================================
data-gen:
	@echo "Starting Real-time Data Simulation..."
	python scripts/datagen/gen_data.py

# =========================================================
# SPARK JOB AND UTILS
# =========================================================
spark-test:
	@echo "Submitting Spark Job to check environment..."
	$(SPARK_SUBMIT) /opt/spark/scripts/spark/check_env.py

bronze:
	@echo "Submitting Bronze ingestion job..."
	$(SPARK_SUBMIT) /opt/spark/scripts/spark/jobs/bronze/ingest_all.py

silver-dim:
	@echo "Submitting Silver Dimensions processing job..."
	$(SPARK_SUBMIT) /opt/spark/scripts/spark/jobs/silver/process_dimensions.py

silver-fact:
	@echo "Submitting Silver Facts processing job..."
	$(SPARK_SUBMIT) /opt/spark/scripts/spark/jobs/silver/process_facts.py

silver:
	@echo "--- [1/2] Processing Silver Dimensions ---"
	$(MAKE) silver-dim
	@echo "--- [2/2] Processing Silver Facts ---"
	$(MAKE) silver-fact

gold-finance:
	@echo "Submitting Gold Financial Mart..."
	$(SPARK_SUBMIT) /opt/spark/scripts/spark/jobs/gold/daily_financial_report.py

gold-loan:
	@echo "Submitting Gold Loan Risk Mart..."
	$(SPARK_SUBMIT) /opt/spark/scripts/spark/jobs/gold/loan_risk_analysis.py

gold-security:
	@echo "Submitting Gold Security Mart..."
	$(SPARK_SUBMIT) /opt/spark/scripts/spark/jobs/gold/security_risk_analysis.py

gold-merchant:
	@echo "Submitting Gold Merchant Consumption Mart..."
	$(SPARK_SUBMIT) /opt/spark/scripts/spark/jobs/gold/merchant_spending_analysis.py

gold-cust360:
	@echo "Submitting Gold Customer 360 Mart..."
	$(SPARK_SUBMIT) /opt/spark/scripts/spark/jobs/gold/customer_360_view.py

gold:
	@echo "Building All Gold Marts"
	$(MAKE) gold-finance
	$(MAKE) gold-loan
	$(MAKE) gold-security
	$(MAKE) gold-merchant
	$(MAKE) gold-cust360
	@echo "Gold Layer Completed"

# =========================================================
# END-TO-END PIPELINE
# =========================================================
etl:
	@echo "==================================================="
	@echo "STARTING FULL END-TO-END PIPELINE"
	@echo "==================================================="
	@echo ""
	@echo ">>> [STEP 1/3] Running Bronze Layer (Ingestion)..."
	$(MAKE) bronze
	@echo ""
	@echo ">>> [STEP 2/3] Running Silver Layer (Transformation)..."
	$(MAKE) silver
	@echo ""
	@echo ">>> [STEP 3/3] Running Gold Layer (Data Marts)..."
	$(MAKE) gold
	@echo ""
	@echo "==================================================="
	@echo "PIPELINE COMPLETED SUCCESSFULLY"
	@echo "==================================================="
