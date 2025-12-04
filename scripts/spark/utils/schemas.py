# from pyspark.sql.types import *
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)


# ========================================================
# 1. DEBEZIUM ENVELOP
# ========================================================
def get_debezium_schema(table_schema: StructType) -> StructType:
    """
    Wrap the specific table schema into the Debezium JSON payload structure.
    Structure: payload -> {before, after, source, op, ts_ms}
    """
    return StructType(
        [
            StructField(
                "payload",
                StructType(
                    [
                        StructField("after", table_schema, True),
                        StructField("before", table_schema, True),
                        StructField("op", StringType(), True),
                        StructField("ts_ms", LongType(), True),
                    ]
                ),
                True,
            )
        ]
    )


# ========================================================
# 2. DIMENSIONS - STATIC DATA
# ========================================================
branch_schema = StructType(
    [
        StructField("id", IntegerType(), True),
        StructField("branch_name", StringType(), True),
        StructField("city", StringType(), True),
        StructField("open_date", IntegerType(), True),
    ]
)

currency_schema = StructType(
    [StructField("code", StringType(), True), StructField("name", StringType(), True)]
)

channel_schema = StructType(
    [
        StructField("id", IntegerType(), True),
        StructField("channel_code", StringType(), True),
        StructField("description", StringType(), True),
    ]
)

merchant_schema = StructType(
    [
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("category", StringType(), True),
    ]
)

device_schema = StructType(
    [
        StructField("id", IntegerType(), True),
        StructField("device_fingerprint", StringType(), True),
        StructField("device_model", StringType(), True),
        StructField("os_version", StringType(), True),
        StructField("is_trusted", BooleanType(), True),
    ]
)


# ========================================================
# 3. CORE ENTITIES
# ========================================================
person_schema = StructType(
    [
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("gender", StringType(), True),
        StructField("birthday", IntegerType(), True),
        StructField("country", StringType(), True),
        StructField("city", StringType(), True),
        StructField("is_blocked", BooleanType(), True),
        StructField("create_time", LongType(), True),
        StructField("update_time", LongType(), True),
    ]
)

account_schema = StructType(
    [
        StructField("id", IntegerType(), True),
        StructField("owner_id", IntegerType(), True),
        StructField("branch_id", IntegerType(), True),
        StructField("account_type", StringType(), True),
        StructField("currency_code", StringType(), True),
        StructField("balance", DoubleType(), True),
        StructField("nickname", StringType(), True),
        StructField("phone_number", StringType(), True),
        StructField("email", StringType(), True),
        StructField("create_time", LongType(), True),
        StructField("update_time", LongType(), True),
        StructField("is_blocked", BooleanType(), True),
        StructField("account_level", StringType(), True),
    ]
)

loan_account_schema = StructType(
    [
        StructField("id", IntegerType(), True),
        StructField("account_id", IntegerType(), True),
        StructField("currency_code", StringType(), True),
        StructField("amount", DoubleType(), True),
        StructField("interest_rate", DoubleType(), True),
        StructField("term_months", IntegerType(), True),
        StructField("start_date", IntegerType(), True),
        StructField("end_date", IntegerType(), True),
        StructField("status", StringType(), True),
        StructField("remaining_balance", DoubleType(), True),
    ]
)


# ========================================================
# 4. FACTS / EVENTS
# ========================================================
exchange_rate_schema = StructType(
    [
        StructField("id", IntegerType(), True),
        StructField("from_currency", StringType(), True),
        StructField("to_currency", StringType(), True),
        StructField("rate", DoubleType(), True),
        StructField("effective_date", IntegerType(), True),
    ]
)

transfer_schema = StructType(
    [
        StructField("id", IntegerType(), True),
        StructField("txn_time", LongType(), True),
        StructField("create_time", LongType(), True),
        StructField("from_account_id", IntegerType(), True),
        StructField("to_account_id", IntegerType(), True),
        StructField("merchant_id", IntegerType(), True),
        StructField("amount", DoubleType(), True),
        StructField("currency_code", StringType(), True),
        StructField("channel_id", IntegerType(), True),
        StructField("comment", StringType(), True),
        StructField("status", StringType(), True),
    ]
)

sign_in_schema = StructType(
    [
        StructField("id", IntegerType(), True),
        StructField("account_id", IntegerType(), True),
        StructField("device_id", IntegerType(), True),
        StructField("sign_in_time", LongType(), True),
        StructField("ip_address", StringType(), True),
        StructField("location_city", StringType(), True),
        StructField("status", StringType(), True),
    ]
)

loan_payment_schema = StructType(
    [
        StructField("id", IntegerType(), True),
        StructField("loan_id", IntegerType(), True),
        StructField("amount", DoubleType(), True),
        StructField("payment_date", LongType(), True),
        StructField("late_days", IntegerType(), True),
    ]
)
