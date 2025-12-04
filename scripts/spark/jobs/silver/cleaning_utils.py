from pyspark.sql.functions import (
    abs,
    coalesce,
    col,
    count,
    current_date,
    expr,
    initcap,
    length,
    lit,
    regexp_replace,
    trim,
    upper,
    when,
    year,
)


def check_nulls(df):
    """
    Calculates the number of null values per columns.
    """
    df.select([count(when(col(c).isNull(), c)).alias(c) for c in df.columns]).show()


def clean_text(col_name: str):
    """
    This function trims whitespace and converts to title case.
    """
    return initcap(trim(col(col_name)))


def clean_gender(col_name: str):
    """
    Normalizes gender values to specific categories.
    """
    c = upper(trim(col(col_name)))

    return (
        when(c.isin("M", "MALE"), "Male")
        .when(c.isin("F", "FEMALE"), "Female")
        .otherwise("Unknown")
    )


def handle_null_string(col_name: str, default_val: str = "Unknown"):
    """
    Fills NULL values with a default string.
    """
    return coalesce(clean_text(col_name), lit(default_val))


def clean_money(col_name: str):
    """
    Ensures monetary values are non-negative.
    """
    return abs(col(col_name))


def convert_validate_birthday(col_name: str):
    """
    Converts Debezium integer date offsets to actual dates and nulls out values outside the valid year range.
    """
    base = expr(f"date_add('1970-01-01', cast({col_name} as int))")
    curr_year = year(current_date())
    birth_year = year(base)

    return when((birth_year >= 1920) & (birth_year <= curr_year), base).otherwise(
        lit(None)
    )


def clean_phone(col_name: str):
    """
    Removes non-digit characters using Regex.
    """
    digits = regexp_replace(col(col_name), "[^0-9]", "")

    return when((length(digits) >= 9) & (length(digits) <= 12), digits).otherwise(
        lit(None)
    )


def normalize_account_types(col_name: str):
    """
    Map messy types to standard categories.
    """
    c = upper(trim(col(col_name)))

    return (
        when(c.rlike("CHECK|CHK"), "CHECKING")
        .when(c.rlike("SAVING|SAV"), "SAVING")
        .when(c.rlike("VIP|V\\.I\\.P"), "VIP")
        .when(c.rlike("BUSINESS|BUSSINESS"), "BUSINESS")
        .otherwise("UNKNOWN")
    )


def clean_timestamp(col_name: str):
    """
    Convert Debezium timestamp to Spark timestamp.
    Postgres Debezium sends microseconds, Spark uses seconds/milliseconds.
    """
    return (col(col_name) / 1000000).cast("timestamp")


def clean_device_version(col_name: str):
    """
    Cleans Postgres Array string format.
    Example: '{"iOS 26"}' -> 'iOS 26'
    Removes: {, }, "
    """
    return regexp_replace(col(col_name), '[\\{\\"\\}]', "")
