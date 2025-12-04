import logging
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, row_number
from pyspark.sql.window import Window
from delta.tables import DeltaTable

# logging configuration
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger(__name__)


def deduplicate_micro_batch(df: DataFrame, unique_keys: list) -> DataFrame:
    """
    Keeps only the latest record per unique key within the micro-batch.
    """
    return (
        df.withColumn(
            "rank",
            row_number().over(
                Window.partitionBy(*unique_keys).orderBy(col("cdc_timestamp").desc())
            ),
        )
        .filter(col("rank") == 1)
        .drop("rank")
    )


def upsert_scd1(
    micro_batch_df: DataFrame, batch_id: int, target_path: str, unique_keys: list
):
    """
    Perform SCD Type 1: Upsert/Merge - Update existing, insert new.
    No history tracking.
    """

    if micro_batch_df.count() == 0:
        return

    # deduplicate the batch
    deduped_df = deduplicate_micro_batch(micro_batch_df, unique_keys)

    # check if delta table exists on first run
    if not DeltaTable.isDeltaTable(micro_batch_df.sparkSession, target_path):
        logger.info(
            f"The target table at {target_path} does not exist. Initializing..."
        )
        deduped_df.write.format("delta").mode("append").save(target_path)

        return

    target_table = DeltaTable.forPath(micro_batch_df.sparkSession, target_path)

    # the join condition string
    join_condition = " AND ".join([f"target.{k} = source.{k}" for k in unique_keys])

    # execute merge
    target_table.alias("target").merge(
        deduped_df.alias("source"), join_condition
    ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

    logger.info(f"Batch {batch_id} processed (SCD1) successfully for {target_path}.")


def upsert_scd2(
    micro_batch_df: DataFrame, batch_id: int, target_path: str, unique_keys: list
):
    """
    Perform SCD Type 2 Merge Operation.

    Function Description:
    - Input Data: [A_new]

    - Split Input:
        * [A_new_with_key]: records matching existing keys in the target
        * [A_new_no_key]: records with new keys not in the target

    - Delta Merge Logic:
        * For [A_new_with_key] matching [A_old] in the warehouse -> update [A_old] as historical (close it)
        * For [A_new_no_key] not matching any existing record -> insert [A_new] as current

    - Result in Warehouse:
        * Contains both [A_old] (closed) and [A_new] (current)
        * Fully conforms to SCD Type 2 standard
    """

    if micro_batch_df.count() == 0:
        return

    # deduplicate the batch
    deduped_df = deduplicate_micro_batch(micro_batch_df, unique_keys)

    # add columns for SCD Type 2 logic:
    # - is_current: marks the record as current
    # - end_time: null because the record is still active
    # - effective_time: converts cdc_timestamp from milliseconds to timestamp
    staged_df = (
        deduped_df.withColumn("is_current", lit(True))
        .withColumn("end_time", lit(None).cast("timestamp"))
        .withColumn("effective_time", (col("cdc_timestamp") / 1000).cast("timestamp"))
    )

    # check if delta table exists on first run
    if not DeltaTable.isDeltaTable(micro_batch_df.sparkSession, target_path):
        logger.info(
            f"The target table at {target_path} does not exist. Initializing..."
        )
        staged_df.write.format("delta").mode("append").save(target_path)

    # THE MERGE LOGIC
    target_table = DeltaTable.forPath(micro_batch_df.sparkSession, target_path)

    # 1. construct the union source
    # the join condition string
    join_condition = " AND ".join([f"target.{k} = source.{k}" for k in unique_keys])

    # - create a mergeKey column:
    # -- update rows? -> mergeKey = original key
    # -- insert rows? mergeKey = null
    key_col = unique_keys[0]
    updates_df = staged_df.withColumn("mergeKey", col(key_col))
    inserts_df = staged_df.withColumn("mergeKey", lit(None))

    # - combine them
    source_df = updates_df.unionByName(inserts_df)

    # 2. execute merge
    target_table.alias("target").merge(
        source_df.alias("source"),
        f"target.{key_col} = source.mergeKey AND target.is_current = True",
    ).whenMatchedUpdate(
        condition=f"{join_condition} AND source.effective_time > target.effective_time",
        set={"is_current": lit(False), "end_time": col("source.effective_time")},
    ).whenNotMatchedInsert(
        condition="source.mergeKey is NULL",
        values={
            **{c: col(f"source.{c}") for c in staged_df.columns},
            "is_current": lit(True),
            "end_time": lit(None),
        },
    ).execute()

    logger.info(f"Batch {batch_id} processed (SCD 2) successfully for {target_path}.")
