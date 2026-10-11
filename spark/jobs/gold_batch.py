from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


# Configuration

SILVER_DATABASE = "fraud_silver"
SILVER_TABLE = "transactions"
SILVER_PATH = "hdfs://namenode:8020/lakehouse/silver/transactions"

GOLD_DATABASE = "fraud_gold"
GOLD_BASE_PATH = "hdfs://namenode:8020/lakehouse/gold"
WAREHOUSE_PATH = "hdfs://namenode:8020/lakehouse/warehouse"

STATE_EVENT_TYPES = ("INSERT", "UPDATE", "SNAPSHOT_READ")

# Catalog helpers

def register_silver_table(spark: SparkSession) -> None:
    """Register the existing Silver Delta path in Hive Metastore."""
    spark.sql(
        f"""
        CREATE DATABASE IF NOT EXISTS {SILVER_DATABASE}
        LOCATION '{WAREHOUSE_PATH}/{SILVER_DATABASE}.db'
        """
    )

    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {SILVER_DATABASE}.{SILVER_TABLE}
        USING DELTA
        LOCATION '{SILVER_PATH}'
        """
    )


def create_gold_database(spark: SparkSession) -> None:
    """Create the Gold database in the shared Hive Metastore."""
    spark.sql(
        f"""
        CREATE DATABASE IF NOT EXISTS {GOLD_DATABASE}
        LOCATION '{WAREHOUSE_PATH}/{GOLD_DATABASE}.db'
        """
    )


def write_gold_table(spark: SparkSession, dataframe: DataFrame, table_name: str,) -> None:
    """Write a DataFrame to Delta and register it in Hive Metastore."""
    table_path = f"{GOLD_BASE_PATH}/{table_name}"

    (
        dataframe.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(table_path)
    )

    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {GOLD_DATABASE}.{table_name}
        USING DELTA
        LOCATION '{table_path}'
        """
    )

    print(f"Written Gold table: {GOLD_DATABASE}.{table_name}")


# Silver preparation: CDC event log -> latest transaction state

def prepare_current_transactions(silver_events: DataFrame) -> DataFrame:
    """Reconstruct the latest valid transaction state from Silver CDC events."""
    required_columns = {
        "EventId",
        "EventType",
        "CDCOperation",
        "TransactionId",
        "KafkaTimestamp",
        "KafkaPartition",
        "KafkaOffset",
        "ProcessedAt",
        "DataQualityStatus",
        "Step",
        "TransactionType",
        "Amount",
        "NameOrig",
        "NameDest",
        "IsFraud",
        "IsFlaggedFraud",
    }

    missing_columns = sorted(required_columns - set(silver_events.columns))
    if missing_columns:
        raise ValueError(
            "Silver schema is missing required columns: "
            + ", ".join(missing_columns)
        )

    # Defensive event-level deduplication. EventId is the unique
    # Kafka topic/partition/offset identifier created by silver_stream.py.
    events = silver_events.dropDuplicates(["EventId"])

    # Events without a transaction key cannot participate in state
    # reconstruction. They remain available in Silver for investigation.
    keyed_events = events.filter(F.col("TransactionId").isNotNull())

    latest_event_window = (
        Window
        .partitionBy("TransactionId")
        .orderBy(
            F.col("KafkaTimestamp").desc_nulls_last(),
            F.col("KafkaPartition").desc_nulls_last(),
            F.col("KafkaOffset").desc_nulls_last(),
            F.col("ProcessedAt").desc_nulls_last(),
            F.col("EventId").desc(),
        )
    )

    latest_events = (
        keyed_events
        .withColumn(
            "_event_rank",
            F.row_number().over(latest_event_window),
        )
        .filter(F.col("_event_rank") == 1)
        .drop("_event_rank")
    )

    current_state = latest_events.filter(
        F.col("EventType").isin(*STATE_EVENT_TYPES)
    )

    valid_current_state = current_state.filter(
        F.col("DataQualityStatus") == "VALID"
    )

    return (
        valid_current_state
        .withColumn("Step", F.col("Step").cast("long"))
        .withColumn("Amount", F.col("Amount").cast("decimal(18,2)"))
        .withColumn("IsFraud", F.col("IsFraud").cast("boolean"))
        .withColumn(
            "IsFlaggedFraud",
            F.col("IsFlaggedFraud").cast("boolean"),
        )
    )


# Gold marts

def build_transaction_metrics(current_transactions: DataFrame) -> DataFrame:
    return (
        current_transactions
        .groupBy("Step", "TransactionType")
        .agg(
            F.count("*").alias("transaction_count"),
            F.sum("Amount").alias("total_amount"),
            F.avg("Amount").alias("average_amount"),
            F.sum(
                F.when(F.col("IsFraud") == True, 1).otherwise(0)
            ).alias("fraud_count"),
            F.sum(
                F.when(
                    F.col("IsFraud") == True,
                    F.col("Amount"),
                ).otherwise(F.lit(0).cast("decimal(18,2)"))
            ).alias("fraud_amount"),
            F.sum(
                F.when(F.col("IsFlaggedFraud") == True, 1).otherwise(0)
            ).alias("flagged_fraud_count"),
        )
        .withColumn(
            "fraud_rate_percent",
            F.round(
                F.col("fraud_count") / F.col("transaction_count") * 100,
                4,
            ),
        )
    )


def build_fraud_metrics_by_type(current_transactions: DataFrame) -> DataFrame:
    return (
        current_transactions
        .groupBy("TransactionType")
        .agg(
            F.count("*").alias("transaction_count"),
            F.sum("Amount").alias("total_amount"),
            F.sum(
                F.when(F.col("IsFraud") == True, 1).otherwise(0)
            ).alias("fraud_count"),
            F.sum(
                F.when(
                    F.col("IsFraud") == True,
                    F.col("Amount"),
                ).otherwise(F.lit(0).cast("decimal(18,2)"))
            ).alias("fraud_amount"),
            F.sum(
                F.when(F.col("IsFlaggedFraud") == True, 1).otherwise(0)
            ).alias("flagged_fraud_count"),
        )
        .withColumn(
            "fraud_rate_percent",
            F.round(
                F.col("fraud_count") / F.col("transaction_count") * 100,
                4,
            ),
        )
    )


def build_suspicious_origin_accounts(
    current_transactions: DataFrame,
) -> DataFrame:
    return (
        current_transactions
        .filter(F.col("NameOrig").isNotNull())
        .groupBy("NameOrig")
        .agg(
            F.count("*").alias("transaction_count"),
            F.sum("Amount").alias("total_amount"),
            F.sum(
                F.when(F.col("IsFraud") == True, 1).otherwise(0)
            ).alias("fraud_count"),
            F.sum(
                F.when(
                    F.col("IsFraud") == True,
                    F.col("Amount"),
                ).otherwise(F.lit(0).cast("decimal(18,2)"))
            ).alias("fraud_amount"),
        )
        .filter(F.col("fraud_count") > 0)
        .withColumn(
            "fraud_rate_percent",
            F.round(
                F.col("fraud_count") / F.col("transaction_count") * 100,
                4,
            ),
        )
    )


# Main batch pipeline

def main() -> None:
    spark = (
        SparkSession.builder
        .appName("FraudGoldBatch")
        .enableHiveSupport()
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    try:
        # 1. Ensure the existing Silver Delta path is catalogued.
        register_silver_table(spark)

        # 2. Create the Gold database.
        create_gold_database(spark)

        # 3. Read Silver through Hive Metastore.
        silver_events = spark.table(
            f"{SILVER_DATABASE}.{SILVER_TABLE}"
        )

        # 4. Reconstruct the current state before calculating KPIs.
        current_transactions = prepare_current_transactions(silver_events)

        # 5. Build and write Gold data marts.
        transaction_metrics = build_transaction_metrics(current_transactions)
        write_gold_table(
            spark,
            transaction_metrics,
            "transaction_metrics_by_step_type",
        )

        fraud_metrics = build_fraud_metrics_by_type(current_transactions)
        write_gold_table(
            spark,
            fraud_metrics,
            "fraud_metrics_by_type",
        )

        suspicious_accounts = build_suspicious_origin_accounts(current_transactions)
        write_gold_table(
            spark,
            suspicious_accounts,
            "suspicious_origin_accounts",
        )

        print(
            "Gold batch pipeline completed successfully. "
            "Gold marts were rebuilt from the latest valid transaction state."
        )

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
