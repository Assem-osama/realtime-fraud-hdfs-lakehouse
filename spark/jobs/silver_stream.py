from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    coalesce,
    concat_ws,
    current_timestamp,
    to_timestamp,
    when,
    lit,
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    BooleanType,
)

# 1. Configuration

KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
KAFKA_TOPIC = "sqlserver.FraudDB.dbo.Transactions"

SILVER_PATH = "hdfs://namenode:8020/lakehouse/silver/transactions"

CHECKPOINT_PATH = ("hdfs://namenode:8020/lakehouse/checkpoints/silver_transactions")

# 2. Spark Session

spark = (
    SparkSession.builder
    .appName("RealtimeFraudSilver")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# 3. Debezium JSON Schema
#    Financial values are strings in the incoming JSON.
#    We convert them explicitly to decimal later.

transaction_schema = StructType([
    StructField("TransactionId", StringType(), True),
    StructField("SourceRowNumber", StringType(), True),
    StructField("Step", StringType(), True),
    StructField("TransactionType", StringType(), True),
    StructField("Amount", StringType(), True),
    StructField("NameOrig", StringType(), True),
    StructField("OldBalanceOrg", StringType(), True),
    StructField("NewBalanceOrig", StringType(), True),
    StructField("NameDest", StringType(), True),
    StructField("OldBalanceDest", StringType(), True),
    StructField("NewBalanceDest", StringType(), True),
    StructField("IsFraud", BooleanType(), True),
    StructField("IsFlaggedFraud", BooleanType(), True),
    StructField("CreatedAt", StringType(), True),
])

source_schema = StructType([
    StructField("db", StringType(), True),
    StructField("schema", StringType(), True),
    StructField("table", StringType(), True),
    StructField("name", StringType(), True),
])

debezium_schema = StructType([
    StructField("before", transaction_schema, True),
    StructField("after", transaction_schema, True),
    StructField("op", StringType(), True),
    StructField("source", source_schema, True),
])

# 4. Read Kafka

kafka_stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", "earliest")
    .option("failOnDataLoss", "true")
    .load()
)

# 5. Parse Debezium JSON

parsed_stream = (
    kafka_stream
    .filter(col("value").isNotNull())
    .select(
        col("topic").alias("KafkaTopic"),
        col("partition").alias("KafkaPartition"),
        col("offset").alias("KafkaOffset"),
        col("timestamp").alias("KafkaTimestamp"),
        col("value").cast("string").alias("RawPayload"),
        from_json(
            col("value").cast("string"),
            debezium_schema,
        ).alias("Payload"),
    )
)

# 6. Extract the transaction and CDC operation

extracted_stream = (
    parsed_stream
    .withColumn(
        "EventId",
        concat_ws(
            ":",
            col("KafkaTopic"),
            col("KafkaPartition").cast("string"),
            col("KafkaOffset").cast("string"),
        ),
    )
    .withColumn(
        "EventType",
        when(col("Payload").isNull(), lit("MALFORMED"))
        .when(col("Payload.op") == "c", lit("INSERT"))
        .when(col("Payload.op") == "u", lit("UPDATE"))
        .when(col("Payload.op") == "d", lit("DELETE"))
        .when(col("Payload.op") == "r", lit("SNAPSHOT_READ"))
        .otherwise(lit("UNKNOWN")),
    )
    .withColumn(
        "Transaction",
        coalesce(
            col("Payload.after"),
            col("Payload.before"),
        ),
    )
    .select(
        "EventId",
        "KafkaTopic",
        "KafkaPartition",
        "KafkaOffset",
        "KafkaTimestamp",
        "RawPayload",
        "EventType",
        col("Payload.op").alias("CDCOperation"),
        col("Transaction.TransactionId").alias("TransactionId"),
        col("Transaction.SourceRowNumber").alias("SourceRowNumber"),
        col("Transaction.Step").alias("Step"),
        col("Transaction.TransactionType").alias("TransactionType"),
        col("Transaction.Amount").alias("Amount"),
        col("Transaction.NameOrig").alias("NameOrig"),
        col("Transaction.OldBalanceOrg").alias("OldBalanceOrg"),
        col("Transaction.NewBalanceOrig").alias("NewBalanceOrig"),
        col("Transaction.NameDest").alias("NameDest"),
        col("Transaction.OldBalanceDest").alias("OldBalanceDest"),
        col("Transaction.NewBalanceDest").alias("NewBalanceDest"),
        col("Transaction.IsFraud").alias("IsFraud"),
        col("Transaction.IsFlaggedFraud").alias("IsFlaggedFraud"),
        col("Transaction.CreatedAt").alias("CreatedAt"),
    )
)

# 7. Cast values to analytical data types

typed_stream = (
    extracted_stream
    .withColumn("TransactionId", col("TransactionId").cast("long"))
    .withColumn("SourceRowNumber", col("SourceRowNumber").cast("long"))
    .withColumn("Step", col("Step").cast("integer"))
    .withColumn("Amount", col("Amount").cast("decimal(18,2)"))
    .withColumn(
        "OldBalanceOrg",
        col("OldBalanceOrg").cast("decimal(18,2)"),
    )
    .withColumn(
        "NewBalanceOrig",
        col("NewBalanceOrig").cast("decimal(18,2)"),
    )
    .withColumn(
        "OldBalanceDest",
        col("OldBalanceDest").cast("decimal(18,2)"),
    )
    .withColumn(
        "NewBalanceDest",
        col("NewBalanceDest").cast("decimal(18,2)"),
    )
    .withColumn("CreatedAt", to_timestamp(col("CreatedAt")))
    .withColumn("ProcessedAt", current_timestamp())
)

# 8. Data quality classification
#    Keep invalid events for investigation instead of silently
#    dropping them.

silver_stream = (
    typed_stream
    .withColumn(
        "DataQualityStatus",
        when(
            col("EventType") == "MALFORMED",
            lit("MALFORMED_JSON"),
        )
        .when(
            col("EventType") == "UNKNOWN",
            lit("UNKNOWN_CDC_OPERATION"),
        )
        .when(
            col("EventType") == "DELETE",
            lit("CDC_DELETE_EVENT"),
        )
        .when(
            col("TransactionId").isNull(),
            lit("MISSING_TRANSACTION_ID"),
        )
        .when(
            col("Amount").isNull(),
            lit("INVALID_AMOUNT"),
        )
        .when(
            col("Amount") < 0,
            lit("NEGATIVE_AMOUNT"),
        )
        .when(
            col("TransactionType").isNull(),
            lit("MISSING_TRANSACTION_TYPE"),
        )
        .when(
            ~col("TransactionType").isin(
                "PAYMENT",
                "CASH_OUT",
                "CASH_IN",
                "TRANSFER",
                "DEBIT",
            ),
            lit("UNKNOWN_TRANSACTION_TYPE"),
        )
        .otherwise(lit("VALID")),
    )
)

# 9. Idempotent Delta writes
#    EventId = Kafka topic + partition + offset.
#    MERGE prevents the same Kafka event from being inserted
#    twice if a batch is replayed after a restart.

def write_silver_batch(batch_df, batch_id):
    if batch_df.isEmpty():
        return

    from delta.tables import DeltaTable

    deduplicated_batch = batch_df.dropDuplicates(["EventId"])

    if DeltaTable.isDeltaTable(spark, SILVER_PATH):
        delta_table = DeltaTable.forPath(spark, SILVER_PATH)

        (
            delta_table.alias("target")
            .merge(
                deduplicated_batch.alias("source"),
                "target.EventId = source.EventId",
            )
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        (
            deduplicated_batch.write
            .format("delta")
            .mode("append")
            .save(SILVER_PATH)
        )

    print(
        f"Silver batch {batch_id} processed. "
        f"Input rows: {batch_df.count()}"
    )

# 10. Start the streaming query

query = (
    silver_stream.writeStream
    .foreachBatch(write_silver_batch)
    .option("checkpointLocation", CHECKPOINT_PATH)
    .trigger(processingTime="10 seconds")
    .start()
)

query.awaitTermination()
