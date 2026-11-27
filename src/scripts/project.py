"""
RestaurantSubscribeStreamingService.

Читает акции ресторанов из Kafka, оставляет только действующие сейчас,
джойнит с подписчиками ресторана из Postgres и пишет результат в два стока:
  * Postgres  public.subscribers_feedback  (с пустым полем feedback);
  * Kafka     KAFKA_OUTPUT_TOPIC           (JSON без поля feedback).

Запуск внутри контейнера sprint-stream:
    cd /workspace && source /workspace/kafka-env.sh
    python3 /workspace/project.py
"""
import os

from kafka_config import KAFKA_OPTIONS
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col,
    current_timestamp,
    from_json,
    lit,
    struct,
    to_json,
    unix_timestamp,
)
from pyspark.sql.types import LongType, StringType, StructField, StructType

# ---------------------------------------------------------------------------
# Параметры
# ---------------------------------------------------------------------------
KAFKA_INPUT_TOPIC = os.environ["KAFKA_INPUT_TOPIC"]
KAFKA_OUTPUT_TOPIC = os.environ["KAFKA_OUTPUT_TOPIC"]

PG_URL = "jdbc:postgresql://localhost:5432/de"
PG_PROPERTIES = {
    "user": "jovyan",
    "password": "jovyan",
    "driver": "org.postgresql.Driver",
}
PG_INPUT_TABLE = "public.subscribers_restaurants"
PG_OUTPUT_TABLE = "public.subscribers_feedback"

CHECKPOINT_DIR = os.environ.get(
    "CHECKPOINT_DIR", "/workspace/checkpoints/restaurant_subscribe_streaming"
)

# Порядок полей выходного сообщения (как в примере из задания)
OUTPUT_COLUMNS = [
    "restaurant_id",
    "adv_campaign_id",
    "adv_campaign_content",
    "adv_campaign_owner",
    "adv_campaign_owner_contact",
    "adv_campaign_datetime_start",
    "adv_campaign_datetime_end",
    "client_id",
    "datetime_created",
    "trigger_datetime_created",
]

# Схема входного сообщения
incoming_message_schema = StructType(
    [
        StructField("restaurant_id", StringType(), True),
        StructField("adv_campaign_id", StringType(), True),
        StructField("adv_campaign_content", StringType(), True),
        StructField("adv_campaign_owner", StringType(), True),
        StructField("adv_campaign_owner_contact", StringType(), True),
        StructField("adv_campaign_datetime_start", LongType(), True),
        StructField("adv_campaign_datetime_end", LongType(), True),
        StructField("datetime_created", LongType(), True),
    ]
)


# ---------------------------------------------------------------------------
# Запись микробатча в 2 стока: Postgres (фидбэк) и Kafka (push-триггеры)
# ---------------------------------------------------------------------------
def foreach_batch_function(df: DataFrame, epoch_id: int) -> None:
    # сохраняем df в памяти, чтобы не вычислять его заново для второго стока
    df.persist()
    try:
        if df.rdd.isEmpty():
            return

        # записываем df в PostgreSQL с полем feedback
        (
            df.select(*OUTPUT_COLUMNS)
            .withColumn("feedback", lit(None).cast(StringType()))
            .write.mode("append")
            .jdbc(url=PG_URL, table=PG_OUTPUT_TABLE, properties=PG_PROPERTIES)
        )

        # создаём df для отправки в Kafka: сериализуем строку в JSON в колонку value
        kafka_df = df.select(
            to_json(struct(*[col(c) for c in OUTPUT_COLUMNS])).alias("value")
        )

        # отправляем сообщения в результирующий топик Kafka без поля feedback
        (
            kafka_df.write.format("kafka")
            .options(**KAFKA_OPTIONS)
            .option("topic", KAFKA_OUTPUT_TOPIC)
            .save()
        )
    finally:
        # очищаем память от df
        df.unpersist()


# ---------------------------------------------------------------------------
# Spark-сессия
# ---------------------------------------------------------------------------
spark_jars_packages = ",".join(
    [
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0",
        "org.postgresql:postgresql:42.4.0",
    ]
)

spark = (
    SparkSession.builder.appName("RestaurantSubscribeStreamingService")
    .config("spark.sql.session.timeZone", "UTC")
    .config("spark.jars.packages", spark_jars_packages)
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# ---------------------------------------------------------------------------
# Шаг 2. Читаем из топика Kafka сообщения с акциями от ресторанов
# ---------------------------------------------------------------------------
restaurant_read_stream_df = (
    spark.readStream.format("kafka")
    .options(**KAFKA_OPTIONS)
    .option("subscribe", KAFKA_INPUT_TOPIC)
    .load()
)

# ---------------------------------------------------------------------------
# Шаг 4. Десериализуем JSON из value и фильтруем по времени действия акции.
# Текущее время считается в каждом микробатче (unix-секунды, UTC).
# ---------------------------------------------------------------------------
current_timestamp_utc = unix_timestamp(current_timestamp())

filtered_read_stream_df = (
    restaurant_read_stream_df.select(
        from_json(col("value").cast(StringType()), incoming_message_schema).alias(
            "event"
        )
    )
    .select("event.*")
    .where(col("restaurant_id").isNotNull())
    .where(
        (col("adv_campaign_datetime_start") <= current_timestamp_utc)
        & (col("adv_campaign_datetime_end") >= current_timestamp_utc)
    )
)

# ---------------------------------------------------------------------------
# Шаг 3. Вычитываем всех пользователей с подпиской на рестораны
# ---------------------------------------------------------------------------
subscribers_restaurant_df = (
    spark.read.format("jdbc")
    .option("url", PG_URL)
    .option("driver", PG_PROPERTIES["driver"])
    .option("dbtable", PG_INPUT_TABLE)
    .option("user", PG_PROPERTIES["user"])
    .option("password", PG_PROPERTIES["password"])
    .load()
    .select("client_id", "restaurant_id")
    .distinct()
)

# ---------------------------------------------------------------------------
# Шаг 5. JOIN потока со статикой по restaurant_id (join по имени колонки —
# restaurant_id в результате один, повторов нет). Добавляем время создания
# триггера.
# ---------------------------------------------------------------------------
result_df = (
    filtered_read_stream_df.join(subscribers_restaurant_df, on="restaurant_id", how="inner")
    .withColumn("trigger_datetime_created", unix_timestamp(current_timestamp()).cast("int"))
    .select(*OUTPUT_COLUMNS)
)

# ---------------------------------------------------------------------------
# Шаги 6–8. Запускаем стриминг с записью в 2 стока
# ---------------------------------------------------------------------------
(
    result_df.writeStream.foreachBatch(foreach_batch_function)
    .option("checkpointLocation", CHECKPOINT_DIR)
    .start()
    .awaitTermination()
)
