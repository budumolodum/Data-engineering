from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count, month, dayofweek
import os

spark = SparkSession.builder \
    .appName("DivvyTrips") \
    .getOrCreate()

file_path = "/opt/bitnami/spark/jobs/Divvy_Trips_2019_Q4.csv"
df = spark.read.option("header", "true").csv(file_path)

# Конвертуємо відповідні стовпці в правильні типи
df = df.withColumn("trip_duration", col("tripduration").cast("int")) \
       .withColumn("start_time", col("start_time").cast("timestamp")) \
       .withColumn("gender", col("gender").cast("string"))

# Функція для розрахунку середньої тривалості поїздки на день
def avg_trip_duration_per_day():
    result = df.groupBy("start_time").agg(avg("trip_duration").alias("avg_trip_duration"))
    result.write.csv("/opt/bitnami/spark/jobs/out/avg_trip_duration_per_day.csv", header=True)

# Функція для підрахунку кількості поїздок за день
def trip_count_per_day():
    result = df.groupBy("start_time").agg(count("trip_id").alias("trip_count"))
    result.write.csv("/opt/bitnami/spark/jobs/out/trip_count_per_day.csv", header=True)

# Функція для знаходження найпопулярнішої станції кожного місяця
def popular_station_per_month():
    result = df.groupBy(month("start_time").alias("month"), "from_station_name") \
               .agg(count("trip_id").alias("trip_count")) \
               .orderBy("month", "trip_count", ascending=False) \
               .groupBy("month") \
               .agg({"from_station_name": "first"})
    result.write.csv("/opt/bitnami/spark/jobs/out/popular_station_per_month.csv", header=True)

# Функція для знаходження трійки найпопулярніших станцій за останні два тижні
def top_three_stations_last_two_weeks():
    from pyspark.sql.functions import current_date, date_sub
    last_two_weeks = df.filter(col("start_time") >= date_sub(current_date(), 14))
    result = last_two_weeks.groupBy("start_time", "from_station_name") \
                           .agg(count("trip_id").alias("trip_count")) \
                           .orderBy("start_time", "trip_count", ascending=False) \
                           .groupBy("start_time") \
                           .agg({"from_station_name": "first"})
    result.write.csv("/opt/bitnami/spark/jobs/out/top_three_stations_last_two_weeks.csv", header=True)

# Функція для порівняння середньої тривалості поїздки чоловіків і жінок
def avg_trip_duration_by_gender():
    result = df.groupBy("gender").agg(avg("trip_duration").alias("avg_trip_duration"))
    result.write.csv("/opt/bitnami/spark/jobs/out/avg_trip_duration_by_gender.csv", header=True)

# Викликаємо функції
avg_trip_duration_per_day()
trip_count_per_day()
popular_station_per_month()
top_three_stations_last_two_weeks()
avg_trip_duration_by_gender()

spark.stop()
