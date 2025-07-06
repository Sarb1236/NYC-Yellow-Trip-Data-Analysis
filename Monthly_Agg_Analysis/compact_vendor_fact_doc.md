# Compact Monthly Vendor Activity Table Documentation

This project explores efficient data modeling and compression techniques for NYC Yellow Taxi trip data using PySpark on Microsoft Fabric. The goal is to reduce storage footprint and improve query performance while maintaining the ability to conduct granular day-level analysis.

## Overview: What the Code Does

The notebook ingests raw `yellow_tripdata`, aggregates it at a daily level, and then compresses it into a monthly-level fact table where each vendor has **one row per month**. Within this row, daily values (e.g., trip counts, fares, tips) are stored in arrays, ordered using a **reverse-day index** (e.g., 31st = 0, 30th = 1, etc.).

## Full Code Workflow (Step-by-Step)

### Step 1: Load Raw Data

```python
from pyspark.sql.functions import *
df = spark.read.table("yellow_tripdata")
df.show(50, truncate=False)
```

**Description**: Reads the full NYC Taxi trip data table from Microsoft Fabric Lakehouse. Displays the first 50 rows for inspection.

---

### Step 2: Create Date Columns

```python
from pyspark.sql.functions import dayofmonth, last_day, to_date, trunc

df = df.withColumn("trip_date", to_date("tpepPickupDateTime")) \
       .withColumn("activity_month", trunc("tpepPickupDateTime", "MM")) \
       .withColumn("reverse_day_index", 
                   dayofmonth(last_day("tpepPickupDateTime")) - dayofmonth("tpepPickupDateTime"))
```

**Description**: Adds helper columns to enable time-based aggregation. `reverse_day_index` allows the final arrays to be reverse-ordered.

---

### Step 3: Aggregate Daily Activity

```python
from pyspark.sql.functions import sum as F_sum, count

daily_summary = df.groupBy("vendorID", "activity_month", "reverse_day_index").agg(
    count("*").alias("trip_count"),
    F_sum("passengerCount").alias("total_passengerCount"),
    F_sum("tripDistance").alias("total_tripDistance"),
    F_sum("fareAmount").alias("total_fareAmount"),
    F_sum("extra").alias("total_extra"),
    F_sum("mtaTax").alias("total_mtaTax"),
    F_sum("tipAmount").alias("total_tipAmount"),
    F_sum("tollsAmount").alias("total_tollsAmount"),
    F_sum("totalAmount").alias("total_totalAmount")
)
daily_summary.orderBy("vendorID", "activity_month", "reverse_day_index").show(50, truncate=False)
```

**Description**: Aggregates daily taxi activity per vendor by month and reverse day index.

**Sample Output**:

```
+--------+--------------+-------------------+-----------+---------------------+
|vendorID|activity_month|reverse_day_index  |trip_count|total_fareAmount     |
+--------+--------------+-------------------+-----------+---------------------+
|2       |2009-01-01    |0                  |150        |3450.75              |
|2       |2009-01-01    |1                  |167        |3872.60              |
...
```

---

### Step 4: Ensure All Days Are Present

```python
max_days = 31
base_days = spark.createDataFrame([(i,) for i in range(max_days)], ["reverse_day_index"])
vendor_months = daily_summary.select("vendorID", "activity_month").distinct()
full = vendor_months.crossJoin(base_days)
full_with_summary = full.join(daily_summary, ["vendorID", "activity_month", "reverse_day_index"], how="left")
full_with_summary.orderBy("vendorID", "activity_month", "reverse_day_index").show(50, truncate=False)
```

**Description**: Builds a full calendar grid for each vendor and month, even if some days had no activity.

**Sample Output**: Includes NULLs for days with no trips.

---

###  Step 5: Prepare Data for Compression

```python
from pyspark.sql.functions import coalesce, lit, when

processed_ordered = full_with_summary.withColumn("activity_flag_val", when(col("trip_count") > 0, 1).otherwise(0)) \
    .withColumn("daily_trip_count_val", coalesce(col("trip_count"), lit(0))) \
    .withColumn("daily_passenger_count_val", coalesce(col("total_passengerCount"), lit(0))) \
    .withColumn("daily_trip_distance_val", coalesce(col("total_tripDistance"), lit(0.0))) \
    .withColumn("daily_fare_amount_val", coalesce(col("total_fareAmount"), lit(0.0))) \
    .withColumn("daily_extra_amount_val", coalesce(col("total_extra"), lit(0.0))) \
    .withColumn("daily_mta_tax_val", coalesce(col("total_mtaTax"), lit(0.0))) \
    .withColumn("daily_tip_amount_val", coalesce(col("total_tipAmount"), lit(0.0))) \
    .withColumn("daily_tolls_amount_val", coalesce(col("total_tollsAmount"), lit(0.0))) \
    .withColumn("daily_total_amount_val", coalesce(col("total_totalAmount"), lit(0.0)))
```

**Description**: Handles missing/null values and ensures uniform schema for all array metrics.

---

###  Step 6: Build Compact Fact Table with Arrays

```python
from pyspark.sql.functions import collect_list, sort_array, struct

fact_vendor_activity = processed_ordered.groupBy("vendorID", "activity_month").agg(
    sort_array(
        collect_list(
            struct(
                col("reverse_day_index").alias("idx"),
                col("activity_flag_val").alias("activity_flag"),
                col("daily_trip_count_val").alias("trip_count"),
                col("daily_passenger_count_val").alias("passenger_count"),
                col("daily_trip_distance_val").alias("trip_distance"),
                col("daily_fare_amount_val").alias("fare_amount"),
                col("daily_extra_amount_val").alias("extra_amount"),
                col("daily_mta_tax_val").alias("mta_tax"),
                col("daily_tip_amount_val").alias("tip_amount"),
                col("daily_tolls_amount_val").alias("tolls_amount"),
                col("daily_total_amount_val").alias("total_amount")
            )
        )
    ).alias("collected_data")
).withColumn("activity_flags_array", col("collected_data.activity_flag")) \
 .withColumn("daily_trip_counts_array", col("collected_data.trip_count")) \
 .withColumn("daily_passenger_counts_array", col("collected_data.passenger_count")) \
 .withColumn("daily_trip_distances_array", col("collected_data.trip_distance")) \
 .withColumn("daily_fare_amounts_array", col("collected_data.fare_amount")) \
 .withColumn("daily_extra_amounts_array", col("collected_data.extra_amount")) \
 .withColumn("daily_mta_taxes_array", col("collected_data.mta_tax")) \
 .withColumn("daily_tip_amounts_array", col("collected_data.tip_amount")) \
 .withColumn("daily_tolls_amounts_array", col("collected_data.tolls_amount")) \
 .withColumn("daily_total_amounts_array", col("collected_data.total_amount")) \
 .drop("collected_data")

fact_vendor_activity.orderBy("vendorID", "activity_month").show(5, truncate=False)
```

**Description**: Compresses daily metrics into reverse-ordered arrays.

---

### Step 7: Save Table

```python
fact_vendor_activity.write \
    .mode("overwrite") \
    .format("delta") \
    .saveAsTable("fact_vendor_activity")
```

**Description**: Saves the final compact fact table to the Lakehouse.

---

###  Step 8: Explode and Validate

```sql
-- Explode back to daily granularity
WITH exploded AS (
  SELECT
    vendorID,
    activity_month,
    posexplode(daily_trip_counts_array) AS (reverse_day_index, trip_count),
    daily_passenger_counts_array,
    daily_trip_distances_array,
    daily_fare_amounts_array,
    daily_extra_amounts_array,
    daily_mta_taxes_array,
    daily_tip_amounts_array,
    daily_tolls_amounts_array,
    daily_total_amounts_array
  FROM fact_vendor_activity
),

final_daily AS (
  SELECT
    vendorID,
    activity_month,
    DATE_SUB(LAST_DAY(activity_month), reverse_day_index) AS trip_date,
    trip_count,
    daily_passenger_counts_array[reverse_day_index] AS passenger_count,
    daily_trip_distances_array[reverse_day_index] AS trip_distance,
    daily_fare_amounts_array[reverse_day_index] AS fare_amount,
    daily_extra_amounts_array[reverse_day_index] AS extra,
    daily_mta_taxes_array[reverse_day_index] AS mta_tax,
    daily_tip_amounts_array[reverse_day_index] AS tip_amount,
    daily_tolls_amounts_array[reverse_day_index] AS tolls_amount,
    daily_total_amounts_array[reverse_day_index] AS total_amount
  FROM exploded
)
SELECT * FROM final_daily WHERE vendorID=2 AND activity_month='2019-09-01' ORDER BY 3;
```

**Description**: Converts the monthly arrays back into row-level records for verification, visual inspection, or further analysis.

---

##  Comparison with Other Tables

> **Print Sizes & Counts**

```python
def print_table_size_and_count(table_name):
    desc = spark.sql(f"DESCRIBE DETAIL {table_name}").select("sizeInBytes").collect()[0][0]
    rows = spark.sql(f"SELECT COUNT(*) FROM {table_name}").collect()[0][0]
    print(f"{table_name}: rows={rows}, size={desc/(1024**2):.2f} MB")

for tbl in ["yellow_tripdata","fact_vendor_daily_activity","fact_vendor_activity"]:
    print_table_size_and_count(tbl)
```

---

##  Benefits of This Approach

- **Storage Efficiency:** Reduces rows by 30× per vendor/month, storing daily metrics in arrays.
- **Query Performance:** Smaller fact table means faster scans and joins .
- **Flexibility:** Arrays can be exploded back to day‑level for anomaly detection or trend analysis.
- **Reduced Shuffling:** Minimal data movement when joining with dimension tables (e.g., `dim_vendor`).

---

##  Real-World Use Cases

### 1. NYC Taxi Vendor Activity

- **Traditional Daily Model:** 2 vendors × 30 days = 60 rows per month
- **Compact Model:** 2 rows per month
- **Gain:** 30× row reduction, less storage, faster dashboards.

### 2. IoT Device Telemetry (Hypothetical)

- **Daily Records:** 10 M devices × 30 days = 300 M rows/month
- **Compact Model:** 10 M rows/month with arrays of daily metrics
- **Gain:** 30× reduction, enabling scalable time-series analytics.

### 3. Web Analytics Summary

- Summarize daily user sessions or event counts per site/per month into arrays for quick monthly overviews and drill‑downs.



