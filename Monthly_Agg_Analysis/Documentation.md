# Compact Monthly Vendor Activity Table Documentation

This repository contains a Jupyter Notebook (`fact_vendor_activity_compact.ipynb`) designed to transform detailed NYC Taxi trip records into a highly space-efficient monthly summary for each taxi vendor. This approach is ideal for reducing storage and improving the performance of analytical queries and dashboards.

## What the Code Does

The notebook processes raw NYC Taxi trip data to create a compact fact table, `fact_vendor_activity`, optimized for reporting. Instead of storing millions of individual daily trip records, it aggregates daily activity per vendor, compressing the summarized data into arrays.

The core functionalities include:

1.  **Loading Data**: Reads the `yellow_tripdata` from a specified Delta table (e.g., in Microsoft Fabric).
    ```python
    df = spark.read.table("yellow_tripdata")
    ```
2.  **Creating Date Columns**: Enriches the dataset by deriving `trip_date`, `activity_month` (year and month of pickup), and a `reverse_day_index`. The `reverse_day_index` is particularly useful for time-series analysis, as it counts backward from the last day of the month (e.g., the 31st is index 0, the 30th is index 1).
    ```python
    df = df.withColumn("trip_date", to_date("tpepPickupDateTime")) \
           .withColumn("activity_month", trunc("tpepPickupDateTime", "MM")) \
           .withColumn("reverse_day_index",
                       dayofmonth(last_day("tpepPickupDateTime")) - dayofmonth("tpepPickupDateTime"))
    ```
3.  **Aggregating Daily Activity**: Calculates daily summaries per vendor for each month. This includes key metrics such as total trip count, passenger count, trip distance, fare amount, and various surcharges and tips.
    ```python
    daily_summary  = df.groupBy("vendorID", "activity_month", "reverse_day_index") \
                     .agg(
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
    ```

## Benefits of the Approach

The compact aggregation strategy offers significant advantages, especially for large-scale data analysis:

* **Reduced Storage Footprint**: By summarizing daily data into arrays within monthly records, the resulting fact table is significantly smaller than storing individual daily rows. This leads to substantial storage cost savings.
* **Improved Query Performance**: A smaller, more condensed data model translates to faster query execution times for reporting and dashboards. This means quicker insights and a better user experience.
* **Reduced Data Shuffling**: Because the compact fact table already aggregates data by key dimensions (like `vendorID`), joining it with other smaller dimension tables to bring in additional attributes (e.g., vendor name, vendor location) requires significantly less data shuffling across compute nodes. This makes query execution much faster and more efficient, especially in distributed processing environments.
* **Simplified Data Model**: Consolidating daily activity into monthly summaries per entity (e.g., vendor, user, device) simplifies the overall data model, making it easier to manage, understand, and build upon.
* **Efficient Time-Series Analysis**: The `reverse_day_index` facilitates quick access and analysis of daily trends within a month, particularly useful for understanding recent activity patterns.

## Example Use Cases

### 1. NYC Taxi Vendor Activity (Demonstrated in Notebook)

**Normal Daily Aggregation:**
If you stored every daily summary for each taxi vendor, a single month with 30 days and 2 vendors would result in 60 rows (`2 vendors * 30 days`). Over years, this table would grow to millions or billions of rows, leading to:
* **High Storage Costs**: Accumulating vast amounts of daily data.
* **Slow Query Performance**: Queries for monthly or annual trends would involve scanning and aggregating a very large number of daily rows.

**Compact Aggregation Approach:**
Using this notebook's approach, for the same scenario (2 vendors, 30 days, multiple metrics), you would have only **2 rows** in your `fact_vendor_activity` table (one for each vendor for that month). Each row would contain arrays where each element represents a daily aggregated value.

| vendorID | activity_month | daily_trip_counts_array | daily_fare_amounts_array | ... |
| :------- | :------------- | :---------------------- | :----------------------- | :-- |
| VTS      | 2024-01-01     | `[25000, 24500, ..., 26000]` | `[750000.0, 730000.0, ..., 780000.0]` | ... |

This leads to:
* **Significant Storage Reduction**: Instead of 30 rows per vendor per month, you get just 1.
* **Optimized Trend Analysis**: An entire month's data for a vendor is in a single row, minimizing joins shuffles.

### 2. IoT Device Telemetry Analysis

Consider a large IoT platform monitoring millions of connected devices daily, each sending various telemetry data (e.g., temperature, battery level, uptime, sensor readings).

**Normal Daily Aggregation for IoT Data:**
If the platform stored daily aggregated metrics for each of its 10 million devices individually (e.g., average temperature, min/max battery, total uptime), they would generate **10 million new rows *per day***. Over a month, this would amount to approximately **300 million rows** (`10 million devices * 30 days`).
* **Massive Storage Requirements**: Storing billions of daily device records over time would demand extensive storage infrastructure.
* **Extremely Slow Query Performance**: Aggregating data across millions or billions of daily rows for monthly or quarterly operational reports (e.g., device health trends, anomaly detection) would be time-consuming and computationally expensive.
* **Complex Data Management**: Managing, backing up, and maintaining such a rapidly growing and vast dataset would present significant operational challenges.

**Compact Aggregation Approach for IoT Data:**
Applying this compact aggregation method, the IoT platform could summarize each device's daily activity for an entire month into a **single row per device per month**. This row would contain arrays holding the daily values for various aggregated metrics (e.g., `daily_avg_temp_array`, `daily_min_battery_array`).

For example, a device's monthly activity record might look like this:

| DeviceID | activity_month | daily_avg_temp_array | daily_min_battery_array | ... |
| :------- | :------------- | :------------------- | :---------------------- | :-- |
| DevX     | 2024-01-01     | `[25.1, 25.5, ..., 24.9]` | `[85, 82, ..., 78]` | ... |

This results in:
* **Dramatic Storage Reduction**: Only 10 million rows per month (one per device per month) instead of 300 million. This represents a 30x reduction in row count per month, leading to immense storage and cost savings.
* **Accelerated Analytical Queries**: When engineers or analysts need to review device performance or health trends over a month or quarter, they can query these compact device-monthly summary rows. The entire month's aggregated data for a device is readily available within that single row, drastically reducing the need for large-scale joins or aggregations across massive daily records. This significantly speeds up dashboard loading times and analytical queries.
* **Enhanced Scalability**: The reduced data volume makes the system far more scalable, allowing the platform to manage more devices and conduct more complex analyses without proportional infrastructure expansion.
* **Simplified Anomaly Detection**: Identifying deviations from normal daily behavior within a month becomes easier as all relevant daily metrics are grouped together.

In essence, for companies operating with large-scale IoT data, this compact aggregation method transforms an overwhelming stream of daily telemetry into a highly efficient and performant dataset, enabling quicker insights, better operational monitoring, and more effective decision-making.
