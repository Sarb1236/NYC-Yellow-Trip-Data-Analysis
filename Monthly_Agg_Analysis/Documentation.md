# Compact Monthly Vendor Activity Table Documentation

This repository contains a Jupyter Notebook (`fact_vendor_activity_compact_layman.ipynb`) designed to transform detailed NYC Taxi trip records into a highly space-efficient monthly summary for each taxi vendor. This approach is ideal for reducing storage and improving the performance of analytical queries and dashboards.

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
* **Simplified Data Model**: Consolidating daily activity into monthly summaries per entity (e.g., vendor, user) simplifies the overall data model, making it easier to manage, understand, and build upon.
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
* **Optimized Trend Analysis**: An entire month's data for a vendor is in a single row, minimizing joins and speeding up dashboard loads.

### 2. Facebook Daily Active Users (DAU) Analysis

Imagine Facebook needs to analyze engagement metrics for its 2 billion daily active users (DAU).

**Normal Daily Aggregation for Facebook:**
Storing daily aggregated metrics for each of the 2 billion users individually (e.g., total time spent, number of posts) would generate **2 billion new rows *per day***. Over a month, this would equate to approximately **60 billion rows** (`2 billion users * 30 days`).
* **Massive Storage Requirements**: Exabytes of storage for historical data.
* **Extremely Slow Query Performance**: Aggregating across billions of daily rows for monthly or quarterly reports would be prohibitively slow and resource-intensive.
* **Complex Data Management**: Operational challenges in managing and backing up such a rapidly growing, vast dataset.

**Compact Aggregation Approach for Facebook:**
Applying this method, Facebook could summarize each user's daily activity for an entire month into a **single row per user per month**. This row would contain arrays holding the daily values for various metrics.

| UserID | activity_month | daily_time_spent_array | daily_posts_count_array | ... |
| :----- | :------------- | :--------------------- | :---------------------- | :-- |
| UserA  | 2024-01-01     | `[120, 150, ..., 100]` (minutes) | `[2, 3, ..., 1]` | ... |

This results in:
* **Dramatic Storage Reduction**: Only 2 billion rows per month (one per user per month) instead of 60 billion. This is a 30x reduction in row count, leading to immense storage savings.
* **Accelerated Analytical Queries**: Product managers and data scientists can quickly query these compact user-monthly summaries. All daily data for a user for a month is co-located, dramatically speeding up dashboard loading and analytical computations.
* **Enhanced Scalability**: The reduced data volume makes the system far more scalable, allowing Facebook to handle more users and conduct more complex analyses without proportional infrastructure expansion.
* **Simplified Feature Engineering**: Data scientists can more easily create features based on monthly user behavior patterns.

In summary, this compact aggregation approach is crucial for large-scale data systems, transforming unmanageable volumes of granular data into efficient, performant datasets that enable faster insights and more effective decision-making.
