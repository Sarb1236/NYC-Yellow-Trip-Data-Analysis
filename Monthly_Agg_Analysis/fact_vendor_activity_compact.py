#!/usr/bin/env python
# coding: utf-8

# ## fact_vendor_activity_compact
# 
# New notebook

# 
# # 🛠️ Build a Compact Monthly Vendor Activity Table
# 
# This notebook helps convert detailed NYC Taxi trip records into a **space-efficient monthly summary** for each taxi vendor.
# 
# Instead of storing millions of rows per day, we summarize daily activity per vendor using compressed strings and arrays.
# 
# ### What You'll Learn:
# - How to summarize daily trip info (count, distance, fare) per vendor
# - How to reverse-index days in a month (starting from the end)
# - How to compress daily data into strings like `'2,0,3,5,...'`
# - How to build a compact **fact table** ready for reporting
# 
# This is great for reducing storage and improving performance in dashboards.
# 
# We'll use the `yellow_tripdata` Delta table in Microsoft Fabric.
# 

# In[1]:


# Input table: yellow_tripdata (Fabric Lakehouse Delta table)
# Output table: fact_vendor_activity (vendor-level monthly activity summary)

from pyspark.sql.functions import *
from pyspark.sql import Window

# STEP 1: Load  data
df = spark.read.table("yellow_tripdata")


# In[2]:


df.show(50,truncate=False)


# In[3]:


# -------------------------------------
# STEP 2: Create date columns like:
# - trip_date: the date of pickup (e.g., 2020-01-15)
# - activity_month: just the year and month (e.g., 2020-01)
# - reverse_day_index: counts backward from end of month (e.g., 31st = 0, 30th = 1)
# -------------------------------------
df = df.withColumn("trip_date", to_date("tpepPickupDateTime")) \
       .withColumn("activity_month", trunc("tpepPickupDateTime", "MM")) \
       .withColumn("reverse_day_index", 
                   dayofmonth(last_day("tpepPickupDateTime")) - dayofmonth("tpepPickupDateTime"))


# In[4]:


from pyspark.sql.functions import col

filtered_df=df.filter(
    (col("vendorID") == 2) &
    (col("activity_month") == "2009-01-01")
).orderBy("reverse_day_index")


# In[5]:


filtered_df.show(50,truncate=False)


# In[6]:


# -------------------------------------
# STEP 3: Count daily activity per vendor each month
# For example, how many trips Vendor 1 had on Jan 30, Jan 29, etc.
# -------------------------------------
from pyspark.sql.functions import sum as F_sum, count
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


# In[7]:


from pyspark.sql.functions import col

filtered_df=daily_summary.filter(
    (col("vendorID") == 2) &
    (col("activity_month") == "2009-01-01")
).orderBy("reverse_day_index")


# In[8]:


filtered_df.show(50,truncate=False)


# In[9]:


from pyspark.sql.functions import lit

# -------------------------------------
#  STEP 4: Pivot the data so each row shows all 31 days as columns
# Like a calendar row: [trips_on_day_0, day_1, ..., day_30]
# -------------------------------------
max_days = 31
base_days = spark.createDataFrame([(i,) for i in range(max_days)], ["reverse_day_index"])

# Distinct vendor + month combinations
vendor_months = daily_summary.select("vendorID", "activity_month").distinct()

# Cross join to get all (vendorID, activity_month, reverse_day_index) combinations
full = vendor_months.crossJoin(base_days)







# In[10]:


full.orderBy("activity_month", "reverse_day_index").show(50, truncate=False)


# In[11]:


# Join with summary data to fill missing days
full_with_summary = full.join(
    daily_summary,
    on=["vendorID", "activity_month", "reverse_day_index"],
    how="left"
)


# In[12]:


from pyspark.sql.functions import col

filtered_df=full_with_summary.filter(
    (col("vendorID") == 2) &
    (col("activity_month") == "2009-01-01")
)


# In[13]:


filtered_df.show(50,truncate=False)


# filtered_df.head(30)

# In[14]:


# Important orderby enforce sequence
ordered = full_with_summary.orderBy("reverse_day_index")




# In[15]:


from pyspark.sql.functions import col
filtered_df=ordered.filter(
    (col("vendorID") == 2) &
    (col("activity_month") == "2009-01-01")
)


# In[16]:


filtered_df.show(50, truncate=False)


# In[17]:


from pyspark.sql.functions import col, collect_list, when, struct, sort_array, coalesce, lit
from pyspark.sql.window import Window


# Process the 'ordered' DataFrame to convert NULLs to 0s for the array elements
# and ensure correct data types. Using coalesce(column, default_value) for robustness.
processed_ordered = ordered.withColumn(
    "activity_flag_val", when(col("trip_count").isNotNull() & (col("trip_count") > 0), 1).otherwise(0)
).withColumn(
    "daily_trip_count_val", coalesce(col("trip_count").cast("long"), lit(0))
).withColumn(
    "daily_passenger_count_val", coalesce(col("total_passengerCount").cast("long"), lit(0))
).withColumn(
    "daily_trip_distance_val", coalesce(col("total_tripDistance").cast("double"), lit(0.0))
).withColumn(
    "daily_fare_amount_val", coalesce(col("total_fareAmount").cast("double"), lit(0.0))
).withColumn(
    "daily_extra_amount_val", coalesce(col("total_extra").cast("double"), lit(0.0))
).withColumn(
    "daily_mta_tax_val", coalesce(col("total_mtaTax").cast("double"), lit(0.0))
).withColumn(
    "daily_tip_amount_val", coalesce(col("total_tipAmount").cast("double"), lit(0.0))
).withColumn(
    "daily_tolls_amount_val", coalesce(col("total_tollsAmount").cast("double"), lit(0.0))
).withColumn(
    "daily_total_amount_val", coalesce(col("total_totalAmount").cast("double"), lit(0.0))
)

# Now, group by vendor and month. Inside the aggregation, we will:
# 1. Collect a list of structs, each containing the 'reverse_day_index' (for sorting)
#    and all the prepared metric values.
# 2. Use 'sort_array' on this list of structs to sort them by 'reverse_day_index'.
# 3. Finally, extract the individual metric value arrays from the sorted structs.

fact_vendor_activity = processed_ordered.groupBy("vendorID", "activity_month").agg(
    sort_array(
        collect_list(
            struct(
                col("reverse_day_index").alias("idx"), # Alias for the sorting key
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
).withColumn(
    "activity_flags_array", col("collected_data.activity_flag")
).withColumn(
    "daily_trip_counts_array", col("collected_data.trip_count")
).withColumn(
    "daily_passenger_counts_array", col("collected_data.passenger_count")
).withColumn(
    "daily_trip_distances_array", col("collected_data.trip_distance")
).withColumn(
    "daily_fare_amounts_array", col("collected_data.fare_amount")
).withColumn(
    "daily_extra_amounts_array", col("collected_data.extra_amount")
).withColumn(
    "daily_mta_taxes_array", col("collected_data.mta_tax")
).withColumn(
    "daily_tip_amounts_array", col("collected_data.tip_amount")
).withColumn(
    "daily_tolls_amounts_array", col("collected_data.tolls_amount")
).withColumn(
    "daily_total_amounts_array", col("collected_data.total_amount")
).drop("collected_data") # Drop the intermediate struct array


# In[18]:


from pyspark.sql.functions import col
filtered_df = fact_vendor_activity.filter(
    (col("vendorID") == 2) &
    (col("activity_month") == "2009-01-01")
)


# In[19]:


filtered_df.orderBy("vendorID", "activity_month").show(50, truncate=False)


# In[20]:


# -------------------------------------
# STEP 6: Save the final summary as a compact fact table
# -------------------------------------
fact_vendor_activity.write \
    .mode("overwrite") \
    .format("delta") \
    .saveAsTable("fact_vendor_activity")



# In[21]:


display(
  spark.sql("""
    SELECT * 
    FROM fact_vendor_activity
    WHERE vendorID=2
and activity_month='2009-01-01'
    ORDER BY activity_month
  """)
)


# **Comapre size and row count all three tabels : yellow_tripdata(all data ) ,fact_vendor_daily_activity(summarized at day level by rows),fact_vendor_activity (summarized at monthly  level but keeping day level data in column arrays )**

# In[22]:


def print_table_size_and_count(table_name):
    desc = spark.sql(f"DESCRIBE DETAIL {table_name}").select("sizeInBytes").collect()
    size_bytes = desc[0]['sizeInBytes']
    row_count = spark.sql(f"SELECT COUNT(*) FROM {table_name}").collect()[0][0]
    
    print(f"Table: {table_name}")
    print(f"  Rows: {row_count}")
    print(f"  Size: {size_bytes} bytes")
    print(f"       {size_bytes / (1024**2):.2f} MB")
    print(f"       {size_bytes / (1024**3):.2f} GB\n")

tables = ["fact_vendor_activity", "yellow_tripdata", "fact_vendor_daily_activity"]

for tbl in tables:
    print_table_size_and_count(tbl)


# **Explode compact table and validate with data**

# In[ ]:


get_ipython().run_cell_magic('sql', '', "\nWITH exploded AS (\n  SELECT\n    vendorID,\n    activity_month,\n    posexplode(daily_trip_counts_array) AS (reverse_day_index, trip_count),\n    daily_passenger_counts_array,\n    daily_trip_distances_array,\n    daily_fare_amounts_array,\n    daily_extra_amounts_array,\n    daily_mta_taxes_array,\n    daily_tip_amounts_array,\n    daily_tolls_amounts_array,\n    daily_total_amounts_array\n  FROM fact_vendor_activity\n),\n\nfinal_daily AS (\n  SELECT\n    vendorID,\n    activity_month,\n    DATE_SUB(LAST_DAY(activity_month), reverse_day_index) AS trip_date,\n    trip_count,\n    daily_passenger_counts_array[reverse_day_index] AS passenger_count,\n    daily_trip_distances_array[reverse_day_index] AS trip_distance,\n    daily_fare_amounts_array[reverse_day_index] AS fare_amount,\n    daily_extra_amounts_array[reverse_day_index] AS extra,\n    daily_mta_taxes_array[reverse_day_index] AS mta_tax,\n    daily_tip_amounts_array[reverse_day_index] AS tip_amount,\n    daily_tolls_amounts_array[reverse_day_index] AS tolls_amount,\n    daily_total_amounts_array[reverse_day_index] AS total_amount\n  FROM exploded\n)\n\nSELECT *\nFROM final_daily\nWHERE vendorID=2\nand activity_month='2019-09-01'\n  and trip_count >0\n  order by 3\n\n")


# In[24]:


# The command is not a standard IPython magic command. It is designed for use within Fabric notebooks only.
# %%sql
# SELECT *
# FROM fact_vendor_daily_activity
# WHERE vendorID=2
# and activity_month='2019-09-01'
#   order by 3


# In[31]:


# The command is not a standard IPython magic command. It is designed for use within Fabric notebooks only.
# %%sql
# SELECT *
# FROM yellow_tripdata
# WHERE vendorID=2
# and  year  (tpepPickupDateTime)=2019
# and month(tpepPickupDateTime)=9
#   order by tpepPickupDateTime asc

