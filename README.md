# NYC-Yellow-Trip-Data-Analysis
Proof of Concept (POC) to apply data compression technique to NYC's Yellow Taxi data, and here's a summary of what I did:

- Utilized Spark on Microsoft Fabric to load daily trip records.
- Implemented reverse indexing for each day of the month (31st = 0, 30th = 1, and so forth).
- Condensed 30-31 days of metrics (including trip counts, distances, fares, and tips) into fixed-length arrays, resulting in one row per vendor per month.
- Expanded the arrays to validate against the raw and daily-summary tables.


This technique can give up to 30x reduction in row count without losing analytical flexibility. This approach speeds up joins and aggregations—especially useful when dealing with high-cardinality dimensions and temporal trends.

**Download Raw Yellow Taxi Data** This folder contains notebook to download NYC Taxi data
**Monthyly Agg Analysis** This folder contains code notebook and a documentation for the code.
