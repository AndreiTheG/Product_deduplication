# Product_deduplication

This project aims to consolidate duplicate product entries into a single, enriched record for each unique product. The goal is to maximize the available information while ensuring there are no duplicate entries.

# 1. Read the Dataset:
- The original dataset is loaded from a .parquet file named: veridion_product_deduplication_challenge.snappy.parquet
- It is immediately saved as a .csv file for easier processing.

# 2. Initial Data Preprocessing:
- The .csv file is reloaded into a DataFrame.
- The data is grouped by the unspsc column (which likely represents a standardized product category).
- Within each group, fields are aggregated by:
    - Selecting the longest string (for textual fields).
    - Choosing the maximum or minimum value for numeric fields (like price and production capacity).

# 3. Data Cleaning & Enrichment:
- Null or empty fields (such as "[]") are replaced with:
    - Meaningful default values (e.g. dictionaries for structured fields like pressure rating or price).
    - Placeholder values (None, False, or fallback structures) to preserve data consistency.
- All replacements are done with care to preserve the column’s original structure (e.g., lists or dictionaries).

# 4. Exporting the Cleaned Data:
- The final deduplicated dataset is saved in two formats:
    - CSV: file_without_duplicates.csv
    - Excel: file_with_no_duplicates.xlsx

# 🛠️ Key Features
- Handles complex nested data such as dictionaries and lists.
- Uses ast.literal_eval() to safely convert stringified data structures.
- Robust default values ensure completeness across all fields.
- Written in pure Python with pandas and easily extendable.