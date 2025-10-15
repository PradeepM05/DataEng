from pyspark.sql import functions as F
from pyspark.sql.functions import col, lit, when, concat_ws

join_cols = ['rznl1_reason', 'rznl2_detail', 'rznl3_additionaldetail', 
             'rznl4_additionalsubdetail', 'submaltorgmanager1', 'submaltorgmanager2',
             'submaltorgmanager3', 'submaltorgmanager4', 'submaltorgmanager5', 
             'submaltorgmanager6', 'assignorg1', 'assignorg2', 'sproductgrp1', 
             'sproductgrp2']

# Step 1: Add ID to df_mece
df_mece = df_mece.withColumn("mece_id", F.monotonically_increasing_id())

# Step 2: Create pattern identifier - which columns have non-null values in each row
pattern_parts = [when(col(c).isNotNull(), lit(c)).otherwise(lit("")) for c in join_cols]
df_mece = df_mece.withColumn("pattern", concat_ws("|", *pattern_parts))

# Step 3: Get unique patterns (this is small - maybe 10-50 unique patterns from 3K rows)
unique_patterns = df_mece.select("pattern").distinct().collect()

# Step 4: Initialize result - df1 with mece_id column
result = df1.withColumn("mece_id", lit(None).cast("long"))

# Step 5: For each unique pattern, join df1 with df_mece rows having that pattern
for pattern_row in unique_patterns:
    pattern = pattern_row['pattern']
    
    if not pattern:  # Skip if all columns are null
        continue
    
    # Get the non-null column names for this pattern
    non_null_cols = [c for c in pattern.split("|") if c]
    
    # Get df_mece rows with this specific pattern
    df_mece_pattern = df_mece.filter(col("pattern") == lit(pattern))
    
    # Join df1 with df_mece on ONLY the non-null columns
    # This is the key: we're joining on different column sets for different patterns
    joined = df1.join(
        F.broadcast(df_mece_pattern.select("mece_id", *non_null_cols)),
        on=non_null_cols,
        how="inner"
    ).select(*df1.columns, "mece_id")
    
    # Update result with matched mece_ids
    # Use coalesce to keep first match if multiple patterns match the same df1 row
    result = result.alias("r").join(
        joined.alias("j"),
        on=df1.columns,
        how="left"
    ).withColumn(
        "mece_id",
        F.coalesce(col("r.mece_id"), col("j.mece_id"))
    ).select("r.*", "mece_id")

# Step 6: Join with df_mece to get flu_alignment1
final_result = result.join(
    df_mece.select("mece_id", "flu_alignment1"),
    on="mece_id",
    how="left"
).drop("mece_id")

final_result.show(20, truncate=False)
