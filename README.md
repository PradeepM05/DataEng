from pyspark.sql import functions as F
from pyspark.sql.functions import col, lit, when, concat_ws
from pyspark.sql.window import Window

join_cols = ['rznl1_reason', 'rznl2_detail', 'rznl3_additionaldetail', 
             'rznl4_additionalsubdetail', 'submaltorgmanager1', 'submaltorgmanager2',
             'submaltorgmanager3', 'submaltorgmanager4', 'submaltorgmanager5', 
             'submaltorgmanager6', 'assignorg1', 'assignorg2', 'sproductgrp1', 
             'sproductgrp2']

# Step 1: Add ID to df_mece and create pattern
df_mece = df_mece.withColumn("mece_id", F.monotonically_increasing_id())

pattern_parts = [when(col(c).isNotNull(), lit(c)).otherwise(lit("")) for c in join_cols]
df_mece = df_mece.withColumn("pattern", concat_ws("|", *pattern_parts))

# Step 2: Get unique patterns
unique_patterns = df_mece.select("pattern").distinct().collect()

# Step 3: Initialize result with a unique ID for df1
result = df1.withColumn("df1_id", F.monotonically_increasing_id()) \
            .withColumn("mece_id", lit(None).cast("long"))

# Step 4: Process each pattern
for pattern_row in unique_patterns:
    pattern = pattern_row['pattern']
    
    if not pattern:
        continue
    
    non_null_cols = [c for c in pattern.split("|") if c]
    
    # Get df_mece rows with this pattern
    df_mece_pattern = df_mece.filter(col("pattern") == lit(pattern)) \
                             .select("mece_id", *non_null_cols)
    
    # Join df1 with df_mece_pattern
    joined = result.join(
        F.broadcast(df_mece_pattern),
        on=non_null_cols,
        how="inner"
    )
    
    # Handle multiple matches - keep first mece_id per df1 row
    window_spec = Window.partitionBy("df1_id").orderBy("mece_id")
    matched = joined.withColumn("rn", F.row_number().over(window_spec)) \
                    .filter(col("rn") == 1) \
                    .select("df1_id", col("mece_id").alias("new_mece_id"))
    
    # Update result with new matches (only if mece_id is still null)
    result = result.join(matched, on="df1_id", how="left") \
                   .withColumn("mece_id", 
                              when(col("mece_id").isNull(), col("new_mece_id"))
                              .otherwise(col("mece_id"))) \
                   .drop("new_mece_id")

# Step 5: Join to get flu_alignment1 and clean up
final_result = result.join(
    df_mece.select("mece_id", "flu_alignment1"),
    on="mece_id",
    how="left"
).drop("mece_id", "df1_id")

final_result.show(20, truncate=False)
