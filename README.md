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

# Step 3: Initialize result with df1_id
result = df1.withColumn("df1_id", F.monotonically_increasing_id()) \
            .withColumn("mece_id", lit(None).cast("long"))

# Step 4: Process each pattern
for pattern_row in unique_patterns:
    pattern = pattern_row['pattern']
    
    if not pattern:
        continue
    
    non_null_cols = [c for c in pattern.split("|") if c]
    
    # Get df_mece rows with this pattern - only select what we need
    df_mece_pattern = df_mece.filter(col("pattern") == lit(pattern)) \
                             .select(col("mece_id").alias("matched_mece_id"), *non_null_cols)
    
    # Join using aliases to avoid ambiguity
    joined = result.alias("df1_alias").join(
        F.broadcast(df_mece_pattern.alias("mece_alias")),
        on=non_null_cols,
        how="inner"
    ).select(
        col("df1_alias.df1_id"),
        col("mece_alias.matched_mece_id")
    ).distinct()  # Use distinct to get unique df1_id and mece_id combinations
    
    # If a df1 row matches multiple mece rows, take the first one
    window_spec = Window.partitionBy("df1_id").orderBy("matched_mece_id")
    matched = joined.withColumn("rn", F.row_number().over(window_spec)) \
                    .filter(col("rn") == 1) \
                    .select("df1_id", "matched_mece_id")
    
    # Update result - only if mece_id is still null (first match wins)
    result = result.alias("r").join(matched.alias("m"), on="df1_id", how="left") \
                   .withColumn("mece_id", 
                              when(col("r.mece_id").isNull(), col("m.matched_mece_id"))
                              .otherwise(col("r.mece_id"))) \
                   .select("r.*", "mece_id")

# Step 5: Join to get flu_alignment1
final_result = result.join(
    df_mece.select("mece_id", "flu_alignment1"),
    on="mece_id",
    how="left"
).drop("mece_id", "df1_id")

final_result.show(20, truncate=False)
