from pyspark.sql import functions as F
from pyspark.sql.functions import col, lit, when, concat_ws
from pyspark.sql.window import Window

join_cols = ['rznl1_reason', 'rznl2_detail', 'rznl3_additionaldetail', 
             'rznl4_additionalsubdetail', 'submaltorgmanager1', 'submaltorgmanager2',
             'submaltorgmanager3', 'submaltorgmanager4', 'submaltorgmanager5', 
             'submaltorgmanager6', 'assignorg1', 'assignorg2', 'sproductgrp1', 
             'sproductgrp2']
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Step 1: Add match priority to df2 (count non-null columns)
common_cols = ['rsnl1_reason', 'rsnl2_detail', 'rsnl3_additionaldetail', 
               'rsnl4_additionalsubdetail', 'submaltorgmanager1', 'submaltorgmanager2']

# Count non-null values for priority
df2_with_priority = df2.withColumn(
    'match_priority',
    sum([F.when(F.col(c).isNotNull(), 1).otherwise(0) for c in common_cols])
)

# Step 2: Build dynamic join condition
# Each column in df2: if null, ignore it; if not null, must match df1
join_condition = (
    (df2_with_priority.rsnl1_reason.isNull() | (df1.rsnl1_reason == df2_with_priority.rsnl1_reason)) &
    (df2_with_priority.rsnl2_detail.isNull() | (df1.rsnl2_detail == df2_with_priority.rsnl2_detail)) &
    (df2_with_priority.rsnl3_additionaldetail.isNull() | (df1.rsnl3_additionaldetail == df2_with_priority.rsnl3_additionaldetail)) &
    (df2_with_priority.rsnl4_additionalsubdetail.isNull() | (df1.rsnl4_additionalsubdetail == df2_with_priority.rsnl4_additionalsubdetail)) &
    (df2_with_priority.submaltorgmanager1.isNull() | (df1.submaltorgmanager1 == df2_with_priority.submaltorgmanager1)) &
    (df2_with_priority.submaltorgmanager2.isNull() | (df1.submaltorgmanager2 == df2_with_priority.submaltorgmanager2))
)

# Step 3: Perform left join with broadcast hint (df2 is small)
joined_df = df1.join(
    F.broadcast(df2_with_priority),
    join_condition,
    'left'
)

# Step 4: Handle multiple matches - keep the one with highest priority
# Window spec: partition by df1's unique key, order by priority descending
windowSpec = Window.partitionBy('cmpinid').orderBy(F.desc('match_priority'))

# Add row number
ranked_df = joined_df.withColumn('rank', F.row_number().over(windowSpec))

# Keep only rank 1 (best match for each df1 row)
best_matches = ranked_df.filter(F.col('rank') == 1)

# Step 5: Select final columns - all df1 columns + 5 flu_alignment columns
# Get all original df1 columns
df1_columns = df1.columns

# Columns to add from df2
flu_columns = ['flu_alignment1', 'flu_alignment2', 'flu_alignment3', 
               'flu_alignment4', 'flu_alignment5']

# Final selection
final_df = best_matches.select(
    *df1_columns,
    *flu_columns
)

# Show results
final_df.show(20, truncate=False)
print(f"Final row count: {final_df.count()}")
print(f"Original df1 row count: {df1.count()}")
