from pyspark.sql import functions as F
from pyspark.sql.window import Window

# 1. Clean sproduct column - remove text in brackets and space before it
psa_df_cleaned = psa_df.withColumn(
    'sproduct_cleaned',
    F.regexp_replace(F.col('sproduct'), r'\s*\[.*?\]', '')
)

# 2. For CCA rows, keep only the one with max subsystemid
# First, create a window partitioned by sproductgrp to find max subsystemid for CCA
window_spec = Window.partitionBy('sproductgrp')

psa_df_filtered = psa_df_cleaned.withColumn(
    'max_subsystemid',
    F.when(F.col('sproductgrp') == 'CCA', 
           F.max('subsystemid').over(window_spec))
    .otherwise(F.col('subsystemid'))
)

# Keep rows where:
# - sproductgrp is NOT 'CCA', OR
# - sproductgrp is 'CCA' AND subsystemid equals max_subsystemid
psa_df_final = psa_df_filtered.filter(
    (F.col('sproductgrp') != 'CCA') | 
    (F.col('subsystemid') == F.col('max_subsystemid'))
).drop('max_subsystemid')

# Show results
psa_df_final.orderBy('sproductgrp').show(40, truncate=False)
