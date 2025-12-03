window_spec = Window.partitionBy('sproductgrp')

psa_df_final = psa_df_cleaned.withColumn(
    'max_subsystemid',
    F.max('subsystemid').over(window_spec)
).filter(
    (F.col('sproductgrp') != 'CCA') | 
    ((F.col('sproductgrp') == 'CCA') & (F.col('subsystemid') == F.col('max_subsystemid')))
).drop('max_subsystemid')
