psa_df_cleaned = psa_df.withColumn(
    'sproduct_cleaned',
    F.regexp_replace(F.col('sproduct'), r' \[.*?\]', '')
)
