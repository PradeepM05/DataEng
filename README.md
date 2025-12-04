# Simple left join without any filter conditions
joined_df = (df_rpt_custint_concerns.alias("rpt")
    .join(
        df_mece_flu2,
        join_condition,
        "left"
    ))

# Get list of columns from df_mece_flu2 (excluding join keys)
mece_cols = [col for col in df_mece_flu2.columns if col not in join_cols]

# Conditionally null out mece columns where conditions don't match
for col_name in mece_cols:
    joined_df = joined_df.withColumn(
        col_name,
        f.when(
            (f.col("rpt.cdtl_subcmplnid") == 1) & (f.col("rpt.cncrn_cncrnid") == 1),
            f.col(col_name)
        ).otherwise(f.lit(None))
    )

joined_df = joined_df.filter(f.col("not_key") == 0)
joined_df = joined_df.drop(*(join_cols + ["not_key"]))
