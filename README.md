df_rpt_with_flag = df_rpt_custint_concerns.withColumn(
    "should_join",
    (f.col("cdtl_subcmplnid") == 1) & (f.col("cncrn_cncrnid") == 1)
)

# Left join with the flag condition
joined_df = (df_rpt_with_flag.alias("rpt")
    .join(
        df_mece_flu2,
        (join_condition & f.col("rpt.should_join")),
        "left"
    ))
