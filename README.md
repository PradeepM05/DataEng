################# Flu & Sub-Flu Assignment #################

from pyspark.sql import functions as f
from pyspark.sql.window import Window

# Part 1: Exact match where itmcd contains dot
org_match_1 = (
    spark.table("vctr_lh_cdar_cmpl_ff_sec.cexref_org_table")
    .where((f.col("bactiveflag") == 1) & (f.col("itmcd").like("%.%")))
    .alias("org")
    .join(
        df_rpt_custint_concerns.select("hiercd10dot").distinct().alias("h"),
        f.col("org.itmcd") == f.col("h.hiercd10dot"),
        "inner"
    )
    .select(
        f.lit(1).alias("seqno"),
        f.col("org.lob"),
        f.col("org.sublob"),
        f.col("h.hiercd10dot")
    )
    .distinct()
)

# Part 2: Pattern match where itmcd does NOT contain dot
org_match_2 = (
    spark.table("vctr_lh_cdar_cmpl_ff_sec.cexref_org_table")
    .where((f.col("bactiveflag") == 1) & (~f.col("itmcd").like("%.%")))
    .alias("org")
    .join(
        df_rpt_custint_concerns.select("hiercd10dot").distinct().alias("h"),
        f.col("h.hiercd10dot").rlike(f.concat(f.lit("^"), f.col("org.itmcd"))),
        "inner"
    )
    .select(
        f.lit(2).alias("seqno"),
        f.col("org.lob"),
        f.col("org.sublob"),
        f.col("h.hiercd10dot")
    )
    .distinct()
)

# Union and get first match per hiercd10dot
org_match = (
    org_match_1.union(org_match_2)
    .withColumn("row_no", f.row_number().over(Window.partitionBy("hiercd10dot").orderBy("seqno")))
    .where(f.col("row_no") == 1)
    .select("hiercd10dot", "lob", "sublob")
)

# Assign FLU columns
flu_condition = (f.col("cdtl_subcmpinid") == 1) & (f.col("cncrn_cncrnid") == 1) & (f.col("caseown") == f.col("creatbynbk"))

df_rpt_custint_concerns = (
    df_rpt_custint_concerns
    .join(org_match, "hiercd10dot", "left")
    .withColumn("cex_assigned_flu", f.when(flu_condition, f.coalesce("lob", f.lit("unavailable"))))
    .withColumn("cex_assigned_sub_flu", f.when(flu_condition, f.coalesce("sublob", f.lit("unavailable"))))
    .withColumn("cex_submit_flu", f.when(flu_condition, f.coalesce("lob", f.lit("unavailable"))))
    .withColumn("cex_submit_sub_flu", f.when(flu_condition, f.coalesce("sublob", f.lit("unavailable"))))
    .drop("lob", "sublob")
)
