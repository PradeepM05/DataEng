################# Flu & Sub-Flu Assignment #################
from pyspark.sql import functions as f
from pyspark.sql.window import Window

# Import the table as df
df_org = spark.table("vctr_lh_cdar_cmpl_ff_sec.cexref_org_table").where(f.col("bactiveflag") == 1)
hier_codes = df_rpt_custint_concerns.select("hiercd10dot").distinct()

# Combined matching logic with priority
org_match = (
    # Exact match (priority 1) and pattern match (priority 2) in single query
    df_org.alias("org")
    .join(hier_codes.alias("h"), 
          (f.col("org.itmcd") == f.col("h.hiercd10dot")) |  # Exact match
          ((~f.col("org.itmcd").like("%.%")) & 
           (f.col("h.hiercd10dot").rlike(f.concat(f.lit("^"), f.col("org.itmcd"))))),  # Pattern match
          "inner")
    .select(
        f.when(f.col("org.itmcd") == f.col("h.hiercd10dot"), 1).otherwise(2).alias("seqno"),
        "org.lob", "org.sublob", "h.hiercd10dot"
    )
    .distinct()
    # Get first match per hiercd10dot
    .withColumn("rn", f.row_number().over(Window.partitionBy("hiercd10dot").orderBy("seqno")))
    .where(f.col("rn") == 1)
    .select("hiercd10dot", "lob", "sublob")
)

# Assign FLU columns
flu_cond = (f.col("cdtl_subcmpinid") == 1) & (f.col("cncrn_cncrnid") == 1) & (f.col("caseown") == f.col("creatbynbk"))

df_rpt_custint_concerns = (
    df_rpt_custint_concerns
    .join(org_match, "hiercd10dot", "left")
    .withColumn("cex_assigned_flu", f.when(flu_cond, f.coalesce("lob", f.lit("unavailable"))))
    .withColumn("cex_assigned_sub_flu", f.when(flu_cond, f.coalesce("sublob", f.lit("unavailable"))))
    .withColumn("cex_submit_flu", f.when(flu_cond, f.coalesce("lob", f.lit("unavailable"))))
    .withColumn("cex_submit_sub_flu", f.when(flu_cond, f.coalesce("sublob", f.lit("unavailable"))))
    .drop("lob", "sublob")
)
