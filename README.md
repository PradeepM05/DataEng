pythondf_rpt_custint_concerns_exploded = (df_rpt_custint_concerns
    .withColumn('prd_productgroup1_single', 
                F.explode(F.transform(F.split(F.trim(F.col('prd_productgroup1')), ','), lambda x: F.trim(x))))
    .withColumn('prd_productgroup2_single', 
                F.explode(F.transform(F.split(F.trim(F.col('prd_productgroup2')), ','), lambda x: F.trim(x))))
)





        & (df_mece_flu2.sproductgrp1.isNull() | (df_rpt_custint_concerns_exploded.prd_productgroup1_single == df_mece_flu2.sproductgrp1)) \
        & (df_mece_flu2.sproductgrp2.isNull() | (df_rpt_custint_concerns_exploded.prd_productgroup2_single == df_mece_flu2.sproductgrp2)) )


        # Add the exploded helper columns to the drop list
join_cols_extended = join_cols + ['prd_productgroup1_single', 'prd_productgroup2_single']
joined_df = joined_df.drop(*join_cols_extended)
