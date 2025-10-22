from functools import reduce

not_conditions = [f.col(col).rlike('^(?i)NOT.*') for col in join_cols]
not_key_condition = reduce(lambda a, b: a | b, not_conditions)

df_mece_flu2 = df_mece_flu2.withColumn('not_key', f.when(not_key_condition, 1).otherwise(0))

df_mece_flu2 = df_mece_flu2.filter(f.col('not_key') == 0)
