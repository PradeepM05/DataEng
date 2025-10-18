flu_columns = ['flu_alignment1', 'flu_alignment2', 'flu_alignment3', 
               'flu_alignment4', 'flu_alignment5']

# Identify which columns to group by (common columns that will be used for joining)
# We need to preserve the original structure but aggregate flu values
df2_aggregated = df2_with_priority.groupBy(
    *common_cols,
    'match_priority'
).agg(
    F.concat_ws('|', F.collect_set('flu_alignment1')).alias('flu_alignment1'),
    F.concat_ws('|', F.collect_set('flu_alignment2')).alias('flu_alignment2'),
    F.concat_ws('|', F.collect_set('flu_alignment3')).alias('flu_alignment3'),
    F.concat_ws('|', F.collect_set('flu_alignment4')).alias('flu_alignment4'),
    F.concat_ws('|', F.collect_set('flu_alignment5')).alias('flu_alignment5')
)
