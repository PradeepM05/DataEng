import pandas as pd
import numpy as np
from sklearn.feature_extraction.text import 


SELECT 'sreason' AS column_name, sreason AS distinct_value
FROM (
  SELECT DISTINCT sreason 
  FROM vctr_1h_cdar_cmpl_ff_sec.lnd_mece2_flu_mapping_altx_hist
)
UNION ALL
SELECT 'sdetail', sdetail
FROM (
  SELECT DISTINCT sdetail 
  FROM vctr_1h_cdar_cmpl_ff_sec.lnd_mece2_flu_mapping_altx_hist
)
UNION ALL
SELECT 'sadditionaldetail', sadditionaldetail
FROM (
  SELECT DISTINCT sadditionaldetail 
  FROM vctr_1h_cdar_cmpl_ff_sec.lnd_mece2_flu_mapping_altx_hist
)
UNION ALL
SELECT 'sadditionalsubdetail', sadditionalsubdetail
FROM (
  SELECT DISTINCT sadditionalsubdetail 
  FROM vctr_1h_cdar_cmpl_ff_sec.lnd_mece2_flu_mapping_altx_hist
)
UNION ALL
SELECT 'ssubmitorg1', ssubmitorg1
FROM (
  SELECT DISTINCT ssubmitorg1 
  FROM vctr_1h_cdar_cmpl_ff_sec.lnd_mece2_flu_mapping_altx_hist
)
UNION ALL
SELECT 'ssubmitorg2', ssubmitorg2
FROM (
  SELECT DISTINCT ssubmitorg2 
  FROM vctr_1h_cdar_cmpl_ff_sec.lnd_mece2_flu_mapping_altx_hist
)
UNION ALL
SELECT 'ssubmitorg3', ssubmitorg3
FROM (
  SELECT DISTINCT ssubmitorg3 
  FROM vctr_1h_cdar_cmpl_ff_sec.lnd_mece2_flu_mapping_altx_hist
)
UNION ALL
SELECT 'ssubmitorg4', ssubmitorg4
FROM (
  SELECT DISTINCT ssubmitorg4 
  FROM vctr_1h_cdar_cmpl_ff_sec.lnd_mece2_flu_mapping_altx_hist
)
UNION ALL
SELECT 'ssubmitorg5', ssubmitorg5
FROM (
  SELECT DISTINCT ssubmitorg5 
  FROM vctr_1h_cdar_cmpl_ff_sec.lnd_mece2_flu_mapping_altx_hist
)
UNION ALL
SELECT 'ssubmitorg6', ssubmitorg6
FROM (
  SELECT DISTINCT ssubmitorg6 
  FROM vctr_1h_cdar_cmpl_ff_sec.lnd_mece2_flu_mapping_altx_hist
)
UNION ALL
SELECT 'sassignorg1', sassignorg1
FROM (
  SELECT DISTINCT sassignorg1 
  FROM vctr_1h_cdar_cmpl_ff_sec.lnd_mece2_flu_mapping_altx_hist
)
UNION ALL
SELECT 'sassignorg2', sassignorg2
FROM (
  SELECT DISTINCT sassignorg2 
  FROM vctr_1h_cdar_cmpl_ff_sec.lnd_mece2_flu_mapping_altx_hist
)
UNION ALL
SELECT 'flu_alignment1', flu_alignment1
FROM (
  SELECT DISTINCT flu_alignment1 
  FROM vctr_1h_cdar_cmpl_ff_sec.lnd_mece2_flu_mapping_altx_hist
);


