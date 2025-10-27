SELECT
    r.cmplnid,
    r.cncrn_prodcd,
    r.hiercd10dot,
    COALESCE(org.lob, 'unavailable') AS assign_flu,
    COALESCE(org.sublob, 'unavailable') AS assign_sub_flu,
    COALESCE(org.lob, 'unavailable') AS submit_flu,
    COALESCE(org.sublob, 'unavailable') AS submit_sub_flu
FROM ecr_lh_cdar_cmpl_sec.ecr_rpt_custint_cncrns_d r
LEFT JOIN ctr_lh_cdar_cmpl_ff_sec.CEXREF_Org org
    ON ((r.hiercd10dot = CONCAT(org.ItmCD, '[top_level]'))
        OR (r.hiercd10dot = CONCAT(org.ItmCD, '[equivalent]')))
    AND org.bActiveFlag = 1
LEFT JOIN ecr_lh_cdar_cmpl_sec.prd_ads_d_hierarchy_emp_final_emp emp
    ON r.caseown = emp.emp_sid
    AND r.creatbynbk = emp.emp_sid
WHERE r.cdtl_subcmplnid = 1
    AND r.cncrn_cncrnid = 1
