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









import sys
import time
import os
import pyspark.sql.functions as f
from pyspark.sql.functions import col, to_date

from src.cmn_util.EcrLogger import EcrLogger as log
from src.cmn_util.ecr_ctl_process_log import ecr_ctl_process_log as process_log
import src.cmn_util.incr_strt_dt_util as incr_strt_dt_util
from src.util.CreateSparkSession import CreateSparkSession as sparkSession
import src.util.vap_ecr_src_tgt_tbl_nm as tbl_nm
import src.util.vap_ecr_prcs_hist as vap_ecr_prcs_hist  # <-- ADD THIS

logger = log.getLogger('', 'vap_ecr_rpt_custint_fftxt_m')


class vap_ecr_rpt_custint_fftxt_m():

    def __init__(self):
        self.PROCESS_MODE           = 'MONTHLY_LOCK'
        self.ecr_ctrl_prcss_log_lst = []
        self.STARTED                = tbl_nm.STARTED
        self.FAILED                 = tbl_nm.FAILED
        self.COMPLETED              = tbl_nm.COMPLETED
        self.STEP                   = tbl_nm.tgt_ecr_rpt_custint_fftxt_m
        self.log_pth                = ''
        self.spark                  = sparkSession().getSparksession()

    # ------------------------------------------------------------------ #
    #  Data lock calendar lookup                                           #
    # ------------------------------------------------------------------ #
    def _get_data_lock_row_for_today(self, spark):
        """
        Return today's data lock calendar row (start_date, end_date, report_month)
        or None if today is not a data lock date.
        """
        cal_tbl = tbl_nm.src_data_lock_calendar
        logger.info(f"Reading data lock calendar :: {cal_tbl}")

        df_cal = (spark.table(cal_tbl)
                  .filter(f.to_date(f.col("data_lock_date")) == f.current_date())
                  .select(
                      f.to_date(f.col("start_date")).alias("start_date"),
                      f.to_date(f.col("end_date")).alias("end_date"),
                      f.to_date(f.col("report_month")).alias("report_month"))
                  .limit(1))
        rows = df_cal.collect()
        return rows[0] if rows else None

    # ------------------------------------------------------------------ #
    #  Core processing                                                     #
    # ------------------------------------------------------------------ #
    def create_vap_ecr_rpt_custint_fftxt_m(self, spark):
        try:
            strt_time = time.perf_counter()

            src_tbl        = tbl_nm.tgt_ecr_rpt_custint_fftxt      # daily active FFT
            tgt_active_tbl = tbl_nm.tgt_ecr_rpt_custint_fftxt_m    # monthly active
            tgt_hist_tbl   = tbl_nm.tgt_ecr_rpt_custint_fftxt_m_hist  # monthly hist

            logger.info(f'{tgt_active_tbl} DataLoad Started...')
            self.ecr_ctrl_prcss_log_lst.append(
                process_log.collect_ecr_ctl_process_log(
                    '', tgt_active_tbl, self.STARTED, self.STEP,
                    self.PROCESS_MODE, '', '',
                    self.ecr_ctrl_prcss_log_lst))

            # -------- Data lock date check --------------------------------
            lock_row = self._get_data_lock_row_for_today(spark)
            if lock_row is None:
                logger.info("Today is not a data lock date. "
                            "Skipping monthly load.")
                self.ecr_ctrl_prcss_log_lst.append(
                    process_log.collect_ecr_ctl_process_log(
                        '', tgt_active_tbl, self.COMPLETED, self.STEP,
                        self.PROCESS_MODE,
                        'skipped - not a data lock date',
                        '', self.ecr_ctrl_prcss_log_lst))
                return

            start_date   = lock_row["start_date"]
            end_date     = lock_row["end_date"]
            report_month = lock_row["report_month"]
            logger.info(f"Data lock detected. start_date={start_date} | "
                        f"end_date={end_date} | report_month={report_month}")

            # -------- Pull FFT rows for the lock window -------------------
            df_src = spark.table(src_tbl)
            df_src.printSchema()

            df_locked = df_src.filter(
                (f.to_date(f.col("entrdttm")) >= f.lit(start_date)) &
                (f.to_date(f.col("entrdttm")) <= f.lit(end_date))
            )

            # -------- Add monthly lock columns ----------------------------
            df_locked = (
                df_locked
                .withColumn("locked_src",   f.lit("ecr"))
                .withColumn("locked_dt",    f.current_timestamp().cast("date"))
                .withColumn("report_month", f.lit(report_month).cast("date"))
                .withColumn("ecr_cs_crt_yyyymm",
                            f.col("ecr_cs_crt_yyyymm").cast("int"))
            )

            rec_cnt = df_locked.count()
            logger.info(f"Records to be locked count :: {rec_cnt}")
            df_locked.printSchema()

            if rec_cnt == 0:
                logger.warning("No FFT records found in the reporting month")
                self.ecr_ctrl_prcss_log_lst.append(
                    process_log.collect_ecr_ctl_process_log(
                        '', tgt_active_tbl, self.COMPLETED, self.STEP,
                        self.PROCESS_MODE,
                        'No source rows in reporting month window',
                        '', self.ecr_ctrl_prcss_log_lst))
                return

            # -------- Repartition -----------------------------------------
            df_final = df_locked.repartition(
                tbl_nm.NO_OF_REPARTITIONS,
                tbl_nm.PARTITION_COL_SOR
            )
            df_final.cache()

            # ================================================================
            # KEY CHANGE: Use vap_ecr_prcs_hist.process_hist() instead of
            # blind append.  This deduplicates against existing history using
            # ecr_chksum, writes only net-new / changed records, and then
            # rebuilds the active table from the ranked history — exactly the
            # same pattern as vap_ecr_rpt_custint_fftxt.py.
            # ================================================================

            # Build the dirty-partition frame so process_hist knows which
            # ecr_cs_crt_yyyymm partitions to touch (mirrors fftxt.py pattern)
            df_dirty_partition = (
                df_final
                .select("ecr_sor", "ecr_cs_crt_yyyymm")
                .distinct()
            )

            # Delegate history + active rebuild to the shared utility
            vap_ecr_prcs_hist.process_hist(
                spark,
                tgt_active_tbl,          # ctr_tbl_nm  — monthly active table
                tgt_active_tbl,          # vap_ecr_tbl_nm — same target
                df_final,                # ctr_src_df  — locked FFT rows
                tgt_hist_tbl,            # hist_tbl_nm
                str(start_date),         # incr_start_dt — lock window start
                self.PROCESS_MODE        # process_mode
            )

            # -------- Rebuild active from ranked history ------------------
            # Rank history within each (ecr_sor, cmplnid) and keep only the
            # latest non-deleted record, restricted to dirty partitions.
            from pyspark.sql.window import Window

            windowspec_hist = (Window
                               .partitionBy("ecr_sor", "cmplnid")
                               .orderBy(f.desc("ecr_etl_load_ts")))
            rank_hist = f.row_number().over(windowspec_hist)

            # Load history rows that belong to the dirty partitions only
            hist_df = (
                spark.table(tgt_hist_tbl)
                .join(df_dirty_partition,
                      on=["ecr_sor", "ecr_cs_crt_yyyymm"],
                      how="left_semi")
            )

            hist_ranked = (
                hist_df
                .withColumn("hist_ranking", rank_hist)
                .filter(f.col("hist_ranking") == 1)
                .filter(f.col("ecr_is_rec_del").isNull())   # exclude soft-deletes
            )

            # Select only columns present in the monthly active table schema
            active_cols = [c.name for c in
                           spark.table(tgt_active_tbl).schema]
            final_active_df = (
                hist_ranked
                .select([c for c in active_cols
                         if c in hist_ranked.columns])
                .repartition(tbl_nm.NO_OF_REPARTITIONS,
                             tbl_nm.PARTITION_COL_SOR)
            )

            # Truncate the monthly active HDFS partition then reload
            logger.info("Truncating active monthly table for full load...")
            os.system('hdfs dfs -rm -r -f -skipTrash '
                      + tbl_nm.HDFS_LOC
                      + tbl_nm.ecr_rpt_custint_fftxt_m_fl_nm
                      + '/*')

            final_active_df.write.insertInto(tgt_active_tbl, overwrite=True)

            df_final.unpersist()

            # -------- Success logging -------------------------------------
            self.ecr_ctrl_prcss_log_lst.append(
                process_log.collect_ecr_ctl_process_log(
                    '', tgt_active_tbl, self.COMPLETED, self.STEP,
                    self.PROCESS_MODE, '', '',
                    self.ecr_ctrl_prcss_log_lst))

            end_time = time.perf_counter()
            logger.info(f'{tgt_active_tbl} DataLoad Finished in '
                        f'{round(((end_time - strt_time) / 60), 3)} min(s)')

        except Exception as e:
            logger.exception("Oops ! Exception while processing "
                             "vap_ecr_rpt_custint_fftxt_m :: " + str(e))
            self.ecr_ctrl_prcss_log_lst.append(
                process_log.collect_ecr_ctl_process_log(
                    '', tbl_nm.tgt_ecr_rpt_custint_fftxt_m, self.FAILED,
                    self.STEP, self.PROCESS_MODE, str(e), '',
                    self.ecr_ctrl_prcss_log_lst))
            raise e

    # ------------------------------------------------------------------ #
    #  Entry point                                                         #
    # ------------------------------------------------------------------ #
    def process_tables(self, arg_dict):
        try:
            strt_time = time.perf_counter()
            logger.info("Inside vap_ecr_rpt_custint_fftxt_m main......")

            self.create_vap_ecr_rpt_custint_fftxt_m(self.spark)

            end_time = time.perf_counter()
            logger.info(f'vap_ecr_rpt_custint_fftxt_m DataLoad Finished in '
                        f'{round(((end_time - strt_time) / 60), 3)} min(s)')

        except Exception as e:
            self.ecr_ctrl_prcss_log_lst.append(
                process_log.collect_ecr_ctl_process_log(
                    '', self.STEP, self.FAILED, self.STEP,
                    self.PROCESS_MODE, str(e), '',
                    self.ecr_ctrl_prcss_log_lst))
            logger.exception("Oops ! Exception in "
                             "vap_ecr_rpt_custint_fftxt_m.process_tables :: "
                             "Exiting the system :: " + str(e))
            sys.exit(1)

        finally:
            process_log.write_ctrl_prcss_log_list(
                '', self.spark, self.ecr_ctrl_prcss_log_lst)
            logger.info('Exit - vap_ecr_rpt_custint_fftxt_m !!')
            self.spark.stop()
