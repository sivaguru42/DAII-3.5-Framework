# =============================================================================
# OUTPUT: Generate Summary Reports
# Version: 1.0 | Date: 2026-03-11
# Description: Creates text and markdown summary reports of pipeline run
# =============================================================================

#' Generate run summary report
#'
#' @param df Dataframe with final results
#' @param discovery_only Dataframe with discovery universe companies
#' @param run_dir Output directory path
#' @param run_timestamp Run timestamp
#' @return NULL (writes files to disk)
#' @export
generate_summary_report <- function(df, discovery_only, run_dir, run_timestamp) {

  message("📊 Generating summary report...")

  summary_report <- paste(
    sprintf("# DAII 3.5 Run Summary – %s\n\n", run_timestamp),
    "## 📊 OVERVIEW\n",
    sprintf("- Total Companies Analyzed: %d\n", nrow(df)),
    sprintf("- DUMAC Portfolio Companies: %d\n", sum(df$in_portfolio, na.rm = TRUE)),
    sprintf("- Discovery Universe Companies: %d\n", sum(!df$in_portfolio, na.rm = TRUE)),
    sprintf("- AI Leaders (Q4): %d\n", sum(df$ai_quartile == 4, na.rm = TRUE)),
    sprintf("- Anomalies Detected: %d\n", sum(df$is_anomaly, na.rm = TRUE)),
    "\n## 🎯 TOP DISCOVERY OPPORTUNITIES\n",
    paste(capture.output(print(utils::head(
      discovery_only %>%
        dplyr::filter(discovery_tier == "Tier 1: Top 10 Opportunities") %>%
        dplyr::select(ticker, ai_score, innovation_score, discovery_tier), 10))), 
      collapse = "\n"),
    "\n## 📁 OUTPUT FILES GENERATED\n",
    paste(sprintf("- %s", list.files(run_dir)), collapse = "\n"),
    sep = ""
  )

  writeLines(summary_report, file.path(run_dir, "README.md"))
  message("   ✅ Summary report saved")
}