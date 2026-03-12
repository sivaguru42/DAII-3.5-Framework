# =============================================================================
# OUTPUT: Export CSV Files
# Version: 1.0 | Date: 2026-03-11
# Description: Saves all pipeline outputs to CSV files
# =============================================================================

#' Export all pipeline outputs to CSV
#'
#' @param df Full dataframe with all results
#' @param run_dir Output directory path
#' @param run_timestamp Run timestamp
#' @return NULL (writes files to disk)
#' @export
export_all_outputs <- function(df, run_dir, run_timestamp) {

  message("💾 Exporting all outputs to CSV...")

  # 1. Core company data
  write.csv(df, 
            file.path(run_dir, paste0(run_timestamp, "_01_company_features_full.csv")), 
            row.names = FALSE)

  # 2. AI scores (simplified)
  write.csv(df[, c("ticker", "ai_score", "innovation_score", "ai_quartile", 
                   "innovation_quartile", "ai_label", "innovation_label", "in_portfolio")], 
            file.path(run_dir, paste0(run_timestamp, "_02_ai_scores.csv")), 
            row.names = FALSE)

  # 3. Portfolio companies only
  portfolio_only <- df %>% dplyr::filter(in_portfolio == TRUE)
  
  # Quality Innovators
  qual_innov <- portfolio_only %>%
    dplyr::filter(quality_innovators_weight > 0) %>%
    dplyr::select(ticker, quality_innovators_weight, innovation_score, 
                  volatility, total_fund_weight)
  write.csv(qual_innov, 
            file.path(run_dir, paste0(run_timestamp, "_03_quality_innovators.csv")), 
            row.names = FALSE)

  # AI Concentrated
  ai_conc <- portfolio_only %>%
    dplyr::filter(ai_concentrated_weight > 0) %>%
    dplyr::select(ticker, ai_concentrated_weight, ai_score, total_fund_weight)
  write.csv(ai_conc,
            file.path(run_dir, paste0(run_timestamp, "_04_ai_concentrated.csv")),
            row.names = FALSE)

  # Balanced Growth
  balanced <- portfolio_only %>%
    dplyr::filter(balanced_growth_weight > 0) %>%
    dplyr::select(ticker, balanced_growth_weight, innovation_score, ai_score, total_fund_weight)
  write.csv(balanced,
            file.path(run_dir, paste0(run_timestamp, "_05_balanced_growth.csv")),
            row.names = FALSE)

  # 4. Discovery universe
  discovery_only <- df %>% dplyr::filter(in_portfolio == FALSE)
  
  # Full discovery universe
  write.csv(discovery_only %>%
              dplyr::select(ticker, innovation_score, ai_score, discovery_tier,
                           innovation_rank, ai_rank, combined_rank,
                           discovery_quality_weight, discovery_ai_weight, 
                           discovery_balanced_weight) %>%
              dplyr::arrange(combined_rank),
            file.path(run_dir, paste0(run_timestamp, "_06_discovery_full_universe.csv")),
            row.names = FALSE)

  # 5. Anomaly files
  write.csv(df[, c("ticker", "anomaly_score", "is_anomaly", "ai_score", 
                   "innovation_score", "in_portfolio")],
            file.path(run_dir, paste0(run_timestamp, "_07_anomaly_scores.csv")),
            row.names = FALSE)

  # 6. Feature importance
  if (exists("feature_importance") && !is.null(feature_importance)) {
    write.csv(feature_importance,
              file.path(run_dir, paste0(run_timestamp, "_08_feature_importance.csv")),
              row.names = FALSE)
  }

  message("   ✅ All CSV files exported successfully")
}