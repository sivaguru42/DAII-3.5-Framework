# ============================================================================
# AGGRESSIVE AI ANALYSIS – FOR UNICORN HUNTING
# New file: R/04_modules/07_aggressive_ai_analysis.r
# ============================================================================

run_aggressive_ai_analysis <- function(company_features, output_dir) {
  
  message("🔍 Running aggressive AI analysis for unicorn hunting...")
  
  # 1. Calculate aggressive AI scores
  company_features$ai_score_aggressive <- calculate_ai_score_aggressive(company_features)
  
  # 2. Identify high-potential candidates
  company_features <- identify_high_potential(company_features)
  
  # 3. Reclassify with lower threshold
  aggressive_threshold <- quantile(company_features$ai_score_aggressive, 0.65, na.rm = TRUE)  # Top 35%
  
  company_features <- company_features %>%
    mutate(
      ai_label_aggressive = case_when(
        ai_score_aggressive >= aggressive_threshold ~ "AI Leader (Aggressive)",
        ai_score_aggressive >= quantile(ai_score_aggressive, 0.5, na.rm = TRUE) ~ "AI Adopter",
        ai_score_aggressive >= quantile(ai_score_aggressive, 0.25, na.rm = TRUE) ~ "AI Follower",
        TRUE ~ "AI Laggard"
      )
    )
  
  # 4. Generate unicorn watchlist
  unicorn_watchlist <- company_features %>%
    filter(high_potential_flag == TRUE | high_potential_score %in% c("Top Unicorn Candidate", "Emerging Unicorn Candidate")) %>%
    arrange(desc(ai_score_aggressive)) %>%
    select(ticker, company_name, ai_score_aggressive, revenue_growth, volatility, 
           high_potential_score, rd_intensity, patent_activity)
  
  # 5. Save outputs
  write.csv(unicorn_watchlist, file.path(output_dir, "unicorn_watchlist.csv"), row.names = FALSE)
  
  # 6. Summary
  cat("\n📊 AGGRESSIVE AI ANALYSIS SUMMARY:\n")
  cat("   Standard AI Leaders:", sum(company_features$ai_label == "AI Leader"), "\n")
  cat("   Aggressive AI Leaders:", sum(company_features$ai_label_aggressive == "AI Leader (Aggressive)"), "\n")
  cat("   High-Potential Unicorn Candidates:", sum(company_features$high_potential_flag), "\n")
  cat("   Top Unicorn Candidates:", sum(company_features$high_potential_score == "Top Unicorn Candidate"), "\n")
  
  return(list(
    data = company_features,
    unicorn_watchlist = unicorn_watchlist
  ))
}