# ============================================================================
# MODULE 4: AGGREGATION FRAMEWORK - Weighted Score Calculation
# ============================================================================
#
# PURPOSE: Aggregate component scores into final DAII 3.5 scores
#
# STATISTICAL CONCEPTS:
# 1. Weighted Mean: Linear combination with predetermined weights
# 2. Weight Sensitivity: Analysis of weight changes on rankings
# 3. Score Stability: Assessment of small changes on final scores
# 4. Rank Correlation: Spearman/Kendall correlation of different weightings
#
# THEORETICAL CONSIDERATIONS:
# 1. Weight Determination: Based on innovation theory and empirical testing
# 2. Weight Stability: Test robustness to weight variations
# 3. Component Independence: Assumption in linear aggregation
# 4. Scale Interpretation: 0-100 as innovation percentile
#
# DESIGN FEATURES:
# 1. Configurable weights: Easy adjustment for sensitivity analysis
# 2. Multiple weighting schemes: Test alternatives
# 3. Portfolio-level aggregation: Market-value weighted averages
# 4. Industry benchmarking: Relative performance assessment
#
# ============================================================================

calculate_daii_scores <- function(scores_data,
                                  ticker_col,
                                  weights_config = list(
                                    default = c(R_D = 0.30, Analyst = 0.20, 
                                               Patent = 0.25, News = 0.10, 
                                               Growth = 0.15),
                                    alternative1 = c(R_D = 0.35, Analyst = 0.15,
                                                    Patent = 0.25, News = 0.10,
                                                    Growth = 0.15),
                                    alternative2 = c(R_D = 0.25, Analyst = 0.25,
                                                    Patent = 0.20, News = 0.15,
                                                    Growth = 0.15)
                                  ),
                                  industry_col = "GICS.Ind.Grp.Name") {
  #' Calculate DAII 3.5 Scores with Multiple Weighting Schemes
  #' 
  #' @param scores_data Data with component scores
  #' @param ticker_col Ticker column name
  #' @param weights_config List of weight configurations
  #' @param industry_col Industry classification column
  #' @return Data with multiple DAII score versions and sensitivity analysis
  #' 
  #' STATISTICAL CONCEPT: Multi-scenario analysis for weight sensitivity
  
  cat("\n⚖️ CALCULATING DAII 3.5 SCORES WITH WEIGHT SENSITIVITY\n")
  cat(paste(rep("=", 60), collapse = ""), "\n")
  
  daii_data <- scores_data
  
  # Apply each weighting scheme
  weight_names <- names(weights_config)
  
  for(weight_name in weight_names) {
    weights <- weights_config[[weight_name]]
    
    # Calculate weighted score
    score_col <- paste0("DAII_3.5_", weight_name)
    quartile_col <- paste0("DAII_Quartile_", weight_name)
    
    daii_data[[score_col]] <- round(
      daii_data$R_D_Score * weights["R_D"] +
      daii_data$Analyst_Score * weights["Analyst"] +
      daii_data$Patent_Score * weights["Patent"] +
      daii_data$News_Score * weights["News"] +
      daii_data$Growth_Score * weights["Growth"],
      1
    )
    
    # Create quartiles for this weighting
    daii_data[[quartile_col]] <- cut(
      daii_data[[score_col]],
      breaks = quantile(daii_data[[score_col]], 
                       probs = c(0, 0.25, 0.5, 0.75, 1), 
                       na.rm = TRUE),
      labels = c("Q4 (Low)", "Q3", "Q2", "Q1 (High)"),
      include.lowest = TRUE
    )
    
    cat(sprintf("\n📊 Weighting Scheme: %s\n", weight_name))
    cat(sprintf("   Weights: R&D=%.0f%%, Analyst=%.0f%%, Patent=%.0f%%, ",
                weights["R_D"] * 100, weights["Analyst"] * 100, 
                weights["Patent"] * 100))
    cat(sprintf("News=%.0f%%, Growth=%.0f%%\n",
                weights["News"] * 100, weights["Growth"] * 100))
    cat(sprintf("   Score range: %.1f to %.1f\n",
                min(daii_data[[score_col]], na.rm = TRUE),
                max(daii_data[[score_col]], na.rm = TRUE)))
    cat(sprintf("   Mean: %.1f, SD: %.1f\n",
                mean(daii_data[[score_col]], na.rm = TRUE),
                sd(daii_data[[score_col]], na.rm = TRUE)))
  }
  
  # Default to first weighting scheme
  daii_data$DAII_3.5_Score <- daii_data[[paste0("DAII_3.5_", weight_names[1])]]
  daii_data$DAII_Quartile <- daii_data[[paste0("DAII_Quartile_", weight_names[1])]]
  
  # WEIGHT SENSITIVITY ANALYSIS
  cat("\n🔍 WEIGHT SENSITIVITY ANALYSIS\n")
  
  sensitivity_results <- analyze_weight_sensitivity(daii_data, weights_config)
  
  # INDUSTRY BENCHMARKING
  cat("\n🏢 INDUSTRY BENCHMARKING\n")
  
  if(industry_col %in% names(daii_data)) {
    industry_benchmarks <- daii_data %>%
      group_by(!!sym(industry_col)) %>%
      summarise(
        Industry_Count = n(),
        Industry_Mean_DAII = mean(DAII_3.5_Score, na.rm = TRUE),
        Industry_Median_DAII = median(DAII_3.5_Score, na.rm = TRUE),
        Industry_SD_DAII = sd(DAII_3.5_Score, na.rm = TRUE),
        .groups = 'drop'
      ) %>%
      arrange(desc(Industry_Mean_DAII))
    
    # Add industry percentile rank
    daii_data$Industry_DAII_Percentile <- sapply(1:nrow(daii_data), function(i) {
      industry <- daii_data[[industry_col]][i]
      score <- daii_data$DAII_3.5_Score[i]
      industry_scores <- daii_data$DAII_3.5_Score[daii_data[[industry_col]] == industry]
      round(100 * sum(score >= industry_scores, na.rm = TRUE) / 
            sum(!is.na(industry_scores)), 1)
    })
    
    cat("   Top 5 Industries by Average DAII:\n")
    print(head(industry_benchmarks, 5))
  }
  
  # COMPONENT CONTRIBUTION ANALYSIS
  cat("\n📈 COMPONENT CONTRIBUTION ANALYSIS\n")
  
  contribution_analysis <- daii_data %>%
    mutate(
      R_D_Contribution = R_D_Score * weights_config$default["R_D"],
      Analyst_Contribution = Analyst_Score * weights_config$default["Analyst"],
      Patent_Contribution = Patent_Score * weights_config$default["Patent"],
      News_Contribution = News_Score * weights_config$default["News"],
      Growth_Contribution = Growth_Score * weights_config$default["Growth"]
    ) %>%
    group_by(DAII_Quartile) %>%
    summarise(
      Avg_R_D_Contribution = mean(R_D_Contribution, na.rm = TRUE),
      Avg_Analyst_Contribution = mean(Analyst_Contribution, na.rm = TRUE),
      Avg_Patent_Contribution = mean(Patent_Contribution, na.rm = TRUE),
      Avg_News_Contribution = mean(News_Contribution, na.rm = TRUE),
      Avg_Growth_Contribution = mean(Growth_Contribution, na.rm = TRUE),
      .groups = 'drop'
    )
  
  print(contribution_analysis)
  
  return(list(
    daii_data = daii_data,
    weights_config = weights_config,
    sensitivity_results = sensitivity_results,
    industry_benchmarks = if(exists("industry_benchmarks")) industry_benchmarks else NULL,
    contribution_analysis = contribution_analysis
  ))
}

analyze_weight_sensitivity <- function(daii_data, weights_config) {
  #' Analyze Sensitivity to Weight Changes
  #' 
  #' @param daii_data Data with multiple DAII score versions
  #' @param weights_config List of weight configurations
  #' @return Sensitivity analysis results
  #' 
  #' STATISTICAL CONCEPT: Rank stability analysis under parameter uncertainty
  
  cat("   Analyzing rank changes across weighting schemes...\n")
  
  weight_names <- names(weights_config)
  score_cols <- paste0("DAII_3.5_", weight_names)
  
  # Extract scores
  scores_matrix <- as.matrix(daii_data[, score_cols])
  colnames(scores_matrix) <- weight_names
  
  # Calculate rank correlations
  rank_correlations <- matrix(NA, nrow = length(weight_names), 
                              ncol = length(weight_names))
  rownames(rank_correlations) <- weight_names
  colnames(rank_correlations) <- weight_names
  
  for(i in 1:length(weight_names)) {
    for(j in 1:length(weight_names)) {
      rank_correlations[i, j] <- cor(
        rank(scores_matrix[, i], na.last = "keep"),
        rank(scores_matrix[, j], na.last = "keep"),
        use = "complete.obs",
        method = "spearman"
      )
    }
  }
  
  # Calculate average rank change
  default_ranks <- rank(scores_matrix[, 1], na.last = "keep")
  rank_changes <- data.frame(
    Weight_Scheme = weight_names[-1],
    Avg_Rank_Change = sapply(2:ncol(scores_matrix), function(j) {
      mean(abs(default_ranks - rank(scores_matrix[, j], na.last = "keep")), 
           na.rm = TRUE)
    }),
    Spearman_Correlation = rank_correlations[1, -1],
    Top10_Overlap = sapply(2:ncol(scores_matrix), function(j) {
      default_top10 <- order(scores_matrix[, 1], decreasing = TRUE)[1:10]
      alt_top10 <- order(scores_matrix[, j], decreasing = TRUE)[1:10]
      length(intersect(default_top10, alt_top10))
    }),
    stringsAsFactors = FALSE
  )
  
  # Identify most sensitive companies
  score_differences <- scores_matrix[, -1] - scores_matrix[, 1]
  max_change_companies <- apply(abs(score_differences), 2, which.max)
  
  sensitivity_summary <- list(
    rank_correlations = rank_correlations,
    rank_changes = rank_changes,
    max_change_companies = max_change_companies,
    interpretation = "Higher correlations indicate more stable rankings"
  )
  
  cat("   Rank Correlations Between Weighting Schemes:\n")
  print(round(rank_correlations, 3))
  
  cat("\n   Top 10 Overlap Between Default and Alternatives:\n")
  print(rank_changes[, c("Weight_Scheme", "Top10_Overlap", "Spearman_Correlation")])
  
  return(sensitivity_summary)
}