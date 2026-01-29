# ============================================================================
# MODULE 3: SCORING ENGINE - Component Score Calculation
# ============================================================================
#
# PURPOSE: Calculate normalized component scores (0-100 scale)
#
# STATISTICAL CONCEPTS:
# 1. Normalization: Min-max scaling to 0-100 range
# 2. Transformation: Log transformations for skewed distributions
# 3. Winsorization: Implicit through normalization bounds
# 4. Standardization: Z-score alternative (not used here)
#
# THEORETICAL JUSTIFICATIONS:
# 1. R&D Intensity: Log transform addresses extreme skew from market cap variation
# 2. Patent Count: Log transform for count data (Poisson-like distribution)
# 3. Analyst Ratings: Linear normalization preserves ordinal nature
# 4. News Sentiment: Direct normalization of continuous scores
# 5. Growth Rates: Direct normalization, outlier robust
#
# DESIGN DECISIONS:
# 1. 0-100 scale: Intuitive interpretation, percentile-like
# 2. Company-level normalization: Relative to sample, not absolute
# 3. Handle zeros/infinites: Add small constants before log transforms
# 4. Component independence: Calculate separately before weighting
#
# ============================================================================

calculate_component_scores <- function(imputed_data,
                                       ticker_col,
                                       weights_config = list(
                                         R_D = 0.30,
                                         Analyst = 0.20,
                                         Patent = 0.25,
                                         News = 0.10,
                                         Growth = 0.15
                                       )) {
  #' Calculate Normalized Component Scores (0-100 scale)
  #' 
  #' @param imputed_data Data with imputed numeric columns
  #' @param ticker_col Ticker column name
  #' @param weights_config List of component weights
  #' @return Data frame with component scores and intermediate calculations
  #' 
  #' STATISTICAL CONCEPT: Multi-component scoring with normalization
  
  cat("\n🧮 CALCULATING COMPONENT SCORES\n")
  cat(paste(rep("-", 50), collapse = ""), "\n")
  
  scores_data <- imputed_data
  
  # 1. CALCULATE R&D INTENSITY SCORE (30% weight)
  cat("\n1️⃣ R&D INTENSITY SCORE (Weight: 30%)\n")
  
  # Calculate raw R&D intensity
  scores_data$R_D_Intensity_Raw <- scores_data$R.D.Exp_num / scores_data$Mkt.Cap_num
  
  # Handle infinite/NA values
  inf_mask <- is.infinite(scores_data$R_D_Intensity_Raw) | 
              is.na(scores_data$R_D_Intensity_Raw)
  
  if(any(inf_mask)) {
    median_val <- median(scores_data$R_D_Intensity_Raw[!inf_mask], na.rm = TRUE)
    scores_data$R_D_Intensity_Raw[inf_mask] <- median_val
    cat(sprintf("   Fixed %d infinite/NA values with median: %.6f\n", 
                sum(inf_mask), median_val))
  }
  
  # Apply log transformation (CRITICAL STEP)
  scores_data$R_D_Intensity_Log <- log(scores_data$R_D_Intensity_Raw + 1e-10)
  
  # Normalize to 0-100 scale
  scores_data$R_D_Score <- normalize_to_100(scores_data$R_D_Intensity_Log)
  
  cat(sprintf("   Raw range: %.2e to %.2e\n", 
              min(scores_data$R_D_Intensity_Raw, na.rm = TRUE),
              max(scores_data$R_D_Intensity_Raw, na.rm = TRUE)))
  cat(sprintf("   Log range: %.2f to %.2f\n",
              min(scores_data$R_D_Intensity_Log, na.rm = TRUE),
              max(scores_data$R_D_Intensity_Log, na.rm = TRUE)))
  cat(sprintf("   Score range: %.1f to %.1f\n",
              min(scores_data$R_D_Score, na.rm = TRUE),
              max(scores_data$R_D_Score, na.rm = TRUE)))
  
  # 2. CALCULATE ANALYST SENTIMENT SCORE (20% weight)
  cat("\n2️⃣ ANALYST SENTIMENT SCORE (Weight: 20%)\n")
  
  scores_data$Analyst_Score <- normalize_to_100(scores_data$BEst.Analyst.Rtg_num)
  
  cat(sprintf("   Raw range: %.1f to %.1f\n",
              min(scores_data$BEst.Analyst.Rtg_num, na.rm = TRUE),
              max(scores_data$BEst.Analyst.Rtg_num, na.rm = TRUE)))
  cat(sprintf("   Score range: %.1f to %.1f\n",
              min(scores_data$Analyst_Score, na.rm = TRUE),
              max(scores_data$Analyst_Score, na.rm = TRUE)))
  
  # 3. CALCULATE PATENT ACTIVITY SCORE (25% weight)
  cat("\n3️⃣ PATENT ACTIVITY SCORE (Weight: 25%)\n")
  
  # Apply log transformation to patent count
  scores_data$Patents_Log <- log(scores_data$Patents...Trademarks...Copy.Rgt_num + 1)
  scores_data$Patent_Score <- normalize_to_100(scores_data$Patents_Log)
  
  cat(sprintf("   Raw range: %.0f to %.0f\n",
              min(scores_data$Patents...Trademarks...Copy.Rgt_num, na.rm = TRUE),
              max(scores_data$Patents...Trademarks...Copy.Rgt_num, na.rm = TRUE)))
  cat(sprintf("   Log range: %.2f to %.2f\n",
              min(scores_data$Patents_Log, na.rm = TRUE),
              max(scores_data$Patents_Log, na.rm = TRUE)))
  cat(sprintf("   Score range: %.1f to %.1f\n",
              min(scores_data$Patent_Score, na.rm = TRUE),
              max(scores_data$Patent_Score, na.rm = TRUE)))
  
  # 4. CALCULATE NEWS SENTIMENT SCORE (10% weight)
  cat("\n4️⃣ NEWS SENTIMENT SCORE (Weight: 10%)\n")
  
  scores_data$News_Score <- normalize_to_100(scores_data$News.Sent_num)
  
  cat(sprintf("   Raw range: %.2f to %.2f\n",
              min(scores_data$News.Sent_num, na.rm = TRUE),
              max(scores_data$News.Sent_num, na.rm = TRUE)))
  cat(sprintf("   Score range: %.1f to %.1f\n",
              min(scores_data$News_Score, na.rm = TRUE),
              max(scores_data$News_Score, na.rm = TRUE)))
  
  # 5. CALCULATE GROWTH MOMENTUM SCORE (15% weight)
  cat("\n5️⃣ GROWTH MOMENTUM SCORE (Weight: 15%)\n")
  
  scores_data$Growth_Score <- normalize_to_100(scores_data$Rev...1.Yr.Gr_num)
  
  cat(sprintf("   Raw range: %.1f%% to %.1f%%\n",
              min(scores_data$Rev...1.Yr.Gr_num, na.rm = TRUE),
              max(scores_data$Rev...1.Yr.Gr_num, na.rm = TRUE)))
  cat(sprintf("   Score range: %.1f to %.1f\n",
              min(scores_data$Growth_Score, na.rm = TRUE),
              max(scores_data$Growth_Score, na.rm = TRUE)))
  
  # 6. CALCULATE DAII 3.5 COMPOSITE SCORE
  cat("\n6️⃣ CALCULATING DAII 3.5 COMPOSITE SCORE\n")
  
  scores_data$DAII_3.5_Score <- round(
    scores_data$R_D_Score * weights_config$R_D +
    scores_data$Analyst_Score * weights_config$Analyst +
    scores_data$Patent_Score * weights_config$Patent +
    scores_data$News_Score * weights_config$News +
    scores_data$Growth_Score * weights_config$Growth,
    1
  )
  
  # Create quartiles
  scores_data$DAII_Quartile <- cut(
    scores_data$DAII_3.5_Score,
    breaks = quantile(scores_data$DAII_3.5_Score, 
                      probs = c(0, 0.25, 0.5, 0.75, 1), 
                      na.rm = TRUE),
    labels = c("Q4 (Low)", "Q3", "Q2", "Q1 (High)"),
    include.lowest = TRUE
  )
  
  cat(sprintf("   DAII 3.5 Score range: %.1f to %.1f\n",
              min(scores_data$DAII_3.5_Score, na.rm = TRUE),
              max(scores_data$DAII_3.5_Score, na.rm = TRUE)))
  cat(sprintf("   Mean DAII: %.1f, Median: %.1f, SD: %.1f\n",
              mean(scores_data$DAII_3.5_Score, na.rm = TRUE),
              median(scores_data$DAII_3.5_Score, na.rm = TRUE),
              sd(scores_data$DAII_3.5_Score, na.rm = TRUE)))
  
  # 7. CALCULATE COMPONENT CORRELATIONS
  cat("\n7️⃣ ANALYZING COMPONENT CORRELATIONS\n")
  
  component_cols <- c("R_D_Score", "Analyst_Score", "Patent_Score", 
                      "News_Score", "Growth_Score")
  
  cor_matrix <- cor(scores_data[, component_cols], use = "complete.obs")
  daii_correlations <- cor(scores_data[, component_cols], 
                          scores_data$DAII_3.5_Score, 
                          use = "complete.obs")
  
  cat("   Correlation with DAII 3.5:\n")
  for(i in seq_along(daii_correlations)) {
    comp_name <- rownames(daii_correlations)[i]
    corr_value <- daii_correlations[i]
    cat(sprintf("   • %-15s: r = %.3f\n", comp_name, corr_value))
  }
  
  # 8. VALIDATE SCORE DISTRIBUTIONS
  cat("\n8️⃣ VALIDATING SCORE DISTRIBUTIONS\n")
  
  distribution_stats <- data.frame(
    Component = component_cols,
    Mean = sapply(scores_data[, component_cols], mean, na.rm = TRUE),
    Median = sapply(scores_data[, component_cols], median, na.rm = TRUE),
    SD = sapply(scores_data[, component_cols], sd, na.rm = TRUE),
    Skewness = sapply(scores_data[, component_cols], 
                     function(x) moments::skewness(x, na.rm = TRUE)),
    Kurtosis = sapply(scores_data[, component_cols],
                     function(x) moments::kurtosis(x, na.rm = TRUE)),
    stringsAsFactors = FALSE
  )
  
  print(distribution_stats)
  
  return(list(
    scores_data = scores_data,
    component_stats = distribution_stats,
    correlations = list(
      component_matrix = cor_matrix,
      daii_correlations = daii_correlations
    ),
    weights_applied = weights_config,
    quartile_breakpoints = quantile(scores_data$DAII_3.5_Score, 
                                   probs = c(0, 0.25, 0.5, 0.75, 1), 
                                   na.rm = TRUE)
  ))
}

normalize_to_100 <- function(x, cap_extremes = TRUE, 
                             lower_bound = 0.01, upper_bound = 0.99) {
  #' Normalize Vector to 0-100 Scale
  #' 
  #' @param x Numeric vector to normalize
  #' @param cap_extremes Whether to cap extreme values
  #' @param lower_bound Lower percentile cap (if capping)
  #' @param upper_bound Upper percentile cap (if capping)
  #' @return Normalized vector (0-100 scale)
  #' 
  #' STATISTICAL CONCEPT: Min-max scaling with optional winsorization
  
  if(all(is.na(x))) {
    return(rep(NA, length(x)))
  }
  
  # Handle infinite values
  x_finite <- x[is.finite(x) & !is.na(x)]
  
  if(length(x_finite) == 0) {
    return(rep(50, length(x)))  # Default to midpoint
  }
  
  # Optional winsorization
  if(cap_extremes && length(x_finite) > 10) {
    lower_threshold <- quantile(x_finite, lower_bound, na.rm = TRUE)
    upper_threshold <- quantile(x_finite, upper_bound, na.rm = TRUE)
    
    x_capped <- pmin(pmax(x, lower_threshold, na.rm = TRUE), 
                     upper_threshold, na.rm = TRUE)
    x_finite <- x_capped[is.finite(x_capped) & !is.na(x_capped)]
  } else {
    x_capped <- x
  }
  
  # Calculate min and max
  min_val <- min(x_finite, na.rm = TRUE)
  max_val <- max(x_finite, na.rm = TRUE)
  
  # Handle case where all values are identical
  if(max_val == min_val) {
    return(rep(50, length(x_capped)))
  }
  
  # Apply normalization
  normalized <- 100 * (x_capped - min_val) / (max_val - min_val)
  
  # Ensure bounds
  normalized <- pmin(pmax(normalized, 0, na.rm = TRUE), 100, na.rm = TRUE)
  
  return(normalized)
}