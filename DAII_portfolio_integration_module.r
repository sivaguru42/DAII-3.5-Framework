# ============================================================================
# MODULE 5: PORTFOLIO INTEGRATION - Holdings Mapping & Aggregation
# ============================================================================
#
# PURPOSE: Map company scores to holdings and calculate portfolio metrics
#
# STATISTICAL CONCEPTS:
# 1. Weighted Portfolio Statistics: Market-value weighted averages
# 2. Concentration Analysis: Herfindahl-Hirschman Index (HHI)
# 3. Active Share: Deviation from benchmark innovation profile
# 4. Risk Decomposition: Contribution of holdings to portfolio innovation risk
#
# THEORETICAL FRAMEWORK:
# 1. Portfolio Theory (Markowitz): Innovation as a portfolio characteristic
# 2. Factor Investing: Innovation as a systematic factor
# 3. Active Management: Innovation tilt as active decision
# 4. Risk Management: Innovation concentration as risk factor
#
# DESIGN FEATURES:
# 1. Multiple weighting schemes: Equal, market-cap, fund-specific
# 2. Fund-level aggregation: Analyze by investment manager
# 3. Industry decomposition: Innovation exposure by sector
# 4. Contribution analysis: Which holdings drive portfolio innovation?
#
# ============================================================================

integrate_with_portfolio <- function(holdings_data,
                                     daii_scores,
                                     ticker_col,
                                     weight_col = "fund_weight",
                                     fund_col = "fund_name") {
  #' Integrate DAII Scores with Portfolio Holdings
  #' 
  #' @param holdings_data Original holdings data
  #' @param daii_scores Company-level DAII scores
  #' @param ticker_col Ticker column name
  #' @param weight_col Column containing position weights
  #' @param fund_col Column containing fund names
  #' @return Integrated data with portfolio-level analytics
  #' 
  #' STATISTICAL CONCEPT: Portfolio-weighted aggregation of innovation scores
  
  cat("\n🏦 INTEGRATING DAII SCORES WITH PORTFOLIO HOLDINGS\n")
  cat(paste(rep("=", 60), collapse = ""), "\n")
  
  # 1. MERGE SCORES WITH HOLDINGS
  cat("\n1️⃣ MERGING COMPANY SCORES WITH HOLDINGS\n")
  
  # Select score columns to merge
  score_cols <- c(ticker_col, "DAII_3.5_Score", "DAII_Quartile",
                  "R_D_Score", "Analyst_Score", "Patent_Score", 
                  "News_Score", "Growth_Score", "GICS.Ind.Grp.Name")
  
  score_cols <- intersect(score_cols, names(daii_scores))
  
  # Perform merge
  integrated_data <- merge(
    holdings_data,
    daii_scores[, score_cols],
    by = ticker_col,
    all.x = TRUE
  )
  
  cat(sprintf("   Merged DAII scores with %d holdings\n", nrow(integrated_data)))
  cat(sprintf("   Holdings with DAII scores: %d (%.1f%%)\n",
              sum(!is.na(integrated_data$DAII_3.5_Score)),
              100 * sum(!is.na(integrated_data$DAII_3.5_Score)) / 
              nrow(integrated_data)))
  
  # 2. CLEAN AND VALIDATE WEIGHTS
  cat("\n2️⃣ PREPARING PORTFOLIO WEIGHTS\n")
  
  weight_analysis <- clean_portfolio_weights(integrated_data, weight_col)
  integrated_data <- weight_analysis$cleaned_data
  
  cat(sprintf("   Weight column: %s\n", weight_col))
  cat(sprintf("   Total portfolio weight: %.2f%%\n", 
              sum(integrated_data[[weight_col]], na.rm = TRUE) * 100))
  cat(sprintf("   Missing weights: %d\n", 
              sum(is.na(integrated_data[[weight_col]]))))
  
  # 3. CALCULATE PORTFOLIO-LEVEL METRICS
  cat("\n3️⃣ CALCULATING PORTFOLIO INNOVATION METRICS\n")
  
  portfolio_metrics <- calculate_portfolio_metrics(
    integrated_data, 
    weight_col, 
    fund_col
  )
  
  # 4. FUND-LEVEL ANALYSIS
  cat("\n4️⃣ ANALYZING INNOVATION BY FUND\n")
  
  if(fund_col %in% names(integrated_data)) {
    fund_analysis <- analyze_fund_innovation(integrated_data, weight_col, fund_col)
    cat(sprintf("   Number of funds: %d\n", length(unique(integrated_data[[fund_col]]))))
  } else {
    fund_analysis <- NULL
    cat("   No fund column found for fund-level analysis\n")
  }
  
  # 5. INDUSTRY EXPOSURE ANALYSIS
  cat("\n5️⃣ ANALYZING INDUSTRY INNOVATION EXPOSURE\n")
  
  if("GICS.Ind.Grp.Name" %in% names(integrated_data)) {
    industry_analysis <- analyze_industry_exposure(integrated_data, weight_col)
  } else {
    industry_analysis <- NULL
  }
  
  # 6. CONCENTRATION ANALYSIS
  cat("\n6️⃣ ANALYZING INNOVATION CONCENTRATION\n")
  
  concentration_analysis <- analyze_innovation_concentration(
    integrated_data, 
    weight_col
  )
  
  # 7. CREATE PORTFOLIO SUMMARY
  portfolio_summary <- create_portfolio_summary(
    integrated_data,
    portfolio_metrics,
    fund_analysis,
    industry_analysis,
    concentration_analysis
  )
  
  return(list(
    integrated_data = integrated_data,
    portfolio_metrics = portfolio_metrics,
    fund_analysis = fund_analysis,
    industry_analysis = industry_analysis,
    concentration_analysis = concentration_analysis,
    portfolio_summary = portfolio_summary,
    weight_analysis = weight_analysis
  ))
}

clean_portfolio_weights <- function(data, weight_col) {
  #' Clean and Validate Portfolio Weights
  #' 
  #' @param data Integrated holdings data
  #' @param weight_col Weight column name
  #' @return Cleaned data with validated weights
  #' 
  #' STATISTICAL CONCEPT: Data cleaning with validation checks
  
  cleaned_data <- data
  
  # Check if weight column exists
  if(!weight_col %in% names(cleaned_data)) {
    cat(sprintf("⚠️ Weight column '%s' not found. Creating equal weights.\n", weight_col))
    cleaned_data$fund_weight_equal <- 1 / nrow(cleaned_data)
    weight_col <- "fund_weight_equal"
  }
  
  # Convert to numeric if needed
  if(!is.numeric(cleaned_data[[weight_col]])) {
    cleaned_data[[weight_col]] <- as.numeric(
      gsub("[^0-9.Ee+-]", "", cleaned_data[[weight_col]])
    )
  }
  
  # Handle missing weights
  missing_weights <- is.na(cleaned_data[[weight_col]])
  if(any(missing_weights)) {
    cat(sprintf("   Imputing %d missing weights with mean\n", sum(missing_weights)))
    mean_weight <- mean(cleaned_data[[weight_col]], na.rm = TRUE)
    cleaned_data[[weight_col]][missing_weights] <- mean_weight
  }
  
  # Handle negative weights (short positions)
  negative_weights <- cleaned_data[[weight_col]] < 0
  if(any(negative_weights)) {
    cat(sprintf("   Found %d negative weights (short positions)\n", sum(negative_weights)))
    # For innovation scoring, we take absolute value but track separately
    cleaned_data$weight_original <- cleaned_data[[weight_col]]
    cleaned_data[[weight_col]] <- abs(cleaned_data[[weight_col]])
  }
  
  # Normalize to sum to 1 (if not already)
  weight_sum <- sum(cleaned_data[[weight_col]], na.rm = TRUE)
  if(abs(weight_sum - 1) > 0.01) {
    cat(sprintf("   Normalizing weights (sum = %.3f)\n", weight_sum))
    cleaned_data[[weight_col]] <- cleaned_data[[weight_col]] / weight_sum
  }
  
  # Create equal weights version for comparison
  cleaned_data$equal_weight <- 1 / nrow(cleaned_data)
  
  return(list(
    cleaned_data = cleaned_data,
    weight_column = weight_col,
    weight_summary = summary(cleaned_data[[weight_col]]),
    missing_imputed = sum(missing_weights),
    negatives_found = if(exists("negative_weights")) sum(negative_weights) else 0
  ))
}

calculate_portfolio_metrics <- function(data, weight_col, fund_col = NULL) {
  #' Calculate Portfolio Innovation Metrics
  #' 
  #' @param data Integrated holdings data
  #' @param weight_col Weight column name
  #' @param fund_col Fund column name (optional)
  #' @return Portfolio-level innovation metrics
  #' 
  #' STATISTICAL CONCEPT: Weighted portfolio statistics
  
  metrics <- list()
  
  # Overall portfolio metrics
  metrics$overall <- list(
    total_holdings = nrow(data),
    unique_companies = length(unique(data$Ticker)),
    portfolio_daii = weighted.mean(data$DAII_3.5_Score, 
                                  data[[weight_col]], 
                                  na.rm = TRUE),
    portfolio_daii_equal = mean(data$DAII_3.5_Score, na.rm = TRUE),
    daii_range = paste(
      round(min(data$DAII_3.5_Score, na.rm = TRUE), 1),
      "-",
      round(max(data$DAII_3.5_Score, na.rm = TRUE), 1)
    ),
    daii_sd = sd(data$DAII_3.5_Score, na.rm = TRUE),
    quartile_distribution = table(data$DAII_Quartile),
    quartile_percentages = prop.table(table(data$DAII_Quartile)) * 100
  )
  
  # Component contributions
  component_cols <- c("R_D_Score", "Analyst_Score", "Patent_Score", 
                     "News_Score", "Growth_Score")
  
  metrics$component_contributions <- sapply(component_cols, function(col) {
    if(col %in% names(data)) {
      weighted.mean(data[[col]], data[[weight_col]], na.rm = TRUE)
    } else {
      NA
    }
  })
  
  # Fund-level metrics (if fund column exists)
  if(!is.null(fund_col) && fund_col %in% names(data)) {
    fund_metrics <- data %>%
      group_by(!!sym(fund_col)) %>%
      summarise(
        fund_holdings = n(),
        fund_weight = sum(!!sym(weight_col), na.rm = TRUE),
        fund_daii = weighted.mean(DAII_3.5_Score, !!sym(weight_col), na.rm = TRUE),
        fund_daii_sd = sqrt(
          sum((DAII_3.5_Score - fund_daii)^2 * !!sym(weight_col), na.rm = TRUE) /
          sum(!!sym(weight_col), na.rm = TRUE)
        ),
        top_innovator = Ticker[which.max(DAII_3.5_Score)],
        top_innovator_score = max(DAII_3.5_Score, na.rm = TRUE),
        .groups = 'drop'
      ) %>%
      arrange(desc(fund_daii))
    
    metrics$fund_level <- fund_metrics
  }
  
  # Active share of innovation (deviation from equal weight)
  metrics$active_share <- list(
    daii_difference = metrics$overall$portfolio_daii - 
                      metrics$overall$portfolio_daii_equal,
    relative_difference = (metrics$overall$portfolio_daii / 
                          metrics$overall$portfolio_daii_equal - 1) * 100
  )
  
  return(metrics)
}

analyze_fund_innovation <- function(data, weight_col, fund_col) {
  #' Analyze Innovation Characteristics by Fund
  #' 
  #' @param data Integrated holdings data
  #' @param weight_col Weight column name
  #' @param fund_col Fund column name
  #' @return Fund-level innovation analysis
  #' 
  #' STATISTICAL CONCEPT: Stratified analysis by fund manager
  
  fund_stats <- data %>%
    group_by(!!sym(fund_col)) %>%
    summarise(
      n_holdings = n(),
      total_weight = sum(!!sym(weight_col), na.rm = TRUE),
      weighted_daii = weighted.mean(DAII_3.5_Score, !!sym(weight_col), na.rm = TRUE),
      daii_sd = sqrt(
        sum((DAII_3.5_Score - weighted_daii)^2 * !!sym(weight_col), na.rm = TRUE) /
        sum(!!sym(weight_col), na.rm = TRUE)
      ),
      daii_min = min(DAII_3.5_Score, na.rm = TRUE),
      daii_max = max(DAII_3.5_Score, na.rm = TRUE),
      daii_range = max - min,
      q1_count = sum(DAII_Quartile == "Q1 (High)", na.rm = TRUE),
      q1_percent = 100 * q1_count / n_holdings,
      q4_count = sum(DAII_Quartile == "Q4 (Low)", na.rm = TRUE),
      q4_percent = 100 * q4_count / n_holdings,
      .groups = 'drop'
    ) %>%
    arrange(desc(weighted_daii))
  
  # Calculate fund innovation contribution
  total_daii <- weighted.mean(data$DAII_3.5_Score, data[[weight_col]], na.rm = TRUE)
  
  fund_stats <- fund_stats %>%
    mutate(
      contribution_to_total = total_weight * weighted_daii / total_daii * 100,
      innovation_tilt = ifelse(weighted_daii > total_daii, "Above Average", "Below Average")
    )
  
  return(fund_stats)
}

analyze_industry_exposure <- function(data, weight_col) {
  #' Analyze Innovation Exposure by Industry
  #' 
  #' @param data Integrated holdings data
  #' @param weight_col Weight column name
  #' @return Industry-level innovation analysis
  
  industry_stats <- data %>%
    group_by(GICS.Ind.Grp.Name) %>%
    summarise(
      industry_weight = sum(!!sym(weight_col), na.rm = TRUE),
      industry_daii = weighted.mean(DAII_3.5_Score, !!sym(weight_col), na.rm = TRUE),
      n_companies = n_distinct(Ticker),
      daii_sd = sd(DAII_3.5_Score, na.rm = TRUE),
      daii_min = min(DAII_3.5_Score, na.rm = TRUE),
      daii_max = max(DAII_3.5_Score, na.rm = TRUE),
      .groups = 'drop'
    ) %>%
    arrange(desc(industry_daii))
  
  # Calculate industry contribution to portfolio innovation
  total_daii <- weighted.mean(data$DAII_3.5_Score, data[[weight_col]], na.rm = TRUE)
  
  industry_stats <- industry_stats %>%
    mutate(
      innovation_contribution = industry_weight * industry_daii,
      contribution_percent = 100 * innovation_contribution / 
                           sum(innovation_contribution, na.rm = TRUE),
      innovation_tilt = ifelse(industry_daii > total_daii, 
                              "Innovation Leader", 
                              "Innovation Lagger")
    )
  
  return(industry_stats)
}

analyze_innovation_concentration <- function(data, weight_col) {
  #' Analyze Concentration of Innovation in Portfolio
  #' 
  #' @param data Integrated holdings data
  #' @param weight_col Weight column name
  #' @return Concentration analysis results
  
  # Herfindahl-Hirschman Index for innovation concentration
  daii_contributions <- data$DAII_3.5_Score * data[[weight_col]]
  hhi_daii <- sum((daii_contributions / sum(daii_contributions, na.rm = TRUE))^2, 
                  na.rm = TRUE) * 10000
  
  # Top 10 concentration
  sorted_by_contribution <- data[order(-daii_contributions), ]
  top10_contribution <- sum(daii_contributions[1:min(10, nrow(data))], na.rm = TRUE) /
                       sum(daii_contributions, na.rm = TRUE) * 100
  
  # Innovation Gini coefficient
  sorted_daii <- sort(data$DAII_3.5_Score[!is.na(data$DAII_3.5_Score)])
  n <- length(sorted_daii)
  cumulative_daii <- cumsum(sorted_daii)
  total_daii <- sum(sorted_daii, na.rm = TRUE)
  
  # Calculate Gini using trapezoidal integration
  gini_daii <- 1 - 2 * sum(cumulative_daii) / (n * total_daii) + 1/n
  
  return(list(
    hhi_daii = hhi_daii,
    hhi_interpretation = ifelse(hhi_daii < 1500, "Unconcentrated",
                               ifelse(hhi_daii < 2500, "Moderately Concentrated",
                                      "Highly Concentrated")),
    top10_concentration = top10_contribution,
    gini_coefficient = gini_daii,
    interpretation = "Higher HHI/Gini indicates more concentrated innovation"
  ))
}

create_portfolio_summary <- function(integrated_data,
                                     portfolio_metrics,
                                     fund_analysis,
                                     industry_analysis,
                                     concentration_analysis) {
  #' Create Comprehensive Portfolio Summary
  #' 
  #' @param integrated_data Integrated holdings data
  #' @param portfolio_metrics Portfolio-level metrics
  #' @param fund_analysis Fund-level analysis
  #' @param industry_analysis Industry analysis
  #' @param concentration_analysis Concentration analysis
  #' @return Comprehensive portfolio summary
  
  summary_df <- data.frame(
    Metric = c(
      "Total Holdings",
      "Unique Companies",
      "Portfolio DAII 3.5 Score (Weighted)",
      "Portfolio DAII 3.5 Score (Equal Weight)",
      "DAII Score Range",
      "DAII Score Standard Deviation",
      "Companies in Q1 (High Innovators)",
      "Companies in Q4 (Low Innovators)",
      "Innovation HHI Index",
      "Innovation HHI Interpretation",
      "Top 10 Concentration (%)",
      "Innovation Gini Coefficient"
    ),
    Value = c(
      portfolio_metrics$overall$total_holdings,
      portfolio_metrics$overall$unique_companies,
      round(portfolio_metrics$overall$portfolio_daii, 1),
      round(portfolio_metrics$overall$portfolio_daii_equal, 1),
      portfolio_metrics$overall$daii_range,
      round(portfolio_metrics$overall$daii_sd, 1),
      sum(portfolio_metrics$overall$quartile_distribution["Q1 (High)"]),
      sum(portfolio_metrics$overall$quartile_distribution["Q4 (Low)"]),
      round(concentration_analysis$hhi_daii, 0),
      concentration_analysis$hhi_interpretation,
      round(concentration_analysis$top10_concentration, 1),
      round(concentration_analysis$gini_coefficient, 3)
    ),
    stringsAsFactors = FALSE
  )
  
  # Add component contributions
  component_df <- data.frame(
    Metric = paste(names(portfolio_metrics$component_contributions), "Contribution"),
    Value = round(portfolio_metrics$component_contributions, 1),
    stringsAsFactors = FALSE
  )
  
  summary_df <- rbind(summary_df, component_df)
  
  return(summary_df)
}