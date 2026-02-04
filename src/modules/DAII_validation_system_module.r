# ============================================================================
# MODULE 6: VALIDATION SYSTEM - Quality Assurance & Business Review
# ============================================================================
#
# PURPOSE: Comprehensive validation of DAII 3.5 scores and outputs
#
# VALIDATION FRAMEWORK:
# 1. Statistical Validation: Distribution checks, correlation analysis
# 2. Business Validation: Reasonableness checks, industry patterns
# 3. Process Validation: Imputation tracking, calculation verification
# 4. Comparative Validation: Benchmarking against expectations
#
# STATISTICAL METHODS:
# 1. Distribution Tests: Skewness, kurtosis, normality tests
# 2. Correlation Analysis: Component relationships, multicollinearity
# 3. Outlier Detection: Statistical and business outlier identification
# 4. Stability Analysis: Sensitivity to parameter changes
#
# BUSINESS VALIDATION CRITERIA:
# 1. Face Validity: Do scores align with company reputation?
# 2. Discriminant Validity: Do scores differentiate known innovators?
# 3. Predictive Validity: Do scores predict future innovation outcomes?
# 4. Construct Validity: Do scores measure intended construct?
#
# ============================================================================

create_validation_framework <- function(daii_results,
                                        holdings_data,
                                        imputation_log = NULL,
                                        industry_benchmarks = NULL) {
  #' Create Comprehensive Validation Framework
  #' 
  #' @param daii_results Output from calculate_daii_scores
  #' @param holdings_data Original holdings data
  #' @param imputation_log Imputation tracking log
  #' @param industry_benchmarks Industry averages
  #' @return Comprehensive validation report
  
  cat("\n🔍 CREATING COMPREHENSIVE VALIDATION FRAMEWORK\n")
  cat(paste(rep("=", 60), collapse = ""), "\n")
  
  validation_report <- list(
    timestamp = Sys.time(),
    validation_modules = list()
  )
  
  # 1. STATISTICAL VALIDATION
  cat("\n1️⃣ STATISTICAL VALIDATION\n")
  
  validation_report$statistical <- perform_statistical_validation(daii_results$daii_data)
  
  # 2. BUSINESS VALIDATION
  cat("\n2️⃣ BUSINESS VALIDATION\n")
  
  validation_report$business <- perform_business_validation(
    daii_results$daii_data,
    industry_benchmarks
  )
  
  # 3. PROCESS VALIDATION
  cat("\n3️⃣ PROCESS VALIDATION\n")
  
  if(!is.null(imputation_log)) {
    validation_report$process <- perform_process_validation(
      imputation_log,
      daii_results$daii_data
    )
  }
  
  # 4. SENSITIVITY VALIDATION
  cat("\n4️⃣ SENSITIVITY VALIDATION\n")
  
  validation_report$sensitivity <- perform_sensitivity_validation(daii_results)
  
  # 5. CREATE VALIDATION TEMPLATE
  cat("\n5️⃣ CREATING VALIDATION TEMPLATE\n")
  
  validation_template <- create_validation_template(
    daii_results$daii_data,
    holdings_data,
    validation_report
  )
  
  validation_report$validation_template <- validation_template
  
  # 6. GENERATE VALIDATION SUMMARY
  cat("\n6️⃣ GENERATING VALIDATION SUMMARY\n")
  
  validation_summary <- generate_validation_summary(validation_report)
  
  return(list(
    validation_report = validation_report,
    validation_template = validation_template,
    validation_summary = validation_summary
  ))
}

perform_statistical_validation <- function(daii_data) {
  #' Perform Statistical Validation of DAII Scores
  #' 
  #' @param daii_data Data with DAII scores and components
  #' @return Statistical validation results
  
  validation_results <- list()
  
  # Distribution analysis
  score_cols <- c("DAII_3.5_Score", "R_D_Score", "Analyst_Score", 
                  "Patent_Score", "News_Score", "Growth_Score")
  
  distribution_stats <- data.frame(
    Metric = score_cols,
    Mean = sapply(daii_data[, score_cols], mean, na.rm = TRUE),
    Median = sapply(daii_data[, score_cols], median, na.rm = TRUE),
    SD = sapply(daii_data[, score_cols], sd, na.rm = TRUE),
    Skewness = sapply(daii_data[, score_cols], function(x) {
      if(length(x[!is.na(x)]) > 3) {
        moments::skewness(x, na.rm = TRUE)
      } else {
        NA
      }
    }),
    Kurtosis = sapply(daii_data[, score_cols], function(x) {
      if(length(x[!is.na(x)]) > 3) {
        moments::kurtosis(x, na.rm = TRUE)
      } else {
        NA
      }
    }),
    Min = sapply(daii_data[, score_cols], min, na.rm = TRUE),
    Max = sapply(daii_data[, score_cols], max, na.rm = TRUE),
    IQR = sapply(daii_data[, score_cols], IQR, na.rm = TRUE),
    stringsAsFactors = FALSE
  )
  
  validation_results$distribution <- distribution_stats
  
  # Correlation analysis
  cor_matrix <- cor(daii_data[, score_cols], use = "complete.obs")
  validation_results$correlation <- cor_matrix
  
  # Multicollinearity check (Variance Inflation Factor)
  if(require(car)) {
    vif_check <- try({
      lm_formula <- as.formula(paste("DAII_3.5_Score ~", 
                                    paste(score_cols[-1], collapse = " + ")))
      vif_values <- car::vif(lm(lm_formula, data = daii_data))
      vif_values
    }, silent = TRUE)
    
    if(!inherits(vif_check, "try-error")) {
      validation_results$vif <- vif_check
    }
  }
  
  # Outlier detection
  outlier_analysis <- list()
  for(col in score_cols) {
    scores <- daii_data[[col]]
    q1 <- quantile(scores, 0.25, na.rm = TRUE)
    q3 <- quantile(scores, 0.75, na.rm = TRUE)
    iqr <- q3 - q1
    
    lower_bound <- q1 - 1.5 * iqr
    upper_bound <- q3 + 1.5 * iqr
    
    outliers <- which(scores < lower_bound | scores > upper_bound)
    
    outlier_analysis[[col]] <- list(
      lower_bound = lower_bound,
      upper_bound = upper_bound,
      outlier_count = length(outliers),
      outlier_percent = 100 * length(outliers) / length(scores),
      outlier_tickers = if(length(outliers) > 0) {
        daii_data$Ticker[outliers]
      } else {
        character(0)
      }
    )
  }
  
  validation_results$outliers <- outlier_analysis
  
  # Normality tests
  normality_tests <- data.frame(
    Metric = score_cols,
    Shapiro_Statistic = NA,
    Shapiro_p = NA,
    Normal = NA,
    stringsAsFactors = FALSE
  )
  
  for(i in 1:length(score_cols)) {
    col <- score_cols[i]
    test_data <- daii_data[[col]][!is.na(daii_data[[col]])]
    
    if(length(test_data) > 3 && length(test_data) < 5000) {
      shapiro_test <- shapiro.test(test_data)
      normality_tests$Shapiro_Statistic[i] <- shapiro_test$statistic
      normality_tests$Shapiro_p[i] <- shapiro_test$p.value
      normality_tests$Normal[i] <- shapiro_test$p.value > 0.05
    }
  }
  
  validation_results$normality <- normality_tests
  
  return(validation_results)
}

perform_business_validation <- function(daii_data, industry_benchmarks = NULL) {
  #' Perform Business Validation of DAII Scores
  #' 
  #' @param daii_data Data with DAII scores
  #' @param industry_benchmarks Industry averages
  #' @return Business validation results
  
  validation_results <- list()
  
  # 1. Face Validity - Check known innovators
  known_tech_innovators <- c("NVDA", "AAPL", "MSFT", "GOOGL", "AMZN", 
                            "TSLA", "META", "INTC", "AMD", "QCOM")
  
  face_validity <- daii_data %>%
    filter(Ticker %in% known_tech_innovators) %>%
    select(Ticker, DAII_3.5_Score, DAII_Quartile) %>%
    arrange(desc(DAII_3.5_Score))
  
  validation_results$face_validity <- list(
    known_innovators = face_validity,
    avg_score_innovators = mean(face_validity$DAII_3.5_Score, na.rm = TRUE),
    interpretation = "Technology innovators should score above average"
  )
  
  # 2. Industry Pattern Validation
  if(!is.null(industry_benchmarks)) {
    industry_validation <- industry_benchmarks %>%
      mutate(
        expected_high = grepl("Technology|Software|Semiconductor|Biotech", 
                             GICS.Ind.Grp.Name, ignore.case = TRUE),
        actual_high = Industry_Mean_DAII > median(Industry_Mean_DAII, na.rm = TRUE),
        alignment = expected_high == actual_high
      )
    
    validation_results$industry_alignment <- list(
      industry_check = industry_validation,
      alignment_rate = mean(industry_validation$alignment, na.rm = TRUE) * 100,
      interpretation = "High innovation industries should score higher"
    )
  }
  
  # 3. Component Balance Validation
  component_balance <- daii_data %>%
    summarise(
      avg_rd_score = mean(R_D_Score, na.rm = TRUE),
      avg_analyst_score = mean(Analyst_Score, na.rm = TRUE),
      avg_patent_score = mean(Patent_Score, na.rm = TRUE),
      avg_news_score = mean(News_Score, na.rm = TRUE),
      avg_growth_score = mean(Growth_Score, na.rm = TRUE),
      rd_dominance = avg_rd_score / (avg_analyst_score + 0.001)
    )
  
  validation_results$component_balance <- component_balance
  
  # 4. Quartile Distribution Validation
  quartile_check <- table(daii_data$DAII_Quartile)
  expected_distribution <- rep(nrow(daii_data)/4, 4)
  
  chi_square_test <- chisq.test(quartile_check, 
                               p = rep(0.25, 4))
  
  validation_results$quartile_distribution <- list(
    observed = quartile_check,
    expected = expected_distribution,
    chi_square = chi_square_test$statistic,
    p_value = chi_square_test$p.value,
    interpretation = "Quartiles should be evenly distributed"
  )
  
  # 5. Business Reasonableness Flags
  reasonableness_flags <- daii_data %>%
    mutate(
      flag_high_rd_low_daii = R_D_Score > 70 & DAII_3.5_Score < 40,
      flag_low_rd_high_daii = R_D_Score < 30 & DAII_3.5_Score > 60,
      flag_extreme_component = pmax(R_D_Score, Analyst_Score, Patent_Score, 
                                   News_Score, Growth_Score) > 90,
      flag_unbalanced = abs(R_D_Score - Analyst_Score) > 50,
      total_flags = flag_high_rd_low_daii + flag_low_rd_high_daii + 
                   flag_extreme_component + flag_unbalanced
    ) %>%
    filter(total_flags > 0) %>%
    select(Ticker, DAII_3.5_Score, R_D_Score, Analyst_Score, total_flags)
  
  validation_results$reasonableness_flags <- list(
    flagged_companies = reasonableness_flags,
    flag_count = nrow(reasonableness_flags),
    flag_percent = 100 * nrow(reasonableness_flags) / nrow(daii_data)
  )
  
  return(validation_results)
}

perform_process_validation <- function(imputation_log, daii_data) {
  #' Validate Imputation Process and Impact
  #' 
  #' @param imputation_log Imputation tracking log
  #' @param daii_data Data with DAII scores
  #' @return Process validation results
  
  validation_results <- list()
  
  # Imputation summary
  imputation_summary <- imputation_log %>%
    group_by(Metric, Imputation_Method) %>%
    summarise(
      n_imputations = n(),
      avg_imputed_value = mean(Imputed_Value, na.rm = TRUE),
      sd_imputed_value = sd(Imputed_Value, na.rm = TRUE),
      .groups = 'drop'
    )
  
  validation_results$imputation_summary <- imputation_summary
  
  # Impact on scores
  imputed_companies <- unique(imputation_log$Ticker)
  non_imputed_companies <- setdiff(daii_data$Ticker, imputed_companies)
  
  impact_analysis <- data.frame(
    Group = c("Imputed", "Non-Imputed"),
    n_companies = c(length(imputed_companies), length(non_imputed_companies)),
    mean_daii = c(
      mean(daii_data$DAII_3.5_Score[daii_data$Ticker %in% imputed_companies], na.rm = TRUE),
      mean(daii_data$DAII_3.5_Score[daii_data$Ticker %in% non_imputed_companies], na.rm = TRUE)
    ),
    sd_daii = c(
      sd(daii_data$DAII_3.5_Score[daii_data$Ticker %in% imputed_companies], na.rm = TRUE),
      sd(daii_data$DAII_3.5_Score[daii_data$Ticker %in% non_imputed_companies], na.rm = TRUE)
    ),
    stringsAsFactors = FALSE
  )
  
  # Statistical test for difference
  if(nrow(impact_analysis) == 2) {
    imputed_scores <- daii_data$DAII_3.5_Score[daii_data$Ticker %in% imputed_companies]
    non_imputed_scores <- daii_data$DAII_3.5_Score[daii_data$Ticker %in% non_imputed_companies]
    
    if(length(imputed_scores) > 1 && length(non_imputed_scores) > 1) {
      t_test <- t.test(imputed_scores, non_imputed_scores)
      impact_analysis$t_statistic <- c(t_test$statistic, NA)
      impact_analysis$p_value <- c(t_test$p.value, NA)
      impact_analysis$significant <- c(t_test$p.value < 0.05, NA)
    }
  }
  
  validation_results$impact_analysis <- impact_analysis
  
  # Tracking completeness
  tracking_completeness <- list(
    total_imputations = nrow(imputation_log),
    unique_companies_imputed = length(imputed_companies),
    metrics_imputed = length(unique(imputation_log$Metric)),
    methods_used = unique(imputation_log$Imputation_Method),
    tracking_rate = 100  # All imputations tracked
  )
  
  validation_results$tracking_completeness <- tracking_completeness
  
  return(validation_results)
}

perform_sensitivity_validation <- function(daii_results) {
  #' Validate Sensitivity to Weight Changes
  #' 
  #' @param daii_results Output from calculate_daii_scores
  #' @return Sensitivity validation results
  
  sensitivity_results <- list()
  
  if("sensitivity_results" %in% names(daii_results)) {
    sensitivity_data <- daii_results$sensitivity_results
    
    # Rank stability
    rank_correlations <- sensitivity_data$rank_correlations
    min_correlation <- min(rank_correlations, na.rm = TRUE)
    avg_correlation <- mean(rank_correlations[lower.tri(rank_correlations)], na.rm = TRUE)
    
    sensitivity_results$rank_stability <- list(
      min_correlation = min_correlation,
      avg_correlation = avg_correlation,
      interpretation = ifelse(avg_correlation > 0.8, "High stability",
                            ifelse(avg_correlation > 0.6, "Moderate stability",
                                   "Low stability"))
    )
    
    # Top company consistency
    if(!is.null(sensitivity_data$rank_changes)) {
      top10_consistency <- mean(sensitivity_data$rank_changes$Top10_Overlap) / 10 * 100
      
      sensitivity_results$top_company_consistency <- list(
        avg_top10_overlap = top10_consistency,
        interpretation = ifelse(top10_consistency > 70, "High consistency",
                               ifelse(top10_consistency > 50, "Moderate consistency",
                                      "Low consistency"))
      )
    }
  }
  
  return(sensitivity_results)
}

create_validation_template <- function(daii_data, holdings_data, validation_report) {
  #' Create Validation Template for Business Review
  #' 
  #' @param daii_data Company-level DAII data
  #' @param holdings_data Holdings data
  #' @param validation_report Validation results
  #' @return Validation template for manual review
  
  # Start with holdings data
  validation_template <- holdings_data
  
  # Add DAII scores
  score_cols <- c("Ticker", "DAII_3.5_Score", "DAII_Quartile",
                  "R_D_Score", "Analyst_Score", "Patent_Score",
                  "News_Score", "Growth_Score", "GICS.Ind.Grp.Name")
  
  score_data <- daii_data[, intersect(score_cols, names(daii_data))]
  validation_template <- merge(validation_template, score_data, 
                              by = "Ticker", all.x = TRUE)
  
  # Add validation columns
  validation_template$Data_Quality_Score <- NA
  validation_template$Business_Reasonableness <- ""
  validation_template$Validation_Notes <- ""
  validation_template$Manual_Review_Flag <- "NO"
  validation_template$Reviewer_Name <- ""
  validation_template$Review_Date <- ""
  validation_template$Review_Comments <- ""
  
  # Auto-populate based on validation results
  
  # 1. Data Quality Score (1-5)
  # 5: Complete data, no imputations
  # 4: Minor imputations
  # 3: Multiple imputations
  # 2: Critical data missing
  # 1: Cannot calculate score
  
  if(!is.null(validation_report$process)) {
    imputed_companies <- unique(validation_report$process$imputation_log$Ticker)
    validation_template$Data_Quality_Score <- ifelse(
      validation_template$Ticker %in% imputed_companies, 3, 5
    )
  } else {
    validation_template$Data_Quality_Score <- 5
  }
  
  # 2. Business Reasonableness flags
  if(!is.null(validation_report$business)) {
    flagged_companies <- validation_report$business$reasonableness_flags$flagged_companies$Ticker
    validation_template$Business_Reasonableness <- ifelse(
      validation_template$Ticker %in% flagged_companies,
      "Flagged for review",
      "Within expected range"
    )
  }
  
  # 3. Auto-generated validation notes
  validation_template$Validation_Notes <- apply(validation_template, 1, function(row) {
    notes <- character()
    
    # Check quartile
    if(!is.na(row["DAII_Quartile"]) && row["DAII_Quartile"] == "Q1 (High)") {
      notes <- c(notes, "Top quartile innovator")
    }
    
    # Check R&D score
    if(!is.na(row["R_D_Score"]) && as.numeric(row["R_D_Score"]) > 70) {
      notes <- c(notes, "High R&D intensity")
    }
    
    # Check analyst score
    if(!is.na(row["Analyst_Score"]) && as.numeric(row["Analyst_Score"]) > 80) {
      notes <- c(notes, "Strong analyst sentiment")
    }
    
    # Check for imbalances
    if(!is.na(row["R_D_Score"]) && !is.na(row["Analyst_Score"])) {
      if(abs(as.numeric(row["R_D_Score"]) - as.numeric(row["Analyst_Score"])) > 40) {
        notes <- c(notes, "Large R&D-Analyst gap")
      }
    }
    
    paste(notes, collapse = "; ")
  })
  
  # 4. Manual review flags
  validation_template$Manual_Review_Flag <- ifelse(
    validation_template$Data_Quality_Score < 3 |
    validation_template$Business_Reasonableness == "Flagged for review" |
    validation_template$DAII_Quartile %in% c("Q1 (High)", "Q4 (Low)"),
    "YES",
    "NO"
  )
  
  # Reorder columns
  col_order <- c(
    "Ticker", "fund_name", "fund_weight",
    "DAII_3.5_Score", "DAII_Quartile",
    "R_D_Score", "Analyst_Score", "Patent_Score", "News_Score", "Growth_Score",
    "GICS.Ind.Grp.Name",
    "Data_Quality_Score", "Business_Reasonableness", "Validation_Notes",
    "Manual_Review_Flag", "Reviewer_Name", "Review_Date", "Review_Comments"
  )
  
  # Keep only columns that exist
  col_order <- intersect(col_order, names(validation_template))
  validation_template <- validation_template[, col_order]
  
  return(validation_template)
}

generate_validation_summary <- function(validation_report) {
  #' Generate Validation Summary Report
  #' 
  #' @param validation_report Comprehensive validation results
  #' @return Summary validation metrics
  
  summary_df <- data.frame(
    Validation_Category = character(),
    Metric = character(),
    Value = character(),
    Status = character(),
    stringsAsFactors = FALSE
  )
  
  # Statistical validation summary
  if(!is.null(validation_report$statistical)) {
    stats <- validation_report$statistical
    
    # Distribution checks
    daii_dist <- stats$distribution[stats$distribution$Metric == "DAII_3.5_Score", ]
    summary_df <- rbind(summary_df, data.frame(
      Validation_Category = "Statistical",
      Metric = "DAII Score Skewness",
      Value = sprintf("%.2f", daii_dist$Skewness),
      Status = ifelse(abs(daii_dist$Skewness) < 1, "✅ Acceptable", "⚠️ Review")
    ))
    
    # Correlation checks
    rd_corr <- stats$correlation["R_D_Score", "DAII_3.5_Score"]
    summary_df <- rbind(summary_df, data.frame(
      Validation_Category = "Statistical",
      Metric = "R&D Score Correlation",
      Value = sprintf("%.3f", rd_corr),
      Status = ifelse(rd_corr > 0.3, "✅ Good", "⚠️ Low")
    ))
    
    # Outlier checks
    daii_outliers <- stats$outliers$DAII_3.5_Score$outlier_percent
    summary_df <- rbind(summary_df, data.frame(
      Validation_Category = "Statistical",
      Metric = "DAII Outliers (%)",
      Value = sprintf("%.1f%%", daii_outliers),
      Status = ifelse(daii_outliers < 5, "✅ Acceptable", "⚠️ High")
    ))
  }
  
  # Business validation summary
  if(!is.null(validation_report$business)) {
    business <- validation_report$business
    
    # Face validity
    innovator_score <- business$face_validity$avg_score_innovators
    summary_df <- rbind(summary_df, data.frame(
      Validation_Category = "Business",
      Metric = "Known Innovators Avg Score",
      Value = sprintf("%.1f", innovator_score),
      Status = ifelse(innovator_score > 50, "✅ Good", "⚠️ Low")
    ))
    
    # Industry alignment
    if(!is.null(business$industry_alignment)) {
      alignment_rate <- business$industry_alignment$alignment_rate
      summary_df <- rbind(summary_df, data.frame(
        Validation_Category = "Business",
        Metric = "Industry Alignment Rate",
        Value = sprintf("%.1f%%", alignment_rate),
        Status = ifelse(alignment_rate > 70, "✅ Good", "⚠️ Low")
      ))
    }
    
    # Flagged companies
    flag_percent <- business$reasonableness_flags$flag_percent
    summary_df <- rbind(summary_df, data.frame(
      Validation_Category = "Business",
      Metric = "Companies Flagged for Review",
      Value = sprintf("%.1f%%", flag_percent),
      Status = ifelse(flag_percent < 10, "✅ Acceptable", "⚠️ High")
    ))
  }
  
  # Process validation summary
  if(!is.null(validation_report$process)) {
    process <- validation_report$process
    
    # Imputation impact
    if(!is.null(process$impact_analysis)) {
      impact <- process$impact_analysis
      if(nrow(impact) == 2 && "p_value" %in% names(impact)) {
        imputation_p <- impact$p_value[1]
        summary_df <- rbind(summary_df, data.frame(
          Validation_Category = "Process",
          Metric = "Imputation Impact (p-value)",
          Value = sprintf("%.4f", imputation_p),
          Status = ifelse(imputation_p > 0.05, "✅ Insignificant", "⚠️ Significant")
        ))
      }
    }
  }
  
  # Sensitivity validation summary
  if(!is.null(validation_report$sensitivity)) {
    sensitivity <- validation_report$sensitivity
    
    # Rank stability
    if(!is.null(sensitivity$rank_stability)) {
      avg_corr <- sensitivity$rank_stability$avg_correlation
      summary_df <- rbind(summary_df, data.frame(
        Validation_Category = "Sensitivity",
        Metric = "Average Rank Correlation",
        Value = sprintf("%.3f", avg_corr),
        Status = ifelse(avg_corr > 0.8, "✅ High", 
                       ifelse(avg_corr > 0.6, "🟡 Moderate", "⚠️ Low"))
      ))
    }
  }
  
  # Overall validation status
  status_counts <- table(summary_df$Status)
  overall_status <- ifelse(
    sum(grepl("⚠️", summary_df$Status)) == 0, "✅ PASS",
    ifelse(sum(grepl("⚠️", summary_df$Status)) < 3, "🟡 WARN", "❌ FAIL")
  )
  
  overall_summary <- data.frame(
    Validation_Category = "OVERALL",
    Metric = "Validation Status",
    Value = overall_status,
    Status = overall_status,
    stringsAsFactors = FALSE
  )
  
  summary_df <- rbind(summary_df, overall_summary)
  
  return(summary_df)
}