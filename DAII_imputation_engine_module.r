# ============================================================================
# MODULE 2: IMPUTATION ENGINE - Missing Data Handling
# ============================================================================
# 
# PURPOSE: Handle missing values systematically with tracking
# 
# STATISTICAL THEORY:
# 1. Missing Data Mechanisms:
#    - MCAR (Missing Completely At Random): Use mean/median imputation
#    - MAR (Missing At Random): Use regression/conditional mean imputation  
#    - MNAR (Missing Not At Random): Flag for investigation
# 
# 2. Imputation Methods:
#    - Median: Robust to outliers (R&D, Patents, Growth)
#    - Mean: For normally distributed data (Analyst Ratings)
#    - Industry Mean: Context-aware imputation
# 
# 3. Imputation Diagnostics:
#    - Track all imputations for transparency
#    - Compare imputed vs non-imputed distributions
#    - Assess impact on final scores
# 
# DESIGN DECISIONS:
# 1. Company-level imputation: Impute before scoring
# 2. Separate treatment by variable type
# 3. Preservation of uncertainty through tracking
# 
# ============================================================================

impute_missing_values <- function(company_data, 
                                  ticker_col,
                                  imputation_methods = list(
                                    "BEst.Analyst.Rtg" = "mean",
                                    "default" = "median"
                                  ),
                                  industry_col = "GICS.Ind.Grp.Name") {
  #' Impute Missing Values with Method Selection
  #' 
  #' @param company_data Company-level data frame
  #' @param ticker_col Ticker column name
  #' @param imputation_methods List mapping columns to imputation methods
  #' @param industry_col Industry classification column (optional)
  #' @return List with imputed data and imputation log
  #' 
  #' STATISTICAL CONCEPT: Multiple imputation strategies based on data type
  
  cat("\n🔄 IMPUTING MISSING VALUES\n")
  cat(paste(rep("-", 50), collapse = ""), "\n")
  
  # Create copy for imputation
  imputed_data <- company_data
  
  # Define numeric columns to impute
  numeric_cols <- c(
    "R.D.Exp", "Mkt.Cap", "Rev...1.Yr.Gr", 
    "BEst.Analyst.Rtg", "Patents...Trademarks...Copy.Rgt", "News.Sent"
  )
  
  # Track all imputations
  imputation_log <- data.frame(
    Ticker = character(),
    Metric = character(),
    Original_Value = character(),
    Imputed_Value = numeric(),
    Imputation_Method = character(),
    Imputation_Level = character(),
    stringsAsFactors = FALSE
  )
  
  # Process each numeric column
  for(col in intersect(numeric_cols, names(imputed_data))) {
    cat(sprintf("\n📈 Processing: %s\n", col))
    
    # Convert to numeric, handling various formats
    num_col_name <- paste0(col, "_num")
    imputed_data[[num_col_name]] <- as.numeric(
      gsub("[^0-9.Ee+-]", "", imputed_data[[col]])
    )
    
    # Identify missing values (NA or special missing indicators)
    missing_indicators <- c("#N/A", "N/A", "NA", "NULL", "", "Field Not Applicable")
    is_missing <- is.na(imputed_data[[num_col_name]]) | 
                  imputed_data[[col]] %in% missing_indicators |
                  grepl(paste(missing_indicators, collapse = "|"), 
                        imputed_data[[col]], ignore.case = TRUE)
    
    missing_count <- sum(is_missing)
    
    if(missing_count > 0) {
      cat(sprintf("   Missing values: %d (%.1f%%)\n", 
                  missing_count, 100 * missing_count / nrow(imputed_data)))
      
      # Select imputation method
      method <- ifelse(col %in% names(imputation_methods),
                       imputation_methods[[col]],
                       imputation_methods$default)
      
      # Calculate imputation value
      if(method == "industry_mean" && industry_col %in% names(imputed_data)) {
        # Industry-level imputation
        cat("   Using industry-level imputation\n")
        
        for(industry in unique(imputed_data[[industry_col]])) {
          industry_mask <- imputed_data[[industry_col]] == industry & !is_missing
          industry_values <- imputed_data[[num_col_name]][industry_mask]
          
          if(length(industry_values) > 0) {
            if(col == "BEst.Analyst.Rtg") {
              impute_val <- mean(industry_values, na.rm = TRUE)
            } else {
              impute_val <- median(industry_values, na.rm = TRUE)
            }
            
            # Apply to missing values in this industry
            industry_missing <- imputed_data[[industry_col]] == industry & is_missing
            imputed_data[[num_col_name]][industry_missing] <- impute_val
            
            # Log imputations
            for(idx in which(industry_missing)) {
              ticker <- imputed_data[[ticker_col]][idx]
              orig_val <- imputed_data[[col]][idx]
              
              imputation_log <- rbind(imputation_log, data.frame(
                Ticker = ticker,
                Metric = col,
                Original_Value = ifelse(is.na(orig_val), "NA", as.character(orig_val)),
                Imputed_Value = impute_val,
                Imputation_Method = paste("Industry", method),
                Imputation_Level = industry,
                stringsAsFactors = FALSE
              ))
            }
          }
        }
      } else {
        # Global imputation
        available_values <- imputed_data[[num_col_name]][!is_missing]
        
        if(length(available_values) > 0) {
          if(method == "mean" || col == "BEst.Analyst.Rtg") {
            impute_val <- mean(available_values, na.rm = TRUE)
            method_name <- "Mean"
          } else if(method == "median") {
            impute_val <- median(available_values, na.rm = TRUE)
            method_name <- "Median"
          } else if(method == "mode") {
            impute_val <- as.numeric(names(sort(table(available_values), decreasing = TRUE))[1])
            method_name <- "Mode"
          } else {
            impute_val <- median(available_values, na.rm = TRUE)
            method_name <- "Median (default)"
          }
          
          # Apply imputation
          imputed_data[[num_col_name]][is_missing] <- impute_val
          
          cat(sprintf("   Imputed with %s: %.4f\n", method_name, impute_val))
          
          # Log imputations
          for(idx in which(is_missing)) {
            ticker <- imputed_data[[ticker_col]][idx]
            orig_val <- imputed_data[[col]][idx]
            
            imputation_log <- rbind(imputation_log, data.frame(
              Ticker = ticker,
              Metric = col,
              Original_Value = ifelse(is.na(orig_val), "NA", as.character(orig_val)),
              Imputed_Value = impute_val,
              Imputation_Method = method_name,
              Imputation_Level = "Global",
              stringsAsFactors = FALSE
            ))
          }
        }
      }
    } else {
      cat("   No missing values found\n")
    }
  }
  
  # Create imputation summary
  summary_stats <- imputation_log %>%
    group_by(Metric, Imputation_Method) %>%
    summarise(
      Count = n(),
      Avg_Imputed_Value = mean(Imputed_Value, na.rm = TRUE),
      .groups = 'drop'
    )
  
  cat("\n📋 IMPUTATION SUMMARY:\n")
  print(summary_stats)
  
  # Compare imputed vs non-imputed distributions
  comparison_report <- list()
  for(col in numeric_cols) {
    num_col <- paste0(col, "_num")
    if(num_col %in% names(imputed_data)) {
      imputed_indices <- imputed_data[[ticker_col]] %in% 
                         unique(imputation_log$Ticker[imputation_log$Metric == col])
      
      if(any(imputed_indices)) {
        comparison_report[[col]] <- list(
          Imputed_Mean = mean(imputed_data[[num_col]][imputed_indices], na.rm = TRUE),
          NonImputed_Mean = mean(imputed_data[[num_col]][!imputed_indices], na.rm = TRUE),
          Imputed_Count = sum(imputed_indices),
          NonImputed_Count = sum(!imputed_indices)
        )
      }
    }
  }
  
  return(list(
    imputed_data = imputed_data,
    imputation_log = imputation_log,
    summary_stats = summary_stats,
    comparison_report = comparison_report
  ))
}

validate_imputation_impact <- function(imputation_results, 
                                       original_data,
                                       ticker_col) {
  #' Validate Imputation Impact on Score Distributions
  #' 
  #' @param imputation_results Output from impute_missing_values
  #' @param original_data Original company data
  #' @param ticker_col Ticker column name
  #' @return Validation report with statistical tests
  #' 
  #' STATISTICAL CONCEPT: Assess whether imputation introduces bias
  
  cat("\n🔍 VALIDATING IMPUTATION IMPACT\n")
  cat(paste(rep("-", 50), collapse = ""), "\n")
  
  imputed_data <- imputation_results$imputed_data
  imputation_log <- imputation_results$imputation_log
  
  # Identify imputed companies
  imputed_companies <- unique(imputation_log$Ticker)
  non_imputed_companies <- setdiff(
    original_data[[ticker_col]], 
    imputed_companies
  )
  
  cat(sprintf(" • Companies with imputed values: %d\n", length(imputed_companies)))
  cat(sprintf(" • Companies with complete data: %d\n", length(non_imputed_companies)))
  
  # Statistical comparison
  validation_tests <- list()
  numeric_cols <- grep("_num$", names(imputed_data), value = TRUE)
  
  for(num_col in numeric_cols) {
    base_col <- gsub("_num$", "", num_col)
    
    imputed_values <- imputed_data[[num_col]][
      imputed_data[[ticker_col]] %in% imputed_companies
    ]
    
    non_imputed_values <- imputed_data[[num_col]][
      imputed_data[[ticker_col]] %in% non_imputed_companies
    ]
    
    if(length(imputed_values) > 1 && length(non_imputed_values) > 1) {
      # T-test for difference in means
      t_test <- try(t.test(imputed_values, non_imputed_values), silent = TRUE)
      
      # Effect size (Cohen's d)
      if(!inherits(t_test, "try-error")) {
        pooled_sd <- sqrt(
          ((length(imputed_values) - 1) * var(imputed_values) + 
           (length(non_imputed_values) - 1) * var(non_imputed_values)) /
          (length(imputed_values) + length(non_imputed_values) - 2)
        )
        
        cohens_d <- abs(mean(imputed_values) - mean(non_imputed_values)) / pooled_sd
        
        validation_tests[[base_col]] <- list(
          t_statistic = t_test$statistic,
          p_value = t_test$p.value,
          cohens_d = cohens_d,
          imputed_mean = mean(imputed_values),
          non_imputed_mean = mean(non_imputed_values),
          difference = mean(imputed_values) - mean(non_imputed_values),
          significant = t_test$p.value < 0.05
        )
      }
    }
  }
  
  # Print validation results
  cat("\n📊 STATISTICAL VALIDATION RESULTS:\n")
  cat("   Metric           | p-value  | Cohen's d | Significant?\n")
  cat("   -----------------------------------------------------\n")
  
  for(metric in names(validation_tests)) {
    test <- validation_tests[[metric]]
    cat(sprintf("   %-16s | %.4f   | %.3f     | %s\n",
                metric,
                test$p_value,
                test$cohens_d,
                ifelse(test$significant, "YES", "NO")))
  }
  
  # Interpretation guidelines
  cat("\n📝 INTERPRETATION GUIDELINES:\n")
  cat(" • Cohen's d < 0.2: Negligible effect\n")
  cat(" • Cohen's d 0.2-0.5: Small effect\n")  
  cat(" • Cohen's d 0.5-0.8: Medium effect\n")
  cat(" • Cohen's d > 0.8: Large effect\n")
  cat(" • p-value < 0.05: Statistically significant difference\n")
  
  return(list(
    imputed_companies = imputed_companies,
    non_imputed_companies = non_imputed_companies,
    validation_tests = validation_tests,
    interpretation = "Cohen's d measures practical significance of imputation impact"
  ))
}