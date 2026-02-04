# ============================================================================
# COMPLETE DAII 3.5 FRAMEWORK CODE COMPILATION (1/20/2026)
TABLE OF CONTENTS
THEORETICAL FOUNDATION - Innovation Measurement Framework
DATA PIPELINE - Data Loading & Preprocessing
IMPUTATION ENGINE - Missing Data Handling
SCORING ENGINE - Component Score Calculations
AGGREGATION FRAMEWORK - Weighted DAII 3.5 Calculation
PORTFOLIO INTEGRATION - DUMAC Holdings Mapping
VALIDATION SYSTEM - Quality Assurance & Business Review
VISUALIZATION SUITE - Data Representation & Insights
OUTPUT PACKAGE - Complete Deliverables Generation
REPRODUCIBILITY - Configuration & Customization
# ============================================================================
# ============================================================================
# DAII 3.5
# ============================================================================
# 
# THEORETICAL FRAMEWORK:
# ----------------------
# DAII 3.5 measures company innovation across 5 dimensions:
# 
# 1. R&D INTENSITY (30%) - Core innovation investment
#    - Theory: Schumpeterian "Creative Destruction" - Innovation drives growth
#    - Metric: R&D Expense / Market Capitalization
#    - Transformation: Log(x + ε) to handle extreme skewness
#    - Why 30%?: Primary driver of long-term innovation capability
# 
# 2. ANALYST SENTIMENT (20%) - Market perception of innovation
#    - Theory: Efficient Market Hypothesis - Prices reflect available information
#    - Metric: Bloomberg Best Analyst Rating (1-5 scale, normalized to 0-100)
#    - Why 20%?: Reduced from 25% to balance market perception with actual R&D
# 
# 3. PATENT ACTIVITY (25%) - Intellectual property creation
#    - Theory: Endogenous Growth Theory - Knowledge creation drives growth
#    - Metric: Log(Patents + Trademarks + Copyrights + 1)
#    - Transformation: Log to normalize count data
#    - Why 25%?: Increased from 20% to emphasize formal IP protection
# 
# 4. NEWS SENTIMENT (10%) - Media perception of innovation
#    - Theory: Behavioral Finance - Media sentiment influences perceptions
#    - Metric: News sentiment score (normalized to 0-100)
#    - Why 10%?: Reduced from 15% as noisy, short-term signal
# 
# 5. GROWTH MOMENTUM (15%) - Business performance supporting innovation
#    - Theory: Innovation Diffusion Theory - Growth enables further innovation
#    - Metric: Revenue growth (1-year, normalized to 0-100)
#    - Why 15%?: Stable weighting - links innovation to business results
# 
# STATISTICAL PRINCIPLES APPLIED:
# --------------------------------
# 1. Normalization: Min-Max scaling to 0-100 for comparability
# 2. Log Transformation: For highly skewed distributions (R&D, Patents)
# 3. Winsorization: Implicit through normalization (handles extreme values)
# 4. Missing Data Imputation: Median/Mean imputation with tracking
# 5. Weighted Aggregation: Linear combination with sum to 100%
# 6. Quartile Classification: Equal-frequency bins for portfolio construction
# 
# INNOVATION THEORY INTEGRATION:
# ------------------------------
# - Schumpeter (1934): Innovation as new combinations
# - Romer (1990): Endogenous growth through knowledge accumulation
# - Teece (1997): Dynamic capabilities for innovation
# - Christensen (1997): Disruptive innovation theory
# 
# PRACTICAL DESIGN DECISIONS:
# ---------------------------
# 1. Company-level scoring: Innovation is a company attribute, not holding attribute
# 2. Holdings aggregation: Multiple fund holdings of same company get same score
# 3. Portfolio weighting: Market-value weighted innovation score
# 4. Industry benchmarking: Controls for sector innovation patterns
# 5. Validation framework: Combines statistical and business validation
# 
# ============================================================================


# ============================================================================
# MODULE 1: DATA PIPELINE - Loading & Preprocessing
# ============================================================================
# 
# PURPOSE: Load, validate, and prepare raw data for DAII calculation
# 
# DESIGN PRINCIPLES:
# 1. Robustness: Handle various data formats and missing patterns
# 2. Transparency: Track all data transformations
# 3. Reproducibility: Version control for data inputs
# 4. Scalability: Handle from 200 to 2,000+ companies
# 
# STATISTICAL CONCEPTS:
# - Data validation: Range checks, type validation, completeness assessment
# - Missing data patterns: MCAR, MAR, MNAR assessment
# - Data cleaning: Outlier detection, consistency checks
# 
# ============================================================================

initialize_daii_environment <- function(working_dir = NULL) {
  #' Initialize DAII 3.5 Analysis Environment
  #' 
  #' @param working_dir Path to working directory (optional)
  #' @return List of environment settings
  #' 
  #' CONCEPT: Environment setup ensures reproducibility across sessions
  
  cat("🎯 INITIALIZING DAII 3.5 ANALYSIS ENVIRONMENT\n")
  cat(paste(rep("=", 70), collapse = ""), "\n")
  
  # Set working directory
  if(!is.null(working_dir)) {
    setwd(working_dir)
    cat(sprintf("📂 Working directory: %s\n", getwd()))
  }
  
  # Create directory structure
  dirs_to_create <- c(
    "01_Raw_Data",
    "02_Imputed_Data", 
    "03_Intermediate",
    "04_Results",
    "05_Visualizations",
    "06_Validation",
    "07_Comparison",
    "00_DUMAC_Data"
  )
  
  for(dir in dirs_to_create) {
    if(!dir.exists(dir)) {
      dir.create(dir, showWarnings = FALSE)
      cat(sprintf("📁 Created directory: %s/\n", dir))
    }
  }
  
  # Load required packages
  required_packages <- c(
    "dplyr",        # Data manipulation
    "tidyr",        # Data tidying
    "ggplot2",      # Visualization
    "corrplot",     # Correlation visualization
    "gridExtra",    # Multiple plots
    "psych",        # Psychometric analysis
    "reshape2",     # Data reshaping
    "openxlsx",     # Excel output
    "rmarkdown",    # Reporting
    "knitr"         # Dynamic reporting
  )
  
  cat("\n📦 LOADING REQUIRED PACKAGES:\n")
  for(pkg in required_packages) {
    if(!require(pkg, character.only = TRUE)) {
      install.packages(pkg, dependencies = TRUE)
      library(pkg, character.only = TRUE)
    }
    cat(sprintf("✅ %s\n", pkg))
  }
  
  # Return environment settings
  return(list(
    working_dir = getwd(),
    directories = dirs_to_create,
    packages = required_packages,
    timestamp = format(Sys.time(), "%Y-%m-%d %H:%M:%S")
  ))
}

load_and_validate_data <- function(data_path, 
                                   expected_cols = NULL,
                                   min_companies = 100,
                                   max_missing_pct = 0.5) {
  #' Load and Validate Input Data
  #' 
  #' @param data_path Path to input CSV file
  #' @param expected_cols Expected column names (optional)
  #' @param min_companies Minimum number of companies required
  #' @param max_missing_pct Maximum allowed missing data percentage
  #' @return List containing validated data and validation report
  #' 
  #' STATISTICAL CONCEPT: Data quality assessment before processing
  
  cat("\n📥 LOADING AND VALIDATING INPUT DATA\n")
  cat(paste(rep("-", 50), collapse = ""), "\n")
  
  # Load data
  if(!file.exists(data_path)) {
    stop(sprintf("❌ Data file not found: %s", data_path))
  }
  
  data <- read.csv(data_path, stringsAsFactors = FALSE, check.names = FALSE)
  cat(sprintf("✅ Data loaded: %d rows, %d columns\n", 
              nrow(data), ncol(data)))
  
  # Identify ticker column (first column assumed)
  ticker_col <- names(data)[1]
  cat(sprintf("📊 Ticker column identified: %s\n", ticker_col))
  
  # Basic validation
  validation_report <- list(
    file_info = list(
      path = data_path,
      size_bytes = file.size(data_path),
      load_time = Sys.time()
    ),
    structure = list(
      total_rows = nrow(data),
      total_cols = ncol(data),
      ticker_col = ticker_col,
      unique_companies = length(unique(data[[ticker_col]]))
    ),
    missing_data = list(),
    data_types = list()
  )
  
  # Check minimum companies requirement
  if(validation_report$structure$unique_companies < min_companies) {
    warning(sprintf("⚠️ Only %d unique companies (minimum %d required)",
                    validation_report$structure$unique_companies, min_companies))
  }
  
  # Validate expected columns if provided
  if(!is.null(expected_cols)) {
    missing_cols <- setdiff(expected_cols, names(data))
    if(length(missing_cols) > 0) {
      warning(sprintf("⚠️ Missing expected columns: %s", 
                      paste(missing_cols, collapse = ", ")))
    }
  }
  
  # Analyze data types
  for(col in names(data)) {
    validation_report$data_types[[col]] <- class(data[[col]])
  }
  
  # Analyze missing data patterns
  numeric_cols <- grep("R\\.D\\.Exp|Mkt\\.Cap|Rev|BEst\\.Analyst|Patents|News", 
                       names(data), value = TRUE, ignore.case = TRUE)
  
  missing_summary <- data.frame(
    Column = character(),
    Missing_Count = integer(),
    Missing_Percent = numeric(),
    stringsAsFactors = FALSE
  )
  
  for(col in numeric_cols) {
    if(col %in% names(data)) {
      # Count various missing indicators
      missing_patterns <- c("#N/A", "N/A", "NA", "NULL", "", "Field Not Applicable")
      missing_count <- sum(
        is.na(data[[col]]) | 
          data[[col]] %in% missing_patterns |
          grepl(paste(missing_patterns, collapse = "|"), data[[col]], ignore.case = TRUE)
      )
      
      missing_pct <- round(100 * missing_count / nrow(data), 1)
      
      missing_summary <- rbind(missing_summary, data.frame(
        Column = col,
        Missing_Count = missing_count,
        Missing_Percent = missing_pct
      ))
      
      # Flag high missing percentages
      if(missing_pct > max_missing_pct * 100) {
        warning(sprintf("⚠️ High missing data in %s: %.1f%%", col, missing_pct))
      }
    }
  }
  
  validation_report$missing_data$summary <- missing_summary
  
  # Print validation summary
  cat("\n📋 DATA VALIDATION SUMMARY:\n")
  cat(sprintf(" • Total holdings (rows): %d\n", nrow(data)))
  cat(sprintf(" • Unique companies: %d\n", validation_report$structure$unique_companies))
  cat(sprintf(" • Columns requiring imputation: %d\n", 
              sum(missing_summary$Missing_Percent > 0)))
  
  if(nrow(missing_summary) > 0) {
    cat("\n📊 MISSING DATA ANALYSIS:\n")
    print(missing_summary)
  }
  
  # Detect holdings structure
  fund_col <- grep("fund", names(data), ignore.case = TRUE, value = TRUE)[1]
  if(!is.null(fund_col)) {
    unique_funds <- length(unique(data[[fund_col]]))
    cat(sprintf("\n🏦 PORTFOLIO STRUCTURE DETECTED:\n"))
    cat(sprintf(" • Fund column: %s\n", fund_col))
    cat(sprintf(" • Unique funds: %d\n", unique_funds))
    
    # Analyze holdings distribution
    holdings_per_company <- table(data[[ticker_col]])
    cat(sprintf(" • Average holdings per company: %.1f\n", 
                mean(as.numeric(holdings_per_company))))
    cat(sprintf(" • Max holdings per company: %d\n", 
                max(as.numeric(holdings_per_company))))
    
    validation_report$portfolio_structure <- list(
      fund_column = fund_col,
      unique_funds = unique_funds,
      holdings_distribution = summary(as.numeric(holdings_per_company))
    )
  }
  
  return(list(
    data = data,
    ticker_col = ticker_col,
    validation_report = validation_report
  ))
}

extract_unique_companies <- function(data, ticker_col, 
                                     exclude_cols = NULL,
                                     min_components = 3) {
  #' Extract Unique Companies for Scoring
  #' 
  #' @param data Full holdings data
  #' @param ticker_col Ticker column name
  #' @param exclude_cols Columns to exclude (fund-specific)
  #' @param min_components Minimum component columns required
  #' @return Unique company-level data
  #' 
  #' DESIGN PRINCIPLE: Innovation measured at company level, not holding level
  
  cat("\n🏢 EXTRACTING UNIQUE COMPANIES FOR SCORING\n")
  cat(paste(rep("-", 50), collapse = ""), "\n")
  
  # Identify fund-specific columns to exclude
  if(is.null(exclude_cols)) {
    exclude_cols <- grep("fund|weight|allocation|shares|position|date", 
                         names(data), ignore.case = TRUE, value = TRUE)
  }
  
  # Keep company-level columns only
  company_cols <- setdiff(names(data), exclude_cols)
  
  # Extract unique companies (remove duplicate rows for same company)
  unique_companies <- data[!duplicated(data[[ticker_col]]), company_cols]
  
  cat(sprintf("✅ Extracted %d unique companies from %d holdings\n", 
              nrow(unique_companies), nrow(data)))
  
  # Verify required component columns exist
  required_component_cols <- c(
    "R.D.Exp", "Mkt.Cap", "Rev...1.Yr.Gr", 
    "BEst.Analyst.Rtg", "Patents...Trademarks...Copy.Rgt", "News.Sent"
  )
  
  missing_components <- setdiff(required_component_cols, names(unique_companies))
  if(length(missing_components) > 0) {
    warning(sprintf("⚠️ Missing component columns: %s", 
                    paste(missing_components, collapse = ", ")))
  }
  
  # Check data completeness for scoring
  component_check <- list()
  for(col in intersect(required_component_cols, names(unique_companies))) {
    non_missing <- sum(!is.na(unique_companies[[col]]) & 
                         !grepl("#N/A|N/A", unique_companies[[col]]))
    component_check[[col]] <- round(100 * non_missing / nrow(unique_companies), 1)
  }
  
  cat("\n📊 COMPANY-LEVEL DATA COMPLETENESS:\n")
  for(col in names(component_check)) {
    cat(sprintf(" • %-35s: %6.1f%% complete\n", col, component_check[[col]]))
  }
  
  return(list(
    companies = unique_companies,
    company_cols = company_cols,
    component_completeness = component_check
  ))
}

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

# ============================================================================
# MODULE 7: VISUALIZATION SUITE - Data Representation & Insights
# ============================================================================
#
# PURPOSE: Generate comprehensive visualizations for DAII 3.5 analysis
#
# VISUALIZATION PHILOSOPHY:
# 1. Clarity: Clear communication of complex information
# 2. Accuracy: Faithful representation of data
# 3. Insight: Reveal patterns and relationships
# 4. Actionability: Support decision-making
#
# ============================================================================

create_daii_visualizations <- function(daii_data,
                                       portfolio_results = NULL,
                                       validation_report = NULL,
                                       output_dir = "05_Visualizations") {
  #' Create Comprehensive DAII 3.5 Visualizations
  #' 
  #' @param daii_data Company-level DAII data
  #' @param portfolio_results Portfolio integration results
  #' @param validation_report Validation results
  #' @param output_dir Output directory for plots
  #' @return List of generated visualizations
  
  cat("\n🎨 CREATING COMPREHENSIVE DAII 3.5 VISUALIZATIONS\n")
  cat(paste(rep("=", 60), collapse = ""), "\n")
  
  # Create output directory
  if(!dir.exists(output_dir)) {
    dir.create(output_dir, showWarnings = FALSE)
  }
  
  visualization_list <- list()
  
  # 1. DISTRIBUTION VISUALIZATIONS
  cat("\n1️⃣ DISTRIBUTION VISUALIZATIONS\n")
  distribution_plots <- create_distribution_visualizations(daii_data, output_dir)
  visualization_list$distributions <- distribution_plots
  
  # 2. RELATIONSHIP VISUALIZATIONS
  cat("\n2️⃣ RELATIONSHIP VISUALIZATIONS\n")
  relationship_plots <- create_relationship_visualizations(daii_data, output_dir)
  visualization_list$relationships <- relationship_plots
  
  # 3. COMPOSITION VISUALIZATIONS
  cat("\n3️⃣ COMPOSITION VISUALIZATIONS\n")
  composition_plots <- create_composition_visualizations(daii_data, output_dir)
  visualization_list$composition <- composition_plots
  
  # 4. PORTFOLIO VISUALIZATIONS (if portfolio data available)
  if(!is.null(portfolio_results)) {
    cat("\n4️⃣ PORTFOLIO VISUALIZATIONS\n")
    portfolio_plots <- create_portfolio_visualizations(
      daii_data, portfolio_results, output_dir
    )
    visualization_list$portfolio <- portfolio_plots
  }
  
  # 5. VALIDATION VISUALIZATIONS (if validation data available)
  if(!is.null(validation_report)) {
    cat("\n5️⃣ VALIDATION VISUALIZATIONS\n")
    validation_plots <- create_validation_visualizations(
      validation_report, output_dir
    )
    visualization_list$validation <- validation_plots
  }
  
  # 6. CREATE VISUALIZATION SUMMARY REPORT
  cat("\n6️⃣ CREATING VISUALIZATION SUMMARY REPORT\n")
  
  summary_report <- create_visualization_summary(
    visualization_list,
    output_dir
  )
  
  cat(sprintf("\n✅ Visualizations saved to: %s/\n", output_dir))
  cat(sprintf("   Total plots generated: %d\n", length(unlist(visualization_list))))
  
  return(list(
    plots = visualization_list,
    summary = summary_report,
    output_directory = output_dir
  ))
}

create_distribution_visualizations <- function(daii_data, output_dir) {
  #' Create Distribution Visualizations
  #' 
  #' @param daii_data Company-level DAII data
  #' @param output_dir Output directory
  #' @return List of distribution plots
  
  plots <- list()
  
  # Define color scheme
  daii_colors <- c(
    "Q1 (High)" = "#2E8B57",  # Sea Green
    "Q2" = "#87CEEB",         # Sky Blue
    "Q3" = "#FFD700",         # Gold
    "Q4 (Low)" = "#CD5C5C"    # Indian Red
  )
  
  # 1. DAII 3.5 Score Distribution (Histogram with Density)
  p1 <- ggplot(daii_data, aes(x = DAII_3.5_Score)) +
    geom_histogram(aes(y = ..density..), 
                   bins = 30, 
                   fill = "#4B9CD3", 
                   alpha = 0.7) +
    geom_density(color = "#1F4E79", size = 1.2) +
    geom_vline(aes(xintercept = mean(DAII_3.5_Score, na.rm = TRUE)),
               color = "#FF6B6B", 
               size = 1, 
               linetype = "dashed") +
    labs(
      title = "Distribution of DAII 3.5 Scores",
      subtitle = "Histogram with Density Curve",
      x = "DAII 3.5 Score",
      y = "Density"
    ) +
    theme_minimal() +
    theme(
      plot.title = element_text(size = 16, face = "bold"),
      plot.subtitle = element_text(size = 12, color = "gray50")
    )
  
  plots$daii_histogram <- p1
  ggsave(file.path(output_dir, "01_daii_distribution.png"), 
         p1, width = 10, height = 6, dpi = 300)
  
  # 2. Component Score Distributions (Faceted)
  component_data <- daii_data %>%
    select(Ticker, R_D_Score, Analyst_Score, Patent_Score, 
           News_Score, Growth_Score) %>%
    pivot_longer(cols = -Ticker, 
                 names_to = "Component", 
                 values_to = "Score")
  
  # Define component names
  component_labels <- c(
    "R_D_Score" = "R&D Score",
    "Analyst_Score" = "Analyst Score", 
    "Patent_Score" = "Patent Score",
    "News_Score" = "News Score",
    "Growth_Score" = "Growth Score"
  )
  
  component_data$Component <- factor(
    component_data$Component,
    levels = names(component_labels),
    labels = component_labels
  )
  
  p2 <- ggplot(component_data, aes(x = Score, fill = Component)) +
    geom_histogram(bins = 25, alpha = 0.7) +
    facet_wrap(~ Component, scales = "free", ncol = 3) +
    scale_fill_brewer(palette = "Set2") +
    labs(
      title = "Distribution of Component Scores",
      subtitle = "All scores normalized to 0-100 scale",
      x = "Score",
      y = "Count"
    ) +
    theme_minimal() +
    theme(
      plot.title = element_text(size = 16, face = "bold"),
      strip.text = element_text(size = 10, face = "bold"),
      legend.position = "none"
    )
  
  plots$component_distributions <- p2
  ggsave(file.path(output_dir, "02_component_distributions.png"), 
         p2, width = 12, height = 8, dpi = 300)
  
  # 3. Box Plot by Quartile
  p3 <- ggplot(daii_data, aes(x = DAII_Quartile, y = DAII_3.5_Score, fill = DAII_Quartile)) +
    geom_boxplot(alpha = 0.8, outlier.color = "#FF6B6B") +
    geom_jitter(width = 0.2, alpha = 0.3, size = 1) +
    scale_fill_manual(values = daii_colors) +
    labs(
      title = "DAII 3.5 Score Distribution by Quartile",
      subtitle = "Box plot with individual company points",
      x = "Innovation Quartile",
      y = "DAII 3.5 Score"
    ) +
    theme_minimal() +
    theme(
      plot.title = element_text(size = 16, face = "bold"),
      axis.text.x = element_text(angle = 45, hjust = 1)
    )
  
  plots$quartile_boxplot <- p3
  ggsave(file.path(output_dir, "03_quartile_boxplot.png"), 
         p3, width = 10, height = 7, dpi = 300)
  
  # 4. Violin Plot of Component Scores by DAII Quartile
  component_quartile_data <- daii_data %>%
    select(Ticker, DAII_Quartile, R_D_Score, Analyst_Score, 
           Patent_Score, News_Score, Growth_Score) %>%
    pivot_longer(cols = c(R_D_Score, Analyst_Score, Patent_Score, 
                          News_Score, Growth_Score),
                 names_to = "Component",
                 values_to = "Score")
  
  component_quartile_data$Component <- factor(
    component_quartile_data$Component,
    levels = c("R_D_Score", "Analyst_Score", "Patent_Score", 
               "News_Score", "Growth_Score"),
    labels = c("R&D", "Analyst", "Patent", "News", "Growth")
  )
  
  p4 <- ggplot(component_quartile_data, 
               aes(x = DAII_Quartile, y = Score, fill = DAII_Quartile)) +
    geom_violin(alpha = 0.7, scale = "width") +
    geom_boxplot(width = 0.1, fill = "white", alpha = 0.5) +
    facet_wrap(~ Component, ncol = 3) +
    scale_fill_manual(values = daii_colors) +
    labs(
      title = "Component Score Distributions by Innovation Quartile",
      subtitle = "Violin plots show density, box plots show quartiles",
      x = "DAII Quartile",
      y = "Component Score"
    ) +
    theme_minimal() +
    theme(
      plot.title = element_text(size = 16, face = "bold"),
      strip.text = element_text(size = 10, face = "bold"),
      axis.text.x = element_text(angle = 45, hjust = 1),
      legend.position = "none"
    )
  
  plots$component_violin <- p4
  ggsave(file.path(output_dir, "04_component_violin.png"), 
         p4, width = 14, height = 10, dpi = 300)
  
  return(plots)
}

create_relationship_visualizations <- function(daii_data, output_dir) {
  #' Create Relationship Visualizations
  #' 
  #' @param daii_data Company-level DAII data
  #' @param output_dir Output directory
  #' @return List of relationship plots
  
  plots <- list()
  
  # 1. Correlation Matrix Heatmap
  correlation_cols <- c("DAII_3.5_Score", "R_D_Score", "Analyst_Score", 
                        "Patent_Score", "News_Score", "Growth_Score")
  
  cor_matrix <- cor(daii_data[, correlation_cols], use = "complete.obs")
  
  # Melt correlation matrix
  cor_melted <- melt(cor_matrix)
  
  p1 <- ggplot(cor_melted, aes(x = Var1, y = Var2, fill = value)) +
    geom_tile(color = "white") +
    geom_text(aes(label = sprintf("%.2f", value)), 
              color = "black", size = 4) +
    scale_fill_gradient2(low = "#CD5C5C", mid = "white", high = "#2E8B57",
                         midpoint = 0, limit = c(-1, 1), 
                         name = "Correlation") +
    labs(
      title = "Correlation Matrix of DAII Components",
      subtitle = "Pearson correlation coefficients",
      x = "",
      y = ""
    ) +
    theme_minimal() +
    theme(
      plot.title = element_text(size = 16, face = "bold"),
      axis.text.x = element_text(angle = 45, hjust = 1),
      legend.position = "right"
    ) +
    coord_fixed()
  
  plots$correlation_heatmap <- p1
  ggsave(file.path(output_dir, "05_correlation_heatmap.png"), 
         p1, width = 10, height = 8, dpi = 300)
  
  # 2. Scatter Plot Matrix
  scatter_data <- daii_data[, correlation_cols]
  colnames(scatter_data) <- c("DAII", "R&D", "Analyst", "Patent", "News", "Growth")
  
  # Create custom panel functions
  panel.cor <- function(x, y, digits = 2, prefix = "", cex.cor, ...) {
    usr <- par("usr"); on.exit(par(usr))
    par(usr = c(0, 1, 0, 1))
    r <- abs(cor(x, y, use = "complete.obs"))
    txt <- format(c(r, 0.123456789), digits = digits)[1]
    txt <- paste0(prefix, txt)
    if(missing(cex.cor)) cex.cor <- 0.8/strwidth(txt)
    text(0.5, 0.5, txt, cex = cex.cor * r)
  }
  
  panel.hist <- function(x, ...) {
    usr <- par("usr"); on.exit(par(usr))
    par(usr = c(usr[1:2], 0, 1.5))
    h <- hist(x, plot = FALSE)
    breaks <- h$breaks; nB <- length(breaks)
    y <- h$counts; y <- y/max(y)
    rect(breaks[-nB], 0, breaks[-1], y, col = "#4B9CD3", ...)
  }
  
  panel.scatter <- function(x, y, ...) {
    points(x, y, pch = 19, col = rgb(0, 0, 0, 0.3), cex = 0.6)
    abline(lm(y ~ x), col = "#FF6B6B", lwd = 2)
  }
  
  # Save scatter plot matrix
  png(file.path(output_dir, "06_scatter_matrix.png"), 
      width = 12, height = 10, units = "in", res = 300)
  pairs(scatter_data,
        upper.panel = panel.cor,
        diag.panel = panel.hist,
        lower.panel = panel.scatter,
        main = "Scatter Plot Matrix of DAII Components")
  dev.off()
  
  plots$scatter_matrix <- "06_scatter_matrix.png"
  
  # 3. R&D vs Analyst Score with DAII Color
  p3 <- ggplot(daii_data, aes(x = R_D_Score, y = Analyst_Score, color = DAII_3.5_Score)) +
    geom_point(size = 3, alpha = 0.7) +
    geom_smooth(method = "lm", se = FALSE, color = "#1F4E79") +
    scale_color_gradient2(low = "#CD5C5C", mid = "#FFD700", high = "#2E8B57",
                          midpoint = median(daii_data$DAII_3.5_Score, na.rm = TRUE),
                          name = "DAII 3.5 Score") +
    labs(
      title = "R&D Score vs Analyst Score",
      subtitle = "Color represents DAII 3.5 Score",
      x = "R&D Score",
      y = "Analyst Score"
    ) +
    theme_minimal() +
    theme(
      plot.title = element_text(size = 16, face = "bold"),
      legend.position = "right"
    )
  
  plots$rd_vs_analyst <- p3
  ggsave(file.path(output_dir, "07_rd_vs_analyst.png"), 
         p3, width = 10, height = 7, dpi = 300)
  
  # 4. Parallel Coordinates Plot (Top 20 companies)
  top_20 <- daii_data %>%
    arrange(desc(DAII_3.5_Score)) %>%
    head(20)
  
  # Prepare data for parallel coordinates
  parallel_data <- top_20 %>%
    select(Ticker, R_D_Score, Analyst_Score, Patent_Score, 
           News_Score, Growth_Score, DAII_3.5_Score) %>%
    mutate(Company = paste0(Ticker, " (", round(DAII_3.5_Score, 1), ")")) %>%
    select(-Ticker, -DAII_3.5_Score)
  
  # Reshape for plotting
  parallel_long <- parallel_data %>%
    pivot_longer(cols = -Company, names_to = "Component", values_to = "Score")
  
  parallel_long$Component <- factor(
    parallel_long$Component,
    levels = c("R_D_Score", "Analyst_Score", "Patent_Score", "News_Score", "Growth_Score"),
    labels = c("R&D", "Analyst", "Patent", "News", "Growth")
  )
  
  p4 <- ggplot(parallel_long, aes(x = Component, y = Score, group = Company, color = Company)) +
    geom_line(size = 1, alpha = 0.7) +
    geom_point(size = 2) +
    scale_color_viridis_d(name = "Company (DAII Score)") +
    labs(
      title = "Parallel Coordinates: Top 20 Innovators",
      subtitle = "Component score profiles",
      x = "Component",
      y = "Score"
    ) +
    theme_minimal() +
    theme(
      plot.title = element_text(size = 16, face = "bold"),
      axis.text.x = element_text(angle = 45, hjust = 1),
      legend.position = "right",
      legend.text = element_text(size = 8)
    )
  
  plots$parallel_coordinates <- p4
  ggsave(file.path(output_dir, "08_parallel_coordinates.png"), 
         p4, width = 12, height = 8, dpi = 300)
  
  return(plots)
}

create_composition_visualizations <- function(daii_data, output_dir) {
  #' Create Composition Visualizations
  #' 
  #' @param daii_data Company-level DAII data
  #' @param output_dir Output directory
  #' @return List of composition plots
  
  plots <- list()
  
  # 1. Pie Chart of Quartile Distribution
  quartile_counts <- as.data.frame(table(daii_data$DAII_Quartile))
  colnames(quartile_counts) <- c("Quartile", "Count")
  
  # Calculate percentages
  quartile_counts$Percentage <- round(100 * quartile_counts$Count / sum(quartile_counts$Count), 1)
  quartile_counts$Label <- paste0(quartile_counts$Quartile, "\n", 
                                  quartile_counts$Count, " (", 
                                  quartile_counts$Percentage, "%)")
  
  p1 <- ggplot(quartile_counts, aes(x = "", y = Count, fill = Quartile)) +
    geom_bar(stat = "identity", width = 1) +
    coord_polar("y", start = 0) +
    geom_text(aes(label = Label), 
              position = position_stack(vjust = 0.5),
              color = "white", size = 4, fontface = "bold") +
    scale_fill_manual(values = c("Q1 (High)" = "#2E8B57",
                                 "Q2" = "#87CEEB",
                                 "Q3" = "#FFD700",
                                 "Q4 (Low)" = "#CD5C5C")) +
    labs(
      title = "Distribution of Companies by Innovation Quartile",
      fill = "Innovation Quartile"
    ) +
    theme_void() +
    theme(
      plot.title = element_text(size = 16, face = "bold", hjust = 0.5),
      legend.position = "bottom"
    )
  
  plots$quartile_pie <- p1
  ggsave(file.path(output_dir, "09_quartile_pie.png"), 
         p1, width = 8, height = 8, dpi = 300)
  
  # 2. Stacked Bar Chart: Component Contribution by Quartile
  component_contributions <- daii_data %>%
    group_by(DAII_Quartile) %>%
    summarise(
      R_D = mean(R_D_Score, na.rm = TRUE) * 0.30,
      Analyst = mean(Analyst_Score, na.rm = TRUE) * 0.20,
      Patent = mean(Patent_Score, na.rm = TRUE) * 0.25,
      News = mean(News_Score, na.rm = TRUE) * 0.10,
      Growth = mean(Growth_Score, na.rm = TRUE) * 0.15,
      .groups = 'drop'
    ) %>%
    pivot_longer(cols = c(R_D, Analyst, Patent, News, Growth),
                 names_to = "Component",
                 values_to = "Contribution")
  
  p2 <- ggplot(component_contributions, 
               aes(x = DAII_Quartile, y = Contribution, fill = Component)) +
    geom_bar(stat = "identity", position = "stack") +
    scale_fill_brewer(palette = "Set2", 
                      labels = c("R&D", "Analyst", "Patent", "News", "Growth")) +
    labs(
      title = "Component Contribution to DAII Score by Quartile",
      subtitle = "Stacked by weighted component contributions",
      x = "Innovation Quartile",
      y = "Weighted Contribution"
    ) +
    theme_minimal() +
    theme(
      plot.title = element_text(size = 16, face = "bold"),
      axis.text.x = element_text(angle = 45, hjust = 1)
    )
  
  plots$component_stacked <- p2
  ggsave(file.path(output_dir, "10_component_stacked.png"), 
         p2, width = 10, height = 7, dpi = 300)
  
  # 3. Radar Chart for Top 5 Companies
  top_5 <- daii_data %>%
    arrange(desc(DAII_3.5_Score)) %>%
    head(5)
  
  # Prepare radar chart data
  radar_data <- top_5 %>%
    select(Ticker, R_D_Score, Analyst_Score, Patent_Score, 
           News_Score, Growth_Score) %>%
    mutate_at(vars(-Ticker), ~./100)  # Normalize to 0-1 for radar chart
  
  # Create radar chart
  png(file.path(output_dir, "11_radar_top5.png"), 
      width = 10, height = 8, units = "in", res = 300)
  
  # Set up radar chart parameters
  opar <- par()
  par(mar = c(1, 2, 2, 1))
  
  # Create empty radar chart
  radarchart(
    rbind(rep(1, 5), rep(0, 5), radar_data[, -1]),
    axistype = 1,
    pcol = c("#2E8B57", "#87CEEB", "#FFD700", "#CD5C5C", "#9370DB"),
    pfcol = c(rgb(46/255, 139/255, 87/255, 0.3),
              rgb(135/255, 206/255, 235/255, 0.3),
              rgb(255/255, 215/255, 0, 0.3),
              rgb(205/255, 92/255, 92/255, 0.3),
              rgb(147/255, 112/255, 219/255, 0.3)),
    plwd = 2,
    plty = 1,
    cglcol = "grey",
    cglty = 1,
    axislabcol = "grey",
    caxislabels = c("0", "25", "50", "75", "100"),
    cglwd = 0.8,
    vlcex = 0.8,
    title = "Radar Chart: Component Scores of Top 5 Innovators"
  )
  
  # Add legend
  legend(
    "topright",
    legend = paste0(radar_data$Ticker, " (", 
                    round(top_5$DAII_3.5_Score, 1), ")"),
    bty = "n",
    pch = 20,
    col = c("#2E8B57", "#87CEEB", "#FFD700", "#CD5C5C", "#9370DB"),
    text.col = "black",
    cex = 0.8,
    pt.cex = 1.5
  )
  
  par(opar)
  dev.off()
  
  plots$radar_top5 <- "11_radar_top5.png"
  
  # 4. Treemap of Industry Distribution
  if("GICS.Ind.Grp.Name" %in% names(daii_data)) {
    industry_summary <- daii_data %>%
      group_by(GICS.Ind.Grp.Name) %>%
      summarise(
        Count = n(),
        Avg_DAII = mean(DAII_3.5_Score, na.rm = TRUE),
        .groups = 'drop'
      ) %>%
      filter(Count >= 3)  # Only show industries with at least 3 companies
    
    if(require(treemap) && nrow(industry_summary) > 0) {
      png(file.path(output_dir, "12_industry_treemap.png"), 
          width = 12, height = 8, units = "in", res = 300)
      
      treemap(
        industry_summary,
        index = "GICS.Ind.Grp.Name",
        vSize = "Count",
        vColor = "Avg_DAII",
        type = "value",
        palette = "RdYlGn",
        title = "Industry Distribution by Company Count (Size) and DAII Score (Color)",
        title.legend = "Average DAII Score",
        fontsize.labels = 10,
        fontcolor.labels = "white",
        bg.labels = 0,
        align.labels = list(c("center", "center")),
        overlap.labels = 0.5,
        inflate.labels = TRUE
      )
      
      dev.off()
      
      plots$industry_treemap <- "12_industry_treemap.png"
    }
  }
  
  return(plots)
}

create_portfolio_visualizations <- function(daii_data, portfolio_results, output_dir) {
  #' Create Portfolio Visualizations
  #' 
  #' @param daii_data Company-level DAII data
  #' @param portfolio_results Portfolio integration results
  #' @param output_dir Output directory
  #' @return List of portfolio plots
  
  plots <- list()
  
  # Extract portfolio data
  integrated_data <- portfolio_results$integrated_data
  portfolio_metrics <- portfolio_results$portfolio_metrics
  
  # 1. Portfolio DAII Distribution (Weighted vs Equal)
  p1 <- ggplot(integrated_data, aes(x = DAII_3.5_Score)) +
    geom_histogram(aes(y = ..density.., weight = fund_weight),
                   bins = 30, fill = "#4B9CD3", alpha = 0.5,
                   position = "identity") +
    geom_density(aes(weight = fund_weight), 
                 color = "#1F4E79", size = 1.2) +
    geom_histogram(aes(y = ..density..), 
                   bins = 30, fill = "#FF6B6B", alpha = 0.3,
                   position = "identity") +
    geom_density(color = "#CD5C5C", size = 1.2, linetype = "dashed") +
    geom_vline(aes(xintercept = portfolio_metrics$overall$portfolio_daii),
               color = "#1F4E79", size = 1, linetype = "solid") +
    geom_vline(aes(xintercept = portfolio_metrics$overall$portfolio_daii_equal),
               color = "#CD5C5C", size = 1, linetype = "dashed") +
    annotate("text", 
             x = portfolio_metrics$overall$portfolio_daii,
             y = 0.02,
             label = paste("Weighted:", round(portfolio_metrics$overall$portfolio_daii, 1)),
             color = "#1F4E79", hjust = -0.1) +
    annotate("text",
             x = portfolio_metrics$overall$portfolio_daii_equal,
             y = 0.02,
             label = paste("Equal:", round(portfolio_metrics$overall$portfolio_daii_equal, 1)),
             color = "#CD5C5C", hjust = 1.1) +
    labs(
      title = "Portfolio DAII Distribution: Weighted vs Equal Weight",
      subtitle = "Solid: Weighted by position size | Dashed: Equal weight",
      x = "DAII 3.5 Score",
      y = "Density"
    ) +
    theme_minimal() +
    theme(
      plot.title = element_text(size = 16, face = "bold")
    )
  
  plots$portfolio_distribution <- p1
  ggsave(file.path(output_dir, "13_portfolio_distribution.png"), 
         p1, width = 10, height = 7, dpi = 300)
  
  # 2. Fund-Level Innovation Comparison
  if(!is.null(portfolio_metrics$fund_level)) {
    fund_data <- portfolio_metrics$fund_level %>%
      arrange(desc(fund_daii)) %>%
      head(10)  # Top 10 funds by DAII
    
    p2 <- ggplot(fund_data, aes(x = reorder(fund_name, fund_daii), y = fund_daii)) +
      geom_bar(stat = "identity", aes(fill = fund_daii), alpha = 0.8) +
      geom_hline(yintercept = portfolio_metrics$overall$portfolio_daii,
                 color = "#FF6B6B", size = 1, linetype = "dashed") +
      geom_text(aes(label = paste0(round(fund_daii, 1), "\n(", 
                                   round(fund_weight * 100, 1), "%)")),
                hjust = -0.1, size = 3) +
      scale_fill_gradient(low = "#FFD700", high = "#2E8B57", 
                          name = "Fund DAII") +
      labs(
        title = "Top 10 Funds by Innovation Score",
        subtitle = paste("Overall portfolio DAII:", 
                         round(portfolio_metrics$overall$portfolio_daii, 1)),
        x = "Fund",
        y = "Weighted DAII Score"
      ) +
      coord_flip(ylim = c(min(fund_data$fund_daii) * 0.9, 
                          max(fund_data$fund_daii) * 1.1)) +
      theme_minimal() +
      theme(
        plot.title = element_text(size = 16, face = "bold"),
        axis.text.y = element_text(size = 9)
      )
    
    plots$fund_comparison <- p2
    ggsave(file.path(output_dir, "14_fund_comparison.png"), 
           p2, width = 12, height = 8, dpi = 300)
  }
  
  # 3. Innovation Contribution Waterfall Chart
  if(!is.null(portfolio_results$industry_analysis)) {
    industry_data <- portfolio_results$industry_analysis %>%
      arrange(desc(innovation_contribution)) %>%
      head(10)  # Top 10 industries by contribution
    
    # Calculate cumulative sum for waterfall
    industry_data <- industry_data %>%
      mutate(
        end = cumsum(contribution_percent),
        start = c(0, head(end, -1)),
        Industry = reorder(GICS.Ind.Grp.Name, contribution_percent)
      )
    
    p3 <- ggplot(industry_data) +
      geom_rect(aes(xmin = as.numeric(Industry) - 0.4,
                    xmax = as.numeric(Industry) + 0.4,
                    ymin = start,
                    ymax = end,
                    fill = innovation_tilt)) +
      geom_text(aes(x = Industry, y = (start + end)/2,
                    label = paste0(GICS.Ind.Grp.Name, "\n",
                                   round(contribution_percent, 1), "%")),
                size = 3, color = "white", fontface = "bold") +
      scale_fill_manual(values = c("Innovation Leader" = "#2E8B57",
                                   "Innovation Lagger" = "#CD5C5C")) +
      labs(
        title = "Top 10 Industries: Innovation Contribution",
        subtitle = "Percentage of portfolio innovation contributed by each industry",
        x = "",
        y = "Cumulative Contribution (%)",
        fill = "Innovation Status"
      ) +
      theme_minimal() +
      theme(
        plot.title = element_text(size = 16, face = "bold"),
        axis.text.x = element_blank(),
        axis.ticks.x = element_blank()
      )
    
    plots$industry_waterfall <- p3
    ggsave(file.path(output_dir, "15_industry_waterfall.png"), 
           p3, width = 12, height = 8, dpi = 300)
  }
  
  # 4. Innovation Concentration Plot (Lorenz Curve)
  # Calculate Lorenz curve for innovation
  sorted_by_daii <- integrated_data[order(integrated_data$DAII_3.5_Score), ]
  sorted_by_daii$cumulative_weight <- cumsum(sorted_by_daii$fund_weight) / 
    sum(sorted_by_daii$fund_weight, na.rm = TRUE)
  sorted_by_daii$cumulative_daii <- cumsum(
    sorted_by_daii$DAII_3.5_Score * sorted_by_daii$fund_weight
  ) / sum(sorted_by_daii$DAII_3.5_Score * sorted_by_daii$fund_weight, na.rm = TRUE)
  
  # Perfect equality line
  equality_df <- data.frame(
    x = seq(0, 1, length.out = 100),
    y = seq(0, 1, length.out = 100)
  )
  
  p4 <- ggplot() +
    geom_line(data = equality_df, aes(x = x, y = y), 
              color = "gray50", linetype = "dashed", size = 1) +
    geom_line(data = sorted_by_daii, 
              aes(x = cumulative_weight, y = cumulative_daii),
              color = "#2E8B57", size = 1.5) +
    geom_ribbon(data = sorted_by_daii,
                aes(x = cumulative_weight, 
                    ymin = cumulative_weight, 
                    ymax = cumulative_daii),
                fill = "#2E8B57", alpha = 0.3) +
    labs(
      title = "Lorenz Curve: Innovation Concentration in Portfolio",
      subtitle = paste("Gini Coefficient:", 
                       round(portfolio_results$concentration_analysis$gini_coefficient, 3)),
      x = "Cumulative Proportion of Portfolio (by weight)",
      y = "Cumulative Proportion of Innovation"
    ) +
    theme_minimal() +
    theme(
      plot.title = element_text(size = 16, face = "bold")
    )
  
  plots$lorenz_curve <- p4
  ggsave(file.path(output_dir, "16_lorenz_curve.png"), 
         p4, width = 10, height = 8, dpi = 300)
  
  return(plots)
}

create_validation_visualizations <- function(validation_report, output_dir) {
  #' Create Validation Visualizations
  #' 
  #' @param validation_report Validation results
  #' @param output_dir Output directory
  #' @return List of validation plots
  
  plots <- list()
  
  # 1. Validation Summary Dashboard
  if(!is.null(validation_report$validation_summary)) {
    summary_data <- validation_report$validation_summary
    
    # Create validation status plot
    status_data <- summary_data %>%
      filter(Validation_Category != "OVERALL") %>%
      mutate(
        Status_Simple = case_when(
          grepl("✅", Status) ~ "Pass",
          grepl("🟡", Status) ~ "Warning",
          grepl("⚠️", Status) ~ "Review",
          TRUE ~ "Unknown"
        )
      )
    
    status_counts <- status_data %>%
      group_by(Validation_Category, Status_Simple) %>%
      summarise(Count = n(), .groups = 'drop')
    
    p1 <- ggplot(status_counts, aes(x = Validation_Category, y = Count, fill = Status_Simple)) +
      geom_bar(stat = "identity", position = "stack") +
      scale_fill_manual(values = c("Pass" = "#2E8B57",
                                   "Warning" = "#FFD700",
                                   "Review" = "#CD5C5C",
                                   "Unknown" = "gray50")) +
      geom_text(aes(label = Count), 
                position = position_stack(vjust = 0.5),
                color = "white", fontface = "bold") +
      labs(
        title = "Validation Status by Category",
        subtitle = "Count of validation metrics by status",
        x = "Validation Category",
        y = "Count",
        fill = "Status"
      ) +
      theme_minimal() +
      theme(
        plot.title = element_text(size = 16, face = "bold"),
        axis.text.x = element_text(angle = 45, hjust = 1)
      )
    
    plots$validation_status <- p1
    ggsave(file.path(output_dir, "17_validation_status.png"), 
           p1, width = 10, height = 7, dpi = 300)
  }
  
  # 2. Imputation Impact Visualization
  if(!is.null(validation_report$process)) {
    impact_data <- validation_report$process$impact_analysis
    
    if(nrow(impact_data) == 2) {
      p2 <- ggplot(impact_data, aes(x = Group, y = mean_daii, fill = Group)) +
        geom_bar(stat = "identity", alpha = 0.8) +
        geom_errorbar(aes(ymin = mean_daii - sd_daii,
                          ymax = mean_daii + sd_daii),
                      width = 0.2) +
        geom_text(aes(label = sprintf("%.1f", mean_daii)),
                  vjust = -0.5, fontface = "bold") +
        scale_fill_manual(values = c("Imputed" = "#FFD700",
                                     "Non-Imputed" = "#2E8B57")) +
        labs(
          title = "Imputation Impact on DAII Scores",
          subtitle = "Comparison of companies with and without imputed values",
          x = "Data Group",
          y = "Mean DAII Score (± SD)"
        ) +
        theme_minimal() +
        theme(
          plot.title = element_text(size = 16, face = "bold"),
          legend.position = "none"
        )
      
      plots$imputation_impact <- p2
      ggsave(file.path(output_dir, "18_imputation_impact.png"), 
             p2, width = 8, height = 7, dpi = 300)
    }
  }
  
  # 3. Component Correlation Validation
  if(!is.null(validation_report$statistical)) {
    cor_matrix <- validation_report$statistical$correlation
    
    # Create annotated correlation plot
    cor_melted <- melt(cor_matrix)
    
    p3 <- ggplot(cor_melted, aes(x = Var1, y = Var2, fill = value)) +
      geom_tile(color = "white") +
      geom_text(aes(label = sprintf("%.2f", value)), 
                color = ifelse(abs(cor_melted$value) > 0.5, "white", "black"),
                fontface = ifelse(abs(cor_melted$value) > 0.7, "bold", "plain")) +
      scale_fill_gradient2(low = "#CD5C5C", mid = "white", high = "#2E8B57",
                           midpoint = 0, limit = c(-1, 1),
                           name = "Correlation") +
      labs(
        title = "Validation: Component Correlation Matrix",
        subtitle = "Bold text indicates correlations > |0.7|",
        x = "",
        y = ""
      ) +
      theme_minimal() +
      theme(
        plot.title = element_text(size = 16, face = "bold"),
        axis.text.x = element_text(angle = 45, hjust = 1)
      ) +
      coord_fixed()
    
    plots$correlation_validation <- p3
    ggsave(file.path(output_dir, "19_correlation_validation.png"), 
           p3, width = 10, height = 8, dpi = 300)
  }
  
  # 4. Known Innovators Validation Plot
  if(!is.null(validation_report$business$face_validity)) {
    innovator_data <- validation_report$business$face_validity$known_innovators
    
    p4 <- ggplot(innovator_data, aes(x = reorder(Ticker, DAII_3.5_Score), 
                                     y = DAII_3.5_Score, 
                                     fill = DAII_Quartile)) +
      geom_bar(stat = "identity", alpha = 0.8) +
      geom_hline(yintercept = 50, color = "#FF6B6B", 
                 linetype = "dashed", size = 1) +
      geom_text(aes(label = round(DAII_3.5_Score, 1)), 
                vjust = -0.5, fontface = "bold") +
      scale_fill_manual(values = c("Q1 (High)" = "#2E8B57",
                                   "Q2" = "#87CEEB",
                                   "Q3" = "#FFD700",
                                   "Q4 (Low)" = "#CD5C5C")) +
      labs(
        title = "Known Technology Innovators: DAII Validation",
        subtitle = "Dashed line shows average score (50)",
        x = "Company",
        y = "DAII 3.5 Score",
        fill = "Quartile"
      ) +
      theme_minimal() +
      theme(
        plot.title = element_text(size = 16, face = "bold"),
        axis.text.x = element_text(angle = 45, hjust = 1)
      )
    
    plots$known_innovators <- p4
    ggsave(file.path(output_dir, "20_known_innovators.png"), 
           p4, width = 10, height = 7, dpi = 300)
  }
  
  # 5. Quartile Distribution Validation
  if(!is.null(validation_report$business$quartile_distribution)) {
    quartile_data <- as.data.frame(
      validation_report$business$quartile_distribution$observed
    )
    colnames(quartile_data) <- c("Quartile", "Observed")
    quartile_data$Expected <- validation_report$business$quartile_distribution$expected
    
    # Reshape for plotting
    quartile_long <- quartile_data %>%
      pivot_longer(cols = c(Observed, Expected),
                   names_to = "Type",
                   values_to = "Count")
    
    p5 <- ggplot(quartile_long, aes(x = Quartile, y = Count, fill = Type)) +
      geom_bar(stat = "identity", position = "dodge", alpha = 0.8) +
      geom_text(aes(label = Count), 
                position = position_dodge(width = 0.9),
                vjust = -0.5, fontface = "bold") +
      scale_fill_manual(values = c("Observed" = "#4B9CD3",
                                   "Expected" = "#FFD700")) +
      labs(
        title = "Quartile Distribution Validation",
        subtitle = paste("Chi-square p-value:", 
                         round(validation_report$business$quartile_distribution$p_value, 4)),
        x = "Innovation Quartile",
        y = "Count",
        fill = "Distribution"
      ) +
      theme_minimal() +
      theme(
        plot.title = element_text(size = 16, face = "bold"),
        axis.text.x = element_text(angle = 45, hjust = 1)
      )
    
    plots$quartile_validation <- p5
    ggsave(file.path(output_dir, "21_quartile_validation.png"), 
           p5, width = 10, height = 7, dpi = 300)
  }
  
  return(plots)
}

create_visualization_summary <- function(visualization_list, output_dir) {
  #' Create Visualization Summary Report
  #' 
  #' @param visualization_list List of all visualizations
  #' @param output_dir Output directory
  #' @return Summary report
  
  summary_report <- data.frame(
    Category = character(),
    Visualization_Type = character(),
    File_Name = character(),
    Description = character(),
    stringsAsFactors = FALSE
  )
  
  # Define visualization descriptions
  viz_descriptions <- list(
    "daii_histogram" = "Distribution of DAII 3.5 scores with density curve",
    "component_distributions" = "Histograms of individual component scores",
    "quartile_boxplot" = "Box plot of DAII scores by innovation quartile",
    "component_violin" = "Violin plots of component scores by quartile",
    "correlation_heatmap" = "Heatmap of component correlations",
    "scatter_matrix" = "Scatter plot matrix with correlations",
    "rd_vs_analyst" = "Scatter plot of R&D vs Analyst scores colored by DAII",
    "parallel_coordinates" = "Parallel coordinates plot of top 20 innovators",
    "quartile_pie" = "Pie chart of quartile distribution",
    "component_stacked" = "Stacked bar chart of component contributions by quartile",
    "radar_top5" = "Radar chart of top 5 innovators' component scores",
    "industry_treemap" = "Treemap of industry distribution",
    "portfolio_distribution" = "Portfolio DAII distribution (weighted vs equal)",
    "fund_comparison" = "Bar chart of top 10 funds by innovation score",
    "industry_waterfall" = "Waterfall chart of industry innovation contributions",
    "lorenz_curve" = "Lorenz curve showing innovation concentration",
    "validation_status" = "Dashboard of validation status by category",
    "imputation_impact" = "Bar chart comparing imputed vs non-imputed companies",
    "correlation_validation" = "Annotated correlation matrix for validation",
    "known_innovators" = "Bar chart of known technology innovators",
    "quartile_validation" = "Comparison of observed vs expected quartile distribution"
  )
  
  # Build summary
  for(category in names(visualization_list)) {
    for(viz_name in names(visualization_list[[category]])) {
      if(viz_name %in% names(viz_descriptions)) {
        file_name <- ifelse(
          is.character(visualization_list[[category]][[viz_name]]),
          visualization_list[[category]][[viz_name]],
          paste0(viz_name, ".png")
        )
        
        summary_report <- rbind(summary_report, data.frame(
          Category = category,
          Visualization_Type = viz_name,
          File_Name = file_name,
          Description = viz_descriptions[[viz_name]],
          stringsAsFactors = FALSE
        ))
      }
    }
  }
  
  # Save summary to CSV
  write.csv(summary_report, 
            file.path(output_dir, "visualization_summary.csv"),
            row.names = FALSE)
  
  # Create HTML gallery
  create_html_gallery(visualization_list, output_dir)
  
  return(summary_report)
}

create_html_gallery <- function(visualization_list, output_dir) {
  #' Create HTML Gallery of Visualizations
  #' 
  #' @param visualization_list List of visualizations
  #' @param output_dir Output directory
  
  html_file <- file.path(output_dir, "visualization_gallery.html")
  
  # Start HTML document
  html_content <- '
  <!DOCTYPE html>
  <html lang="en">
  <head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>DAII 3.5 Visualization Gallery</title>
    <style>
      body {
        font-family: Arial, sans-serif;
        margin: 40px;
        background-color: #f5f5f5;
      }
      h1 {
        color: #2c3e50;
        border-bottom: 2px solid #3498db;
        padding-bottom: 10px;
      }
      h2 {
        color: #34495e;
        margin-top: 30px;
        padding: 10px;
        background-color: #ecf0f1;
        border-radius: 5px;
      }
      .gallery {
        display: grid;
        grid-template-columns: repeat(auto-fill, minmax(300px, 1fr));
        gap: 20px;
        margin-top: 20px;
      }
      .viz-card {
        background-color: white;
        border-radius: 8px;
        padding: 15px;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        transition: transform 0.2s;
      }
      .viz-card:hover {
        transform: translateY(-5px);
        box-shadow: 0 4px 8px rgba(0,0,0,0.2);
      }
      .viz-card img {
        width: 100%;
        height: auto;
        border-radius: 4px;
      }
      .viz-title {
        font-weight: bold;
        margin-top: 10px;
        color: #2c3e50;
      }
      .viz-desc {
        font-size: 12px;
        color: #7f8c8d;
        margin-top: 5px;
      }
      .category-section {
        margin-bottom: 40px;
      }
      .timestamp {
        color: #95a5a6;
        font-size: 12px;
        margin-bottom: 20px;
      }
    </style>
  </head>
  <body>
    <h1>📊 DAII 3.5 Visualization Gallery</h1>
    <div class="timestamp">Generated: ' 
  
  html_content <- paste0(html_content, Sys.time(), '</div>')
  
  # Add each category
  for(category in names(visualization_list)) {
    html_content <- paste0(html_content, '
    <div class="category-section">
      <h2>', toupper(category), ' VISUALIZATIONS</h2>
      <div class="gallery">')
    
    for(viz_name in names(visualization_list[[category]])) {
      file_name <- ifelse(
        is.character(visualization_list[[category]][[viz_name]]),
        visualization_list[[category]][[viz_name]],
        paste0(viz_name, ".png")
      )
      
      # Create descriptive title
      viz_title <- gsub("_", " ", viz_name)
      viz_title <- paste0(toupper(substr(viz_title, 1, 1)), 
                          substr(viz_title, 2, nchar(viz_title)))
      
      html_content <- paste0(html_content, '
        <div class="viz-card">
          <img src="', file_name, '" alt="', viz_title, '">
          <div class="viz-title">', viz_title, '</div>
          <div class="viz-desc">', category, ' visualization</div>
        </div>')
    }
    
    html_content <- paste0(html_content, '
      </div>
    </div>')
  }
  
  # Close HTML
  html_content <- paste0(html_content, '
  </body>
  </html>')
  
  # Write to file
  writeLines(html_content, html_file)
  
  cat(sprintf("   HTML gallery created: %s\n", html_file))
}

# ============================================================================
# MODULE 8: OUTPUT PACKAGE - Complete Deliverables Generation
# ============================================================================
#
# PURPOSE: Generate final output files and deliverables
#
# OUTPUT CATEGORIES:
# 1. Raw Data Files: Processed data in multiple formats
# 2. Analysis Results: Scores, rankings, quartiles
# 3. Portfolio Reports: Fund-level and portfolio-level analysis
# 4. Validation Reports: Quality assurance documentation
# 5. Executive Summaries: High-level insights and recommendations
# 6. Visualization Packages: Complete set of charts and graphs
#
# FILE FORMATS:
# 1. CSV: For data interchange and analysis
# 2. Excel: For business user consumption
# 3. PDF: For formal reporting and distribution
# 4. HTML: For interactive exploration
# 5. RData: For reproducibility and further analysis
#
# ============================================================================

generate_daii_outputs <- function(daii_results,
                                  portfolio_results,
                                  validation_results,
                                  output_dir = "04_Results") {
  #' Generate Complete DAII 3.5 Output Package
  #' 
  #' @param daii_results DAII scoring results
  #' @param portfolio_results Portfolio integration results
  #' @param validation_results Validation results
  #' @param output_dir Output directory
  #' @return List of generated output files
  
  cat("\n📦 GENERATING COMPLETE DAII 3.5 OUTPUT PACKAGE\n")
  cat(paste(rep("=", 60), collapse = ""), "\n")
  
  # Create output directory structure
  timestamp <- format(Sys.time(), "%Y%m%d_%H%M%S")
  run_dir <- file.path(output_dir, paste0("DAII_3.5_Run_", timestamp))
  
  dirs_to_create <- c(
    "01_Company_Scores",
    "02_Portfolio_Analysis",
    "03_Validation_Reports",
    "04_Executive_Summaries",
    "05_Raw_Data",
    "06_Visualizations"
  )
  
  for(dir_name in dirs_to_create) {
    dir_path <- file.path(run_dir, dir_name)
    if(!dir.exists(dir_path)) {
      dir.create(dir_path, recursive = TRUE, showWarnings = FALSE)
    }
  }
  
  output_files <- list()
  
  # 1. COMPANY-LEVEL SCORES
  cat("\n1️⃣ GENERATING COMPANY-LEVEL SCORE FILES\n")
  company_outputs <- generate_company_score_outputs(
    daii_results$daii_data,
    file.path(run_dir, "01_Company_Scores")
  )
  output_files$company_scores <- company_outputs
  
  # 2. PORTFOLIO ANALYSIS REPORTS
  cat("\n2️⃣ GENERATING PORTFOLIO ANALYSIS REPORTS\n")
  portfolio_outputs <- generate_portfolio_outputs(
    portfolio_results,
    file.path(run_dir, "02_Portfolio_Analysis")
  )
  output_files$portfolio_reports <- portfolio_outputs
  
  # 3. VALIDATION REPORTS
  cat("\n3️⃣ GENERATING VALIDATION REPORTS\n")
  validation_outputs <- generate_validation_outputs(
    validation_results,
    file.path(run_dir, "03_Validation_Reports")
  )
  output_files$validation_reports <- validation_outputs
  
  # 4. EXECUTIVE SUMMARIES
  cat("\n4️⃣ GENERATING EXECUTIVE SUMMARIES\n")
  executive_outputs <- generate_executive_summaries(
    daii_results,
    portfolio_results,
    validation_results,
    file.path(run_dir, "04_Executive_Summaries")
  )
  output_files$executive_summaries <- executive_outputs
  
  # 5. RAW DATA EXPORTS
  cat("\n5️⃣ EXPORTING RAW DATA FILES\n")
  raw_data_outputs <- export_raw_data(
    daii_results,
    portfolio_results,
    file.path(run_dir, "05_Raw_Data")
  )
  output_files$raw_data <- raw_data_outputs
  
  # 6. CREATE OUTPUT SUMMARY
  cat("\n6️⃣ CREATING OUTPUT SUMMARY\n")
  output_summary <- create_output_summary(
    output_files,
    run_dir
  )
  
  # 7. CREATE MASTER INDEX FILE
  create_master_index(run_dir, output_files, timestamp)
  
  cat(sprintf("\n✅ Output package generated: %s\n", run_dir))
  cat(sprintf("   Total files created: %d\n", length(unlist(output_files))))
  
  return(list(
    output_directory = run_dir,
    output_files = output_files,
    output_summary = output_summary,
    timestamp = timestamp
  ))
}

generate_company_score_outputs <- function(daii_data, output_dir) {
  #' Generate Company-Level Score Outputs
  #' 
  #' @param daii_data Company-level DAII data
  #' @param output_dir Output directory
  #' @return List of generated files
  
  files <- list()
  
  # 1. Complete Company Scores (CSV)
  file_name <- "daii_3.5_company_scores_complete.csv"
  file_path <- file.path(output_dir, file_name)
  
  # Select and order columns
  score_cols <- c(
    "Ticker", "Company.Name", "GICS.Ind.Grp.Name",
    "DAII_3.5_Score", "DAII_Quartile",
    "R_D_Score", "Analyst_Score", "Patent_Score", "News_Score", "Growth_Score",
    "R.D.Exp_num", "Mkt.Cap_num", "BEst.Analyst.Rtg_num",
    "Patents...Trademarks...Copy.Rgt_num", "News.Sent_num", "Rev...1.Yr.Gr_num"
  )
  
  score_cols <- intersect(score_cols, names(daii_data))
  output_data <- daii_data[, score_cols]
  
  # Sort by DAII score
  output_data <- output_data[order(-output_data$DAII_3.5_Score), ]
  
  write.csv(output_data, file_path, row.names = FALSE)
  files$complete_scores_csv <- file_path
  
  # 2. Top 100 Innovators (CSV)
  file_name <- "daii_3.5_top_100_innovators.csv"
  file_path <- file.path(output_dir, file_name)
  
  top_100 <- output_data %>%
    head(100) %>%
    mutate(Rank = row_number())
  
  write.csv(top_100, file_path, row.names = FALSE)
  files$top_100_csv <- file_path
  
  # 3. Quartile Breakdown (CSV)
  file_name <- "daii_3.5_quartile_breakdown.csv"
  file_path <- file.path(output_dir, file_name)
  
  quartile_summary <- daii_data %>%
    group_by(DAII_Quartile) %>%
    summarise(
      Count = n(),
      Percent = round(100 * n() / nrow(daii_data), 1),
      Avg_DAII = round(mean(DAII_3.5_Score, na.rm = TRUE), 1),
      Min_DAII = round(min(DAII_3.5_Score, na.rm = TRUE), 1),
      Max_DAII = round(max(DAII_3.5_Score, na.rm = TRUE), 1),
      Avg_R_D = round(mean(R_D_Score, na.rm = TRUE), 1),
      Avg_Analyst = round(mean(Analyst_Score, na.rm = TRUE), 1),
      Avg_Patent = round(mean(Patent_Score, na.rm = TRUE), 1),
      Avg_News = round(mean(News_Score, na.rm = TRUE), 1),
      Avg_Growth = round(mean(Growth_Score, na.rm = TRUE), 1),
      .groups = 'drop'
    )
  
  write.csv(quartile_summary, file_path, row.names = FALSE)
  files$quartile_breakdown_csv <- file_path
  
  # 4. Industry Rankings (CSV)
  if("GICS.Ind.Grp.Name" %in% names(daii_data)) {
    file_name <- "daii_3.5_industry_rankings.csv"
    file_path <- file.path(output_dir, file_name)
    
    industry_rankings <- daii_data %>%
      group_by(GICS.Ind.Grp.Name) %>%
      summarise(
        Companies = n(),
        Avg_DAII = round(mean(DAII_3.5_Score, na.rm = TRUE), 1),
        Median_DAII = round(median(DAII_3.5_Score, na.rm = TRUE), 1),
        Min_DAII = round(min(DAII_3.5_Score, na.rm = TRUE), 1),
        Max_DAII = round(max(DAII_3.5_Score, na.rm = TRUE), 1),
        Std_Dev = round(sd(DAII_3.5_Score, na.rm = TRUE), 1),
        Q1_Count = sum(DAII_Quartile == "Q1 (High)", na.rm = TRUE),
        Q1_Percent = round(100 * Q1_Count / n(), 1),
        .groups = 'drop'
      ) %>%
      arrange(desc(Avg_DAII)) %>%
      mutate(Industry_Rank = row_number())
    
    write.csv(industry_rankings, file_path, row.names = FALSE)
    files$industry_rankings_csv <- file_path
  }
  
  # 5. Excel Workbook with Multiple Sheets
  file_name <- "daii_3.5_company_scores.xlsx"
  file_path <- file.path(output_dir, file_name)
  
  wb <- createWorkbook()
  
  # Sheet 1: Complete Scores
  addWorksheet(wb, "All_Companies")
  writeData(wb, "All_Companies", output_data)
  
  # Sheet 2: Top 100
  addWorksheet(wb, "Top_100_Innovators")
  writeData(wb, "Top_100_Innovators", top_100)
  
  # Sheet 3: Quartile Summary
  addWorksheet(wb, "Quartile_Analysis")
  writeData(wb, "Quartile_Analysis", quartile_summary)
  
  # Sheet 4: Industry Analysis
  if(exists("industry_rankings")) {
    addWorksheet(wb, "Industry_Rankings")
    writeData(wb, "Industry_Rankings", industry_rankings)
  }
  
  # Apply formatting
  # Add conditional formatting for quartiles
  quartile_colors <- c("Q1 (High)" = "green", "Q2" = "lightgreen", 
                       "Q3" = "yellow", "Q4 (Low)" = "red")
  
  # Save workbook
  saveWorkbook(wb, file_path, overwrite = TRUE)
  files$excel_workbook <- file_path
  
  return(files)
}

generate_portfolio_outputs <- function(portfolio_results, output_dir) {
  #' Generate Portfolio Analysis Outputs
  #' 
  #' @param portfolio_results Portfolio integration results
  #' @param output_dir Output directory
  #' @return List of generated files
  
  files <- list()
  
  # Extract data
  integrated_data <- portfolio_results$integrated_data
  portfolio_metrics <- portfolio_results$portfolio_metrics
  fund_analysis <- portfolio_results$fund_analysis
  industry_analysis <- portfolio_results$industry_analysis
  portfolio_summary <- portfolio_results$portfolio_summary
  
  # 1. Portfolio Holdings with DAII Scores (CSV)
  file_name <- "portfolio_holdings_with_daii_scores.csv"
  file_path <- file.path(output_dir, file_name)
  
  # Select and order columns
  holding_cols <- c(
    "fund_name", "Ticker", "Company.Name", "fund_weight",
    "DAII_3.5_Score", "DAII_Quartile",
    "R_D_Score", "Analyst_Score", "Patent_Score", "News_Score", "Growth_Score",
    "GICS.Ind.Grp.Name"
  )
  
  holding_cols <- intersect(holding_cols, names(integrated_data))
  portfolio_data <- integrated_data[, holding_cols]
  
  # Sort by fund and DAII score
  portfolio_data <- portfolio_data[order(portfolio_data$fund_name, 
                                         -portfolio_data$DAII_3.5_Score), ]
  
  write.csv(portfolio_data, file_path, row.names = FALSE)
  files$portfolio_holdings_csv <- file_path
  
  # 2. Fund-Level Analysis (CSV)
  if(!is.null(fund_analysis)) {
    file_name <- "fund_level_innovation_analysis.csv"
    file_path <- file.path(output_dir, file_name)
    
    write.csv(fund_analysis, file_path, row.names = FALSE)
    files$fund_analysis_csv <- file_path
  }
  
  # 3. Industry Exposure Analysis (CSV)
  if(!is.null(industry_analysis)) {
    file_name <- "portfolio_industry_innovation_exposure.csv"
    file_path <- file.path(output_dir, file_name)
    
    write.csv(industry_analysis, file_path, row.names = FALSE)
    files$industry_exposure_csv <- file_path
  }
  
  # 4. Portfolio Summary Report (CSV)
  file_name <- "portfolio_innovation_summary.csv"
  file_path <- file.path(output_dir, file_name)
  
  write.csv(portfolio_summary, file_path, row.names = FALSE)
  files$portfolio_summary_csv <- file_path
  
  # 5. Top and Bottom Holdings Report
  file_name <- "portfolio_innovation_extremes.csv"
  file_path <- file.path(output_dir, file_name)
  
  # Top 10 holdings by innovation contribution
  portfolio_data$innovation_contribution <- 
    portfolio_data$fund_weight * portfolio_data$DAII_3.5_Score
  
  top_10 <- portfolio_data %>%
    arrange(desc(innovation_contribution)) %>%
    head(10) %>%
    mutate(Rank = row_number(),
           Contribution_Percent = round(100 * innovation_contribution / 
                                          sum(portfolio_data$innovation_contribution, na.rm = TRUE), 2))
  
  bottom_10 <- portfolio_data %>%
    arrange(innovation_contribution) %>%
    head(10) %>%
    mutate(Rank = row_number(),
           Contribution_Percent = round(100 * innovation_contribution / 
                                          sum(portfolio_data$innovation_contribution, na.rm = TRUE), 2))
  
  extremes_report <- list(
    Top_10_Innovation_Contributors = top_10,
    Bottom_10_Innovation_Contributors = bottom_10
  )
  
  # Write to separate sheets in Excel
  file_name <- "portfolio_innovation_extremes.xlsx"
  file_path <- file.path(output_dir, file_name)
  
  wb <- createWorkbook()
  addWorksheet(wb, "Top_10_Contributors")
  addWorksheet(wb, "Bottom_10_Contributors")
  
  writeData(wb, "Top_10_Contributors", top_10)
  writeData(wb, "Bottom_10_Contributors", bottom_10)
  
  saveWorkbook(wb, file_path, overwrite = TRUE)
  files$extremes_excel <- file_path
  
  # 6. Complete Portfolio Analysis Workbook
  file_name <- "complete_portfolio_analysis.xlsx"
  file_path <- file.path(output_dir, file_name)
  
  wb <- createWorkbook()
  
  # Add sheets
  addWorksheet(wb, "Portfolio_Holdings")
  writeData(wb, "Portfolio_Holdings", portfolio_data)
  
  if(!is.null(fund_analysis)) {
    addWorksheet(wb, "Fund_Analysis")
    writeData(wb, "Fund_Analysis", fund_analysis)
  }
  
  if(!is.null(industry_analysis)) {
    addWorksheet(wb, "Industry_Exposure")
    writeData(wb, "Industry_Exposure", industry_analysis)
  }
  
  addWorksheet(wb, "Portfolio_Summary")
  writeData(wb, "Portfolio_Summary", portfolio_summary)
  
  addWorksheet(wb, "Top_Contributors")
  writeData(wb, "Top_Contributors", top_10)
  
  addWorksheet(wb, "Bottom_Contributors")
  writeData(wb, "Bottom_Contributors", bottom_10)
  
  # Add portfolio metrics sheet
  metrics_df <- data.frame(
    Metric = names(unlist(portfolio_metrics$overall)),
    Value = as.character(unlist(portfolio_metrics$overall))
  )
  
  addWorksheet(wb, "Portfolio_Metrics")
  writeData(wb, "Portfolio_Metrics", metrics_df)
  
  # Save workbook
  saveWorkbook(wb, file_path, overwrite = TRUE)
  files$complete_portfolio_excel <- file_path
  
  return(files)
}

generate_validation_outputs <- function(validation_results, output_dir) {
  #' Generate Validation Reports
  #' 
  #' @param validation_results Validation results
  #' @param output_dir Output directory
  #' @return List of generated files
  
  files <- list()
  
  validation_report <- validation_results$validation_report
  validation_template <- validation_results$validation_template
  validation_summary <- validation_results$validation_summary
  
  # 1. Validation Summary (CSV)
  file_name <- "daii_3.5_validation_summary.csv"
  file_path <- file.path(output_dir, file_name)
  
  write.csv(validation_summary, file_path, row.names = FALSE)
  files$validation_summary_csv <- file_path
  
  # 2. Validation Template (CSV)
  file_name <- "validation_review_template.csv"
  file_path <- file.path(output_dir, file_name)
  
  write.csv(validation_template, file_path, row.names = FALSE)
  files$validation_template_csv <- file_path
  
  # 3. Statistical Validation Report (CSV)
  if(!is.null(validation_report$statistical)) {
    # Distribution statistics
    file_name <- "statistical_validation_distributions.csv"
    file_path <- file.path(output_dir, file_name)
    
    write.csv(validation_report$statistical$distribution, 
              file_path, row.names = FALSE)
    files$statistical_distributions_csv <- file_path
    
    # Correlation matrix
    file_name <- "statistical_validation_correlations.csv"
    file_path <- file.path(output_dir, file_name)
    
    write.csv(as.data.frame(validation_report$statistical$correlation), 
              file_path, row.names = TRUE)
    files$statistical_correlations_csv <- file_path
    
    # Outlier analysis
    outlier_data <- do.call(rbind, lapply(
      names(validation_report$statistical$outliers),
      function(metric) {
        data.frame(
          Metric = metric,
          Outlier_Count = validation_report$statistical$outliers[[metric]]$outlier_count,
          Outlier_Percent = validation_report$statistical$outliers[[metric]]$outlier_percent,
          stringsAsFactors = FALSE
        )
      }
    ))
    
    file_name <- "statistical_validation_outliers.csv"
    file_path <- file.path(output_dir, file_name)
    
    write.csv(outlier_data, file_path, row.names = FALSE)
    files$statistical_outliers_csv <- file_path
  }
  
  # 4. Business Validation Report (CSV)
  if(!is.null(validation_report$business)) {
    # Known innovators
    file_name <- "business_validation_known_innovators.csv"
    file_path <- file.path(output_dir, file_name)
    
    write.csv(validation_report$business$face_validity$known_innovators,
              file_path, row.names = FALSE)
    files$known_innovators_csv <- file_path
    
    # Reasonableness flags
    file_name <- "business_validation_flags.csv"
    file_path <- file.path(output_dir, file_name)
    
    write.csv(validation_report$business$reasonableness_flags$flagged_companies,
              file_path, row.names = FALSE)
    files$reasonableness_flags_csv <- file_path
    
    # Quartile distribution test
    quartile_test <- data.frame(
      Test = "Chi-square quartile distribution",
      Chi_Square = validation_report$business$quartile_distribution$chi_square,
      P_Value = validation_report$business$quartile_distribution$p_value,
      Interpretation = ifelse(
        validation_report$business$quartile_distribution$p_value > 0.05,
        "Quartiles evenly distributed",
        "Quartiles not evenly distributed"
      ),
      stringsAsFactors = FALSE
    )
    
    file_name <- "business_validation_quartile_test.csv"
    file_path <- file.path(output_dir, file_name)
    
    write.csv(quartile_test, file_path, row.names = FALSE)
    files$quartile_test_csv <- file_path
  }
  
  # 5. Process Validation Report (CSV)
  if(!is.null(validation_report$process)) {
    # Imputation summary
    file_name <- "process_validation_imputation_summary.csv"
    file_path <- file.path(output_dir, file_name)
    
    write.csv(validation_report$process$imputation_summary,
              file_path, row.names = FALSE)
    files$imputation_summary_csv <- file_path
    
    # Impact analysis
    file_name <- "process_validation_imputation_impact.csv"
    file_path <- file.path(output_dir, file_name)
    
    write.csv(validation_report$process$impact_analysis,
              file_path, row.names = FALSE)
    files$imputation_impact_csv <- file_path
  }
  
  # 6. Complete Validation Workbook
  file_name <- "complete_validation_report.xlsx"
  file_path <- file.path(output_dir, file_name)
  
  wb <- createWorkbook()
  
  # Summary sheet
  addWorksheet(wb, "Validation_Summary")
  writeData(wb, "Validation_Summary", validation_summary)
  
  # Template sheet
  addWorksheet(wb, "Review_Template")
  writeData(wb, "Review_Template", validation_template)
  
  # Statistical validation sheets
  if(!is.null(validation_report$statistical)) {
    addWorksheet(wb, "Statistical_Distributions")
    writeData(wb, "Statistical_Distributions", 
              validation_report$statistical$distribution)
    
    addWorksheet(wb, "Statistical_Correlations")
    writeData(wb, "Statistical_Correlations", 
              as.data.frame(validation_report$statistical$correlation),
              rowNames = TRUE)
  }
  
  # Business validation sheets
  if(!is.null(validation_report$business)) {
    addWorksheet(wb, "Known_Innovators")
    writeData(wb, "Known_Innovators",
              validation_report$business$face_validity$known_innovators)
    
    addWorksheet(wb, "Reasonableness_Flags")
    writeData(wb, "Reasonableness_Flags",
              validation_report$business$reasonableness_flags$flagged_companies)
  }
  
  # Process validation sheets
  if(!is.null(validation_report$process)) {
    addWorksheet(wb, "Imputation_Summary")
    writeData(wb, "Imputation_Summary",
              validation_report$process$imputation_summary)
    
    addWorksheet(wb, "Imputation_Impact")
    writeData(wb, "Imputation_Impact",
              validation_report$process$impact_analysis)
  }
  
  saveWorkbook(wb, file_path, overwrite = TRUE)
  files$complete_validation_excel <- file_path
  
  return(files)
}

generate_executive_summaries <- function(daii_results,
                                         portfolio_results,
                                         validation_results,
                                         output_dir) {
  #' Generate Executive Summary Reports
  #' 
  #' @param daii_results DAII scoring results
  #' @param portfolio_results Portfolio integration results
  #' @param validation_results Validation results
  #' @param output_dir Output directory
  #' @return List of generated files
  
  files <- list()
  
  # 1. Executive Summary (Text Report)
  file_name <- "daii_3.5_executive_summary.txt"
  file_path <- file.path(output_dir, file_name)
  
  summary_text <- create_executive_summary_text(
    daii_results,
    portfolio_results,
    validation_results
  )
  
  writeLines(summary_text, file_path)
  files$executive_summary_txt <- file_path
  
  # 2. Key Findings (CSV)
  file_name <- "daii_3.5_key_findings.csv"
  file_path <- file.path(output_dir, file_name)
  
  key_findings <- create_key_findings_table(
    daii_results,
    portfolio_results,
    validation_results
  )
  
  write.csv(key_findings, file_path, row.names = FALSE)
  files$key_findings_csv <- file_path
  
  # 3. Recommendations Report (CSV)
  file_name <- "daii_3.5_recommendations.csv"
  file_path <- file.path(output_dir, file_name)
  
  recommendations <- create_recommendations_table(
    daii_results,
    portfolio_results
  )
  
  write.csv(recommendations, file_path, row.names = FALSE)
  files$recommendations_csv <- file_path
  
  # 4. Executive Dashboard (HTML)
  file_name <- "daii_3.5_executive_dashboard.html"
  file_path <- file.path(output_dir, file_name)
  
  dashboard_html <- create_executive_dashboard(
    daii_results,
    portfolio_results,
    validation_results
  )
  
  writeLines(dashboard_html, file_path)
  files$executive_dashboard_html <- file_path
  
  # 5. Presentation Slides (HTML)
  file_name <- "daii_3.5_presentation.html"
  file_path <- file.path(output_dir, file_name)
  
  presentation_html <- create_presentation_html(
    daii_results,
    portfolio_results
  )
  
  writeLines(presentation_html, file_path)
  files$presentation_html <- file_path
  
  return(files)
}

create_executive_summary_text <- function(daii_results,
                                          portfolio_results,
                                          validation_results) {
  #' Create Executive Summary Text Report
  
  summary_text <- paste(
    "========================================================================",
    "                    DAII 3.5 - EXECUTIVE SUMMARY",
    "========================================================================",
    "",
    paste("Report Date:", format(Sys.time(), "%Y-%m-%d %H:%M:%S")),
    "",
    "OVERVIEW",
    "---------",
    paste("Total Companies Analyzed:", nrow(daii_results$daii_data)),
    paste("Portfolio Holdings:", portfolio_results$portfolio_metrics$overall$total_holdings),
    paste("Unique Companies in Portfolio:", portfolio_results$portfolio_metrics$overall$unique_companies),
    "",
    "KEY RESULTS",
    "-----------",
    paste("Portfolio DAII Score (Weighted):", 
          round(portfolio_results$portfolio_metrics$overall$portfolio_daii, 1)),
    paste("Portfolio DAII Score (Equal Weight):", 
          round(portfolio_results$portfolio_metrics$overall$portfolio_daii_equal, 1)),
    paste("DAII Score Range:", portfolio_results$portfolio_metrics$overall$daii_range),
    "",
    "TOP INNOVATORS",
    "--------------",
    sep = "\n"
  )
  
  # Add top 5 companies
  top_5 <- daii_results$daii_data %>%
    arrange(desc(DAII_3.5_Score)) %>%
    head(5)
  
  for(i in 1:nrow(top_5)) {
    summary_text <- paste(summary_text,
                          paste(i, ".", top_5$Ticker[i], 
                                " - ", round(top_5$DAII_3.5_Score[i], 1),
                                " (", top_5$DAII_Quartile[i], ")", sep = ""),
                          sep = "\n")
  }
  
  # Add portfolio insights
  summary_text <- paste(summary_text,
                        "",
                        "PORTFOLIO INSIGHTS",
                        "-----------------",
                        sep = "\n")
  
  if(!is.null(portfolio_results$fund_analysis)) {
    top_fund <- portfolio_results$fund_analysis %>%
      arrange(desc(weighted_daii)) %>%
      head(1)
    
    summary_text <- paste(summary_text,
                          paste("Highest Innovation Fund:", top_fund$fund_name[1]),
                          paste("Fund DAII Score:", round(top_fund$weighted_daii[1], 1)),
                          "",
                          sep = "\n")
  }
  
  # Add validation status
  if(!is.null(validation_results$validation_summary)) {
    overall_status <- validation_results$validation_summary %>%
      filter(Metric == "Validation Status") %>%
      pull(Value)
    
    summary_text <- paste(summary_text,
                          "VALIDATION STATUS",
                          "-----------------",
                          paste("Overall Validation:", overall_status),
                          "",
                          sep = "\n")
  }
  
  # Add recommendations
  summary_text <- paste(summary_text,
                        "RECOMMENDATIONS",
                        "--------------",
                        "1. Review top innovators for potential investment opportunities",
                        "2. Analyze low-scoring holdings for innovation risk",
                        "3. Consider rebalancing to increase innovation exposure",
                        "4. Monitor innovation scores quarterly for trend analysis",
                        "",
                        "========================================================================",
                        sep = "\n")
  
  return(summary_text)
}

create_key_findings_table <- function(daii_results,
                                      portfolio_results,
                                      validation_results) {
  #' Create Key Findings Table
  
  key_findings <- data.frame(
    Category = character(),
    Finding = character(),
    Metric = character(),
    Value = character(),
    Interpretation = character(),
    stringsAsFactors = FALSE
  )
  
  # Overall portfolio innovation
  key_findings <- rbind(key_findings, data.frame(
    Category = "Portfolio Innovation",
    Finding = "Overall Innovation Score",
    Metric = "Weighted DAII",
    Value = as.character(round(portfolio_results$portfolio_metrics$overall$portfolio_daii, 1)),
    Interpretation = ifelse(portfolio_results$portfolio_metrics$overall$portfolio_daii > 50,
                            "Above average innovation", "Below average innovation"),
    stringsAsFactors = FALSE
  ))
  
  # Innovation concentration
  concentration <- portfolio_results$concentration_analysis
  key_findings <- rbind(key_findings, data.frame(
    Category = "Portfolio Innovation",
    Finding = "Innovation Concentration",
    Metric = "HHI Index",
    Value = as.character(round(concentration$hhi_daii, 0)),
    Interpretation = concentration$hhi_interpretation,
    stringsAsFactors = FALSE
  ))
  
  # Top industry
  if(!is.null(portfolio_results$industry_analysis)) {
    top_industry <- portfolio_results$industry_analysis %>%
      arrange(desc(industry_daii)) %>%
      head(1)
    
    key_findings <- rbind(key_findings, data.frame(
      Category = "Industry Analysis",
      Finding = "Most Innovative Industry",
      Metric = "Industry Name",
      Value = top_industry$GICS.Ind.Grp.Name[1],
      Interpretation = paste("Average DAII:", round(top_industry$industry_daii[1], 1)),
      stringsAsFactors = FALSE
    ))
  }
  
  # Validation status
  if(!is.null(validation_results$validation_summary)) {
    validation_status <- validation_results$validation_summary %>%
      filter(Metric == "Validation Status") %>%
      pull(Value)
    
    key_findings <- rbind(key_findings, data.frame(
      Category = "Data Quality",
      Finding = "Overall Validation",
      Metric = "Status",
      Value = validation_status,
      Interpretation = "Data quality and score reliability",
      stringsAsFactors = FALSE
    ))
  }
  
  # Component strengths
  component_strengths <- daii_results$daii_data %>%
    summarise(
      R_D_Strength = mean(R_D_Score, na.rm = TRUE),
      Analyst_Strength = mean(Analyst_Score, na.rm = TRUE),
      Patent_Strength = mean(Patent_Score, na.rm = TRUE)
    )
  
  strongest_component <- which.max(c(
    component_strengths$R_D_Strength,
    component_strengths$Analyst_Strength,
    component_strengths$Patent_Strength
  ))
  
  component_names <- c("R&D Capability", "Analyst Sentiment", "Patent Activity")
  
  key_findings <- rbind(key_findings, data.frame(
    Category = "Component Analysis",
    Finding = "Strongest Innovation Component",
    Metric = "Component",
    Value = component_names[strongest_component],
    Interpretation = "Driving overall innovation scores",
    stringsAsFactors = FALSE
  ))
  
  return(key_findings)
}

create_recommendations_table <- function(daii_results,
                                         portfolio_results) {
  #' Create Recommendations Table
  
  recommendations <- data.frame(
    Priority = character(),
    Recommendation = character(),
    Action = character(),
    Impact = character(),
    Timeline = character(),
    stringsAsFactors = FALSE
  )
  
  # Analyze portfolio for recommendations
  portfolio_data <- portfolio_results$integrated_data
  
  # Recommendation 1: Increase exposure to top innovators
  top_innovators <- portfolio_data %>%
    filter(DAII_Quartile == "Q1 (High)") %>%
    summarise(
      current_weight = sum(fund_weight, na.rm = TRUE),
      count = n()
    )
  
  if(top_innovators$current_weight < 0.25) { # Less than 25% in top quartile
    recommendations <- rbind(recommendations, data.frame(
      Priority = "High",
      Recommendation = "Increase exposure to top innovators",
      Action = "Rebalance portfolio to increase weight in Q1 companies",
      Impact = "Potentially increase portfolio innovation score by 10-15%",
      Timeline = "Next rebalancing cycle",
      stringsAsFactors = FALSE
    ))
  }
  
  # Recommendation 2: Reduce exposure to low innovators
  low_innovators <- portfolio_data %>%
    filter(DAII_Quartile == "Q4 (Low)") %>%
    summarise(
      current_weight = sum(fund_weight, na.rm = TRUE),
      count = n()
    )
  
  if(low_innovators$current_weight > 0.15) { # More than 15% in bottom quartile
    recommendations <- rbind(recommendations, data.frame(
      Priority = "Medium",
      Recommendation = "Reduce exposure to low innovators",
      Action = "Review Q4 holdings for divestment opportunities",
      Impact = "Reduce innovation risk in portfolio",
      Timeline = "6-12 months",
      stringsAsFactors = FALSE
    ))
  }
  
  # Recommendation 3: Diversify innovation sources
  hhi <- portfolio_results$concentration_analysis$hhi_daii
  if(hhi > 1500) { # Moderately concentrated
    recommendations <- rbind(recommendations, data.frame(
      Priority = "Medium",
      Recommendation = "Diversify innovation sources",
      Action = "Spread innovation exposure across more companies/industries",
      Impact = "Reduce concentration risk, improve stability",
      Timeline = "Next investment cycle",
      stringsAsFactors = FALSE
    ))
  }
  
  # Recommendation 4: Monitor specific industries
  if(!is.null(portfolio_results$industry_analysis)) {
    industry_gap <- portfolio_results$industry_analysis %>%
      filter(industry_weight > 0.05) %>% # Industries with >5% weight
      summarise(
        max_daii = max(industry_daii, na.rm = TRUE),
        min_daii = min(industry_daii, na.rm = TRUE),
        gap = max_daii - min_daii
      )
    
    if(industry_gap$gap > 20) { # Large innovation gap between industries
      recommendations <- rbind(recommendations, data.frame(
        Priority = "Low",
        Recommendation = "Address industry innovation gaps",
        Action = "Review industry allocation to balance innovation exposure",
        Impact = "More balanced innovation profile",
        Timeline = "Strategic review",
        stringsAsFactors = FALSE
      ))
    }
  }
  
  # Default recommendation if none generated
  if(nrow(recommendations) == 0) {
    recommendations <- rbind(recommendations, data.frame(
      Priority = "Low",
      Recommendation = "Maintain current innovation strategy",
      Action = "Continue monitoring innovation scores quarterly",
      Impact = "Ongoing innovation tracking",
      Timeline = "Continuous",
      stringsAsFactors = FALSE
    ))
  }
  
  return(recommendations)
}

create_executive_dashboard <- function(daii_results,
                                       portfolio_results,
                                       validation_results) {
  #' Create Executive Dashboard HTML
  
  dashboard_html <- '
  <!DOCTYPE html>
  <html lang="en">
  <head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>DAII 3.5 Executive Dashboard</title>
    <style>
      body {
        font-family: Arial, sans-serif;
        margin: 20px;
        background-color: #f5f5f5;
      }
      .header {
        background: linear-gradient(135deg, #1F4E79 0%, #2E8B57 100%);
        color: white;
        padding: 20px;
        border-radius: 10px;
        margin-bottom: 20px;
      }
      .kpi-container {
        display: grid;
        grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
        gap: 20px;
        margin-bottom: 30px;
      }
      .kpi-card {
        background-color: white;
        padding: 20px;
        border-radius: 8px;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        text-align: center;
      }
      .kpi-value {
        font-size: 36px;
        font-weight: bold;
        margin: 10px 0;
      }
      .kpi-label {
        color: #666;
        font-size: 14px;
      }
      .section {
        background-color: white;
        padding: 20px;
        border-radius: 8px;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        margin-bottom: 20px;
      }
      .section-title {
        color: #1F4E79;
        border-bottom: 2px solid #2E8B57;
        padding-bottom: 10px;
        margin-bottom: 20px;
      }
      .table {
        width: 100%;
        border-collapse: collapse;
      }
      .table th, .table td {
        padding: 10px;
        text-align: left;
        border-bottom: 1px solid #ddd;
      }
      .table th {
        background-color: #f2f2f2;
        font-weight: bold;
      }
      .status-good { color: #2E8B57; }
      .status-warning { color: #FFD700; }
      .status-alert { color: #CD5C5C; }
      .visualization {
        text-align: center;
        margin: 20px 0;
      }
      .visualization img {
        max-width: 100%;
        height: auto;
        border-radius: 5px;
      }
      .timestamp {
        color: #666;
        font-size: 12px;
        margin-top: 20px;
        text-align: center;
      }
    </style>
  </head>
  <body>
    <div class="header">
      <h1>🚀 DAII 3.5 Executive Dashboard</h1>
      <p>Digital Acceleration & Innovation Index Analysis</p>
      <p>Generated: '
  
  dashboard_html <- paste0(dashboard_html, format(Sys.time(), "%Y-%m-%d %H:%M:%S"), '</p>
    </div>')
  
  # KPI Section
  dashboard_html <- paste0(dashboard_html, '
    <div class="kpi-container">
      <div class="kpi-card">
        <div class="kpi-label">Portfolio DAII Score</div>
        <div class="kpi-value">', 
                           round(portfolio_results$portfolio_metrics$overall$portfolio_daii, 1), 
                           '</div>
        <div>', 
                           ifelse(portfolio_results$portfolio_metrics$overall$portfolio_daii > 50,
                                  '<span class="status-good">Above Average</span>',
                                  '<span class="status-alert">Below Average</span>'),
                           '</div>
      </div>
      
      <div class="kpi-card">
        <div class="kpi-label">Top Quartile Holdings</div>
        <div class="kpi-value">',
                           sum(portfolio_results$portfolio_metrics$overall$quartile_distribution["Q1 (High)"]),
                           '</div>
        <div>Companies</div>
      </div>
      
      <div class="kpi-card">
        <div class="kpi-label">Innovation Concentration</div>
        <div class="kpi-value">',
                           round(portfolio_results$concentration_analysis$hhi_daii, 0),
                           '</div>
        <div>', portfolio_results$concentration_analysis$hhi_interpretation, '</div>
      </div>
      
      <div class="kpi-card">
        <div class="kpi-label">Validation Status</div>
        <div class="kpi-value">')
  
  if(!is.null(validation_results$validation_summary)) {
    overall_status <- validation_results$validation_summary %>%
      filter(Metric == "Validation Status") %>%
      pull(Value)
    
    dashboard_html <- paste0(dashboard_html, overall_status)
  } else {
    dashboard_html <- paste0(dashboard_html, "Pending")
  }
  
  dashboard_html <- paste0(dashboard_html, '</div>
        <div>Data Quality</div>
      </div>
    </div>')
  
  # Top Innovators Section
  dashboard_html <- paste0(dashboard_html, '
    <div class="section">
      <h2 class="section-title">🏆 Top 5 Innovators</h2>
      <table class="table">
        <thead>
          <tr>
            <th>Rank</th>
            <th>Ticker</th>
            <th>DAII Score</th>
            <th>Quartile</th>
            <th>R&D Score</th>
            <th>Industry</th>
          </tr>
        </thead>
        <tbody>')
  
  top_5 <- daii_results$daii_data %>%
    arrange(desc(DAII_3.5_Score)) %>%
    head(5)
  
  for(i in 1:nrow(top_5)) {
    dashboard_html <- paste0(dashboard_html, '
          <tr>
            <td>', i, '</td>
            <td><strong>', top_5$Ticker[i], '</strong></td>
            <td>', round(top_5$DAII_3.5_Score[i], 1), '</td>
            <td>', top_5$DAII_Quartile[i], '</td>
            <td>', round(top_5$R_D_Score[i], 1), '</td>
            <td>', 
                             ifelse("GICS.Ind.Grp.Name" %in% names(top_5), 
                                    top_5$GICS.Ind.Grp.Name[i], "N/A"),
                             '</td>
          </tr>')
  }
  
  dashboard_html <- paste0(dashboard_html, '
        </tbody>
      </table>
    </div>')
  
  # Portfolio Insights Section
  dashboard_html <- paste0(dashboard_html, '
    <div class="section">
      <h2 class="section-title">📊 Portfolio Insights</h2>
      <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 20px;">')
  
  # Left column: Portfolio distribution
  dashboard_html <- paste0(dashboard_html, '
        <div>
          <h3>Quartile Distribution</h3>
          <table class="table">
            <thead>
              <tr>
                <th>Quartile</th>
                <th>Count</th>
                <th>% of Portfolio</th>
              </tr>
            </thead>
            <tbody>')
  
  quartile_dist <- portfolio_results$portfolio_metrics$overall$quartile_distribution
  quartile_perc <- portfolio_results$portfolio_metrics$overall$quartile_percentages
  
  for(q in names(quartile_dist)) {
    dashboard_html <- paste0(dashboard_html, '
              <tr>
                <td>', q, '</td>
                <td>', quartile_dist[q], '</td>
                <td>', round(quartile_perc[q], 1), '%</td>
              </tr>')
  }
  
  dashboard_html <- paste0(dashboard_html, '
            </tbody>
          </table>
        </div>')
  
  # Right column: Component contributions
  dashboard_html <- paste0(dashboard_html, '
        <div>
          <h3>Component Contributions</h3>
          <table class="table">
            <thead>
              <tr>
                <th>Component</th>
                <th>Weight</th>
                <th>Avg Score</th>
              </tr>
            </thead>
            <tbody>')
  
  component_weights <- c("R&D" = 0.30, "Analyst" = 0.20, "Patent" = 0.25, 
                         "News" = 0.10, "Growth" = 0.15)
  
  component_scores <- portfolio_results$portfolio_metrics$component_contributions
  
  for(comp in names(component_scores)) {
    comp_name <- gsub("_Score", "", comp)
    comp_name <- gsub("_", " ", comp_name)
    
    dashboard_html <- paste0(dashboard_html, '
              <tr>
                <td>', comp_name, '</td>
                <td>', component_weights[comp_name] * 100, '%</td>
                <td>', round(component_scores[comp], 1), '</td>
              </tr>')
  }
  
  dashboard_html <- paste0(dashboard_html, '
            </tbody>
          </table>
        </div>
      </div>
    </div>')
  
  # Recommendations Section
  dashboard_html <- paste0(dashboard_html, '
    <div class="section">
      <h2 class="section-title">🎯 Key Recommendations</h2>')
  
  recommendations <- create_recommendations_table(daii_results, portfolio_results)
  
  for(i in 1:min(3, nrow(recommendations))) {
    priority_class <- paste0("status-", 
                             tolower(gsub("\\s+", "", recommendations$Priority[i])))
    
    dashboard_html <- paste0(dashboard_html, '
      <div style="margin-bottom: 15px; padding: 15px; background-color: #f9f9f9; border-radius: 5px;">
        <div style="display: flex; justify-content: space-between; align-items: center;">
          <h3 style="margin: 0;">', recommendations$Recommendation[i], '</h3>
          <span class="', priority_class, '" style="font-weight: bold;">',
                             recommendations$Priority[i], '</span>
        </div>
        <p><strong>Action:</strong> ', recommendations$Action[i], '</p>
        <p><strong>Impact:</strong> ', recommendations$Impact[i], '</p>
        <p><strong>Timeline:</strong> ', recommendations$Timeline[i], '</p>
      </div>')
  }
  
  dashboard_html <- paste0(dashboard_html, '
    </div>')
  
  # Close HTML
  dashboard_html <- paste0(dashboard_html, '
    <div class="timestamp">
      <p>DAII 3.5 Analysis completed on ', format(Sys.time(), "%Y-%m-%d"), '</p>
      <p>For detailed analysis, refer to the complete output package.</p>
    </div>
  </body>
  </html>')
  
  return(dashboard_html)
}

export_raw_data <- function(daii_results, portfolio_results, output_dir) {
  #' Export Raw Data Files
  #' 
  #' @param daii_results DAII scoring results
  #' @param portfolio_results Portfolio integration results
  #' @param output_dir Output directory
  #' @return List of generated files
  
  files <- list()
  
  # 1. Raw DAII Data (RDS format for reproducibility)
  file_name <- "daii_3.5_raw_data.rds"
  file_path <- file.path(output_dir, file_name)
  
  saveRDS(daii_results, file_path)
  files$raw_rds <- file_path
  
  # 2. Processed Data (CSV)
  file_name <- "daii_3.5_processed_data.csv"
  file_path <- file.path(output_dir, file_name)
  
  write.csv(daii_results$daii_data, file_path, row.names = FALSE)
  files$processed_csv <- file_path
  
  # 3. Portfolio Data (RDS)
  file_name <- "portfolio_analysis_raw.rds"
  file_path <- file.path(output_dir, file_name)
  
  saveRDS(portfolio_results, file_path)
  files$portfolio_rds <- file_path
  
  # 4. Configuration Settings
  file_name <- "daii_3.5_configuration.json"
  file_path <- file.path(output_dir, file_name)
  
  config <- list(
    version = "3.5",
    analysis_date = Sys.time(),
    weights = list(
      r_d = 0.30,
      analyst = 0.20,
      patent = 0.25,
      news = 0.10,
      growth = 0.15
    ),
    normalization_method = "min-max scaling",
    imputation_method = "median/mean with industry adjustment",
    quartile_method = "equal frequency"
  )
  
  writeLines(jsonlite::toJSON(config, pretty = TRUE), file_path)
  files$config_json <- file_path
  
  # 5. Session Information
  file_name <- "session_info.txt"
  file_path <- file.path(output_dir, file_name)
  
  sink(file_path)
  print(sessionInfo())
  sink()
  
  files$session_info <- file_path
  
  return(files)
}

create_output_summary <- function(output_files, run_dir) {
  #' Create Output Summary
  
  summary_df <- data.frame(
    File_Type = character(),
    File_Name = character(),
    Size_KB = numeric(),
    Description = character(),
    stringsAsFactors = FALSE
  )
  
  # Process all files
  for(category in names(output_files)) {
    for(file_type in names(output_files[[category]])) {
      file_path <- output_files[[category]][[file_type]]
      
      if(file.exists(file_path)) {
        file_size <- round(file.info(file_path)$size / 1024, 1)  # KB
        
        # Get relative path
        rel_path <- gsub(paste0(run_dir, "/"), "", file_path)
        
        # Create description
        description <- switch(category,
                              "company_scores" = "Company-level DAII scores and rankings",
                              "portfolio_reports" = "Portfolio analysis and holdings",
                              "validation_reports" = "Validation and quality assurance",
                              "executive_summaries" = "Executive summaries and dashboards",
                              "raw_data" = "Raw data and configuration files",
                              "Other outputs"
        )
        
        summary_df <- rbind(summary_df, data.frame(
          File_Type = category,
          File_Name = rel_path,
          Size_KB = file_size,
          Description = description,
          stringsAsFactors = FALSE
        ))
      }
    }
  }
  
  # Save summary
  summary_path <- file.path(run_dir, "output_summary.csv")
  write.csv(summary_df, summary_path, row.names = FALSE)
  
  # Create summary report
  summary_report <- paste(
    "=========================================",
    "       DAII 3.5 OUTPUT SUMMARY",
    "=========================================",
    "",
    paste("Output Directory:", run_dir),
    paste("Total Files Generated:", nrow(summary_df)),
    paste("Total Size:", round(sum(summary_df$Size_KB) / 1024, 1), "MB"),
    "",
    "FILE CATEGORIES:",
    "----------------",
    sep = "\n"
  )
  
  # Add category summaries
  category_summary <- summary_df %>%
    group_by(File_Type) %>%
    summarise(
      File_Count = n(),
      Total_Size_KB = sum(Size_KB)
    )
  
  for(i in 1:nrow(category_summary)) {
    summary_report <- paste(summary_report,
                            paste(category_summary$File_Type[i], ":",
                                  category_summary$File_Count[i], "files,",
                                  round(category_summary$Total_Size_KB[i] / 1024, 1), "MB"),
                            sep = "\n")
  }
  
  # Add largest files
  summary_report <- paste(summary_report,
                          "",
                          "LARGEST FILES:",
                          "-------------",
                          sep = "\n")
  
  largest_files <- summary_df %>%
    arrange(desc(Size_KB)) %>%
    head(5)
  
  for(i in 1:nrow(largest_files)) {
    summary_report <- paste(summary_report,
                            paste(i, ".", largest_files$File_Name[i],
                                  "(", round(largest_files$Size_KB[i] / 1024, 1), "MB)"),
                            sep = "\n")
  }
  
  # Write summary report
  report_path <- file.path(run_dir, "output_summary_report.txt")
  writeLines(summary_report, report_path)
  
  return(list(
    summary_data = summary_df,
    category_summary = category_summary,
    report_path = report_path
  ))
}

create_master_index <- function(run_dir, output_files, timestamp) {
  #' Create Master Index HTML File
  
  index_html <- '
  <!DOCTYPE html>
  <html lang="en">
  <head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>DAII 3.5 Output Package Index</title>
    <style>
      body {
        font-family: Arial, sans-serif;
        margin: 40px;
        background-color: #f5f5f5;
      }
      .header {
        background: linear-gradient(135deg, #1F4E79 0%, #2E8B57 100%);
        color: white;
        padding: 30px;
        border-radius: 10px;
        margin-bottom: 30px;
      }
      .section {
        background-color: white;
        padding: 20px;
        border-radius: 8px;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        margin-bottom: 20px;
      }
      .section-title {
        color: #1F4E79;
        border-bottom: 2px solid #2E8B57;
        padding-bottom: 10px;
        margin-bottom: 20px;
      }
      .file-list {
        list-style-type: none;
        padding: 0;
      }
      .file-list li {
        padding: 10px;
        border-bottom: 1px solid #eee;
        display: flex;
        justify-content: space-between;
        align-items: center;
      }
      .file-list li:hover {
        background-color: #f9f9f9;
      }
      .file-link {
        color: #1F4E79;
        text-decoration: none;
        font-weight: bold;
      }
      .file-link:hover {
        text-decoration: underline;
      }
      .file-size {
        color: #666;
        font-size: 12px;
      }
      .category-icon {
        font-size: 24px;
        margin-right: 10px;
      }
      .quick-links {
        display: grid;
        grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
        gap: 20px;
        margin-bottom: 30px;
      }
      .quick-link-card {
        background-color: white;
        padding: 20px;
        border-radius: 8px;
        text-align: center;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        transition: transform 0.2s;
      }
      .quick-link-card:hover {
        transform: translateY(-5px);
        box-shadow: 0 4px 8px rgba(0,0,0,0.2);
      }
      .quick-link-card a {
        text-decoration: none;
        color: #1F4E79;
        font-weight: bold;
      }
      .timestamp {
        color: #666;
        font-size: 12px;
        margin-top: 30px;
        text-align: center;
      }
    </style>
  </head>
  <body>
    <div class="header">
      <h1>📦 DAII 3.5 Output Package</h1>
      <p>Complete analysis results and deliverables</p>
      <p><strong>Run ID:</strong> '
  
  index_html <- paste0(index_html, timestamp, '</p>
      <p><strong>Generated:</strong> ', format(Sys.time(), "%Y-%m-%d %H:%M:%S"), '</p>
    </div>')
  
  # Quick Links
  index_html <- paste0(index_html, '
    <div class="quick-links">
      <div class="quick-link-card">
        <span class="category-icon">🏢</span>
        <h3><a href="01_Company_Scores/">Company Scores</a></h3>
        <p>Individual company DAII scores and rankings</p>
      </div>
      <div class="quick-link-card">
        <span class="category-icon">🏦</span>
        <h3><a href="02_Portfolio_Analysis/">Portfolio Analysis</a></h3>
        <p>Portfolio-level innovation metrics and insights</p>
      </div>
      <div class="quick-link-card">
        <span class="category-icon">🔍</span>
        <h3><a href="03_Validation_Reports/">Validation Reports</a></h3>
        <p>Data quality and validation results</p>
      </div>
      <div class="quick-link-card">
        <span class="category-icon">📈</span>
        <h3><a href="04_Executive_Summaries/">Executive Summaries</a></h3>
        <p>High-level insights and recommendations</p>
      </div>
    </div>')
  
  # File Index by Category
  for(category in names(output_files)) {
    category_name <- gsub("_", " ", category)
    category_name <- paste0(toupper(substr(category_name, 1, 1)), 
                            substr(category_name, 2, nchar(category_name)))
    
    category_icon <- switch(category,
                            "company_scores" = "🏢",
                            "portfolio_reports" = "🏦",
                            "validation_reports" = "🔍",
                            "executive_summaries" = "📈",
                            "raw_data" = "💾",
                            "📁"
    )
    
    index_html <- paste0(index_html, '
    <div class="section">
      <h2 class="section-title">', category_icon, ' ', category_name, '</h2>
      <ul class="file-list">')
    
    for(file_type in names(output_files[[category]])) {
      file_path <- output_files[[category]][[file_type]]
      rel_path <- gsub(paste0(run_dir, "/"), "", file_path)
      file_name <- basename(file_path)
      
      if(file.exists(file_path)) {
        file_size <- round(file.info(file_path)$size / 1024, 1)
        
        index_html <- paste0(index_html, '
        <li>
          <a href="', rel_path, '" class="file-link">', file_name, '</a>
          <span class="file-size">', file_size, ' KB</span>
        </li>')
      }
    }
    
    index_html <- paste0(index_html, '
      </ul>
    </div>')
  }
  
  # Instructions
  index_html <- paste0(index_html, '
    <div class="section">
      <h2 class="section-title">📋 How to Use This Package</h2>
      <h3>For Investment Analysts:</h3>
      <ul>
        <li>Review <strong>Company Scores</strong> for individual company innovation metrics</li>
        <li>Analyze <strong>Portfolio Analysis</strong> for fund-level insights</li>
        <li>Check <strong>Top Innovators</strong> for investment opportunities</li>
      </ul>
      
      <h3>For Portfolio Managers:</h3>
      <ul>
        <li>Examine <strong>Portfolio Summary</strong> for overall innovation health</li>
        <li>Review <strong>Recommendations</strong> for portfolio adjustments</li>
        <li>Monitor <strong>Industry Exposure</strong> for sector allocation</li>
      </ul>
      
      <h3>For Risk Management:</h3>
      <ul>
        <li>Check <strong>Validation Reports</strong> for data quality</li>
        <li>Review <strong>Concentration Analysis</strong> for risk assessment</li>
        <li>Monitor <strong>Low Innovators</strong> for potential divestment</li>
      </ul>
    </div>')
  
  # Close HTML
  index_html <- paste0(index_html, '
    <div class="timestamp">
      <p>DAII 3.5 Analysis Framework v3.5</p>
      <p>For questions or support, contact the Innovation Analytics Team</p>
    </div>
  </body>
  </html>')
  
  # Write index file
  index_path <- file.path(run_dir, "index.html")
  writeLines(index_html, index_path)
  
  return(index_path)
}

# ============================================================================
# MODULE 9: REPRODUCIBILITY - Configuration & Customization
# ============================================================================
#
# PURPOSE: Ensure reproducibility and enable customization
#
# REPRODUCIBILITY FEATURES:
# 1. Version Control: Track all code and configuration versions
# 2. Dependency Management: Capture package versions and system info
# 3. Seed Setting: Ensure random processes are reproducible
# 4. Configuration Files: Externalize all parameters
#
# CUSTOMIZATION FEATURES:
# 1. Weight Configuration: Adjust component weights
# 2. Threshold Customization: Modify quartile and outlier thresholds
# 3. Industry Classification: Custom industry mappings
# 4. Output Formatting: Customize reports and visualizations
#
# ============================================================================

create_reproducible_analysis <- function(config_file = "daii_config.yaml") {
  #' Create Reproducible DAII 3.5 Analysis
  #' 
  #' @param config_file Path to configuration file
  #' @return Complete analysis with reproducibility features
  
  cat("\n🔬 CREATING REPRODUCIBLE DAII 3.5 ANALYSIS\n")
  cat(paste(rep("=", 60), collapse = ""), "\n")
  
  # 1. SET UP REPRODUCIBILITY
  reproducibility_setup <- setup_reproducibility(config_file)
  
  # 2. LOAD CONFIGURATION
  config <- load_daii_configuration(config_file)
  
  # 3. SET SEED FOR RANDOM PROCESSES
  set.seed(config$random_seed)
  cat(sprintf("   Random seed set: %d\n", config$random_seed))
  
  # 4. CAPTURE SYSTEM INFORMATION
  system_info <- capture_system_info()
  
  # 5. CREATE ANALYSIS PIPELINE
  analysis_pipeline <- create_analysis_pipeline(config)
  
  # 6. RUN ANALYSIS
  cat("\n🏃 RUNNING ANALYSIS PIPELINE\n")
  
  results <- execute_analysis_pipeline(
    analysis_pipeline,
    config$data_file,
    config$output_directory
  )
  
  # 7. CREATE REPRODUCIBILITY PACKAGE
  reproducibility_package <- create_reproducibility_package(
    config,
    system_info,
    analysis_pipeline,
    results,
    config$output_directory
  )
  
  # 8. VALIDATE REPRODUCIBILITY
  reproducibility_check <- validate_reproducibility(results, config)
  
  cat(sprintf("\n✅ Reproducible analysis complete\n"))
  cat(sprintf("   Output directory: %s\n", config$output_directory))
  cat(sprintf("   Reproducibility package: %s\n", 
              reproducibility_package$package_path))
  
  return(list(
    results = results,
    config = config,
    reproducibility = list(
      setup = reproducibility_setup,
      system_info = system_info,
      package = reproducibility_package,
      check = reproducibility_check
    )
  ))
}

setup_reproducibility <- function(config_file) {
  #' Set Up Reproducibility Framework
  
  setup_info <- list(
    timestamp = Sys.time(),
    r_version = R.version.string,
    platform = R.version$platform,
    config_file = config_file,
    working_directory = getwd()
  )
  
  # Create reproducibility directory
  repro_dir <- "00_Reproducibility"
  if(!dir.exists(repro_dir)) {
    dir.create(repro_dir, showWarnings = FALSE)
  }
  
  # Save session information
  sink(file.path(repro_dir, "session_info.txt"))
  print(sessionInfo())
  sink()
  
  # Save git information if available
  if(file.exists(".git")) {
    tryCatch({
      git_info <- system("git log --oneline -1", intern = TRUE)
      setup_info$git_commit <- git_info
      
      writeLines(git_info, file.path(repro_dir, "git_commit.txt"))
    }, error = function(e) {
      cat("Git information not available\n")
    })
  }
  
  return(setup_info)
}

load_daii_configuration <- function(config_file) {
  #' Load DAII Configuration from YAML File
  
  if(!file.exists(config_file)) {
    cat(sprintf("Configuration file not found: %s\n", config_file))
    cat("Creating default configuration...\n")
    config <- create_default_configuration()
    save_configuration(config, config_file)
  } else {
    if(require(yaml)) {
      config <- yaml::read_yaml(config_file)
    } else {
      stop("YAML package required for configuration loading")
    }
  }
  
  # Validate configuration
  config <- validate_configuration(config)
  
  cat("\n📋 LOADED CONFIGURATION:\n")
  cat(sprintf("   • Data file: %s\n", config$data_file))
  cat(sprintf("   • Output directory: %s\n", config$output_directory))
  cat(sprintf("   • Component weights: R&D=%.0f%%, Analyst=%.0f%%, Patent=%.0f%%, News=%.0f%%, Growth=%.0f%%\n",
              config$weights$r_d * 100, config$weights$analyst * 100,
              config$weights$patent * 100, config$weights$news * 100,
              config$weights$growth * 100))
  
  return(config)
}

create_default_configuration <- function() {
  #' Create Default DAII Configuration
  
  config <- list(
    version = "3.5",
    data_file = "input_data.csv",
    output_directory = "DAII_Output",
    
    # Component weights
    weights = list(
      r_d = 0.30,
      analyst = 0.20,
      patent = 0.25,
      news = 0.10,
      growth = 0.15
    ),
    
    # Data processing
    imputation = list(
      method = "median",
      industry_imputation = TRUE,
      max_missing_percent = 50
    ),
    
    # Normalization
    normalization = list(
      method = "min_max",
      lower_bound = 0.01,
      upper_bound = 0.99
    ),
    
    # Scoring
    scoring = list(
      log_transform = c("r_d_intensity", "patent_count"),
      epsilon = 1e-10
    ),
    
    # Portfolio integration
    portfolio = list(
      weight_column = "fund_weight",
      fund_column = "fund_name",
      calculate_hhi = TRUE,
      calculate_gini = TRUE
    ),
    
    # Validation
    validation = list(
      statistical_tests = TRUE,
      business_validation = TRUE,
      sensitivity_analysis = TRUE
    ),
    
    # Output
    output = list(
      formats = c("csv", "excel", "html"),
      create_dashboard = TRUE,
      create_presentation = TRUE
    ),
    
    # Reproducibility
    reproducibility = list(
      random_seed = 2024,
      save_raw_data = TRUE,
      save_configuration = TRUE
    )
  )
  
  return(config)
}

save_configuration <- function(config, config_file) {
  #' Save Configuration to YAML File
  
  if(require(yaml)) {
    yaml::write_yaml(config, config_file)
    cat(sprintf("Configuration saved: %s\n", config_file))
  } else {
    warning("YAML package not available. Configuration not saved.")
  }
}

validate_configuration <- function(config) {
  #' Validate Configuration Parameters
  
  # Check required fields
  required_fields <- c("data_file", "output_directory", "weights")
  missing_fields <- setdiff(required_fields, names(config))
  
  if(length(missing_fields) > 0) {
    stop(sprintf("Missing required configuration fields: %s",
                 paste(missing_fields, collapse = ", ")))
  }
  
  # Validate weights sum to 1
  weights_sum <- sum(unlist(config$weights))
  if(abs(weights_sum - 1) > 0.001) {
    warning(sprintf("Weights sum to %.3f (should be 1). Normalizing...", weights_sum))
    
    # Normalize weights
    for(w in names(config$weights)) {
      config$weights[[w]] <- config$weights[[w]] / weights_sum
    }
  }
  
  # Set defaults for missing optional fields
  defaults <- create_default_configuration()
  
  for(section in names(defaults)) {
    if(!section %in% names(config)) {
      config[[section]] <- defaults[[section]]
    } else {
      for(param in names(defaults[[section]])) {
        if(!param %in% names(config[[section]])) {
          config[[section]][[param]] <- defaults[[section]][[param]]
        }
      }
    }
  }
  
  return(config)
}

capture_system_info <- function() {
  #' Capture System Information for Reproducibility
  
  system_info <- list(
    r_version = R.version.string,
    platform = R.version$platform,
    operating_system = Sys.info()["sysname"],
    machine = Sys.info()["machine"],
    user = Sys.info()["user"],
    timezone = Sys.timezone(),
    locale = Sys.getlocale(),
    working_directory = getwd(),
    memory_limit = ifelse(.Platform$OS.type == "windows",
                          memory.limit(),
                          NA)
  )
  
  # Capture package versions
  packages <- c("dplyr", "tidyr", "ggplot2", "yaml", "openxlsx", "corrplot")
  package_versions <- sapply(packages, function(pkg) {
    if(requireNamespace(pkg, quietly = TRUE)) {
      as.character(packageVersion(pkg))
    } else {
      "Not installed"
    }
  })
  
  system_info$package_versions <- as.list(package_versions)
  
  # Save to file
  repro_dir <- "00_Reproducibility"
  if(!dir.exists(repro_dir)) {
    dir.create(repro_dir, showWarnings = FALSE)
  }
  
  writeLines(
    jsonlite::toJSON(system_info, pretty = TRUE),
    file.path(repro_dir, "system_info.json")
  )
  
  return(system_info)
}

create_analysis_pipeline <- function(config) {
  #' Create Analysis Pipeline from Configuration
  
  pipeline <- list(
    steps = list(),
    dependencies = list(),
    parameters = config
  )
  
  # Define pipeline steps
  pipeline$steps <- list(
    list(
      id = "initialize",
      function_name = "initialize_daii_environment",
      parameters = list(working_dir = NULL)
    ),
    list(
      id = "load_data",
      function_name = "load_and_validate_data",
      parameters = list(
        data_path = config$data_file,
        min_companies = 100,
        max_missing_pct = config$imputation$max_missing_percent / 100
      )
    ),
    list(
      id = "extract_companies",
      function_name = "extract_unique_companies",
      parameters = list(
        exclude_cols = c("fund_name", "fund_weight", "Position.Size")
      )
    ),
    list(
      id = "impute_missing",
      function_name = "impute_missing_values",
      parameters = list(
        imputation_methods = list(
          "BEst.Analyst.Rtg" = "mean",
          "default" = config$imputation$method
        ),
        industry_col = "GICS.Ind.Grp.Name"
      )
    ),
    list(
      id = "calculate_scores",
      function_name = "calculate_component_scores",
      parameters = list(
        weights_config = config$weights
      )
    ),
    list(
      id = "calculate_daii",
      function_name = "calculate_daii_scores",
      parameters = list(
        weights_config = list(
          default = c(R_D = config$weights$r_d,
                      Analyst = config$weights$analyst,
                      Patent = config$weights$patent,
                      News = config$weights$news,
                      Growth = config$weights$growth)
        )
      )
    ),
    list(
      id = "integrate_portfolio",
      function_name = "integrate_with_portfolio",
      parameters = list(
        weight_col = config$portfolio$weight_column,
        fund_col = config$portfolio$fund_column
      )
    ),
    list(
      id = "validate_results",
      function_name = "create_validation_framework",
      parameters = list()
    ),
    list(
      id = "create_visualizations",
      function_name = "create_daii_visualizations",
      parameters = list(
        output_dir = "05_Visualizations"
      )
    ),
    list(
      id = "generate_outputs",
      function_name = "generate_daii_outputs",
      parameters = list(
        output_dir = config$output_directory
      )
    )
  )
  
  # Define dependencies
  pipeline$dependencies <- list(
    load_data = c(),
    extract_companies = "load_data",
    impute_missing = "extract_companies",
    calculate_scores = "impute_missing",
    calculate_daii = "calculate_scores",
    integrate_portfolio = "calculate_daii",
    validate_results = "calculate_daii",
    create_visualizations = c("calculate_daii", "integrate_portfolio", "validate_results"),
    generate_outputs = c("calculate_daii", "integrate_portfolio", "validate_results")
  )
  
  # Save pipeline definition
  pipeline_file <- file.path("00_Reproducibility", "analysis_pipeline.json")
  writeLines(
    jsonlite::toJSON(pipeline, pretty = TRUE),
    pipeline_file
  )
  
  cat(sprintf("Analysis pipeline created: %s\n", pipeline_file))
  cat(sprintf("Number of steps: %d\n", length(pipeline$steps)))
  
  return(pipeline)
}

execute_analysis_pipeline <- function(pipeline, data_file, output_dir) {
  #' Execute Analysis Pipeline
  
  results <- list()
  execution_log <- list()
  
  # Initialize environment
  env_result <- do.call(pipeline$steps[[1]]$function_name,
                        pipeline$steps[[1]]$parameters)
  
  results[[pipeline$steps[[1]]$id]] <- env_result
  execution_log[[pipeline$steps[[1]]$id]] <- list(
    start_time = Sys.time(),
    status = "completed"
  )
  
  # Execute remaining steps
  for(i in 2:length(pipeline$steps)) {
    step <- pipeline$steps[[i]]
    
    cat(sprintf("\n▶️ Executing step %d/%d: %s\n", 
                i, length(pipeline$steps), step$id))
    
    # Check dependencies
    if(step$id %in% names(pipeline$dependencies)) {
      deps <- pipeline$dependencies[[step$id]]
      missing_deps <- setdiff(deps, names(results))
      
      if(length(missing_deps) > 0) {
        warning(sprintf("Missing dependencies for %s: %s",
                        step$id, paste(missing_deps, collapse = ", ")))
      }
    }
    
    # Prepare parameters
    params <- step$parameters
    
    # Add data from previous steps
    if(step$id == "load_data") {
      params$data_path <- data_file
    } else if(step$id == "extract_companies") {
      params$data <- results$load_data$data
      params$ticker_col <- results$load_data$ticker_col
    } else if(step$id == "impute_missing") {
      params$company_data <- results$extract_companies$companies
      params$ticker_col <- results$load_data$ticker_col
    } else if(step$id == "calculate_scores") {
      params$imputed_data <- results$impute_missing$imputed_data
      params$ticker_col <- results$load_data$ticker_col
    } else if(step$id == "calculate_daii") {
      params$scores_data <- results$calculate_scores$scores_data
      params$ticker_col <- results$load_data$ticker_col
    } else if(step$id == "integrate_portfolio") {
      params$holdings_data <- results$load_data$data
      params$daii_scores <- results$calculate_daii$daii_data
      params$ticker_col <- results$load_data$ticker_col
    } else if(step$id == "validate_results") {
      params$daii_results <- results$calculate_daii
      params$holdings_data <- results$load_data$data
      params$imputation_log <- results$impute_missing$imputation_log
    } else if(step$id == "create_visualizations") {
      params$daii_data <- results$calculate_daii$daii_data
      params$portfolio_results <- results$integrate_portfolio
      params$validation_report <- results$validate_results$validation_report
    } else if(step$id == "generate_outputs") {
      params$daii_results <- results$calculate_daii
      params$portfolio_results <- results$integrate_portfolio
      params$validation_results <- results$validate_results
    }
    
    # Execute step
    start_time <- Sys.time()
    
    tryCatch({
      result <- do.call(step$function_name, params)
      results[[step$id]] <- result
      
      execution_log[[step$id]] <- list(
        start_time = start_time,
        end_time = Sys.time(),
        duration = difftime(Sys.time(), start_time, units = "secs"),
        status = "completed",
        error = NA
      )
      
      cat(sprintf("   ✅ Completed in %.1f seconds\n", 
                  as.numeric(execution_log[[step$id]]$duration)))
      
    }, error = function(e) {
      execution_log[[step$id]] <- list(
        start_time = start_time,
        end_time = Sys.time(),
        duration = difftime(Sys.time(), start_time, units = "secs"),
        status = "failed",
        error = e$message
      )
      
      cat(sprintf("   ❌ Failed: %s\n", e$message))
    })
  }
  
  # Save execution log
  execution_summary <- data.frame(
    Step = names(execution_log),
    Status = sapply(execution_log, function(x) x$status),
    Duration_Seconds = sapply(execution_log, function(x) as.numeric(x$duration)),
    Error_Message = sapply(execution_log, function(x) ifelse(is.na(x$error), "", x$error)),
    stringsAsFactors = FALSE
  )
  
  write.csv(execution_summary,
            file.path(output_dir, "execution_log.csv"),
            row.names = FALSE)
  
  return(results)
}

create_reproducibility_package <- function(config,
                                           system_info,
                                           analysis_pipeline,
                                           results,
                                           output_dir) {
  #' Create Reproducibility Package
  
  repro_dir <- file.path(output_dir, "00_Reproducibility")
  if(!dir.exists(repro_dir)) {
    dir.create(repro_dir, recursive = TRUE, showWarnings = FALSE)
  }
  
  package_files <- list()
  
  # 1. Save configuration
  config_file <- file.path(repro_dir, "configuration.yaml")
  yaml::write_yaml(config, config_file)
  package_files$configuration <- config_file
  
  # 2. Save system information
  system_file <- file.path(repro_dir, "system_information.json")
  writeLines(jsonlite::toJSON(system_info, pretty = TRUE), system_file)
  package_files$system_info <- system_file
  
  # 3. Save pipeline definition
  pipeline_file <- file.path(repro_dir, "analysis_pipeline.json")
  writeLines(jsonlite::toJSON(analysis_pipeline, pretty = TRUE), pipeline_file)
  package_files$pipeline <- pipeline_file
  
  # 4. Save session info
  session_file <- file.path(repro_dir, "detailed_session_info.txt")
  sink(session_file)
  print(sessionInfo())
  sink()
  package_files$session_info <- session_file
  
  # 5. Save function definitions
  functions_file <- file.path(repro_dir, "function_definitions.R")
  save_function_definitions(functions_file)
  package_files$functions <- functions_file
  
  # 6. Save data checksums
  checksums_file <- file.path(repro_dir, "data_checksums.txt")
  calculate_data_checksums(checksums_file, config$data_file)
  package_files$checksums <- checksums_file
  
  # 7. Create reproducibility report
  report_file <- file.path(repro_dir, "reproducibility_report.html")
  create_reproducibility_report(report_file, config, system_info, package_files)
  package_files$report <- report_file
  
  # 8. Create README
  readme_file <- file.path(repro_dir, "README.md")
  create_reproducibility_readme(readme_file, package_files)
  package_files$readme <- readme_file
  
  # 9. Package everything into a zip file
  package_path <- file.path(output_dir, 
                            paste0("daii_3.5_reproducibility_",
                                   format(Sys.time(), "%Y%m%d_%H%M%S"),
                                   ".zip"))
  
  zip_files <- unlist(package_files)
  zip::zip(package_path, files = zip_files, root = output_dir)
  
  cat(sprintf("Reproducibility package created: %s\n", package_path))
  
  return(list(
    package_files = package_files,
    package_path = package_path
  ))
}

save_function_definitions <- function(output_file) {
  #' Save Function Definitions for Reproducibility
  
  # Get all functions in the global environment that start with specific patterns
  function_patterns <- c("^initialize_", "^load_", "^extract_", "^impute_",
                         "^calculate_", "^integrate_", "^create_", "^generate_",
                         "^analyze_", "^validate_", "^normalize_")
  
  all_functions <- ls(envir = .GlobalEnv)
  daii_functions <- character()
  
  for(pattern in function_patterns) {
    daii_functions <- c(daii_functions, 
                        grep(pattern, all_functions, value = TRUE))
  }
  
  # Remove duplicates
  daii_functions <- unique(daii_functions)
  
  # Write function definitions to file
  sink(output_file)
  
  cat("# DAII 3.5 Function Definitions\n")
  cat(paste("# Generated:", Sys.time(), "\n\n"))
  
  for(func_name in daii_functions) {
    func <- get(func_name, envir = .GlobalEnv)
    
    if(is.function(func)) {
      cat(paste(rep("#", 80), collapse = ""), "\n")
      cat(sprintf("# Function: %s\n", func_name))
      cat(paste(rep("#", 80), collapse = ""), "\n\n")
      
      # Get function code
      func_code <- capture.output(print(func))
      
      # Remove attributes
      func_code <- func_code[!grepl("^<bytecode|^<environment", func_code)]
      
      # Write function code
      cat(paste(func_code, collapse = "\n"))
      cat("\n\n")
    }
  }
  
  sink()
  
  cat(sprintf("Saved %d function definitions to %s\n", 
              length(daii_functions), output_file))
}

calculate_data_checksums <- function(output_file, data_file) {
  #' Calculate Data Checksums for Verification
  
  if(!file.exists(data_file)) {
    warning(sprintf("Data file not found: %s", data_file))
    return(NULL)
  }
  
  checksums <- list(
    file_name = basename(data_file),
    file_size = file.size(data_file),
    last_modified = file.info(data_file)$mtime,
    md5 = tools::md5sum(data_file),
    sha256 = digest::digest(data_file, algo = "sha256", file = TRUE)
  )
  
  # Write checksums to file
  sink(output_file)
  cat("DATA CHECKSUMS FOR REPRODUCIBILITY\n")
  cat("==================================\n\n")
  
  cat(sprintf("File: %s\n", checksums$file_name))
  cat(sprintf("Size: %s bytes\n", format(checksums$file_size, big.mark = ",")))
  cat(sprintf("Modified: %s\n", checksums$last_modified))
  cat(sprintf("MD5: %s\n", checksums$md5))
  cat(sprintf("SHA256: %s\n", checksums$sha256))
  
  sink()
  
  return(checksums)
}

create_reproducibility_report <- function(output_file, config, system_info, package_files) {
  #' Create Reproducibility Report
  
  report_html <- '
  <!DOCTYPE html>
  <html lang="en">
  <head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>DAII 3.5 Reproducibility Report</title>
    <style>
      body {
        font-family: Arial, sans-serif;
        margin: 40px;
        background-color: #f5f5f5;
      }
      .header {
        background: linear-gradient(135deg, #1F4E79 0%, #2E8B57 100%);
        color: white;
        padding: 30px;
        border-radius: 10px;
        margin-bottom: 30px;
      }
      .section {
        background-color: white;
        padding: 20px;
        border-radius: 8px;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        margin-bottom: 20px;
      }
      .section-title {
        color: #1F4E79;
        border-bottom: 2px solid #2E8B57;
        padding-bottom: 10px;
        margin-bottom: 20px;
      }
      .info-grid {
        display: grid;
        grid-template-columns: repeat(auto-fit, minmax(300px, 1fr));
        gap: 20px;
        margin-bottom: 20px;
      }
      .info-card {
        background-color: #f9f9f9;
        padding: 15px;
        border-radius: 5px;
        border-left: 4px solid #2E8B57;
      }
      .info-card h3 {
        margin-top: 0;
        color: #1F4E79;
      }
      .file-list {
        list-style-type: none;
        padding: 0;
      }
      .file-list li {
        padding: 8px;
        border-bottom: 1px solid #eee;
      }
      .file-list li:hover {
        background-color: #f9f9f9;
      }
      .verification-badge {
        display: inline-block;
        padding: 5px 10px;
        background-color: #2E8B57;
        color: white;
        border-radius: 3px;
        font-size: 12px;
        margin-right: 10px;
      }
      .timestamp {
        color: #666;
        font-size: 12px;
        margin-top: 30px;
        text-align: center;
      }
    </style>
  </head>
  <body>
    <div class="header">
      <h1>🔬 DAII 3.5 Reproducibility Report</h1>
      <p>Complete documentation for reproducing the analysis</p>
      <p><strong>Generated:</strong> '
  
  report_html <- paste0(report_html, format(Sys.time(), "%Y-%m-%d %H:%M:%S"), '</p>
    </div>')
  
  # System Information
  report_html <- paste0(report_html, '
    <div class="section">
      <h2 class="section-title">💻 System Information</h2>
      <div class="info-grid">
        <div class="info-card">
          <h3>R Environment</h3>
          <p><strong>R Version:</strong> ', system_info$r_version, '</p>
          <p><strong>Platform:</strong> ', system_info$platform, '</p>
          <p><strong>OS:</strong> ', system_info$operating_system, '</p>
        </div>
        
        <div class="info-card">
          <h3>Analysis Settings</h3>
          <p><strong>Working Directory:</strong> ', system_info$working_directory, '</p>
          <p><strong>Timezone:</strong> ', system_info$timezone, '</p>
          <p><strong>Locale:</strong> ', system_info$locale, '</p>
        </div>
        
        <div class="info-card">
          <h3>Key Packages</h3>')
  
  # Add package versions
  for(pkg in names(system_info$package_versions)) {
    report_html <- paste0(report_html, '
          <p><strong>', pkg, ':</strong> ', system_info$package_versions[[pkg]], '</p>')
  }
  
  report_html <- paste0(report_html, '
        </div>
      </div>
    </div>')
  
  # Configuration Summary
  report_html <- paste0(report_html, '
    <div class="section">
      <h2 class="section-title">⚙️ Configuration Summary</h2>
      <div class="info-grid">
        <div class="info-card">
          <h3>Data Input</h3>
          <p><strong>Data File:</strong> ', config$data_file, '</p>
          <p><strong>Output Directory:</strong> ', config$output_directory, '</p>
          <p><strong>DAII Version:</strong> ', config$version, '</p>
        </div>
        
        <div class="info-card">
          <h3>Component Weights</h3>
          <p><strong>R&D Intensity:</strong> ', config$weights$r_d * 100, '%</p>
          <p><strong>Analyst Sentiment:</strong> ', config$weights$analyst * 100, '%</p>
          <p><strong>Patent Activity:</strong> ', config$weights$patent * 100, '%</p>
          <p><strong>News Sentiment:</strong> ', config$weights$news * 100, '%</p>
          <p><strong>Growth Momentum:</strong> ', config$weights$growth * 100, '%</p>
        </div>
        
        <div class="info-card">
          <h3>Processing Settings</h3>
          <p><strong>Imputation Method:</strong> ', config$imputation$method, '</p>
          <p><strong>Normalization:</strong> ', config$normalization$method, '</p>
          <p><strong>Random Seed:</strong> ', config$reproducibility$random_seed, '</p>
        </div>
      </div>
    </div>')
  
  # Reproducibility Package Contents
  report_html <- paste0(report_html, '
    <div class="section">
      <h2 class="section-title">📦 Reproducibility Package Contents</h2>
      <ul class="file-list">')
  
  for(file_type in names(package_files)) {
    if(file_type != "report") {  # Don't list this report
      file_path <- package_files[[file_type]]
      file_name <- basename(file_path)
      
      report_html <- paste0(report_html, '
        <li>
          <span class="verification-badge">', toupper(file_type), '</span>
          <strong>', file_name, '</strong>
          <span style="color: #666; font-size: 12px; margin-left: 10px;">
            (', round(file.size(file_path) / 1024, 1), ' KB)
          </span>
        </li>')
    }
  }
  
  report_html <- paste0(report_html, '
      </ul>
    </div>')
  
  # Reproducibility Instructions
  report_html <- paste0(report_html, '
    <div class="section">
      <h2 class="section-title">📋 How to Reproduce This Analysis</h2>
      
      <h3>Step 1: Prepare Environment</h3>
      <div class="info-card">
        <p>1. Install R version ', R.version$major, '.', R.version$minor, '</p>
        <p>2. Install required packages from session_info.txt</p>
        <p>3. Set working directory to: ', getwd(), '</p>
      </div>
      
      <h3>Step 2: Restore Data</h3>
      <div class="info-card">
        <p>1. Place the input data file in the working directory</p>
        <p>2. Verify data integrity using checksums in data_checksums.txt</p>
        <p>3. Ensure the file name matches: ', basename(config$data_file), '</p>
      </div>
      
      <h3>Step 3: Restore Configuration</h3>
      <div class="info-card">
        <p>1. Load the configuration from configuration.yaml</p>
        <p>2. Verify all parameters match the original analysis</p>
        <p>3. Set the random seed to: ', config$reproducibility$random_seed, '</p>
      </div>
      
      <h3>Step 4: Execute Analysis</h3>
      <div class="info-card">
        <p>1. Load function definitions from function_definitions.R</p>
        <p>2. Follow the analysis pipeline in analysis_pipeline.json</p>
        <p>3. Execute each step in the defined order</p>
      </div>
      
      <h3>Step 5: Verify Results</h3>
      <div class="info-card">
        <p>1. Compare output files with original results</p>
        <p>2. Validate key metrics match within acceptable tolerances</p>
        <p>3. Check visualization outputs for consistency</p>
      </div>
    </div>')
  
  # Verification Checklist
  report_html <- paste0(report_html, '
    <div class="section">
      <h2 class="section-title">✅ Verification Checklist</h2>
      
      <div class="info-card">
        <h3>Essential Checks</h3>
        <p>☐ R version matches: ', system_info$r_version, '</p>
        <p>☐ Package versions are compatible</p>
        <p>☐ Input data checksums match</p>
        <p>☐ Configuration parameters are identical</p>
        <p>☐ Random seed is properly set</p>
      </div>
      
      <div class="info-card">
        <h3>Output Verification</h3>
        <p>☐ Portfolio DAII score matches within ±0.1</p>
        <p>☐ Top 10 innovators list is identical</p>
        <p>☐ Quartile distribution matches within ±1%</p>
        <p>☐ Validation status is consistent</p>
        <p>☐ All output files are generated</p>
      </div>
    </div>')
  
  # Close HTML
  report_html <- paste0(report_html, '
    <div class="timestamp">
      <p>DAII 3.5 Reproducibility Framework v3.5</p>
      <p>For support or questions, contact the Innovation Analytics Team</p>
    </div>
  </body>
  </html>')
  
  # Write report
  writeLines(report_html, output_file)
  
  return(output_file)
}

create_reproducibility_readme <- function(output_file, package_files) {
  #' Create README for Reproducibility Package
  
  readme_content <- paste(
    "# DAII 3.5 Reproducibility Package",
    "",
    "This package contains all necessary components to reproduce the DAII 3.5 analysis.",
    "",
    "## Package Contents",
    "",
    sep = "\n"
  )
  
  # List files
  for(file_type in names(package_files)) {
    file_path <- package_files[[file_type]]
    file_name <- basename(file_path)
    
    readme_content <- paste(readme_content,
                            sprintf("- **%s**: %s", file_type, file_name),
                            sep = "\n")
  }
  
  # Add instructions
  readme_content <- paste(readme_content,
                          "",
                          "## How to Use This Package",
                          "",
                          "### 1. Quick Start",
                          "```r",
                          "# Load the reproducibility package",
                          "source('function_definitions.R')",
                          "",
                          "# Load configuration",
                          "config <- yaml::read_yaml('configuration.yaml')",
                          "",
                          "# Set random seed",
                          sprintf("set.seed(%d)", 2024),  # Using default seed
                          "",
                          "# Run the analysis",
                          "results <- create_reproducible_analysis('configuration.yaml')",
                          "```",
                          "",
                          "### 2. Detailed Reproduction",
                          "",
                          "1. **Environment Setup**:",
                          "   - Install R version matching the system information",
                          "   - Install required packages with specified versions",
                          "   - Set the working directory",
                          "",
                          "2. **Data Preparation**:",
                          "   - Place the input data file in the working directory",
                          "   - Verify data integrity using checksums",
                          "",
                          "3. **Configuration**:",
                          "   - Review and load the configuration file",
                          "   - Verify all parameters match the original analysis",
                          "",
                          "4. **Execution**:",
                          "   - Load all function definitions",
                          "   - Execute the analysis pipeline step by step",
                          "   - Monitor progress and check for errors",
                          "",
                          "5. **Verification**:",
                          "   - Compare outputs with original results",
                          "   - Validate key metrics and visualizations",
                          "   - Generate verification report",
                          "",
                          "## Expected Outputs",
                          "",
                          "A successful reproduction should generate:",
                          "- Company-level DAII scores and rankings",
                          "- Portfolio innovation analysis",
                          "- Validation reports and quality checks",
                          "- Executive summaries and dashboards",
                          "- Complete visualization package",
                          "",
                          "## Troubleshooting",
                          "",
                          "### Common Issues",
                          "",
                          "1. **Package Version Conflicts**:",
                          "   - Check session_info.txt for exact versions",
                          "   - Use renv or packrat for version management",
                          "",
                          "2. **Missing Data Files**:",
                          "   - Verify the input data file exists and is accessible",
                          "   - Check file permissions and paths",
                          "",
                          "3. **Configuration Errors**:",
                          "   - Validate YAML syntax in configuration files",
                          "   - Check for missing required parameters",
                          "",
                          "4. **Memory Issues**:",
                          "   - Large datasets may require increased memory",
                          "   - Consider processing in batches if needed",
                          "",
                          "## Support",
                          "",
                          "For assistance with reproduction:",
                          "- Review the reproducibility report",
                          "- Check the execution log for errors",
                          "- Contact the Innovation Analytics Team",
                          "",
                          "---",
                          sprintf("Generated: %s", format(Sys.time(), "%Y-%m-%d %H:%M:%S")),
                          "DAII 3.5 Framework v3.5",
                          sep = "\n")
  
  # Write README
  writeLines(readme_content, output_file)
  
  return(output_file)
}

validate_reproducibility <- function(results, config) {
  #' Validate Reproducibility of Results
  
  validation_results <- list(
    checks = list(),
    status = "pending",
    issues = character()
  )
  
  # Check 1: Results structure
  expected_results <- c("load_data", "extract_companies", "impute_missing",
                        "calculate_scores", "calculate_daii", "integrate_portfolio",
                        "validate_results", "create_visualizations", "generate_outputs")
  
  missing_results <- setdiff(expected_results, names(results))
  
  if(length(missing_results) > 0) {
    validation_results$checks$structure <- list(
      passed = FALSE,
      message = sprintf("Missing results: %s", paste(missing_results, collapse = ", "))
    )
    validation_results$issues <- c(validation_results$issues,
                                   "Missing analysis results")
  } else {
    validation_results$checks$structure <- list(
      passed = TRUE,
      message = "All expected results present"
    )
  }
  
  # Check 2: Data validation
  if("load_data" %in% names(results)) {
    data_validation <- results$load_data$validation_report
    
    validation_results$checks$data_quality <- list(
      passed = TRUE,
      message = sprintf("Loaded %d companies with %d holdings",
                        data_validation$structure$unique_companies,
                        data_validation$structure$total_rows)
    )
  }
  
  # Check 3: Score validation
  if("calculate_daii" %in% names(results)) {
    daii_scores <- results$calculate_daii$daii_data$DAII_3.5_Score
    
    score_stats <- list(
      min = min(daii_scores, na.rm = TRUE),
      max = max(daii_scores, na.rm = TRUE),
      mean = mean(daii_scores, na.rm = TRUE),
      sd = sd(daii_scores, na.rm = TRUE)
    )
    
    # Check score range (should be 0-100)
    if(score_stats$min >= 0 && score_stats$max <= 100) {
      validation_results$checks$score_range <- list(
        passed = TRUE,
        message = sprintf("Scores in valid range: %.1f to %.1f",
                          score_stats$min, score_stats$max)
      )
    } else {
      validation_results$checks$score_range <- list(
        passed = FALSE,
        message = sprintf("Scores out of range: %.1f to %.1f",
                          score_stats$min, score_stats$max)
      )
      validation_results$issues <- c(validation_results$issues,
                                     "Score range validation failed")
    }
  }
  
  # Check 4: Portfolio validation
  if("integrate_portfolio" %in% names(results)) {
    portfolio_metrics <- results$integrate_portfolio$portfolio_metrics$overall
    
    validation_results$checks$portfolio <- list(
      passed = TRUE,
      message = sprintf("Portfolio DAII: %.1f, Holdings: %d",
                        portfolio_metrics$portfolio_daii,
                        portfolio_metrics$total_holdings)
    )
  }
  
  # Check 5: Output validation
  if("generate_outputs" %in% names(results)) {
    output_dir <- results$generate_outputs$output_directory
    
    if(dir.exists(output_dir)) {
      output_files <- list.files(output_dir, recursive = TRUE)
      
      validation_results$checks$output_files <- list(
        passed = TRUE,
        message = sprintf("Generated %d output files", length(output_files))
      )
    } else {
      validation_results$checks$output_files <- list(
        passed = FALSE,
        message = "Output directory not found"
      )
      validation_results$issues <- c(validation_results$issues,
                                     "Output generation failed")
    }
  }
  
  # Overall status
  passed_checks <- sum(sapply(validation_results$checks, function(x) x$passed))
  total_checks <- length(validation_results$checks)
  
  if(length(validation_results$issues) == 0) {
    validation_results$status <- "passed"
    validation_results$summary <- sprintf("All %d checks passed", total_checks)
  } else {
    validation_results$status <- "failed"
    validation_results$summary <- sprintf("%d/%d checks passed, %d issues",
                                          passed_checks, total_checks,
                                          length(validation_results$issues))
  }
  
  # Save validation results
  validation_file <- file.path(config$output_directory,
                               "00_Reproducibility",
                               "reproducibility_validation.json")
  
  writeLines(jsonlite::toJSON(validation_results, pretty = TRUE),
             validation_file)
  
  cat(sprintf("\n🔍 REPRODUCIBILITY VALIDATION: %s\n", validation_results$status))
  cat(sprintf("   %s\n", validation_results$summary))
  
  if(length(validation_results$issues) > 0) {
    cat("\n⚠️ Issues found:\n")
    for(issue in validation_results$issues) {
      cat(sprintf("   • %s\n", issue))
    }
  }
  
  return(validation_results)
}

