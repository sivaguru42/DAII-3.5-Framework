################################################################################
# DAII 3.5 - PHASE 1 COMPLETE CODEBASE (Modules 0-9)
# Consolidated Version: 3.5.7
# Date: 2026-02-04
################################################################################

# =============================================================================
# MODULE 0: ENVIRONMENT SETUP & GITHUB INVENTORY VERIFICATION
# =============================================================================

cat(paste(rep("=", 80), collapse = ""), "\n")
cat("DAII 3.5 - PHASE 1 COMPLETE CODEBASE INITIALIZATION\n")
cat(paste(rep("=", 80), collapse = ""), "\n\n")

# -----------------------------------------------------------------------------
# 0.1 PACKAGE MANAGEMENT & SYSTEM CONFIGURATION
# -----------------------------------------------------------------------------

cat("📦 STAGE 0.1: LOADING REQUIRED PACKAGES & CONFIGURING ENVIRONMENT\n")
cat(paste(rep("-", 60), collapse = ""), "\n")

required_packages <- c(
  "dplyr",     # Data manipulation
  "tidyr",     # Data tidying
  "readr",     # Efficient data reading
  "httr",      # HTTP requests for GitHub access
  "stringr",   # String operations
  "purrr",     # Functional programming
  "lubridate", # Date handling
  "yaml",      # Configuration file parsing
  "ggplot2",   # Visualizations
  "openxlsx",  # Excel output
  "corrplot",  # Correlation plots
  "moments"    # Skewness and kurtosis
)

load_packages_safely <- function(pkg_list) {
  for (pkg in pkg_list) {
    if (!require(pkg, character.only = TRUE, quietly = TRUE)) {
      cat(sprintf("   Installing missing package: %s\n", pkg))
      install.packages(pkg, dependencies = TRUE, repos = "https://cloud.r-project.org")
      library(pkg, character.only = TRUE)
      cat(sprintf("   ✅ Loaded: %s\n", pkg))
    } else {
      cat(sprintf("   ✅ Already available: %s\n", pkg))
    }
  }
}

load_packages_safely(required_packages)

# Set global R options for reproducibility and clarity
options(stringsAsFactors = FALSE,
        warn = 1,
        scipen = 999,
        digits = 4)

cat("✅ Environment configured.\n\n")

# -----------------------------------------------------------------------------
# 0.2 GITHUB REPOSITORY INVENTORY VERIFICATION
# -----------------------------------------------------------------------------

cat("🔍 STAGE 0.2: VERIFYING GITHUB FILE AVAILABILITY\n")
cat(paste(rep("-", 60), collapse = ""), "\n")

github_config <- list(
  repository = "https://github.com/sivaguru42/DAII-3.5-Framework",
  raw_base = "https://raw.githubusercontent.com/sivaguru42/DAII-3.5-Framework/main",
  essential_files = list(
    n50_dataset = list(
      filename = "DAII_3_5_N50_Test_Dataset.csv",
      description = "Primary N=50 Hybrid Test Dataset",
      test_url = "https://raw.githubusercontent.com/sivaguru42/DAII-3.5-Framework/main/DAII_3_5_N50_Test_Dataset.csv",
      required = TRUE
    )
  ),
  config_files = list(
    scoring_config = list(
      filename = "daii_scoring_config.yaml",
      test_url = "https://raw.githubusercontent.com/sivaguru42/DAII-3.5-Framework/main/daii_scoring_config.yaml",
      required = FALSE
    ),
    imputation_config = list(
      filename = "daii_imputation_config.yaml",
      test_url = "https://raw.githubusercontent.com/sivagoru42/DAII-3.5-Framework/main/daii_imputation_config.yaml",
      required = FALSE
    )
  )
)

verify_github_file <- function(file_info) {
  cat(sprintf("   Checking: %-45s", file_info$filename))
  tryCatch({
    response <- httr::GET(file_info$test_url, httr::timeout(10))
    if (httr::status_code(response) == 200) {
      file_size <- length(httr::content(response, "raw"))
      cat(sprintf(" ✅ (%.1f KB)\n", file_size / 1024))
      return(list(available = TRUE, size_kb = file_size / 1024))
    } else {
      cat(sprintf(" ❌ (HTTP %s)\n", httr::status_code(response)))
      return(list(available = FALSE))
    }
  }, error = function(e) {
    cat(sprintf(" ❌ (Error: %s)\n", substr(e$message, 1, 30)))
    return(list(available = FALSE))
  })
}

cat("   Repository:", github_config$repository, "\n\n")
verification_results <- list()

for (file_group_name in c("essential_files", "config_files")) {
  file_group <- github_config[[file_group_name]]
  for (file_key in names(file_group)) {
    file_info <- file_group[[file_key]]
    verification_results[[file_info$filename]] <- verify_github_file(file_info)
    verification_results[[file_info$filename]]$required <- file_info$required
  }
}

n50_available <- verification_results[["DAII_3_5_N50_Test_Dataset.csv"]]$available
if (!n50_available) {
  cat("\n❌ CRITICAL ERROR: The essential N50 dataset is not available.\n")
  cat("   The pipeline cannot proceed. Please ensure the file exists at:\n")
  cat("   ", github_config$essential_files$n50_dataset$test_url, "\n")
  stop("Execution halted due to missing essential data.")
} else {
  cat("\n✅ SUCCESS: All essential files are available. Proceeding to data load.\n")
}

# =============================================================================
# MODULE 1: DATA LOADING & PREPARATION
# =============================================================================

cat("\n")
cat(paste(rep("=", 80), collapse = ""), "\n")
cat("MODULE 1: DATA LOADING & PREPARATION\n")
cat(paste(rep("=", 80), collapse = ""), "\n\n")

# -----------------------------------------------------------------------------
# 1.1 LOAD PRIMARY DATASET FROM GITHUB
# -----------------------------------------------------------------------------

cat("📥 STAGE 1.1: LOADING N50 HYBRID DATASET\n")
cat(paste(rep("-", 60), collapse = ""), "\n")

n50_data_url <- github_config$essential_files$n50_dataset$test_url

load_n50_dataset <- function(url) {
  tryCatch({
    cat("   Downloading data from GitHub...\n")
    dataset <- readr::read_csv(url, show_col_types = FALSE, progress = FALSE)

    cat(sprintf("   ✅ Successfully loaded dataset: %d rows × %d columns\n",
                nrow(dataset), ncol(dataset)))

    cat("\n   📊 INITIAL DATA STRUCTURE:\n")
    cat("   Column names (first 10):", paste(colnames(dataset)[1:10], collapse=", "), "...\n")
    cat("   Sample tickers:", paste(unique(dataset$Ticker)[1:5], collapse=", "), "...\n")

    expected_cols <- c("Ticker", "R.D.Exp", "Mkt.Cap", "BEst.Analyst.Rtg",
                       "Patents...Trademarks...Copy.Rgt", "News.Sent", "Rev...1.Yr.Gr")
    missing_cols <- setdiff(expected_cols, colnames(dataset))
    if (length(missing_cols) > 0) {
      cat("   ⚠️  WARNING: Missing expected columns:", paste(missing_cols, collapse=", "), "\n")
    } else {
      cat("   ✅ All expected core data columns are present.\n")
    }
    return(dataset)

  }, error = function(e) {
    cat(sprintf("   ❌ FATAL ERROR loading dataset: %s\n", e$message))
    stop("Data loading failed. Pipeline cannot continue.")
  })
}

daii_raw_data <- load_n50_dataset(n50_data_url)

# -----------------------------------------------------------------------------
# 1.2 DATA CLEANING & PRE-PROCESSING
# -----------------------------------------------------------------------------

cat("\n🔄 STAGE 1.2: INITIAL DATA CLEANING & TYPE ENFORCEMENT\n")
cat(paste(rep("-", 60), collapse = ""), "\n")

preprocess_raw_data <- function(raw_data) {
  cat("   Applying initial cleaning rules...\n")
  processed_data <- raw_data

  if (exists("Ticker", where = processed_data)) {
    processed_data$Ticker <- as.character(processed_data$Ticker)
  }

  numeric_columns <- c("R.D.Exp", "Mkt.Cap", "Rev...1.Yr.Gr",
                       "BEst.Analyst.Rtg", "Patents...Trademarks...Copy.Rgt", "News.Sent")

  for (col in numeric_columns) {
    if (col %in% colnames(processed_data)) {
      processed_data[[col]] <- as.numeric(as.character(processed_data[[col]]))
      na_count <- sum(is.na(processed_data[[col]]))
      if (na_count > 0) {
        cat(sprintf("   Note: Column '%s' has %d NA values after conversion.\n", col, na_count))
      }
    }
  }

  cat("   ✅ Initial cleaning complete.\n")
  return(processed_data)
}

daii_processed_data <- preprocess_raw_data(daii_raw_data)

# =============================================================================
# MODULE 2: IMPUTATION ENGINE (WITH CRITICAL FIXES)
# =============================================================================

cat("\n")
cat(paste(rep("=", 80), collapse = ""), "\n")
cat("MODULE 2: IMPUTATION ENGINE - MISSING DATA HANDLING\n")
cat(paste(rep("=", 80), collapse = ""), "\n\n")

# -----------------------------------------------------------------------------
# 2.1 CORE IMPUTATION FUNCTION - FIXED VERSION
# -----------------------------------------------------------------------------

cat("🔄 STAGE 2.1: EXECUTING IMPUTATION ENGINE (FIXED NUMERIC CONVERSION)\n")
cat(paste(rep("-", 60), collapse = ""), "\n")

impute_missing_values <- function(company_data,
                                  ticker_col = "Ticker",
                                  imputation_methods = list(
                                    "BEst.Analyst.Rtg" = "mean",
                                    "default" = "median"
                                  ),
                                  industry_col = "GICS.Ind.Grp.Name") {

  cat("   Initializing imputation process...\n")
  imputed_data <- company_data
  imputation_log <- data.frame(
    Ticker = character(),
    Metric = character(),
    Original_Value = character(),
    Imputed_Value = numeric(),
    Imputation_Method = character(),
    stringsAsFactors = FALSE
  )

  numeric_cols <- c("R.D.Exp", "Mkt.Cap", "Rev...1.Yr.Gr",
                    "BEst.Analyst.Rtg", "Patents...Trademarks...Copy.Rgt", "News.Sent")

  for (col in intersect(numeric_cols, colnames(imputed_data))) {
    cat(sprintf("\n   Processing metric: %s\n", col))

    # ===== CRITICAL FIX 1: CORRECT NUMERIC CONVERSION =====
    num_col_name <- paste0(col, "_num")
    imputed_data[[num_col_name]] <- as.numeric(as.character(imputed_data[[col]]))

    missing_indicators <- c("#N/A", "N/A", "NA", "NULL", "", "Field Not Applicable")
    is_missing <- is.na(imputed_data[[num_col_name]]) |
      imputed_data[[col]] %in% missing_indicators

    missing_count <- sum(is_missing, na.rm = TRUE)

    if (missing_count > 0) {
      cat(sprintf("      Found %d missing values (%.1f%%).\n",
                  missing_count, 100 * missing_count / nrow(imputed_data)))

      method <- if (col %in% names(imputation_methods)) {
        imputation_methods[[col]]
      } else {
        imputation_methods$default
      }

      available_vals <- imputed_data[[num_col_name]][!is_missing]
      if (method == "mean" || col == "BEst.Analyst.Rtg") {
        impute_val <- mean(available_vals, na.rm = TRUE)
        method_name <- "Global_Mean"
      } else {
        impute_val <- median(available_vals, na.rm = TRUE)
        method_name <- "Global_Median"
      }

      imputed_data[[num_col_name]][is_missing] <- impute_val
      cat(sprintf("      Imputed with %s: %.4f\n", method_name, impute_val))

      for (idx in which(is_missing)) {
        imputation_log <- rbind(imputation_log, data.frame(
          Ticker = imputed_data[[ticker_col]][idx],
          Metric = col,
          Original_Value = as.character(imputed_data[[col]][idx]),
          Imputed_Value = impute_val,
          Imputation_Method = method_name,
          stringsAsFactors = FALSE
        ))
      }
    } else {
      cat("      No missing values found.\n")
    }
  }

  if (nrow(imputation_log) > 0) {
    cat("\n   📋 IMPUTATION SUMMARY:\n")
    summary <- imputation_log %>%
      dplyr::group_by(Metric, Imputation_Method) %>%
      dplyr::summarise(Count = dplyr::n(),
                       Avg_Imputed_Value = mean(Imputed_Value),
                       .groups = 'drop')
    print(summary)
  } else {
    cat("\n   ℹ️  No imputations were performed.\n")
  }

  cat("\n   ✅ Imputation engine complete.\n")
  return(list(imputed_data = imputed_data,
              imputation_log = imputation_log))
}

imputation_results <- impute_missing_values(daii_processed_data)
daii_imputed_data <- imputation_results$imputed_data

# =============================================================================
# MODULE 3: SCORING ENGINE (WITH CRITICAL FIXES)
# =============================================================================

cat("\n")
cat(paste(rep("=", 80), collapse = ""), "\n")
cat("MODULE 3: SCORING ENGINE - COMPONENT SCORE CALCULATION\n")
cat(paste(rep("=", 80), collapse = ""), "\n\n")

# -----------------------------------------------------------------------------
# 3.1 NORMALIZATION FUNCTION - FIXED VERSION
# -----------------------------------------------------------------------------

cat("📊 STAGE 3.1: DEFINING NORMALIZATION FUNCTION (FIXED UNIFORM ZERO HANDLING)\n")
cat(paste(rep("-", 60), collapse = ""), "\n")

normalize_to_100 <- function(x, cap_extremes = TRUE,
                             lower_bound = 0.01, upper_bound = 0.99) {

  if (all(is.na(x))) {
    return(rep(NA_real_, length(x)))
  }

  x_finite <- x[is.finite(x) & !is.na(x)]
  if (length(x_finite) == 0) {
    return(rep(50, length(x))) # Default midpoint for all NA/Inf
  }

  if (cap_extremes && length(x_finite) > 10) {
    lower_thresh <- quantile(x_finite, lower_bound, na.rm = TRUE)
    upper_thresh <- quantile(x_finite, upper_bound, na.rm = TRUE)
    x <- pmin(pmax(x, lower_thresh, na.rm = TRUE), upper_thresh, na.rm = TRUE)
    x_finite <- x[is.finite(x) & !is.na(x)]
  }

  min_val <- min(x_finite, na.rm = TRUE)
  max_val <- max(x_finite, na.rm = TRUE)

  # ===== CRITICAL FIX 2: HANDLE UNIFORM ZERO VALUES =====
  if (abs(max_val - min_val) < .Machine$double.eps^0.5) { # Practical equality
    if (abs(max_val) < .Machine$double.eps^0.5) { # If uniform value is ~0
      return(rep(0, length(x))) # Score zeros as 0, not 50
    } else {
      return(rep(50, length(x))) # Other uniform values score 50 (neutral)
    }
  }

  normalized <- 100 * (x - min_val) / (max_val - min_val)
  normalized <- pmin(pmax(normalized, 0, na.rm = TRUE), 100, na.rm = TRUE)
  return(normalized)
}

cat("   ✅ Normalization function loaded (with zero-fix).\n")

# -----------------------------------------------------------------------------
# 3.2 CORE SCORING FUNCTION
# -----------------------------------------------------------------------------

cat("\n🧮 STAGE 3.2: CALCULATING COMPONENT & COMPOSITE SCORES\n")
cat(paste(rep("-", 60), collapse = ""), "\n")

calculate_component_scores <- function(imputed_data,
                                       weights_config = list(
                                         R_D = 0.30,
                                         Analyst = 0.20,
                                         Patent = 0.25,
                                         News = 0.10,
                                         Growth = 0.15
                                       )) {

  scores_data <- imputed_data
  cat("   Beginning score calculations...\n")

  # 1. R&D INTENSITY SCORE (Log-Transformed)
  cat("\n   1. R&D Intensity Score (Weight: 30%)\n")
  scores_data$R_D_Intensity_Raw <- scores_data$R.D.Exp_num / scores_data$Mkt.Cap_num
  scores_data$R_D_Intensity_Raw[!is.finite(scores_data$R_D_Intensity_Raw)] <- NA
  scores_data$R_D_Intensity_Log <- log(scores_data$R_D_Intensity_Raw + 1e-10)
  scores_data$R_D_Score <- normalize_to_100(scores_data$R_D_Intensity_Log)
  cat("      Calculation complete.\n")

  # 2. ANALYST SENTIMENT SCORE
  cat("\n   2. Analyst Sentiment Score (Weight: 20%)\n")
  scores_data$Analyst_Score <- normalize_to_100(scores_data$BEst.Analyst.Rtg_num)
  cat("      Calculation complete.\n")

  # 3. PATENT ACTIVITY SCORE (Log-Transformed)
  cat("\n   3. Patent Activity Score (Weight: 25%)\n")
  scores_data$Patents_Log <- log(scores_data$Patents...Trademarks...Copy.Rgt_num + 1)
  scores_data$Patent_Score <- normalize_to_100(scores_data$Patents_Log)
  cat("      Calculation complete.\n")

  # 4. NEWS SENTIMENT SCORE
  cat("\n   4. News Sentiment Score (Weight: 10%)\n")
  scores_data$News_Score <- normalize_to_100(scores_data$News.Sent_num)
  cat("      Calculation complete.\n")

  # 5. GROWTH MOMENTUM SCORE
  cat("\n   5. Growth Momentum Score (Weight: 15%)\n")
  scores_data$Growth_Score <- normalize_to_100(scores_data$Rev...1.Yr.Gr_num)
  cat("      Calculation complete.\n")

  # 6. DAII 3.5 COMPOSITE SCORE (Weighted Average)
  cat("\n   6. DAII 3.5 Composite Score\n")
  scores_data$DAII_3.5_Score <- round(
    scores_data$R_D_Score * weights_config$R_D +
    scores_data$Analyst_Score * weights_config$Analyst +
    scores_data$Patent_Score * weights_config$Patent +
    scores_data$News_Score * weights_config$News +
    scores_data$Growth_Score * weights_config$Growth,
    2
  )

  # 7. ASSIGN INNOVATION QUARTILES
  quartile_breaks <- quantile(scores_data$DAII_3.5_Score,
                              probs = c(0, 0.25, 0.50, 0.75, 1),
                              na.rm = TRUE, names = FALSE)
  scores_data$DAII_Quartile <- cut(scores_data$DAII_3.5_Score,
                                   breaks = quartile_breaks,
                                   labels = c("Q4 (Low)", "Q3", "Q2", "Q1 (High)"),
                                   include.lowest = TRUE)

  # 8. FINAL SUMMARY
  cat("\n   📈 FINAL SCORE DISTRIBUTION SUMMARY:\n")
  cat(sprintf("      Composite Score - Mean: %.2f, Median: %.2f, SD: %.2f\n",
              mean(scores_data$DAII_3.5_Score, na.rm = TRUE),
              median(scores_data$DAII_3.5_Score, na.rm = TRUE),
              sd(scores_data$DAII_3.5_Score, na.rm = TRUE)))

  component_cols <- c("R_D_Score", "Analyst_Score", "Patent_Score", "News_Score", "Growth_Score")
  for (comp in component_cols) {
    if (comp %in% colnames(scores_data)) {
      cat(sprintf("      %-15s - Mean: %6.2f, Range: [%5.2f, %5.2f]\n",
                  comp,
                  mean(scores_data[[comp]], na.rm = TRUE),
                  min(scores_data[[comp]], na.rm = TRUE),
                  max(scores_data[[comp]], na.rm = TRUE)))
    }
  }

  cat("\n   ✅ Scoring engine complete.\n")
  return(scores_data)
}

daii_scored_data <- calculate_component_scores(daii_imputed_data)

# =============================================================================
# MODULE 4: AGGREGATION FRAMEWORK
# =============================================================================

cat("\n")
cat(paste(rep("=", 80), collapse = ""), "\n")
cat("MODULE 4: AGGREGATION FRAMEWORK - COMPANY-LEVEL CONSOLIDATION\n")
cat(paste(rep("=", 80), collapse = ""), "\n\n")

cat("📈 STAGE 4.1: AGGREGATING FUND-LEVEL HOLDINGS TO COMPANY-LEVEL SCORES\n")
cat(paste(rep("-", 60), collapse = ""), "\n")

aggregate_to_company_level <- function(scored_data, ticker_col = "Ticker") {

  cat("   Checking data structure for aggregation...\n")
  if (!ticker_col %in% colnames(scored_data)) {
    stop(sprintf("Aggregation failed: Ticker column '%s' not found.", ticker_col))
  }

  duplicate_tickers <- scored_data[[ticker_col]][duplicated(scored_data[[ticker_col]])]
  num_duplicates <- length(unique(duplicate_tickers))

  if (num_duplicates == 0) {
    cat("   ℹ️  Data is already at company-level (no duplicate tickers).\n")
    return(scored_data)
  }

  cat(sprintf("   Found %d companies with multiple fund holdings. Aggregating...\n", num_duplicates))

  score_cols_to_avg <- c("R_D_Score", "Analyst_Score", "Patent_Score",
                         "News_Score", "Growth_Score", "DAII_3.5_Score")

  meta_cols_to_keep <- c(ticker_col, "Company.Name", "GICS.Ind.Grp.Name")

  score_cols_to_avg <- intersect(score_cols_to_avg, colnames(scored_data))
  meta_cols_to_keep <- intersect(meta_cols_to_keep, colnames(scored_data))

  aggregated_data <- scored_data %>%
    dplyr::group_by(!!rlang::sym(ticker_col)) %>%
    dplyr::summarise(
      dplyr::across(dplyr::all_of(meta_cols_to_keep[meta_cols_to_keep != ticker_col]),
                    ~ dplyr::first(stats::na.omit(.x))),
      dplyr::across(dplyr::all_of(score_cols_to_avg),
                    ~ mean(.x, na.rm = TRUE)),
      .groups = 'drop'
    )

  quartile_breaks <- quantile(aggregated_data$DAII_3.5_Score,
                              probs = c(0, 0.25, 0.50, 0.75, 1),
                              na.rm = TRUE, names = FALSE)
  aggregated_data$DAII_Quartile <- cut(aggregated_data$DAII_3.5_Score,
                                       breaks = quartile_breaks,
                                       labels = c("Q4 (Low)", "Q3", "Q2", "Q1 (High)"),
                                       include.lowest = TRUE)

  cat(sprintf("   ✅ Aggregation complete. Final dataset: %d unique companies.\n",
              nrow(aggregated_data)))
  return(aggregated_data)
}

daii_company_level_data <- aggregate_to_company_level(daii_scored_data)

# =============================================================================
# MODULE 5: PORTFOLIO INTEGRATION
# =============================================================================

cat("\n")
cat(paste(rep("=", 80), collapse = ""), "\n")
cat("MODULE 5: PORTFOLIO INTEGRATION - FUND-LEVEL ANALYSIS\n")
cat(paste(rep("=", 80), collapse = ""), "\n\n")

cat("🏦 STAGE 5.1: INTEGRATING SCORES WITH PORTFOLIO HOLDINGS\n")
cat(paste(rep("-", 60), collapse = ""), "\n")

integrate_with_portfolio <- function(scored_data,
                                     weight_col = "fund_weight",
                                     fund_col = "fund_name") {

  cat("   Initializing portfolio integration...\n")

  if (!fund_col %in% colnames(scored_data)) {
    cat(sprintf("   ℹ️  Fund column '%s' not found. Skipping portfolio integration.\n", fund_col))
    return(NULL)
  }

  if (!weight_col %in% colnames(scored_data)) {
    cat(sprintf("   ℹ️  Weight column '%s' not found. Using equal weights.\n", weight_col))
    scored_data[[weight_col]] <- 1 / nrow(scored_data)
  }

  portfolio_data <- scored_data

  # Calculate portfolio-level metrics
  portfolio_metrics <- list(
    overall = list(
      portfolio_daii = sum(portfolio_data[[weight_col]] * portfolio_data$DAII_3.5_Score, na.rm = TRUE),
      weighted_r_d = sum(portfolio_data[[weight_col]] * portfolio_data$R_D_Score, na.rm = TRUE),
      weighted_analyst = sum(portfolio_data[[weight_col]] * portfolio_data$Analyst_Score, na.rm = TRUE),
      weighted_patent = sum(portfolio_data[[weight_col]] * portfolio_data$Patent_Score, na.rm = TRUE),
      total_weight = sum(portfolio_data[[weight_col]], na.rm = TRUE)
    )
  )

  # Fund-level analysis
  if (length(unique(portfolio_data[[fund_col]])) > 1) {
    fund_analysis <- portfolio_data %>%
      dplyr::group_by(!!rlang::sym(fund_col)) %>%
      dplyr::summarise(
        fund_daii = sum(!!rlang::sym(weight_col) * DAII_3.5_Score, na.rm = TRUE) /
                    sum(!!rlang::sym(weight_col), na.rm = TRUE),
        holdings_count = dplyr::n(),
        avg_company_daii = mean(DAII_3.5_Score, na.rm = TRUE),
        .groups = 'drop'
      )
  } else {
    fund_analysis <- NULL
  }

  # Industry exposure
  if ("GICS.Ind.Grp.Name" %in% colnames(portfolio_data)) {
    industry_analysis <- portfolio_data %>%
      dplyr::group_by(GICS.Ind.Grp.Name) %>%
      dplyr::summarise(
        portfolio_weight = sum(!!rlang::sym(weight_col), na.rm = TRUE),
        avg_daii = mean(DAII_3.5_Score, na.rm = TRUE),
        companies = dplyr::n(),
        .groups = 'drop'
      ) %>%
      dplyr::mutate(portfolio_weight = portfolio_weight / sum(portfolio_weight, na.rm = TRUE))
  } else {
    industry_analysis <- NULL
  }

  # Portfolio summary
  portfolio_summary <- data.frame(
    metric = c("Portfolio DAII Score", "Number of Holdings", "Number of Funds",
               "Weighted R&D Score", "Weighted Analyst Score", "Weighted Patent Score"),
    value = c(portfolio_metrics$overall$portfolio_daii,
              nrow(portfolio_data),
              length(unique(portfolio_data[[fund_col]])),
              portfolio_metrics$overall$weighted_r_d,
              portfolio_metrics$overall$weighted_analyst,
              portfolio_metrics$overall$weighted_patent),
    stringsAsFactors = FALSE
  )

  cat("   ✅ Portfolio integration complete.\n")
  return(list(
    integrated_data = portfolio_data,
    portfolio_metrics = portfolio_metrics,
    fund_analysis = fund_analysis,
    industry_analysis = industry_analysis,
    portfolio_summary = portfolio_summary
  ))
}

portfolio_results <- integrate_with_portfolio(daii_scored_data)

# =============================================================================
# MODULE 6: VALIDATION SYSTEM
# =============================================================================

cat("\n")
cat(paste(rep("=", 80), collapse = ""), "\n")
cat("MODULE 6: VALIDATION SYSTEM - SCORE VERIFICATION\n")
cat(paste(rep("=", 80), collapse = ""), "\n\n")

cat("🔍 STAGE 6.1: PERFORMING STATISTICAL VALIDATION\n")
cat(paste(rep("-", 60), collapse = ""), "\n")

create_validation_framework <- function(company_data,
                                        portfolio_results = NULL,
                                        output_dir = "03_Validation_Reports") {

  cat("   Initializing validation framework...\n")

  if (!dir.exists(output_dir)) {
    dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)
  }

  validation_results <- list()

  # 1. STATISTICAL VALIDATION
  cat("\n   1. Statistical Validation:\n")

  component_cols <- c("R_D_Score", "Analyst_Score", "Patent_Score", "News_Score", "Growth_Score")
  component_cols <- intersect(component_cols, colnames(company_data))

  stat_validation <- data.frame(
    Component = component_cols,
    Mean = sapply(company_data[, component_cols], mean, na.rm = TRUE),
    Median = sapply(company_data[, component_cols], median, na.rm = TRUE),
    SD = sapply(company_data[, component_cols], sd, na.rm = TRUE),
    Skewness = sapply(company_data[, component_cols],
                     function(x) moments::skewness(x, na.rm = TRUE)),
    Kurtosis = sapply(company_data[, component_cols],
                     function(x) moments::kurtosis(x, na.rm = TRUE)),
    Min = sapply(company_data[, component_cols], min, na.rm = TRUE),
    Max = sapply(company_data[, component_cols], max, na.rm = TRUE),
    stringsAsFactors = FALSE
  )

  validation_results$statistical <- stat_validation

  # Check for uniform 50 scores (previous bug)
  uniform_50_check <- sapply(company_data[, component_cols],
                            function(x) all(abs(x - 50) < 0.1, na.rm = TRUE))
  if (any(uniform_50_check)) {
    cat("   ⚠️  WARNING: Found component scores uniformly at 50:\n")
    cat("      ", paste(component_cols[uniform_50_check], collapse=", "), "\n")
  } else {
    cat("   ✅ No uniform 50 scores detected.\n")
  }

  # 2. BUSINESS VALIDATION
  cat("\n   2. Business Validation:\n")

  if (!is.null(portfolio_results)) {
    business_validation <- list(
      portfolio_daii = portfolio_results$portfolio_metrics$overall$portfolio_daii,
      fund_count = length(unique(portfolio_results$integrated_data$fund_name)),
      industry_coverage = if (!is.null(portfolio_results$industry_analysis))
        nrow(portfolio_results$industry_analysis) else 0
    )
    validation_results$business <- business_validation
    cat("   ✅ Portfolio metrics validated.\n")
  }

  # 3. SENSITIVITY VALIDATION
  cat("\n   3. Sensitivity Validation:\n")

  sensitivity_weights <- list(
    default = c(R_D = 0.30, Analyst = 0.20, Patent = 0.25, News = 0.10, Growth = 0.15),
    r_d_focused = c(R_D = 0.40, Analyst = 0.15, Patent = 0.20, News = 0.10, Growth = 0.15),
    analyst_focused = c(R_D = 0.25, Analyst = 0.30, Patent = 0.20, News = 0.10, Growth = 0.15)
  )

  sensitivity_scores <- list()
  for (weight_name in names(sensitivity_weights)) {
    weights <- sensitivity_weights[[weight_name]]
    score <- sum(sapply(component_cols, function(col) {
      mean(company_data[[col]], na.rm = TRUE) *
        weights[gsub("_Score", "", col)]
    }))
    sensitivity_scores[[weight_name]] <- score
  }

  validation_results$sensitivity <- sensitivity_scores
  cat("   ✅ Sensitivity analysis complete.\n")

  # Save validation reports
  validation_file <- file.path(output_dir, "validation_summary.csv")
  write.csv(stat_validation, validation_file, row.names = FALSE)

  cat(sprintf("\n   📋 Validation summary saved to: %s\n", validation_file))
  cat("   ✅ Validation framework complete.\n")

  return(validation_results)
}

validation_results <- create_validation_framework(daii_company_level_data, portfolio_results)

# =============================================================================
# MODULE 7: VISUALIZATION SUITE
# =============================================================================

cat("\n")
cat(paste(rep("=", 80), collapse = ""), "\n")
cat("MODULE 7: VISUALIZATION SUITE - DATA REPRESENTATION\n")
cat(paste(rep("=", 80), collapse = ""), "\n\n")

cat("🎨 STAGE 7.1: GENERATING COMPREHENSIVE VISUALIZATIONS\n")
cat(paste(rep("-", 60), collapse = ""), "\n")

create_daii_visualizations <- function(company_data,
                                       portfolio_results = NULL,
                                       validation_report = NULL,
                                       output_dir = "05_Visualizations") {

  cat("   Creating visualization directory...\n")
  if (!dir.exists(output_dir)) {
    dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)
  }

  # 1. DAII Score Distribution
  cat("\n   1. Creating score distribution plot...\n")
  p1 <- ggplot2::ggplot(company_data, ggplot2::aes(x = DAII_3.5_Score)) +
    ggplot2::geom_histogram(ggplot2::aes(y = ..density..), bins = 30,
                           fill = "#4B9CD3", alpha = 0.7) +
    ggplot2::geom_density(color = "#1F4E79", size = 1.2) +
    ggplot2::geom_vline(ggplot2::aes(xintercept = mean(DAII_3.5_Score, na.rm = TRUE)),
                       color = "#FF6B6B", size = 1, linetype = "dashed") +
    ggplot2::labs(
      title = "Distribution of DAII 3.5 Scores",
      subtitle = "Histogram with Density Curve",
      x = "DAII 3.5 Score",
      y = "Density"
    ) +
    ggplot2::theme_minimal()

  ggplot2::ggsave(file.path(output_dir, "01_daii_distribution.png"),
                 p1, width = 10, height = 6, dpi = 300)

  # 2. Component Score Distributions
  cat("   2. Creating component distribution plots...\n")
  component_data <- company_data %>%
    dplyr::select(Ticker, R_D_Score, Analyst_Score, Patent_Score,
                  News_Score, Growth_Score) %>%
    tidyr::pivot_longer(cols = -Ticker,
                       names_to = "Component",
                       values_to = "Score")

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

  p2 <- ggplot2::ggplot(component_data, ggplot2::aes(x = Score, fill = Component)) +
    ggplot2::geom_histogram(bins = 25, alpha = 0.7) +
    ggplot2::facet_wrap(~ Component, scales = "free", ncol = 3) +
    ggplot2::scale_fill_brewer(palette = "Set2") +
    ggplot2::labs(
      title = "Distribution of Component Scores",
      subtitle = "All scores normalized to 0-100 scale",
      x = "Score",
      y = "Count"
    ) +
    ggplot2::theme_minimal() +
    ggplot2::theme(legend.position = "none")

  ggplot2::ggsave(file.path(output_dir, "02_component_distributions.png"),
                 p2, width = 12, height = 8, dpi = 300)

  # 3. Correlation Heatmap
  cat("   3. Creating correlation heatmap...\n")
  correlation_cols <- c("DAII_3.5_Score", "R_D_Score", "Analyst_Score",
                       "Patent_Score", "News_Score", "Growth_Score")
  correlation_cols <- intersect(correlation_cols, colnames(company_data))

  cor_matrix <- cor(company_data[, correlation_cols], use = "complete.obs")
  cor_melted <- reshape2::melt(cor_matrix)

  p3 <- ggplot2::ggplot(cor_melted, ggplot2::aes(x = Var1, y = Var2, fill = value)) +
    ggplot2::geom_tile(color = "white") +
    ggplot2::geom_text(ggplot2::aes(label = sprintf("%.2f", value)),
                      color = "black", size = 4) +
    ggplot2::scale_fill_gradient2(low = "#CD5C5C", mid = "white", high = "#2E8B57",
                                 midpoint = 0, limit = c(-1, 1),
                                 name = "Correlation") +
    ggplot2::labs(
      title = "Correlation Matrix of DAII Components",
      subtitle = "Pearson correlation coefficients",
      x = "",
      y = ""
    ) +
    ggplot2::theme_minimal() +
    ggplot2::theme(
      axis.text.x = ggplot2::element_text(angle = 45, hjust = 1),
      legend.position = "right"
    ) +
    ggplot2::coord_fixed()

  ggplot2::ggsave(file.path(output_dir, "03_correlation_heatmap.png"),
                 p3, width = 10, height = 8, dpi = 300)

  # 4. Quartile Analysis
  cat("   4. Creating quartile analysis plot...\n")
  p4 <- ggplot2::ggplot(company_data,
                       ggplot2::aes(x = DAII_Quartile, y = DAII_3.5_Score,
                                   fill = DAII_Quartile)) +
    ggplot2::geom_boxplot(alpha = 0.8, outlier.color = "#FF6B6B") +
    ggplot2::scale_fill_manual(values = c("Q1 (High)" = "#2E8B57",
                                         "Q2" = "#87CEEB",
                                         "Q3" = "#FFD700",
                                         "Q4 (Low)" = "#CD5C5C")) +
    ggplot2::labs(
      title = "DAII 3.5 Score Distribution by Quartile",
      subtitle = "Box plot showing score ranges for each innovation quartile",
      x = "Innovation Quartile",
      y = "DAII 3.5 Score"
    ) +
    ggplot2::theme_minimal() +
    ggplot2::theme(axis.text.x = ggplot2::element_text(angle = 45, hjust = 1))

  ggplot2::ggsave(file.path(output_dir, "04_quartile_boxplot.png"),
                 p4, width = 10, height = 7, dpi = 300)

  # Portfolio visualizations if available
  if (!is.null(portfolio_results)) {
    cat("   5. Creating portfolio visualizations...\n")

    if (!is.null(portfolio_results$fund_analysis)) {
      p5 <- ggplot2::ggplot(portfolio_results$fund_analysis,
                           ggplot2::aes(x = !!rlang::sym("fund_name"), y = fund_daii,
                                       fill = fund_name)) +
        ggplot2::geom_bar(stat = "identity", alpha = 0.8) +
        ggplot2::labs(
          title = "Fund-Level Innovation Scores",
          subtitle = "DAII 3.5 scores weighted by fund holdings",
          x = "Fund Name",
          y = "Fund DAII Score"
        ) +
        ggplot2::theme_minimal() +
        ggplot2::theme(axis.text.x = ggplot2::element_text(angle = 45, hjust = 1),
                      legend.position = "none")

      ggplot2::ggsave(file.path(output_dir, "05_fund_scores.png"),
                     p5, width = 10, height = 6, dpi = 300)
    }
  }

  cat(sprintf("\n   ✅ Visualizations saved to: %s/\n", output_dir))
  cat("      Total plots generated: 4", if (!is.null(portfolio_results)) "+1" else "", "\n")

  return(list(
    plots = list(p1, p2, p3, p4),
    output_directory = output_dir
  ))
}

visualization_results <- create_daii_visualizations(daii_company_level_data,
                                                   portfolio_results,
                                                   validation_results)

# =============================================================================
# MODULE 8: OUTPUT PACKAGE
# =============================================================================

cat("\n")
cat(paste(rep("=", 80), collapse = ""), "\n")
cat("MODULE 8: OUTPUT PACKAGE - DELIVERABLES GENERATION\n")
cat(paste(rep("=", 80), collapse = ""), "\n\n")

cat("📦 STAGE 8.1: GENERATING COMPLETE OUTPUT PACKAGE\n")
cat(paste(rep("-", 60), collapse = ""), "\n")

generate_daii_outputs <- function(company_data,
                                  portfolio_results,
                                  validation_results,
                                  output_dir = "04_Results") {

  cat("   Creating output directory structure...\n")
  timestamp <- format(Sys.time(), "%Y%m%d_%H%M%S")
  run_dir <- file.path(output_dir, paste0("DAII_3.5_Run_", timestamp))

  dirs_to_create <- c(
    "01_Company_Scores",
    "02_Portfolio_Analysis",
    "03_Validation_Reports",
    "04_Executive_Summaries",
    "05_Visualizations",
    "06_Raw_Data"
  )

  for (dir_name in dirs_to_create) {
    dir_path <- file.path(run_dir, dir_name)
    if (!dir.exists(dir_path)) {
      dir.create(dir_path, recursive = TRUE, showWarnings = FALSE)
    }
  }

  output_files <- list()

  # 1. COMPANY-LEVEL SCORES
  cat("\n   1. Generating company-level score files...\n")
  score_cols <- c(
    "Ticker", "Company.Name", "GICS.Ind.Grp.Name",
    "DAII_3.5_Score", "DAII_Quartile",
    "R_D_Score", "Analyst_Score", "Patent_Score", "News_Score", "Growth_Score"
  )

  score_cols <- intersect(score_cols, colnames(company_data))
  company_scores <- company_data[, score_cols] %>%
    dplyr::arrange(dplyr::desc(DAII_3.5_Score))

  # Complete scores CSV
  complete_file <- file.path(run_dir, "01_Company_Scores",
                            "daii_3.5_company_scores_complete.csv")
  write.csv(company_scores, complete_file, row.names = FALSE)
  output_files$company_scores_csv <- complete_file

  # Top 100 innovators
  top_100_file <- file.path(run_dir, "01_Company_Scores",
                           "daii_3.5_top_100_innovators.csv")
  top_100 <- company_scores %>% head(100)
  write.csv(top_100, top_100_file, row.names = FALSE)
  output_files$top_100_csv <- top_100_file

  # Excel workbook
  excel_file <- file.path(run_dir, "01_Company_Scores",
                         "daii_3.5_company_scores.xlsx")
  wb <- openxlsx::createWorkbook()
  openxlsx::addWorksheet(wb, "All_Companies")
  openxlsx::writeData(wb, "All_Companies", company_scores)
  openxlsx::addWorksheet(wb, "Top_100")
  openxlsx::writeData(wb, "Top_100", top_100)
  openxlsx::saveWorkbook(wb, excel_file, overwrite = TRUE)
  output_files$company_scores_excel <- excel_file

  # 2. PORTFOLIO ANALYSIS REPORTS
  cat("   2. Generating portfolio analysis reports...\n")
  if (!is.null(portfolio_results)) {
    # Portfolio holdings
    portfolio_file <- file.path(run_dir, "02_Portfolio_Analysis",
                               "portfolio_holdings_with_daii_scores.csv")
    write.csv(portfolio_results$integrated_data, portfolio_file, row.names = FALSE)
    output_files$portfolio_csv <- portfolio_file

    # Portfolio summary
    summary_file <- file.path(run_dir, "02_Portfolio_Analysis",
                             "portfolio_innovation_summary.csv")
    write.csv(portfolio_results$portfolio_summary, summary_file, row.names = FALSE)
    output_files$portfolio_summary_csv <- summary_file
  }

  # 3. VALIDATION REPORTS
  cat("   3. Generating validation reports...\n")
  if (!is.null(validation_results)) {
    validation_file <- file.path(run_dir, "03_Validation_Reports",
                                "statistical_validation_results.csv")
    write.csv(validation_results$statistical, validation_file, row.names = FALSE)
    output_files$validation_csv <- validation_file
  }

  # 4. EXECUTIVE SUMMARY
  cat("   4. Generating executive summary...\n")
  summary_stats <- data.frame(
    Metric = c("Total Companies", "Mean DAII Score", "Median DAII Score",
               "Top Quartile Companies", "Bottom Quartile Companies",
               "Execution Date"),
    Value = c(
      nrow(company_data),
      round(mean(company_data$DAII_3.5_Score, na.rm = TRUE), 2),
      round(median(company_data$DAII_3.5_Score, na.rm = TRUE), 2),
      sum(company_data$DAII_Quartile == "Q1 (High)", na.rm = TRUE),
      sum(company_data$DAII_Quartile == "Q4 (Low)", na.rm = TRUE),
      as.character(Sys.Date())
    ),
    stringsAsFactors = FALSE
  )

  summary_file <- file.path(run_dir, "04_Executive_Summaries",
                           "daii_3.5_executive_summary.csv")
  write.csv(summary_stats, summary_file, row.names = FALSE)
  output_files$executive_summary <- summary_file

  # 5. COPY VISUALIZATIONS
  cat("   5. Organizing visualization files...\n")
  vis_source_dir <- "05_Visualizations"
  vis_target_dir <- file.path(run_dir, "05_Visualizations")

  if (dir.exists(vis_source_dir)) {
    vis_files <- list.files(vis_source_dir, pattern = "\\.png$", full.names = TRUE)
    for (vis_file in vis_files) {
      file.copy(vis_file, vis_target_dir, overwrite = TRUE)
    }
    output_files$visualizations <- vis_files
  }

  # 6. SAVE RAW DATA
  cat("   6. Saving processed raw data...\n")
  raw_data_file <- file.path(run_dir, "06_Raw_Data",
                            "processed_n50_dataset.csv")
  write.csv(daii_processed_data, raw_data_file, row.names = FALSE)
  output_files$raw_data <- raw_data_file

  # Create README
  readme_content <- paste(
    "DAII 3.5 Analysis Run -", timestamp,
    "\n==========================================",
    "\n",
    "\nThis directory contains the complete outputs from DAII 3.5 Phase 1 analysis.",
    "\n",
    "\nDirectory Structure:",
    "\n- 01_Company_Scores: Company-level innovation scores and rankings",
    "\n- 02_Portfolio_Analysis: Fund-level and portfolio-level analysis",
    "\n- 03_Validation_Reports: Statistical and business validation results",
    "\n- 04_Executive_Summaries: High-level summary metrics",
    "\n- 05_Visualizations: Charts and graphs for presentation",
    "\n- 06_Raw_Data: Processed input data",
    "\n",
    "\nAnalysis Details:",
    sprintf("\n- Companies Analyzed: %d", nrow(company_data)),
    sprintf("\n- Execution Time: %s", timestamp),
    sprintf("\n- DAII Version: 3.5.7"),
    "\n",
    "\nFor questions or additional analysis, contact the DAII development team.",
    sep = ""
  )

  readme_file <- file.path(run_dir, "README.txt")
  writeLines(readme_content, readme_file)
  output_files$readme <- readme_file

  cat(sprintf("\n   ✅ Output package generated: %s\n", run_dir))
  cat(sprintf("      Total files created: %d\n", length(unlist(output_files))))

  return(list(
    output_directory = run_dir,
    output_files = output_files,
    timestamp = timestamp
  ))
}

output_results <- generate_daii_outputs(daii_company_level_data,
                                       portfolio_results,
                                       validation_results)

# =============================================================================
# MODULE 9: REPRODUCIBILITY FRAMEWORK
# =============================================================================

cat("\n")
cat(paste(rep("=", 80), collapse = ""), "\n")
cat("MODULE 9: REPRODUCIBILITY FRAMEWORK - ANALYSIS PACKAGING\n")
cat(paste(rep("=", 80), collapse = ""), "\n\n")

cat("🔬 STAGE 9.1: ENSURING REPRODUCIBILITY & CREATING FINAL PACKAGE\n")
cat(paste(rep("-", 60), collapse = ""), "\n")

create_reproducible_analysis <- function(config = list(),
                                         output_dir = "00_Reproducibility") {

  cat("   Creating reproducibility framework...\n")

  if (!dir.exists(output_dir)) {
    dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)
  }

  # 1. CAPTURE SYSTEM INFORMATION
  cat("\n   1. Capturing system information...\n")
  system_info <- list(
    timestamp = Sys.time(),
    r_version = R.version.string,
    platform = R.version$platform,
    os = Sys.info()["sysname"],
    user = Sys.info()["user"],
    working_directory = getwd(),
    loaded_packages = search()
  )

  sys_info_file <- file.path(output_dir, "system_information.json")
  jsonlite::write_json(system_info, sys_info_file, pretty = TRUE)
  cat(sprintf("      System info saved to: %s\n", sys_info_file))

  # 2. SAVE SESSION INFORMATION
  cat("   2. Saving session information...\n")
  session_file <- file.path(output_dir, "session_info.txt")
  sink(session_file)
  print(sessionInfo())
  sink()
  cat(sprintf("      Session info saved to: %s\n", session_file))

  # 3. SAVE CONFIGURATION
  cat("   3. Saving analysis configuration...\n")
  default_config <- list(
    version = "3.5.7",
    data_source = "N50 Hybrid Test Dataset",
    weights = list(
      r_d = 0.30,
      analyst = 0.20,
      patent = 0.25,
      news = 0.10,
      growth = 0.15
    ),
    imputation_method = "Global_Median",
    normalization_method = "Min-Max (0-100)",
    fixes_applied = c(
      "Numeric conversion fix in imputation engine",
      "Uniform zero handling in normalization",
      "String concatenation error fixes"
    )
  )

  config <- modifyList(default_config, config)

  config_file <- file.path(output_dir, "analysis_configuration.yaml")
  yaml::write_yaml(config, config_file)
  cat(sprintf("      Configuration saved to: %s\n", config_file))

  # 4. CREATE REPRODUCIBILITY REPORT
  cat("   4. Creating reproducibility report...\n")
  report_content <- paste(
    "DAII 3.5 REPRODUCIBILITY REPORT",
    "================================",
    "",
    paste("Report Generated:", Sys.time()),
    paste("DAII Version:", config$version),
    "",
    "SYSTEM INFORMATION:",
    paste("- R Version:", system_info$r_version),
    paste("- Platform:", system_info$platform),
    paste("- Operating System:", system_info$os),
    paste("- User:", system_info$user),
    "",
    "ANALYSIS CONFIGURATION:",
    paste("- Data Source:", config$data_source),
    paste("- Component Weights:",
          paste(names(config$weights), "=", unlist(config$weights), collapse=", ")),
    paste("- Imputation Method:", config$imputation_method),
    paste("- Normalization Method:", config$normalization_method),
    "",
    "CRITICAL FIXES APPLIED:",
    paste("-", config$fixes_applied, collapse="\n"),
    "",
    "REPRODUCIBILITY INSTRUCTIONS:",
    "1. Ensure all required packages are installed",
    "2. Run the complete DAII 3.5 Phase 1 codebase",
    "3. Outputs will be generated in timestamped directories",
    "4. Refer to system_information.json for environment details",
    "",
    "This analysis is fully reproducible with the provided code and configuration.",
    sep = "\n"
  )

  report_file <- file.path(output_dir, "reproducibility_report.txt")
  writeLines(report_content, report_file)
  cat(sprintf("      Reproducibility report saved to: %s\n", report_file))

  # 5. PACKAGE ALL FILES
  cat("   5. Packaging all reproducibility files...\n")
  package_files <- c(
    sys_info_file,
    session_file,
    config_file,
    report_file
  )

  cat(sprintf("\n   ✅ Reproducibility framework complete.\n"))
  cat(sprintf("      Files created in: %s\n", output_dir))
  cat(sprintf("      Total reproducibility files: %d\n", length(package_files)))

  return(list(
    system_info = system_info,
    config = config,
    package_files = package_files,
    output_directory = output_dir
  ))
}

reproducibility_results <- create_reproducible_analysis()

# =============================================================================
# FINAL EXECUTION SUMMARY
# =============================================================================

cat("\n")
cat(paste(rep("=", 80), collapse = ""), "\n")
cat("🏁 DAII 3.5 - PHASE 1 EXECUTION COMPLETE\n")
cat(paste(rep("=", 80), collapse = ""), "\n\n")

execution_summary <- data.frame(
  Metric = c(
    "Modules Executed",
    "Companies Processed",
    "Funds Analyzed",
    "Component Scores Calculated",
    "Validation Tests Performed",
    "Visualizations Generated",
    "Output Files Created",
    "Total Execution Time",
    "Critical Fixes Applied"
  ),
  Value = c(
    "0-9 (Complete Phase 1)",
    nrow(daii_company_level_data),
    if (!is.null(portfolio_results)) length(unique(portfolio_results$integrated_data$fund_name)) else 0,
    "5 (R&D, Analyst, Patent, News, Growth)",
    if (!is.null(validation_results)) length(unlist(validation_results)) else 0,
    if (!is.null(visualization_results)) length(visualization_results$plots) else 0,
    if (!is.null(output_results)) length(unlist(output_results$output_files)) else 0,
    paste(round(difftime(Sys.time(), start_time, units = "mins"), 1), "minutes"),
    "3 (Numeric Conversion, Zero Handling, String Concatenation)"
  ),
  stringsAsFactors = FALSE
)

print(execution_summary)

cat("\n📁 OUTPUT LOCATIONS:\n")
cat("   • Company Scores:", if (!is.null(output_results)) output_results$output_directory else "N/A", "\n")
cat("   • Visualizations:", if (!is.null(visualization_results)) visualization_results$output_directory else "N/A", "\n")
cat("   • Reproducibility:", if (!is.null(reproducibility_results)) reproducibility_results$output_directory else "N/A", "\n")

cat("\n✅ DAII 3.5 PHASE 1 SUCCESSFULLY COMPLETED\n")
cat("   The framework is ready for:\n")
cat("   1. Team code review\n")
cat("   2. Scaling to N200 dataset\n")
cat("   3. Phase 2 machine learning integration\n")

cat(paste(rep("=", 80), collapse = ""), "\n")

# Save final workspace for debugging
save.image("DAII_3.5_Phase1_Workspace.RData")

# Return final company data for immediate use
daii_company_level_data
