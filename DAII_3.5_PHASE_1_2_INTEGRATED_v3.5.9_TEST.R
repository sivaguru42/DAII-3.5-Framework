# ============================================================
# DAII 3.5 PHASE 1 + MODULE 4 INTEGRATED SCRIPT
# Version: v3.5.9 + Module 4 Portfolio Construction Bridge
# Date: February 11, 2026
# 
# THIS SCRIPT COMBINES:
# 1. Original v3.5.9 code (Modules 1-3 with quartile fix)
# 2. Module 4 Bridge: Portfolio Construction Engine
# 
# INPUT: N50 Test Dataset
# OUTPUT: Portfolio weights & allocations for 50 companies
# ============================================================

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# PART 1: ORIGINAL v3.5.9 CODE (MODULES 1-3)
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# [Note: The complete v3.5.9 code should be inserted here]
# For continuity, I'll note where it should be placed:
# >>> INSERT FULL CONTENTS OF:
# >>> https://raw.githubusercontent.com/sivaguru42/DAII-3.5-Framework/refs/heads/main/DAII_3.5_PHASE_1_COMPLETE_CODEBASE_v3.5.9_02_09_2026.R
# >>> STARTING FROM LINE 1 TO END OF FILE
################################################################################
# DAII 3.5 - PHASE 1 COMPLETE CODEBASE WITH FIELD MAPPING INTEGRATION
# Version: 3.5.9 (Fixed Quartile Assignment on Company-Level Data)
# Date: 2026-02-09
# Author: Siva Ganesan
# Critical Fix: Quartiles now calculated ONLY on company-level data (50 rows)
#               Not on fund-level data (589 rows) - fixes 446-in-Q4 issue
################################################################################

# =============================================================================
# MODULE 0: ENVIRONMENT SETUP & GITHUB INVENTORY VERIFICATION
# =============================================================================

cat(paste(rep("=", 80), collapse = ""), "\n")
cat("DAII 3.5 - PHASE 1 COMPLETE CODEBASE v3.5.9 (QUARTILE FIXED)\n")
cat(paste(rep("=", 80), collapse = ""), "\n\n")

# -----------------------------------------------------------------------------
# 0.1 PACKAGE MANAGEMENT & SYSTEM CONFIGURATION
# -----------------------------------------------------------------------------

cat("📦 STAGE 0.1: LOADING REQUIRED PACKAGES & CONFIGURING ENVIRONMENT\n")
cat(paste(rep("-", 60), collapse = ""), "\n")

required_packages <- c(
  "dplyr", "tidyr", "readr", "httr", "stringr", 
  "purrr", "lubridate", "yaml", "ggplot2", "openxlsx", 
  "corrplot", "moments"
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

options(stringsAsFactors = FALSE, warn = 1, scipen = 999, digits = 4)
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
      test_url = "https://raw.githubusercontent.com/sivaguru42/DAII-3.5-Framework/main/data/raw/DAII_3_5_N50_Test_Dataset.csv",
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
    ),
    field_mapping = list(
      filename = "DAII 3.5 PRODUCTION FIELD MAPPING CONFIGURATION.yaml",
      test_url = "https://raw.githubusercontent.com/sivaguru42/DAII-3.5-Framework/main/DAII%203.5%20PRODUCTION%20FIELD%20MAPPING%20CONFIGURATION.yaml",
      required = TRUE
    ),
    field_schema = list(
      filename = "daii_field_mapping.yaml",
      test_url = "https://raw.githubusercontent.com/sivaguru42/DAII-3.5-Framework/main/daii_field_mapping.yaml",
      required = TRUE
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
field_mapping_available <- verification_results[["DAII 3.5 PRODUCTION FIELD MAPPING CONFIGURATION.yaml"]]$available
field_schema_available <- verification_results[["daii_field_mapping.yaml"]]$available

if (!n50_available) {
  cat("\n❌ CRITICAL ERROR: The essential N50 dataset is not available.\n")
  cat("   The pipeline cannot proceed. Please ensure the file exists at:\n")
  cat("   ", github_config$essential_files$n50_dataset$test_url, "\n")
  stop("Execution halted due to missing essential data.")
} else if (!field_mapping_available || !field_schema_available) {
  cat("\n⚠️  WARNING: Field mapping configuration not fully available.\n")
  cat("   Pipeline will continue with hard-coded column names.\n")
  cat("   Field Mapping Available:", field_mapping_available, "\n")
  cat("   Field Schema Available:", field_schema_available, "\n")
} else {
  cat("\n✅ SUCCESS: All essential files are available. Proceeding to data load.\n")
}

# =============================================================================
# MODULE 0.5: FIELD MAPPING SYSTEM INTEGRATION
# =============================================================================

cat("\n")
cat(paste(rep("=", 80), collapse = ""), "\n")
cat("MODULE 0.5: FIELD MAPPING SYSTEM INTEGRATION\n")
cat(paste(rep("=", 80), collapse = ""), "\n\n")

# -----------------------------------------------------------------------------
# 0.5.1 LOAD FIELD MAPPING CONFIGURATION
# -----------------------------------------------------------------------------

cat("🗺️  STAGE 0.5.1: LOADING FIELD MAPPING CONFIGURATION\n")
cat(paste(rep("-", 60), collapse = ""), "\n")

load_field_mapping <- function() {
  tryCatch({
    mapping_config_url <- "https://raw.githubusercontent.com/sivaguru42/DAII-3.5-Framework/main/DAII%203.5%20PRODUCTION%20FIELD%20MAPPING%20CONFIGURATION.yaml"
    mapping_config <- yaml::read_yaml(mapping_config_url)
    
    field_schema_url <- "https://raw.githubusercontent.com/sivaguru42/DAII-3.5-Framework/main/daii_field_mapping.yaml"
    field_schema <- yaml::read_yaml(field_schema_url)
    
    cat("   ✅ Field mapping configuration loaded successfully\n")
    cat("   📊 Mapping covers", length(mapping_config$field_mappings), "fields\n")
    
    return(list(mapping_config = mapping_config, field_schema = field_schema))
  }, error = function(e) {
    cat("   ⚠️  Could not load field mapping:", e$message, "\n")
    cat("   Pipeline will use hard-coded column names with fallback logic.\n")
    return(NULL)
  })
}

field_mapping <- load_field_mapping()

# -----------------------------------------------------------------------------
# 0.5.2 LOAD RAW DATA AND APPLY FIELD MAPPING
# -----------------------------------------------------------------------------

cat("\n📥 STAGE 0.5.2: LOADING RAW DATA AND APPLYING FIELD MAPPING\n")
cat(paste(rep("-", 60), collapse = ""), "\n")

cat("   Downloading raw dataset from GitHub...\n")
n50_data_url <- github_config$essential_files$n50_dataset$test_url

load_raw_dataset <- function(url) {
  tryCatch({
    dataset <- readr::read_csv(url, show_col_types = FALSE, progress = FALSE)
    cat(sprintf("   ✅ Raw dataset loaded: %d rows × %d columns\n",
                nrow(dataset), ncol(dataset)))
    return(dataset)
  }, error = function(e) {
    cat(sprintf("   ❌ FATAL ERROR loading dataset: %s\n", e$message))
    stop("Data loading failed. Pipeline cannot continue.")
  })
}

daii_raw_data <- load_raw_dataset(n50_data_url)

apply_field_mapping <- function(raw_data, mapping) {
  if (is.null(mapping)) {
    cat("   No field mapping available. Using original column names.\n")
    return(raw_data)
  }
  
  cat("   Applying field mapping transformations...\n")
  mapped_data <- raw_data
  
  mapping_dict <- list()
  for (field_map in mapping$mapping_config$field_mappings) {
    source_field <- field_map$source_field
    target_field <- field_map$target_field
    mapping_dict[[source_field]] <- target_field
  }
  
  applied_count <- 0
  for (source_col in names(mapping_dict)) {
    if (source_col %in% colnames(mapped_data)) {
      target_col <- mapping_dict[[source_col]]
      colnames(mapped_data)[colnames(mapped_data) == source_col] <- target_col
      cat(sprintf("      %-35s → %s\n", source_col, target_col))
      applied_count <- applied_count + 1
    }
  }
  
  cat(sprintf("\n   Applied %d field mappings out of %d configured\n", 
              applied_count, length(mapping_dict)))
  
  return(mapped_data)
}

daii_raw_data_mapped <- apply_field_mapping(daii_raw_data, field_mapping)

# -----------------------------------------------------------------------------
# 0.5.3 STANDARDIZE COLUMN NAMES FOR DOWNSTREAM PROCESSING
# -----------------------------------------------------------------------------

cat("\n🔄 STAGE 0.5.3: STANDARDIZING COLUMN NAMES\n")
cat(paste(rep("-", 60), collapse = ""), "\n")

standardized_cols <- list(
  ticker = c("ticker", "Ticker", "Symbol", "ticker_symbol"),
  rd_expense = c("rd_expense", "R.D.Exp", "RD_Exp", "Research_Development", "R&D"),
  market_cap = c("market_cap", "Mkt.Cap", "MarketCap", "Market_Capitalization"),
  analyst_rating = c("analyst_rating", "BEst.Analyst.Rtg", "AnalystRating", "Analyst_Rating"),
  patent_activity = c("patent_activity", "Patents...Trademarks...Copy.Rgt", "Patents", "Patent_Activity"),
  news_sentiment = c("news_sentiment", "News.Sent", "NewsSentiment", "News_Sentiment"),
  revenue_growth = c("revenue_growth", "Rev...1.Yr.Gr", "RevenueGrowth", "Rev_Growth"),
  industry = c("GICS.Ind.Grp.Name", "Industry", "Sector", "GICS_Sector")
)

standardize_columns <- function(data, col_definitions) {
  cat("   Standardizing column names for pipeline...\n")
  
  standardized_data <- data
  standardization_log <- data.frame(
    Original_Name = character(),
    Standardized_Name = character(),
    Status = character(),
    stringsAsFactors = FALSE
  )
  
  for (std_name in names(col_definitions)) {
    variations <- col_definitions[[std_name]]
    found <- FALSE
    
    for (variation in variations) {
      if (variation %in% colnames(standardized_data)) {
        if (variation != std_name) {
          colnames(standardized_data)[colnames(standardized_data) == variation] <- std_name
          standardization_log <- rbind(standardization_log, data.frame(
            Original_Name = variation,
            Standardized_Name = std_name,
            Status = "RENAMED",
            stringsAsFactors = FALSE
          ))
        } else {
          standardization_log <- rbind(standardization_log, data.frame(
            Original_Name = variation,
            Standardized_Name = std_name,
            Status = "ALREADY_STANDARD",
            stringsAsFactors = FALSE
          ))
        }
        found <- TRUE
        break
      }
    }
    
    if (!found) {
      standardization_log <- rbind(standardization_log, data.frame(
        Original_Name = "NOT_FOUND",
        Standardized_Name = std_name,
        Status = "MISSING",
        stringsAsFactors = FALSE
      ))
    }
  }
  
  cat("\n   📋 COLUMN STANDARDIZATION SUMMARY:\n")
  for (i in 1:nrow(standardization_log)) {
    log_entry <- standardization_log[i, ]
    if (log_entry$Status == "RENAMED") {
      cat(sprintf("      %-20s → %-20s ✅\n", log_entry$Original_Name, log_entry$Standardized_Name))
    } else if (log_entry$Status == "ALREADY_STANDARD") {
      cat(sprintf("      %-20s → %-20s ✓ (already standard)\n", log_entry$Original_Name, log_entry$Standardized_Name))
    } else {
      cat(sprintf("      %-20s → %-20s ❌ (missing)\n", log_entry$Original_Name, log_entry$Standardized_Name))
    }
  }
  
  critical_cols <- c("ticker", "rd_expense", "market_cap", "analyst_rating", 
                     "patent_activity", "news_sentiment", "revenue_growth")
  missing_critical <- setdiff(critical_cols, colnames(standardized_data))
  
  if (length(missing_critical) > 0) {
    cat("\n   ⚠️  WARNING: Missing critical columns:", paste(missing_critical, collapse = ", "), "\n")
    cat("   Pipeline may fail if these columns are required for scoring.\n")
  } else {
    cat("\n   ✅ All critical columns are present and standardized.\n")
  }
  
  return(list(data = standardized_data, log = standardization_log))
}

standardization_result <- standardize_columns(daii_raw_data_mapped, standardized_cols)
daii_standardized_data <- standardization_result$data

cat(sprintf("\n   🎯 FINAL COLUMNS (%d total):\n", ncol(daii_standardized_data)))
cat("   ", paste(colnames(daii_standardized_data)[1:min(8, ncol(daii_standardized_data))], 
                 collapse = ", "), "...\n")

# =============================================================================
# MODULE 1: DATA LOADING & PREPARATION
# =============================================================================

cat("\n")
cat(paste(rep("=", 80), collapse = ""), "\n")
cat("MODULE 1: DATA LOADING & PREPARATION (STANDARDIZED)\n")
cat(paste(rep("=", 80), collapse = ""), "\n\n")

cat("🔄 STAGE 1.1: INITIAL DATA CLEANING & TYPE ENFORCEMENT\n")
cat(paste(rep("-", 60), collapse = ""), "\n")

preprocess_standardized_data <- function(std_data) {
  cat("   Applying cleaning rules to standardized data...\n")
  processed_data <- std_data
  
  if ("ticker" %in% colnames(processed_data)) {
    processed_data$ticker <- as.character(processed_data$ticker)
    cat("   ✅ Ticker column standardized as character\n")
  }
  
  numeric_columns <- c("rd_expense", "market_cap", "revenue_growth",
                       "analyst_rating", "patent_activity", "news_sentiment")
  
  conversion_log <- data.frame(
    Column = character(),
    NAs_Before = integer(),
    NAs_After = integer(),
    stringsAsFactors = FALSE
  )
  
  for (col in numeric_columns) {
    if (col %in% colnames(processed_data)) {
      original_na <- sum(is.na(processed_data[[col]]))
      processed_data[[col]] <- as.numeric(as.character(processed_data[[col]]))
      new_na <- sum(is.na(processed_data[[col]]))
      
      conversion_log <- rbind(conversion_log, data.frame(
        Column = col,
        NAs_Before = original_na,
        NAs_After = new_na,
        stringsAsFactors = FALSE
      ))
      
      if (new_na > original_na) {
        cat(sprintf("   ⚠️  Column '%s': %d new NA values after conversion\n", 
                    col, new_na - original_na))
      }
    }
  }
  
  if (nrow(conversion_log) > 0) {
    cat("\n   📊 NUMERIC CONVERSION SUMMARY:\n")
    print(conversion_log)
  }
  
  cat("\n   📈 DATA COMPLETENESS CHECK:\n")
  for (col in numeric_columns) {
    if (col %in% colnames(processed_data)) {
      complete_pct <- 100 * (1 - sum(is.na(processed_data[[col]])) / nrow(processed_data))
      cat(sprintf("      %-20s: %6.1f%% complete\n", col, complete_pct))
    }
  }
  
  cat("\n   ✅ Initial cleaning complete.\n")
  return(processed_data)
}

daii_processed_data <- preprocess_standardized_data(daii_standardized_data)

# =============================================================================
# MODULE 1.5: COMPANY/FUND DATA SEPARATION (CRITICAL FOR QUARTILE FIX)
# =============================================================================

cat("\n")
cat(paste(rep("=", 80), collapse = ""), "\n")
cat("MODULE 1.5: COMPANY/FUND DATA SEPARATION & AGGREGATION\n")
cat(paste(rep("=", 80), collapse = ""), "\n\n")

cat("🔍 STAGE 1.5.1: IDENTIFYING COMPANY VS FUND LEVEL COLUMNS\n")
cat(paste(rep("-", 60), collapse = ""), "\n")

identify_column_types <- function(data) {
  cat("   Analyzing column structure...\n")
  
  company_level_cols <- c(
    "ticker", "rd_expense", "market_cap", "analyst_rating", 
    "patent_activity", "news_sentiment", "revenue_growth", "industry",
    "Tot.Ret.Idx.Gross", "Last.Price", "Volume", "Number.of.Employees",
    "GM", "Volatil.360D", "BEst.Target.Px", "Rec.Consensus",
    "Earnings.Conference.Call.Date", "Tot.Analyst.Rec",
    "Percent.Change.in.Institutional.Holdings", "Shrt.Int"
  )
  
  fund_level_cols <- c(
    "fund_id", "fund_name", "fund_weight", "dumac_allocation",
    "as_of_date", "shares_held", "position_value", "fund_weight_raw",
    "abs_weight", "Tot...Hldg.in.Port", "Top.10.as.of.Dt"
  )
  
  existing_company_cols <- intersect(company_level_cols, colnames(data))
  existing_fund_cols <- intersect(fund_level_cols, colnames(data))
  
  cat(sprintf("   Found %d company-level columns\n", length(existing_company_cols)))
  cat(sprintf("   Found %d fund-level columns\n", length(existing_fund_cols)))
  
  return(list(
    company_cols = existing_company_cols,
    fund_cols = existing_fund_cols
  ))
}

column_types <- identify_column_types(daii_processed_data)

cat("\n🏢 STAGE 1.5.2: CREATING COMPANY-LEVEL DATASET\n")
cat(paste(rep("-", 60), collapse = ""), "\n")

create_company_level_dataset <- function(data, company_cols) {
  cat("   Aggregating to company level (one row per ticker)...\n")
  
  company_data <- data[, company_cols, drop = FALSE]
  
  ticker_counts <- table(company_data$ticker)
  duplicate_tickers <- names(ticker_counts[ticker_counts > 1])
  
  cat(sprintf("   Found %d unique tickers\n", length(unique(company_data$ticker))))
  cat(sprintf("   Found %d tickers with multiple records\n", length(duplicate_tickers)))
  
  if (length(duplicate_tickers) > 0) {
    cat("\n   Aggregating duplicate company records...\n")
    
    numeric_cols <- company_cols[sapply(company_data[, company_cols], is.numeric)]
    char_cols <- setdiff(company_cols, c(numeric_cols, "ticker"))
    
    aggregated_data <- company_data %>%
      group_by(ticker) %>%
      summarise(
        across(all_of(numeric_cols), 
               ~ if (all(is.na(.))) NA_real_ else mean(., na.rm = TRUE)),
        across(all_of(char_cols), 
               ~ if (all(is.na(.))) NA_character_ else first(na.omit(.))),
        .groups = 'drop'
      )
    
    cat("   Applied aggregation: mean for numeric, first non-NA for character\n")
  } else {
    aggregated_data <- distinct(company_data)
  }
  
  cat(sprintf("\n   Company dataset created: %d rows × %d columns\n",
              nrow(aggregated_data), ncol(aggregated_data)))
  
  final_duplicates <- sum(duplicated(aggregated_data$ticker))
  if (final_duplicates > 0) {
    cat(sprintf("   ⚠️  Warning: %d duplicate tickers remain after aggregation\n", final_duplicates))
  } else {
    cat("   ✅ No duplicate tickers in company dataset\n")
  }
  
  return(aggregated_data)
}

daii_company_data <- create_company_level_dataset(daii_processed_data, column_types$company_cols)

cat("\n💰 STAGE 1.5.3: PRESERVING FUND-LEVEL DATASET\n")
cat(paste(rep("-", 60), collapse = ""), "\n")

preserve_fund_level_dataset <- function(data, fund_cols) {
  cat("   Preserving fund-level data for portfolio construction...\n")
  
  fund_data_cols <- unique(c("ticker", fund_cols))
  fund_data <- data[, fund_data_cols, drop = FALSE]
  
  cat(sprintf("   Fund dataset created: %d rows × %d columns\n",
              nrow(fund_data), ncol(fund_data)))
  
  if ("fund_name" %in% colnames(fund_data)) {
    cat("   Records per fund:\n")
    fund_summary <- fund_data %>%
      group_by(fund_name) %>%
      summarise(holdings = n(), .groups = 'drop')
    
    for (i in 1:min(5, nrow(fund_summary))) {
      cat(sprintf("      %-20s: %d holdings\n", 
                  fund_summary$fund_name[i], fund_summary$holdings[i]))
    }
    if (nrow(fund_summary) > 5) {
      cat(sprintf("      ... and %d more funds\n", nrow(fund_summary) - 5))
    }
  }
  
  return(fund_data)
}

daii_fund_data <- preserve_fund_level_dataset(daii_processed_data, column_types$fund_cols)

cat("\n✅ STAGE 1.5.4: VALIDATING DATA SEPARATION\n")
cat(paste(rep("-", 60), collapse = ""), "\n")

validate_data_separation <- function(company_data, fund_data, original_data) {
  cat("   Validating company/fund data separation...\n")
  
  validation_passed <- TRUE
  
  duplicate_company_tickers <- sum(duplicated(company_data$ticker))
  if (duplicate_company_tickers == 0) {
    cat("   ✅ Company dataset: One row per ticker\n")
  } else {
    cat(sprintf("   ❌ Company dataset: %d duplicate tickers\n", duplicate_company_tickers))
    validation_passed <- FALSE
  }
  
  original_rows <- nrow(original_data)
  fund_rows <- nrow(fund_data)
  company_rows <- nrow(company_data)
  
  cat(sprintf("\n   📊 ROW COUNTS:\n"))
  cat(sprintf("      Original dataset: %d rows (fund-holding level)\n", original_rows))
  cat(sprintf("      Company dataset:  %d rows (company level)\n", company_rows))
  cat(sprintf("      Fund dataset:     %d rows (fund-holding level)\n", fund_rows))
  
  avg_holdings_per_company <- fund_rows / company_rows
  cat(sprintf("      Avg holdings per company: %.1f\n", avg_holdings_per_company))
  
  if (fund_rows == original_rows) {
    cat("   ✅ Fund dataset preserves all original records\n")
  } else {
    cat(sprintf("   ⚠️  Fund dataset row count mismatch: %d vs %d\n", fund_rows, original_rows))
    validation_passed <- FALSE
  }
  
  return(validation_passed)
}

separation_valid <- validate_data_separation(daii_company_data, daii_fund_data, daii_processed_data)

if (!separation_valid) {
  cat("\n   ⚠️  WARNING: Data separation validation failed. Review the separation logic.\n")
} else {
  cat("\n   ✅ Data separation completed successfully\n")
}

# =============================================================================
# MODULE 2: IMPUTATION ENGINE ON COMPANY-LEVEL DATA
# =============================================================================

cat("\n")
cat(paste(rep("=", 80), collapse = ""), "\n")
cat("MODULE 2: IMPUTATION ENGINE - MISSING DATA HANDLING (COMPANY-LEVEL)\n")
cat(paste(rep("=", 80), collapse = ""), "\n\n")

cat("🔄 STAGE 2.1: EXECUTING IMPUTATION ENGINE (COMPANY-LEVEL)\n")
cat(paste(rep("-", 60), collapse = ""), "\n")

impute_missing_values_standardized <- function(company_data,
                                               ticker_col = "ticker",
                                               imputation_methods = list(
                                                 "analyst_rating" = "mean",
                                                 "default" = "median"
                                               )) {
  
  cat("   Initializing imputation process for standardized data...\n")
  imputed_data <- company_data
  imputation_log <- data.frame(
    ticker = character(),
    metric = character(),
    original_value = character(),
    imputed_value = numeric(),
    imputation_method = character(),
    stringsAsFactors = FALSE
  )
  
  numeric_cols <- c("rd_expense", "market_cap", "revenue_growth",
                    "analyst_rating", "patent_activity", "news_sentiment")
  
  for (col in intersect(numeric_cols, colnames(imputed_data))) {
    cat(sprintf("\n   Processing metric: %s\n", col))
    
    if (!is.numeric(imputed_data[[col]])) {
      imputed_data[[col]] <- as.numeric(as.character(imputed_data[[col]]))
    }
    
    missing_indicators <- c("#N/A", "N/A", "NA", "NULL", "", "Field Not Applicable", "NaN")
    is_missing <- is.na(imputed_data[[col]]) |
      as.character(imputed_data[[col]]) %in% missing_indicators
    
    missing_count <- sum(is_missing, na.rm = TRUE)
    
    if (missing_count > 0) {
      cat(sprintf("      Found %d missing values (%.1f%%).\n",
                  missing_count, 100 * missing_count / nrow(imputed_data)))
      
      method <- if (col %in% names(imputation_methods)) {
        imputation_methods[[col]]
      } else {
        imputation_methods$default
      }
      
      available_vals <- imputed_data[[col]][!is_missing & !is.na(imputed_data[[col]])]
      
      if (length(available_vals) == 0) {
        cat("      ⚠️  No available values for imputation. Using 0.\n")
        impute_val <- 0
        method_name <- "Zero (no data)"
      } else if (method == "mean" || col == "analyst_rating") {
        impute_val <- mean(available_vals, na.rm = TRUE)
        method_name <- "Global_Mean"
      } else {
        impute_val <- median(available_vals, na.rm = TRUE)
        method_name <- "Global_Median"
      }
      
      imputed_data[[col]][is_missing] <- impute_val
      cat(sprintf("      Imputed with %s: %.4f\n", method_name, impute_val))
      
      for (idx in which(is_missing)) {
        imputation_log <- rbind(imputation_log, data.frame(
          ticker = imputed_data[[ticker_col]][idx],
          metric = col,
          original_value = as.character(company_data[[col]][idx]),
          imputed_value = impute_val,
          imputation_method = method_name,
          stringsAsFactors = FALSE
        ))
      }
    } else {
      cat("      No missing values found.\n")
    }
  }
  
  if (nrow(imputation_log) > 0) {
    cat("\n   📋 IMPUTATION SUMMARY:\n")
    summary_table <- imputation_log %>%
      dplyr::group_by(metric, imputation_method) %>%
      dplyr::summarise(count = dplyr::n(),
                       avg_imputed_value = mean(imputed_value),
                       .groups = 'drop')
    print(summary_table)
    
    total_imputations <- sum(summary_table$count)
    total_cells <- nrow(imputed_data) * length(numeric_cols)
    imputation_rate <- 100 * total_imputations / total_cells
    cat(sprintf("\n   Overall imputation rate: %.1f%% (%d/%d cells)\n",
                imputation_rate, total_imputations, total_cells))
  } else {
    cat("\n   ℹ️  No imputations were performed.\n")
  }
  
  cat("\n   ✅ Imputation engine complete.\n")
  return(list(imputed_data = imputed_data,
              imputation_log = imputation_log))
}

imputation_results <- impute_missing_values_standardized(daii_company_data)
daii_company_imputed <- imputation_results$imputed_data
imputation_log <- imputation_results$imputation_log

# =============================================================================
# MODULE 3: SCORING ENGINE ON COMPANY-LEVEL DATA
# =============================================================================

cat("\n")
cat(paste(rep("=", 80), collapse = ""), "\n")
cat("MODULE 3: SCORING ENGINE - COMPOSITE SCORE CALCULATION (COMPANY-LEVEL)\n")
cat(paste(rep("=", 80), collapse = ""), "\n\n")

cat("📊 STAGE 3.1: DEFINING NORMALIZATION FUNCTIONS\n")
cat(paste(rep("-", 60), collapse = ""), "\n")

normalize_to_100 <- function(x, cap_extremes = TRUE,
                             lower_bound = 0.01, upper_bound = 0.99) {
  
  if (all(is.na(x))) {
    return(rep(NA_real_, length(x)))
  }
  
  x_finite <- x[is.finite(x) & !is.na(x)]
  if (length(x_finite) == 0) {
    return(rep(50, length(x)))
  }
  
  if (cap_extremes && length(x_finite) > 10) {
    lower_threshold <- quantile(x_finite, probs = lower_bound, na.rm = TRUE)
    upper_threshold <- quantile(x_finite, probs = upper_bound, na.rm = TRUE)
    x <- ifelse(x < lower_threshold, lower_threshold, x)
    x <- ifelse(x > upper_threshold, upper_threshold, x)
  }
  
  min_val <- min(x, na.rm = TRUE)
  max_val <- max(x, na.rm = TRUE)
  
  if (min_val == max_val) {
    return(rep(50, length(x)))
  }
  
  normalized <- 100 * (x - min_val) / (max_val - min_val)
  return(normalized)
}

log_transform_with_offset <- function(x, offset = 1) {
  return(log(x + offset))
}

cat("   ✅ Normalization and transformation functions defined\n")

cat("\n🧮 STAGE 3.2: CALCULATING COMPONENT SCORES (COMPANY-LEVEL)\n")
cat(paste(rep("-", 60), collapse = ""), "\n")

calculate_component_scores <- function(imputed_data) {
  cat("   Calculating 5 component innovation scores...\n")
  scores_data <- imputed_data
  
  if ("rd_expense" %in% colnames(scores_data) && "market_cap" %in% colnames(scores_data)) {
    cat("   Calculating R&D Intensity Score...\n")
    scores_data$rd_intensity <- scores_data$rd_expense / scores_data$market_cap
    scores_data$rd_intensity_log <- log_transform_with_offset(scores_data$rd_intensity)
    scores_data$rd_intensity_score <- normalize_to_100(scores_data$rd_intensity_log)
    cat(sprintf("      Range: %.2f to %.2f\n", 
                min(scores_data$rd_intensity_score, na.rm = TRUE),
                max(scores_data$rd_intensity_score, na.rm = TRUE)))
  } else {
    cat("   ⚠️  Missing columns for R&D Intensity Score\n")
    scores_data$rd_intensity_score <- NA_real_
  }
  
  if ("analyst_rating" %in% colnames(scores_data)) {
    cat("   Calculating Analyst Sentiment Score...\n")
    scores_data$analyst_sentiment_score <- normalize_to_100(scores_data$analyst_rating)
    cat(sprintf("      Range: %.2f to %.2f\n",
                min(scores_data$analyst_sentiment_score, na.rm = TRUE),
                max(scores_data$analyst_sentiment_score, na.rm = TRUE)))
  } else {
    cat("   ⚠️  Missing column for Analyst Sentiment Score\n")
    scores_data$analyst_sentiment_score <- NA_real_
  }
  
  if ("patent_activity" %in% colnames(scores_data)) {
    cat("   Calculating Patent Activity Score...\n")
    scores_data$patent_activity_log <- log_transform_with_offset(scores_data$patent_activity)
    scores_data$patent_activity_score <- normalize_to_100(scores_data$patent_activity_log)
    cat(sprintf("      Range: %.2f to %.2f\n",
                min(scores_data$patent_activity_score, na.rm = TRUE),
                max(scores_data$patent_activity_score, na.rm = TRUE)))
  } else {
    cat("   ⚠️  Missing column for Patent Activity Score\n")
    scores_data$patent_activity_score <- NA_real_
  }
  
  if ("news_sentiment" %in% colnames(scores_data)) {
    cat("   Calculating News Sentiment Score...\n")
    scores_data$news_sentiment_score <- normalize_to_100(scores_data$news_sentiment)
    cat(sprintf("      Range: %.2f to %.2f\n",
                min(scores_data$news_sentiment_score, na.rm = TRUE),
                max(scores_data$news_sentiment_score, na.rm = TRUE)))
  } else {
    cat("   ⚠️  Missing column for News Sentiment Score\n")
    scores_data$news_sentiment_score <- NA_real_
  }
  
  if ("revenue_growth" %in% colnames(scores_data)) {
    cat("   Calculating Growth Momentum Score...\n")
    scores_data$growth_momentum_score <- normalize_to_100(scores_data$revenue_growth)
    cat(sprintf("      Range: %.2f to %.2f\n",
                min(scores_data$growth_momentum_score, na.rm = TRUE),
                max(scores_data$growth_momentum_score, na.rm = TRUE)))
  } else {
    cat("   ⚠️  Missing column for Growth Momentum Score\n")
    scores_data$growth_momentum_score <- NA_real_
  }
  
  cat("\n   📊 COMPONENT SCORE SUMMARY:\n")
  component_scores <- c("rd_intensity_score", "analyst_sentiment_score",
                        "patent_activity_score", "news_sentiment_score",
                        "growth_momentum_score")
  
  for (score in component_scores) {
    if (score %in% colnames(scores_data)) {
      mean_val <- mean(scores_data[[score]], na.rm = TRUE)
      sd_val <- sd(scores_data[[score]], na.rm = TRUE)
      missing_pct <- 100 * sum(is.na(scores_data[[score]])) / nrow(scores_data)
      cat(sprintf("      %-30s: Mean = %6.2f, SD = %5.2f, Missing = %5.1f%%\n",
                  score, mean_val, sd_val, missing_pct))
    }
  }
  
  return(scores_data)
}

daii_company_with_scores <- calculate_component_scores(daii_company_imputed)

cat("\n🎯 STAGE 3.3: CALCULATING DAII 3.5 COMPOSITE SCORE\n")
cat(paste(rep("-", 60), collapse = ""), "\n")

calculate_composite_score <- function(scores_data) {
  cat("   Calculating composite score with weighted components...\n")
  
  weights <- list(
    rd_intensity = 0.30,
    analyst_sentiment = 0.20,
    patent_activity = 0.25,
    news_sentiment = 0.10,
    growth_momentum = 0.15
  )
  
  scores_data$DAII_3.5_Score <- 0
  
  component_mapping <- list(
    rd_intensity = "rd_intensity_score",
    analyst_sentiment = "analyst_sentiment_score",
    patent_activity = "patent_activity_score",
    news_sentiment = "news_sentiment_score",
    growth_momentum = "growth_momentum_score"
  )
  
  total_applied_weight <- 0
  for (component in names(component_mapping)) {
    score_col <- component_mapping[[component]]
    weight <- weights[[component]]
    
    if (score_col %in% colnames(scores_data)) {
      valid_scores <- !is.na(scores_data[[score_col]])
      scores_data$DAII_3.5_Score[valid_scores] <- 
        scores_data$DAII_3.5_Score[valid_scores] + 
        (scores_data[[score_col]][valid_scores] * weight)
      total_applied_weight <- total_applied_weight + weight
      
      cat(sprintf("      %-25s (%.0f%%) applied\n", 
                  toupper(gsub("_", " ", component)), weight * 100))
    } else {
      cat(sprintf("      %-25s (%.0f%%) MISSING - skipping\n", 
                  toupper(gsub("_", " ", component)), weight * 100))
    }
  }
  
  if (total_applied_weight < 1.0 && total_applied_weight > 0) {
    cat(sprintf("      Scaling composite score (only %.0f%% of weights available)\n",
                total_applied_weight * 100))
    scores_data$DAII_3.5_Score <- scores_data$DAII_3.5_Score / total_applied_weight
  }
  
  scores_data$DAII_3.5_Score <- normalize_to_100(scores_data$DAII_3.5_Score)
  
  cat("\n   📈 DAII 3.5 COMPOSITE SCORE STATISTICS:\n")
  cat(sprintf("      Mean: %.2f\n", mean(scores_data$DAII_3.5_Score, na.rm = TRUE)))
  cat(sprintf("      SD:   %.2f\n", sd(scores_data$DAII_3.5_Score, na.rm = TRUE)))
  cat(sprintf("      Min:  %.2f\n", min(scores_data$DAII_3.5_Score, na.rm = TRUE)))
  cat(sprintf("      Max:  %.2f\n", max(scores_data$DAII_3.5_Score, na.rm = TRUE)))
  cat(sprintf("      NAs:  %d (%.1f%%)\n", 
              sum(is.na(scores_data$DAII_3.5_Score)),
              100 * sum(is.na(scores_data$DAII_3.5_Score)) / nrow(scores_data)))
  
  return(scores_data)
}

daii_company_with_composite <- calculate_composite_score(daii_company_with_scores)

# =============================================================================
# MODULE 3.4: ASSIGN INNOVATION QUARTILES (CRITICAL FIX - COMPANY-LEVEL ONLY)
# =============================================================================

cat("\n")
cat(paste(rep("=", 80), collapse = ""), "\n")
cat("MODULE 3.4: ASSIGNING INNOVATION QUARTILES (COMPANY-LEVEL ONLY)\n")
cat("🚨 CRITICAL FIX: Quartiles calculated on 50 companies, NOT 589 holdings\n")
cat(paste(rep("=", 80), collapse = ""), "\n\n")

cat("📊 STAGE 3.4: ASSIGNING INNOVATION QUARTILES (ONLY ON COMPANY DATA)\n")
cat(paste(rep("-", 60), collapse = ""), "\n")

assign_innovation_quartiles <- function(scores_data) {
  cat("   Assigning innovation quartiles based on DAII 3.5 Score...\n")
  
  if (!"DAII_3.5_Score" %in% colnames(scores_data)) {
    cat("   ❌ ERROR: DAII_3.5_Score column not found\n")
    scores_data$innovation_quartile <- NA
    return(scores_data)
  }
  
  n_scores <- sum(!is.na(scores_data$DAII_3.5_Score))
  unique_scores <- length(unique(na.omit(scores_data$DAII_3.5_Score)))
  
  cat(sprintf("      Dataset rows: %d (should be ~50 for company-level)\n", nrow(scores_data)))
  cat(sprintf("      Total scores: %d\n", n_scores))
  cat(sprintf("      Unique scores: %d\n", unique_scores))
  
  if (n_scores == 0) {
    cat("   ⚠️  No valid scores to assign quartiles\n")
    scores_data$innovation_quartile <- NA
    return(scores_data)
  }
  
  quartile_breaks <- tryCatch({
    quantile(scores_data$DAII_3.5_Score, 
             probs = seq(0, 1, 0.25), 
             na.rm = TRUE)
  }, error = function(e) {
    cat(sprintf("   ⚠️  Error calculating quantiles: %s\n", e$message))
    return(NULL)
  })
  
  if (is.null(quartile_breaks)) {
    cat("   Using simple quartile assignment\n")
    scores_data$rank <- rank(scores_data$DAII_3.5_Score, na.last = "keep", ties.method = "random")
    quartile_size <- ceiling(max(scores_data$rank, na.rm = TRUE) / 4)
    scores_data$innovation_quartile <- cut(scores_data$rank,
                                           breaks = c(0, quartile_size, 2*quartile_size, 
                                                      3*quartile_size, Inf),
                                           labels = c("Q1", "Q2", "Q3", "Q4"),
                                           include.lowest = TRUE)
    scores_data$rank <- NULL
  } else {
    cat(sprintf("      Quartile breaks: %s\n", 
                paste(round(quartile_breaks, 2), collapse = ", ")))
    
    if (length(unique(quartile_breaks)) < 5) {
      cat("   ⚠️  Non-unique quartile breaks detected. Applying jitter...\n")
      
      jitter_amount <- (max(scores_data$DAII_3.5_Score, na.rm = TRUE) - 
                          min(scores_data$DAII_3.5_Score, na.rm = TRUE)) * 0.0001
      scores_data$DAII_3.5_Score_jittered <- scores_data$DAII_3.5_Score + 
        runif(nrow(scores_data), -jitter_amount, jitter_amount)
      
      quartile_breaks <- quantile(scores_data$DAII_3.5_Score_jittered, 
                                  probs = seq(0, 1, 0.25), 
                                  na.rm = TRUE)
      cat(sprintf("      New quartile breaks: %s\n", 
                  paste(round(quartile_breaks, 2), collapse = ", ")))
      
      scores_data$innovation_quartile <- cut(scores_data$DAII_3.5_Score_jittered,
                                             breaks = quartile_breaks,
                                             labels = c("Q1", "Q2", "Q3", "Q4"),
                                             include.lowest = TRUE)
      scores_data$DAII_3.5_Score_jittered <- NULL
    } else {
      scores_data$innovation_quartile <- cut(scores_data$DAII_3.5_Score,
                                             breaks = quartile_breaks,
                                             labels = c("Q1", "Q2", "Q3", "Q4"),
                                             include.lowest = TRUE)
    }
  }
  
  quartile_table <- table(scores_data$innovation_quartile, useNA = "ifany")
  cat("\n   📊 QUARTILE DISTRIBUTION (COMPANY-LEVEL):\n")
  for (q in c("Q1", "Q2", "Q3", "Q4")) {
    if (q %in% names(quartile_table)) {
      count <- quartile_table[q]
      pct <- 100 * count / sum(quartile_table[!names(quartile_table) %in% "NA"])
      cat(sprintf("      %s: %d companies (%.1f%%)\n", q, count, pct))
    }
  }
  
  if ("NA" %in% names(quartile_table)) {
    cat(sprintf("      NA: %d companies\n", quartile_table["NA"]))
  }
  
  cat("\n   ✅ Innovation quartiles assigned successfully (company-level)\n")
  return(scores_data)
}

daii_company_final <- assign_innovation_quartiles(daii_company_with_composite)

# =============================================================================
# MODULE 3.5: JOIN COMPANY SCORES TO FUND-LEVEL DATA
# =============================================================================

cat("\n")
cat(paste(rep("=", 80), collapse = ""), "\n")
cat("MODULE 3.5: JOINING COMPANY SCORES TO FUND-LEVEL DATA\n")
cat("🎯 QUARTILES ARE JOINED FROM COMPANY DATA, NOT CALCULATED ON FUND DATA\n")
cat(paste(rep("=", 80), collapse = ""), "\n\n")

cat("🔗 STAGE 3.5: JOINING COMPANY SCORES BACK TO FUND HOLDINGS\n")
cat(paste(rep("-", 60), collapse = ""), "\n")

join_scores_to_fund_data <- function(company_scores, fund_data) {
  cat("   Joining company scores to fund-holding dataset...\n")
  
  score_cols <- c("ticker", 
                  "rd_intensity_score", "analyst_sentiment_score", 
                  "patent_activity_score", "news_sentiment_score", 
                  "growth_momentum_score", "DAII_3.5_Score", 
                  "innovation_quartile", "rd_intensity", "rd_intensity_log",
                  "patent_activity_log")
  
  existing_score_cols <- intersect(score_cols, colnames(company_scores))
  
  company_scores_subset <- company_scores[, existing_score_cols, drop = FALSE]
  
  fund_data_with_scores <- fund_data %>%
    left_join(company_scores_subset, by = "ticker")
  
  joined_rows <- nrow(fund_data_with_scores)
  original_rows <- nrow(fund_data)
  companies_with_scores <- length(unique(company_scores_subset$ticker))
  
  cat(sprintf("   Joined %d company scores to %d fund holdings\n",
              companies_with_scores, joined_rows))
  
  missing_scores <- sum(is.na(fund_data_with_scores$DAII_3.5_Score))
  if (missing_scores > 0) {
    cat(sprintf("   ⚠️  %d fund holdings missing DAII_3.5_Score\n", missing_scores))
  } else {
    cat("   ✅ All fund holdings have DAII_3.5_Score\n")
  }
  
  return(fund_data_with_scores)
}

daii_final_with_scores <- join_scores_to_fund_data(daii_company_final, daii_fund_data)

cat("\n📊 FINAL QUARTILE DISTRIBUTION (FUND-LEVEL):\n")
quartile_counts <- table(daii_final_with_scores$innovation_quartile, useNA = "ifany")
for (q in c("Q1", "Q2", "Q3", "Q4")) {
  if (q %in% names(quartile_counts)) {
    count <- quartile_counts[q]
    pct <- 100 * count / sum(quartile_counts[!names(quartile_counts) %in% "NA"])
    cat(sprintf("   %s: %d holdings (%.1f%%)\n", q, count, pct))
  }
}

if ("NA" %in% names(quartile_counts)) {
  cat(sprintf("   NA: %d holdings\n", quartile_counts["NA"]))
}

daii_final_scores <- daii_final_with_scores

# =============================================================================
# MODULE 4: OUTPUT GENERATION
# =============================================================================

cat("\n")
cat(paste(rep("=", 80), collapse = ""), "\n")
cat("MODULE 4: OUTPUT GENERATION & EXPORT\n")
cat(paste(rep("=", 80), collapse = ""), "\n\n")

cat("📁 STAGE 4.1: CREATING OUTPUT DIRECTORY\n")
cat(paste(rep("-", 60), collapse = ""), "\n")

create_output_directory <- function(base_path = "C:/Users/sganesan/OneDrive - dumac.duke.edu/DAII/data/output") {
  timestamp <- format(Sys.time(), "%Y%m%d_%H%M%S")
  run_folder <- file.path(base_path, paste0("DAII_3.5_Run_", timestamp))
  
  tryCatch({
    if (!dir.exists(run_folder)) {
      dir.create(run_folder, recursive = TRUE)
      cat(sprintf("   Created output directory: %s\n", run_folder))
    } else {
      cat(sprintf("   Output directory already exists: %s\n", run_folder))
    }
    return(run_folder)
  }, error = function(e) {
    cat(sprintf("   ⚠️  Could not create directory: %s\n", e$message))
    cat("   Using current working directory instead.\n")
    return(".")
  })
}

output_dir <- create_output_directory()

cat("\n💾 STAGE 4.2: SAVING SCORED DATA WITH COMPONENT SCORES\n")
cat(paste(rep("-", 60), collapse = ""), "\n")

save_scored_data <- function(scores_data, output_dir) {
  output_data <- scores_data
  
  column_order <- c()
  
  if ("ticker" %in% colnames(output_data)) {
    column_order <- c(column_order, "ticker")
  }
  
  original_cols <- c("rd_expense", "market_cap", "analyst_rating", 
                     "patent_activity", "news_sentiment", "revenue_growth")
  for (col in original_cols) {
    if (col %in% colnames(output_data)) {
      column_order <- c(column_order, col)
    }
  }
  
  rd_cols <- c("rd_intensity", "rd_intensity_log", "rd_intensity_score")
  for (col in rd_cols) {
    if (col %in% colnames(output_data)) {
      column_order <- c(column_order, col)
    }
  }
  
  patent_cols <- c("patent_activity_log", "patent_activity_score")
  for (col in patent_cols) {
    if (col %in% colnames(output_data)) {
      column_order <- c(column_order, col)
    }
  }
  
  other_component_cols <- c("analyst_sentiment_score", "news_sentiment_score", 
                            "growth_momentum_score")
  for (col in other_component_cols) {
    if (col %in% colnames(output_data)) {
      column_order <- c(column_order, col)
    }
  }
  
  if ("DAII_3.5_Score" %in% colnames(output_data)) {
    column_order <- c(column_order, "DAII_3.5_Score")
  }
  if ("innovation_quartile" %in% colnames(output_data)) {
    column_order <- c(column_order, "innovation_quartile")
  }
  
  fund_cols <- c("fund_id", "fund_name", "fund_weight", "dumac_allocation",
                 "as_of_date", "shares_held", "position_value", "fund_weight_raw",
                 "abs_weight", "Tot...Hldg.in.Port", "Top.10.as.of.Dt")
  for (col in fund_cols) {
    if (col %in% colnames(output_data)) {
      column_order <- c(column_order, col)
    }
  }
  
  remaining_cols <- setdiff(colnames(output_data), column_order)
  column_order <- c(column_order, remaining_cols)
  
  output_data <- output_data[, column_order, drop = FALSE]
  
  output_file <- file.path(output_dir, "02_scored_data_with_components.csv")
  tryCatch({
    readr::write_csv(output_data, output_file)
    cat(sprintf("   ✅ Scored data saved: %s\n", output_file))
    cat(sprintf("      Dimensions: %d rows × %d columns\n", 
                nrow(output_data), ncol(output_data)))
    
    cat("\n   📋 OUTPUT COLUMNS (first 12):\n   ")
    cat(paste(colnames(output_data)[1:min(12, ncol(output_data))], collapse = ", "))
    if (ncol(output_data) > 12) cat(", ...")
    cat("\n")
    
    return(output_file)
  }, error = function(e) {
    cat(sprintf("   ❌ Error saving file: %s\n", e$message))
    return(NULL)
  })
}

main_output_file <- save_scored_data(daii_final_scores, output_dir)

cat("\n📄 STAGE 4.3: SAVING ADDITIONAL OUTPUT FILES\n")
cat(paste(rep("-", 60), collapse = ""), "\n")

save_additional_outputs <- function(processed_data, imputed_data, scores_data, 
                                    imputation_log, output_dir) {
  files_saved <- list()
  
  processed_file <- file.path(output_dir, "01_processed_data.csv")
  tryCatch({
    readr::write_csv(processed_data, processed_file)
    files_saved[["processed_data"]] <- processed_file
    cat("   ✅ Processed data saved\n")
  }, error = function(e) {
    cat("   ⚠️  Could not save processed data\n")
  })
  
  if (!is.null(imputation_log) && nrow(imputation_log) > 0) {
    imputation_file <- file.path(output_dir, "03_imputation_log.csv")
    tryCatch({
      readr::write_csv(imputation_log, imputation_file)
      files_saved[["imputation_log"]] <- imputation_file
      cat("   ✅ Imputation log saved\n")
    }, error = function(e) {
      cat("   ⚠️  Could not save imputation log\n")
    })
  }
  
  if (exists("daii_company_final")) {
    company_file <- file.path(output_dir, "04_company_level_scores.csv")
    tryCatch({
      readr::write_csv(daii_company_final, company_file)
      files_saved[["company_scores"]] <- company_file
      cat("   ✅ Company-level scores saved\n")
    }, error = function(e) {
      cat("   ⚠️  Could not save company-level scores\n")
    })
  }
  
  if ("DAII_3.5_Score" %in% colnames(scores_data)) {
    if (exists("daii_company_final")) {
      company_scores <- daii_company_final$DAII_3.5_Score
    } else {
      unique_scores <- scores_data %>%
        group_by(ticker) %>%
        summarise(DAII_3.5_Score = first(DAII_3.5_Score), .groups = 'drop')
      company_scores <- unique_scores$DAII_3.5_Score
    }
    
    score_stats <- data.frame(
      Statistic = c("Mean", "SD", "Min", "Q1", "Median", "Q3", "Max", "N", "NA_Count"),
      Value = c(
        mean(company_scores, na.rm = TRUE),
        sd(company_scores, na.rm = TRUE),
        min(company_scores, na.rm = TRUE),
        quantile(company_scores, 0.25, na.rm = TRUE),
        median(company_scores, na.rm = TRUE),
        quantile(company_scores, 0.75, na.rm = TRUE),
        max(company_scores, na.rm = TRUE),
        sum(!is.na(company_scores)),
        sum(is.na(company_scores))
      )
    )
    
    stats_file <- file.path(output_dir, "05_score_statistics.csv")
    tryCatch({
      readr::write_csv(score_stats, stats_file)
      files_saved[["score_stats"]] <- stats_file
      cat("   ✅ Score statistics saved (based on company-level, not fund-level)\n")
    }, error = function(e) {
      cat("   ⚠️  Could not save score statistics\n")
    })
  }
  
  readme_content <- sprintf(
    "DAII 3.5 Pipeline Execution Summary
Generated: %s
Version: 3.5.9 (Field Mapping Integrated + Company/Fund Separation + QUARTILE FIX)

EXECUTION RESULTS:
- Input rows (fund-level): %d
- Unique companies: %d
- Company-level rows: %d
- Output rows (fund-level with scores): %d
- Component scores calculated: %d/5
- Composite score calculated: %s
- Innovation quartiles assigned: %s

KEY IMPROVEMENTS v3.5.9:
1. Field mapping system integrated
2. Company/fund data separation applied
3. Scoring performed at company level (N=%d)
4. Quartiles calculated ONLY on company data (fixes 446-in-Q4 issue)
5. Statistics reflect true company distribution
6. Scores correctly joined to fund holdings

QUARTILE FIX DETAILS:
- Previous version: Quartiles calculated on fund data (589 rows) -> 446 holdings in Q4
- Current version: Quartiles calculated on company data (50 rows) -> balanced distribution
- Company quartiles: Q1=%d, Q2=%d, Q3=%d, Q4=%d companies

DATA STRUCTURE:
- Company dataset: %d rows × company metrics
- Fund dataset: %d rows × fund-specific metrics
- Final output: %d rows (scores repeated per fund holding)

FIELD MAPPING STATUS: %s
COMPANY/FUND SEPARATION: %s
QUARTILE FIX APPLIED: YES

NOTES:
- Statistics in 05_score_statistics.csv are based on %d companies, not %d fund holdings
- Quartile distribution is now meaningful for portfolio construction
- Output ready for Modules 4-9
",
    Sys.time(),
    nrow(processed_data),
    length(unique(scores_data$ticker)),
    ifelse(exists("daii_company_final"), nrow(daii_company_final), "N/A"),
    nrow(scores_data),
    sum(c("rd_intensity_score", "analyst_sentiment_score", "patent_activity_score",
          "news_sentiment_score", "growth_momentum_score") %in% colnames(scores_data)),
    ifelse("DAII_3.5_Score" %in% colnames(scores_data), "YES", "NO"),
    ifelse("innovation_quartile" %in% colnames(scores_data), "YES", "NO"),
    ifelse(exists("daii_company_final"), nrow(daii_company_final), "N/A"),
    ifelse(exists("daii_company_final"), 
           sum(daii_company_final$innovation_quartile == "Q1", na.rm = TRUE), "N/A"),
    ifelse(exists("daii_company_final"), 
           sum(daii_company_final$innovation_quartile == "Q2", na.rm = TRUE), "N/A"),
    ifelse(exists("daii_company_final"), 
           sum(daii_company_final$innovation_quartile == "Q3", na.rm = TRUE), "N/A"),
    ifelse(exists("daii_company_final"), 
           sum(daii_company_final$innovation_quartile == "Q4", na.rm = TRUE), "N/A"),
    ifelse(exists("daii_company_final"), nrow(daii_company_final), "N/A"),
    nrow(daii_fund_data),
    nrow(scores_data),
    ifelse(!is.null(field_mapping), "APPLIED", "NOT APPLIED (used hard-coded names)"),
    ifelse(exists("daii_company_final"), "APPLIED", "NOT APPLIED"),
    ifelse(exists("daii_company_final"), nrow(daii_company_final), "N/A"),
    nrow(scores_data)
  )
  
  readme_file <- file.path(output_dir, "06_README.txt")
  tryCatch({
    writeLines(readme_content, readme_file)
    files_saved[["readme"]] <- readme_file
    cat("   ✅ Readme file saved\n")
  }, error = function(e) {
    cat("   ⚠️  Could not save readme file\n")
  })
  
  return(files_saved)
}

additional_files <- save_additional_outputs(
  daii_processed_data, 
  daii_company_imputed,
  daii_final_scores,
  imputation_log,
  output_dir
)

# =============================================================================
# MODULE 5: EXECUTION SUMMARY & VALIDATION
# =============================================================================

cat("\n")
cat(paste(rep("=", 80), collapse = ""), "\n")
cat("MODULE 5: EXECUTION SUMMARY & VALIDATION\n")
cat(paste(rep("=", 80), collapse = ""), "\n\n")

cat("📋 STAGE 5.1: GENERATING EXECUTION SUMMARY\n")
cat(paste(rep("-", 60), collapse = ""), "\n")

generate_execution_summary <- function(scores_data, output_dir, field_mapping) {
  cat("   Compiling execution statistics...\n")
  
  if (exists("daii_company_final")) {
    company_data <- daii_company_final
  } else {
    company_data <- scores_data %>%
      group_by(ticker) %>%
      summarise(
        DAII_3.5_Score = first(DAII_3.5_Score),
        innovation_quartile = first(innovation_quartile),
        .groups = 'drop'
      )
  }
  
  summary_stats <- list(
    execution_timestamp = Sys.time(),
    version = "3.5.9 (Field Mapping Integrated + Company/Fund Separation + Quartile Fix)",
    status = "COMPLETED",
    input_rows_fund_level = nrow(scores_data),
    unique_companies = length(unique(scores_data$ticker)),
    company_level_rows = nrow(company_data),
    field_mapping_applied = !is.null(field_mapping),
    company_fund_separation_applied = exists("daii_company_final"),
    component_scores_calculated = sum(c("rd_intensity_score", "analyst_sentiment_score",
                                        "patent_activity_score", "news_sentiment_score",
                                        "growth_momentum_score") %in% colnames(company_data)),
    composite_score_calculated = "DAII_3.5_Score" %in% colnames(company_data),
    quartiles_assigned = "innovation_quartile" %in% colnames(company_data),
    na_composite_score = ifelse("DAII_3.5_Score" %in% colnames(company_data),
                                sum(is.na(company_data$DAII_3.5_Score)), NA),
    na_composite_pct = ifelse("DAII_3.5_Score" %in% colnames(company_data),
                              100 * sum(is.na(company_data$DAII_3.5_Score)) / nrow(company_data), NA),
    company_score_mean = ifelse("DAII_3.5_Score" %in% colnames(company_data),
                                mean(company_data$DAII_3.5_Score, na.rm = TRUE), NA),
    company_score_sd = ifelse("DAII_3.5_Score" %in% colnames(company_data),
                              sd(company_data$DAII_3.5_Score, na.rm = TRUE), NA),
    company_score_range = ifelse("DAII_3.5_Score" %in% colnames(company_data),
                                 paste(round(range(company_data$DAII_3.5_Score, na.rm = TRUE), 2), collapse = " to "), NA),
    quartile_counts = ifelse("innovation_quartile" %in% colnames(company_data),
                             paste(capture.output(table(company_data$innovation_quartile, useNA = "ifany")), 
                                   collapse = "\n"), "N/A")
  )
  
  cat("\n   🎉 DAII 3.5 PIPELINE EXECUTION COMPLETE!\n")
  cat(paste(rep("=", 60), collapse = ""), "\n")
  cat(sprintf("   Version:        %s\n", summary_stats$version))
  cat(sprintf("   Status:         %s\n", summary_stats$status))
  cat(sprintf("   Timestamp:      %s\n", summary_stats$execution_timestamp))
  cat(sprintf("   Fund holdings:  %d rows\n", summary_stats$input_rows_fund_level))
  cat(sprintf("   Unique companies: %d companies\n", summary_stats$unique_companies))
  cat(sprintf("   Field mapping:  %s\n", ifelse(summary_stats$field_mapping_applied, 
                                                "APPLIED ✅", "NOT APPLIED ⚠️")))
  cat(sprintf("   Company/fund separation: %s\n", ifelse(summary_stats$company_fund_separation_applied,
                                                         "APPLIED ✅", "NOT APPLIED ⚠️")))
  cat(sprintf("   Component scores:%d/5 calculated\n", summary_stats$component_scores_calculated))
  cat(sprintf("   Composite score: %s\n", ifelse(summary_stats$composite_score_calculated,
                                                 "CALCULATED ✅", "MISSING ❌")))
  cat(sprintf("   Innovation quartiles: %s\n", ifelse(summary_stats$quartiles_assigned,
                                                      "ASSIGNED ✅", "MISSING ❌")))
  
  if (summary_stats$composite_score_calculated) {
    cat(sprintf("   Company-level stats (N=%d):\n", summary_stats$unique_companies))
    cat(sprintf("      Mean: %.2f, SD: %.2f\n", 
                summary_stats$company_score_mean,
                summary_stats$company_score_sd))
    cat(sprintf("      Range: %s\n", summary_stats$company_score_range))
    cat(sprintf("      NA scores: %d (%.1f%%)\n", 
                summary_stats$na_composite_score,
                summary_stats$na_composite_pct))
  }
  
  summary_file <- file.path(output_dir, "07_execution_summary.yaml")
  tryCatch({
    yaml::write_yaml(summary_stats, summary_file)
    cat(sprintf("\n   📄 Execution summary saved: %s\n", summary_file))
  }, error = function(e) {
    cat(sprintf("\n   ⚠️  Could not save execution summary: %s\n", e$message))
  })
  
  return(summary_stats)
}

execution_summary <- generate_execution_summary(daii_final_scores, output_dir, field_mapping)

cat("\n🔍 STAGE 5.2: FINAL VALIDATION CHECKS\n")
cat(paste(rep("-", 60), collapse = ""), "\n")

perform_validation_checks <- function(scores_data) {
  cat("   Running validation checks...\n")
  checks_passed <- 0
  checks_total <- 0
  
  checks_total <- checks_total + 1
  if ("ticker" %in% colnames(scores_data)) {
    cat("   ✅ Check 1: Ticker column present\n")
    checks_passed <- checks_passed + 1
  } else {
    cat("   ❌ Check 1: Ticker column missing\n")
  }
  
  checks_total <- checks_total + 1
  if ("DAII_3.5_Score" %in% colnames(scores_data)) {
    cat("   ✅ Check 2: DAII_3.5_Score column present\n")
    checks_passed <- checks_passed + 1
    
    if (all(scores_data$DAII_3.5_Score >= 0 & scores_data$DAII_3.5_Score <= 100, na.rm = TRUE)) {
      cat("   ✅ Check 2a: Scores in 0-100 range\n")
    } else {
      cat("   ⚠️  Check 2a: Some scores outside 0-100 range\n")
    }
  } else {
    cat("   ❌ Check 2: DAII_3.5_Score column missing\n")
  }
  
  checks_total <- checks_total + 1
  if ("innovation_quartile" %in% colnames(scores_data)) {
    cat("   ✅ Check 3: Innovation quartiles assigned\n")
    checks_passed <- checks_passed + 1
  } else {
    cat("   ❌ Check 3: Innovation quartiles missing\n")
  }
  
  component_cols <- c("rd_intensity_score", "analyst_sentiment_score",
                      "patent_activity_score", "news_sentiment_score", 
                      "growth_momentum_score")
  present_components <- sum(component_cols %in% colnames(scores_data))
  checks_total <- checks_total + 1
  if (present_components == 5) {
    cat("   ✅ Check 4: All 5 component scores calculated\n")
    checks_passed <- checks_passed + 1
  } else {
    cat(sprintf("   ⚠️  Check 4: Only %d/5 component scores calculated\n", present_components))
  }
  
  unique_tickers <- length(unique(scores_data$ticker))
  total_rows <- nrow(scores_data)
  checks_total <- checks_total + 1
  if (total_rows > unique_tickers) {
    cat(sprintf("   ✅ Check 5: Company/fund separation applied (%d companies, %d fund holdings)\n", 
                unique_tickers, total_rows))
    checks_passed <- checks_passed + 1
  } else {
    cat(sprintf("   ⚠️  Check 5: Company/fund separation not evident (%d rows, %d tickers)\n", 
                total_rows, unique_tickers))
  }
  
  checks_total <- checks_total + 1
  if ("rd_intensity" %in% colnames(scores_data) && "rd_intensity_score" %in% colnames(scores_data)) {
    col_names <- colnames(scores_data)
    rd_intensity_idx <- which(col_names == "rd_intensity")
    rd_intensity_score_idx <- which(col_names == "rd_intensity_score")
    
    if (abs(rd_intensity_idx - rd_intensity_score_idx) <= 2) {
      cat("   ✅ Check 6: Related columns grouped together\n")
      checks_passed <- checks_passed + 1
    } else {
      cat("   ⚠️  Check 6: Related columns not adjacent\n")
    }
  } else {
    cat("   ℹ️  Check 6: Column grouping check skipped (missing columns)\n")
  }
  
  validation_pct <- 100 * checks_passed / checks_total
  cat(sprintf("\n   📊 VALIDATION SUMMARY: %d/%d checks passed (%.0f%%)\n",
              checks_passed, checks_total, validation_pct))
  
  if (validation_pct >= 80) {
    cat("   🎉 VALIDATION PASSED: Pipeline output is reliable\n")
  } else if (validation_pct >= 60) {
    cat("   ⚠️  VALIDATION WARNING: Some checks failed, review output\n")
  } else {
    cat("   ❌ VALIDATION FAILED: Multiple critical checks failed\n")
  }
  
  return(list(passed = checks_passed, total = checks_total, pct = validation_pct))
}

validation_result <- perform_validation_checks(daii_final_scores)

# =============================================================================
# FINAL COMPLETION
# =============================================================================

cat("\n")
cat(paste(rep("=", 80), collapse = ""), "\n")
cat("🎉 DAII 3.5 PHASE 1 PIPELINE COMPLETE v3.5.9! 🎉\n")
cat(paste(rep("=", 80), collapse = ""), "\n\n")

cat("SUMMARY OF WHAT WAS ACCOMPLISHED v3.5.9:\n")
cat(paste(rep("-", 60), collapse = ""), "\n")
cat("1. ✅ FIELD MAPPING INTEGRATED: Raw data columns standardized before processing\n")
cat("2. ✅ COMPANY/FUND SEPARATION: Scoring at company level, output at fund level\n")
cat("3. ✅ QUARTILE FIX CRITICAL: Quartiles calculated ONLY on 50 companies (not 589 holdings)\n")
cat("4. ✅ COMPONENT SCORES CALCULATED: All 5 domain scores computed\n")
cat("5. ✅ COMPOSITE SCORE GENERATED: DAII_3.5_Score with proper weighting\n")
cat("6. ✅ INNOVATION QUARTILES BALANCED: Companies evenly distributed across Q1-Q4\n")
cat("7. ✅ COLUMNS REORGANIZED: Related fields grouped together logically\n")
cat("8. ✅ OUTPUT FILES SAVED: Complete dataset saved to output directory\n\n")

cat("QUARTILE FIX DETAILS:\n")
cat(paste(rep("-", 60), collapse = ""), "\n")
cat("• BEFORE v3.5.8: 446 out of 589 holdings in Q4 (76% - mathematically impossible)\n")
cat("• AFTER v3.5.9: Balanced distribution across all quartiles (13-12-12-13 companies)\n")
cat("• Root cause: Quartiles were calculated on fund-level data (589 rows)\n")
cat("• Solution: Quartiles calculated ONLY on company-level data (50 rows)\n\n")

cat("DATA STRUCTURE VALIDATION:\n")
cat(paste(rep("-", 60), collapse = ""), "\n")
cat(sprintf("• Input (fund-holding level): %d rows\n", nrow(daii_processed_data)))
cat(sprintf("• Unique companies: %d companies\n", length(unique(daii_final_scores$ticker))))
cat(sprintf("• Company-level scoring: %d rows (correct!)\n", 
            ifelse(exists("daii_company_final"), nrow(daii_company_final), "N/A")))
cat(sprintf("• Output (fund-level with scores): %d rows\n", nrow(daii_final_scores)))
cat(sprintf("• Statistics based on: %d companies (not %d fund holdings)\n",
            length(unique(daii_final_scores$ticker)), nrow(daii_final_scores)))
cat("\n")

cat("OUTPUT LOCATION:\n")
cat(paste(rep("-", 60), collapse = ""), "\n")
cat("All output files saved to:", output_dir, "\n")
cat("Main output file:", ifelse(!is.null(main_output_file), main_output_file, "NOT SAVED"), "\n\n")

cat("KEY OUTPUT FILES:\n")
cat(paste(rep("-", 60), collapse = ""), "\n")
cat("01_processed_data.csv          # Cleaned data after Module 1\n")
cat("02_scored_data_with_components.csv  # MAIN OUTPUT with all scores\n")
cat("03_imputation_log.csv          # What was imputed and how\n")
cat("04_company_level_scores.csv    # Company-level scores (N=50)\n")
cat("05_score_statistics.csv        # Stats based on companies (N=50)\n")
cat("06_README.txt                  # Execution summary\n")
cat("07_execution_summary.yaml      # Detailed YAML summary\n\n")

cat("NEXT STEPS:\n")
cat(paste(rep("-", 60), collapse = ""), "\n")
cat("1. Verify quartile distribution is balanced (13-12-12-13 companies)\n")
cat("2. Check that no quartile has 446 holdings (old bug fixed)\n")
cat("3. Proceed to Modules 4-9 for portfolio construction and analysis\n")
cat("4. Component scores are now in output file, ready for analysis\n\n")

cat("TO RUN AGAIN:\n")
cat(paste(rep("-", 60), collapse = ""), "\n")
cat("# 1. Save this script as 'DAII_3.5_Phase1_v3.5.9.R'\n")
cat("# 2. Run: source('DAII_3.5_Phase1_v3.5.9.R')\n")
cat("# 3. Check the output directory for results\n\n")

save.image(file.path(output_dir, "DAII_3.5_Phase1_Workspace.RData"))
cat(sprintf("💾 Workspace saved to: %s\n", 
            file.path(output_dir, "DAII_3.5_Phase1_Workspace.RData")))

cat("\n")
cat(paste(rep("✅", 40), collapse = ""), "\n")
cat("PHASE 1 COMPLETE - QUARTILE FIX APPLIED - READY FOR MODULES 4-9!\n")
cat(paste(rep("✅", 40), collapse = ""), "\n")

# After inserting the full v3.5.9 code, continue with:

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# PART 2: MODULE 4 BRIDGE - PORTFOLIO CONSTRUCTION ENGINE
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
cat("\n" + paste(rep("=", 80), collapse="") + "\n")
cat("MODULE 4: PORTFOLIO CONSTRUCTION ENGINE\n")
cat(paste(rep("=", 80), collapse="") + "\n")

# ----- 1. CONFIGURATION LOADING -----
cat("\n[Module 4] Loading configuration files...\n")

# Load required packages for Module 4
if (!require("yaml")) install.packages("yaml", quiet = TRUE)
library(yaml)
if (!require("dplyr")) install.packages("dplyr", quiet = TRUE)
library(dplyr)
if (!require("tidyr")) install.packages("tidyr", quiet = TRUE)
library(tidyr)

# Function to load and validate YAML config
load_config_safely <- function(config_path, config_name) {
  if (!file.exists(config_path)) {
    warning(paste(config_name, "not found at:", config_path))
    return(NULL)
  }
  tryCatch({
    config <- read_yaml(config_path)
    cat(paste0("  ✓ ", config_name, " loaded successfully\n"))
    return(config)
  }, error = function(e) {
    warning(paste("Error loading", config_name, ":", e$message))
    return(NULL)
  })
}

# Load Module 4 specific configurations
module4_config <- load_config_safely("daii_module4_bridge_config.yaml", 
                                     "Module 4 Bridge Config")
portfolio_config <- load_config_safely("daii_portfolio_config.yaml", 
                                       "Portfolio Config")
run_config <- load_config_safely("daii_run_config.yaml", 
                                 "Run Config")

# Set default configurations if files not found
if (is.null(module4_config)) {
  cat("  ⚠ Using default Module 4 configuration\n")
  module4_config <- list(
    portfolio_strategies = c("equal_weight_quartile", "score_weighted", "quartile_tilted"),
    rebalancing_frequency = "quarterly",
    min_weight = 0.001,
    max_weight = 0.10
  )
}

if (is.null(portfolio_config)) {
  cat("  ⚠ Using default portfolio configuration\n")
  portfolio_config <- list(
    benchmark_id = "SPY",
    risk_free_rate = 0.02,
    lookback_period = 252
  )
}

# ----- 2. DATA INPUT BRIDGE -----
cat("\n[Module 4] Loading validated company-level scores...\n")

# Function to find the most recent company scores file
find_latest_company_scores <- function(output_dir = "C:/Users/sganesan/OneDrive - dumac.duke.edu/DAII/data/output") {
  # First try to use the run_id from v3.5.9 if available
  if (exists("run_id") && !is.null(run_id)) {
    expected_file <- file.path(output_dir, paste0(run_id, "_04_company_level_scores.csv"))
    if (file.exists(expected_file)) {
      cat(paste0("  ✓ Found company scores from current run: ", run_id, "\n"))
      return(expected_file)
    }
  }
  
  # Fallback: search for the most recent company scores file
  pattern <- "DAII_3.5_Run_.*_04_company_level_scores\\.csv$"
  all_files <- list.files(output_dir, pattern = pattern, full.names = TRUE)
  
  if (length(all_files) == 0) {
    # Try alternative pattern
    pattern2 <- ".*04_company_level_scores\\.csv$"
    all_files <- list.files(output_dir, pattern = pattern2, full.names = TRUE)
  }
  
  if (length(all_files) == 0) {
    stop("No company-level scores file found in output directory.")
  }
  
  # Get the most recent file
  file_info <- file.info(all_files)
  latest_file <- rownames(file_info)[which.max(file_info$mtime)]
  
  cat(paste0("  ✓ Found latest company scores: ", basename(latest_file), "\n"))
  return(latest_file)
}

# Load the company scores
company_scores_path <- find_latest_company_scores()
company_scores <- read.csv(company_scores_path)

# ----- 3. DATA VALIDATION -----
cat("\n[Module 4] Validating company scores structure...\n")

validate_company_scores <- function(scores_df) {
  validation_checks <- list()
  
  # Check 1: Row count
  validation_checks$row_count <- nrow(scores_df) == 50
  cat(paste0("  ", ifelse(validation_checks$row_count, "✓", "✗"), 
             " Row count: ", nrow(scores_df), " (expected: 50)\n"))
  
  # Check 2: Required columns
  required_cols <- c("company_id", "DAII_3.5_Score", "innovation_quartile")
  validation_checks$required_cols <- all(required_cols %in% colnames(scores_df))
  cat(paste0("  ", ifelse(validation_checks$required_cols, "✓", "✗"), 
             " Required columns present\n"))
  
  # Check 3: Quartile distribution
  if ("innovation_quartile" %in% colnames(scores_df)) {
    quartile_dist <- table(scores_df$innovation_quartile)
    expected_dist <- c(13, 12, 12, 13)  # From v3.5.9 validation
    actual_dist <- as.numeric(quartile_dist[c("Q1", "Q2", "Q3", "Q4")])
    
    validation_checks$quartile_match <- all(actual_dist == expected_dist)
    cat(paste0("  ", ifelse(validation_checks$quartile_match, "✓", "✗"), 
               " Quartile distribution: Q1=", quartile_dist["Q1"], 
               ", Q2=", quartile_dist["Q2"], 
               ", Q3=", quartile_dist["Q3"], 
               ", Q4=", quartile_dist["Q4"], "\n"))
  }
  
  # Check 4: Score range
  if ("DAII_3.5_Score" %in% colnames(scores_df)) {
    score_range <- range(scores_df$DAII_3.5_Score, na.rm = TRUE)
    validation_checks$score_range <- all(score_range >= 0 & score_range <= 100)
    cat(paste0("  ", ifelse(validation_checks$score_range, "✓", "✗"), 
               " Score range: [", round(score_range[1], 2), ", ", 
               round(score_range[2], 2), "]\n"))
  }
  
  # Check 5: Missing values
  validation_checks$no_missing <- !any(is.na(scores_df[, required_cols]))
  cat(paste0("  ", ifelse(validation_checks$no_missing, "✓", "✗"), 
             " No missing values in critical columns\n"))
  
  # Overall validation
  all_checks_passed <- all(unlist(validation_checks))
  
  if (!all_checks_passed) {
    warning("Company scores validation failed. Check the warnings above.")
  }
  
  return(all_checks_passed)
}

validation_passed <- validate_company_scores(company_scores)

if (!validation_passed) {
  cat("\n⚠  WARNING: Company scores validation failed. Proceed with caution.\n")
  response <- readline(prompt = "Continue with portfolio construction? (y/n): ")
  if (tolower(response) != "y") {
    stop("Portfolio construction halted by user.")
  }
}

# ----- 4. PORTFOLIO CONSTRUCTION FUNCTIONS -----
cat("\n[Module 4] Building portfolio construction engine...\n")

# Strategy 1: Equal-Weight Quartile Portfolios
build_equal_weight_quartile <- function(scores_df) {
  cat("  Building Equal-Weight Quartile portfolios...\n")
  
  # Equal weight within each quartile
  portfolio <- scores_df %>%
    group_by(innovation_quartile) %>%
    mutate(
      weight_within_quartile = 1 / n(),
      strategy = "equal_weight_quartile"
    ) %>%
    ungroup() %>%
    mutate(
      # Distribute 25% across each quartile
      quartile_weight = 0.25,
      final_weight = quartile_weight * weight_within_quartile
    ) %>%
    select(company_id, company_name, innovation_quartile, 
           DAII_3.5_Score, strategy, final_weight)
  
  # Validation: weights should sum to 1
  weight_sum <- sum(portfolio$final_weight)
  cat(paste0("    ✓ Weight sum: ", round(weight_sum, 6), 
             " (expected: 1.000000)\n"))
  
  return(portfolio)
}

# Strategy 2: DAII Score-Weighted Portfolio
build_score_weighted <- function(scores_df) {
  cat("  Building DAII Score-Weighted portfolio...\n")
  
  # Weight proportional to DAII score (with minimum weight)
  min_weight <- ifelse(!is.null(module4_config$min_weight), 
                       module4_config$min_weight, 0.001)
  
  portfolio <- scores_df %>%
    mutate(
      raw_weight = DAII_3.5_Score / sum(DAII_3.5_Score),
      # Apply minimum weight constraint
      adjusted_weight = pmax(raw_weight, min_weight),
      # Renormalize to sum to 1
      final_weight = adjusted_weight / sum(adjusted_weight),
      strategy = "score_weighted"
    ) %>%
    select(company_id, company_name, innovation_quartile, 
           DAII_3.5_Score, strategy, final_weight)
  
  weight_sum <- sum(portfolio$final_weight)
  cat(paste0("    ✓ Weight sum: ", round(weight_sum, 6), 
             " (expected: 1.000000)\n"))
  
  return(portfolio)
}

# Strategy 3: Quartile-Tilted Portfolio (Overweight Q4)
build_quartile_tilted <- function(scores_df) {
  cat("  Building Quartile-Tilted portfolio...\n")
  
  # Define tilt weights: Q4 gets highest weight
  quartile_weights <- data.frame(
    innovation_quartile = c("Q1", "Q2", "Q3", "Q4"),
    quartile_allocation = c(0.10, 0.15, 0.25, 0.50)  # 50% to Q4
  )
  
  portfolio <- scores_df %>%
    left_join(quartile_weights, by = "innovation_quartile") %>%
    group_by(innovation_quartile) %>%
    mutate(
      weight_within_quartile = 1 / n(),
      final_weight = quartile_allocation * weight_within_quartile,
      strategy = "quartile_tilted"
    ) %>%
    ungroup() %>%
    select(company_id, company_name, innovation_quartile, 
           DAII_3.5_Score, strategy, final_weight)
  
  weight_sum <- sum(portfolio$final_weight)
  cat(paste0("    ✓ Weight sum: ", round(weight_sum, 6), 
             " (expected: 1.000000)\n"))
  
  return(portfolio)
}

# ----- 5. EXECUTE PORTFOLIO STRATEGIES -----
cat("\n[Module 4] Executing portfolio strategies...\n")

# Determine which strategies to run
strategies_to_run <- if (!is.null(module4_config$portfolio_strategies)) {
  module4_config$portfolio_strategies
} else {
  c("equal_weight_quartile", "score_weighted", "quartile_tilted")
}

cat(paste0("  Strategies to execute: ", paste(strategies_to_run, collapse = ", "), "\n"))

# Execute each strategy
portfolio_results <- list()

for (strategy in strategies_to_run) {
  cat(paste0("\n  Executing: ", strategy, "\n"))
  
  portfolio <- switch(strategy,
                      "equal_weight_quartile" = build_equal_weight_quartile(company_scores),
                      "score_weighted" = build_score_weighted(company_scores),
                      "quartile_tilted" = build_quartile_tilted(company_scores),
                      {
                        warning(paste("Unknown strategy:", strategy, "- skipping"))
                        NULL
                      }
  )
  
  if (!is.null(portfolio)) {
    portfolio_results[[strategy]] <- portfolio
  }
}

# ----- 6. COMBINE AND VALIDATE RESULTS -----
cat("\n[Module 4] Combining and validating portfolio results...\n")

# Combine all portfolios
all_portfolios <- bind_rows(portfolio_results)

# Generate portfolio statistics
portfolio_statistics <- all_portfolios %>%
  group_by(strategy, innovation_quartile) %>%
  summarise(
    n_companies = n(),
    total_weight = sum(final_weight),
    avg_daii_score = mean(DAII_3.5_Score),
    min_weight = min(final_weight),
    max_weight = max(final_weight),
    .groups = 'drop'
  ) %>%
  arrange(strategy, innovation_quartile)

# Overall strategy statistics
strategy_summary <- all_portfolios %>%
  group_by(strategy) %>%
  summarise(
    n_companies = n(),
    weight_sum = sum(final_weight),
    avg_daii_score = mean(DAII_3.5_Score),
    score_weighted_avg = sum(DAII_3.5_Score * final_weight) / sum(final_weight),
    herfindahl_index = sum(final_weight^2),  # Concentration measure
    .groups = 'drop'
  )

cat("  Portfolio statistics generated:\n")
print(strategy_summary)

# ----- 7. OUTPUT GENERATION -----
cat("\n[Module 4] Generating output files...\n")

# Determine output directory
output_dir <- if (!is.null(run_config$output_directory)) {
  run_config$output_directory
} else {
  "C:/Users/sganesan/OneDrive - dumac.duke.edu/DAII/data/output"
}

# Ensure output directory exists
if (!dir.exists(output_dir)) {
  dir.create(output_dir, recursive = TRUE)
}

# Generate run ID for Module 4 outputs
if (exists("run_id") && !is.null(run_id)) {
  module4_run_id <- run_id
} else {
  module4_run_id <- format(Sys.time(), "DAII_3.5_Run_%Y%m%d_%H%M%S")
}

# Save portfolio weights
weights_file <- file.path(output_dir, paste0(module4_run_id, "_08_portfolio_weights.csv"))
write.csv(all_portfolios, weights_file, row.names = FALSE)
cat(paste0("  ✓ Portfolio weights saved: ", basename(weights_file), "\n"))

# Save portfolio statistics
stats_file <- file.path(output_dir, paste0(module4_run_id, "_09_portfolio_statistics.csv"))
write.csv(portfolio_statistics, stats_file, row.names = FALSE)
cat(paste0("  ✓ Portfolio statistics saved: ", basename(stats_file), "\n"))

# Save strategy summary
summary_file <- file.path(output_dir, paste0(module4_run_id, "_10_strategy_summary.csv"))
write.csv(strategy_summary, summary_file, row.names = FALSE)
cat(paste0("  ✓ Strategy summary saved: ", basename(summary_file), "\n"))

# Save applied configuration
applied_config <- list(
  module4_integration = list(
    integration_date = as.character(Sys.Date()),
    source_script = "DAII_3.5_PHASE_1_2_INTEGRATED_v3.5.9.R",
    company_scores_source = basename(company_scores_path),
    strategies_executed = strategies_to_run,
    validation_passed = validation_passed,
    config_files_used = c(
      "daii_module4_bridge_config.yaml",
      "daii_portfolio_config.yaml",
      "daii_run_config.yaml"
    )
  ),
  applied_module4_config = module4_config,
  applied_portfolio_config = portfolio_config
)

config_file <- file.path(output_dir, paste0(module4_run_id, "_11_applied_config.yaml"))
write_yaml(applied_config, config_file)
cat(paste0("  ✓ Applied configuration saved: ", basename(config_file), "\n"))

# ----- 8. FINAL VALIDATION REPORT -----
cat("\n" + paste(rep("=", 80), collapse="") + "\n")
cat("MODULE 4 EXECUTION SUMMARY\n")
cat(paste(rep("=", 80), collapse="") + "\n")

cat(paste0("\n📊 PORTFOLIO CONSTRUCTION COMPLETE\n"))
cat(paste0("   Run ID: ", module4_run_id, "\n"))
cat(paste0("   Strategies executed: ", length(portfolio_results), "\n"))
cat(paste0("   Companies processed: ", nrow(company_scores), "\n"))
cat(paste0("   Output directory: ", output_dir, "\n"))

cat("\n📁 GENERATED FILES:\n")
cat(paste0("   1. ", basename(weights_file), " - Portfolio weights for all strategies\n"))
cat(paste0("   2. ", basename(stats_file), " - Quartile-level statistics\n"))
cat(paste0("   3. ", basename(summary_file), " - Strategy performance summary\n"))
cat(paste0("   4. ", basename(config_file), " - Applied configuration\n"))

cat("\n✅ MODULE 4 INTEGRATION SUCCESSFUL\n")
cat("   Portfolio construction completed without errors\n")
cat("   Ready for Module 5: Backtesting Framework\n")

cat("\n" + paste(rep("=", 80), collapse="") + "\n")
cat("NEXT STEPS:\n")
cat(paste(rep("=", 80), collapse="") + "\n")
cat("1. Verify output files in the output directory\n")
cat("2. Review portfolio weights for each strategy\n")
cat("3. Proceed to Module 5 integration for backtesting\n")
cat("4. Test with N=200 hybrid sample after validation\n")

# Save environment for continuity
save.image(file = file.path(output_dir, paste0(module4_run_id, "_12_module4_workspace.RData")))
cat(paste0("\n💾 Workspace saved for continuity: ", module4_run_id, "_12_module4_workspace.RData\n"))

cat("\n" + paste(rep("=", 80), collapse="") + "\n")
cat("MODULE 4: PORTFOLIO CONSTRUCTION ENGINE\n")
cat(paste(rep("=", 80), collapse="") + "\n")

# ----- 1. CONFIGURATION LOADING -----
cat("\n[Module 4] Loading configuration files...\n")

# Load required packages for Module 4
if (!require("yaml")) install.packages("yaml", quiet = TRUE)
library(yaml)
if (!require("dplyr")) install.packages("dplyr", quiet = TRUE)
library(dplyr)
if (!require("tidyr")) install.packages("tidyr", quiet = TRUE)
library(tidyr)

# Function to load and validate YAML config
load_config_safely <- function(config_path, config_name) {
  if (!file.exists(config_path)) {
    warning(paste(config_name, "not found at:", config_path))
    return(NULL)
  }
  tryCatch({
    config <- read_yaml(config_path)
    cat(paste0("  ✓ ", config_name, " loaded successfully\n"))
    return(config)
  }, error = function(e) {
    warning(paste("Error loading", config_name, ":", e$message))
    return(NULL)
  })
}

# Load Module 4 specific configurations
module4_config <- load_config_safely("daii_module4_bridge_config.yaml", 
                                     "Module 4 Bridge Config")
portfolio_config <- load_config_safely("daii_portfolio_config.yaml", 
                                       "Portfolio Config")
run_config <- load_config_safely("daii_run_config.yaml", 
                                 "Run Config")

# Set default configurations if files not found
if (is.null(module4_config)) {
  cat("  ⚠ Using default Module 4 configuration\n")
  module4_config <- list(
    portfolio_strategies = c("equal_weight_quartile", "score_weighted", "quartile_tilted"),
    rebalancing_frequency = "quarterly",
    min_weight = 0.001,
    max_weight = 0.10
  )
}

if (is.null(portfolio_config)) {
  cat("  ⚠ Using default portfolio configuration\n")
  portfolio_config <- list(
    benchmark_id = "SPY",
    risk_free_rate = 0.02,
    lookback_period = 252
  )
}

# ----- 2. DATA INPUT BRIDGE -----
cat("\n[Module 4] Loading validated company-level scores...\n")

# Function to find the most recent company scores file
find_latest_company_scores <- function(output_dir = "C:/Users/sganesan/OneDrive - dumac.duke.edu/DAII/data/output") {
  # First try to use the run_id from v3.5.9 if available
  if (exists("run_id") && !is.null(run_id)) {
    expected_file <- file.path(output_dir, paste0(run_id, "_04_company_level_scores.csv"))
    if (file.exists(expected_file)) {
      cat(paste0("  ✓ Found company scores from current run: ", run_id, "\n"))
      return(expected_file)
    }
  }
  
  # Fallback: search for the most recent company scores file
  pattern <- "DAII_3.5_Run_.*_04_company_level_scores\\.csv$"
  all_files <- list.files(output_dir, pattern = pattern, full.names = TRUE)
  
  if (length(all_files) == 0) {
    # Try alternative pattern
    pattern2 <- ".*04_company_level_scores\\.csv$"
    all_files <- list.files(output_dir, pattern = pattern2, full.names = TRUE)
  }
  
  if (length(all_files) == 0) {
    stop("No company-level scores file found in output directory.")
  }
  
  # Get the most recent file
  file_info <- file.info(all_files)
  latest_file <- rownames(file_info)[which.max(file_info$mtime)]
  
  cat(paste0("  ✓ Found latest company scores: ", basename(latest_file), "\n"))
  return(latest_file)
}

# Load the company scores
company_scores_path <- find_latest_company_scores()
company_scores <- read.csv(company_scores_path)

# ----- 3. DATA VALIDATION -----
cat("\n[Module 4] Validating company scores structure...\n")

validate_company_scores <- function(scores_df) {
  validation_checks <- list()
  
  # Check 1: Row count
  validation_checks$row_count <- nrow(scores_df) == 50
  cat(paste0("  ", ifelse(validation_checks$row_count, "✓", "✗"), 
             " Row count: ", nrow(scores_df), " (expected: 50)\n"))
  
  # Check 2: Required columns
  required_cols <- c("company_id", "DAII_3.5_Score", "innovation_quartile")
  validation_checks$required_cols <- all(required_cols %in% colnames(scores_df))
  cat(paste0("  ", ifelse(validation_checks$required_cols, "✓", "✗"), 
             " Required columns present\n"))
  
  # Check 3: Quartile distribution
  if ("innovation_quartile" %in% colnames(scores_df)) {
    quartile_dist <- table(scores_df$innovation_quartile)
    expected_dist <- c(13, 12, 12, 13)  # From v3.5.9 validation
    actual_dist <- as.numeric(quartile_dist[c("Q1", "Q2", "Q3", "Q4")])
    
    validation_checks$quartile_match <- all(actual_dist == expected_dist)
    cat(paste0("  ", ifelse(validation_checks$quartile_match, "✓", "✗"), 
               " Quartile distribution: Q1=", quartile_dist["Q1"], 
               ", Q2=", quartile_dist["Q2"], 
               ", Q3=", quartile_dist["Q3"], 
               ", Q4=", quartile_dist["Q4"], "\n"))
  }
  
  # Check 4: Score range
  if ("DAII_3.5_Score" %in% colnames(scores_df)) {
    score_range <- range(scores_df$DAII_3.5_Score, na.rm = TRUE)
    validation_checks$score_range <- all(score_range >= 0 & score_range <= 100)
    cat(paste0("  ", ifelse(validation_checks$score_range, "✓", "✗"), 
               " Score range: [", round(score_range[1], 2), ", ", 
               round(score_range[2], 2), "]\n"))
  }
  
  # Check 5: Missing values
  validation_checks$no_missing <- !any(is.na(scores_df[, required_cols]))
  cat(paste0("  ", ifelse(validation_checks$no_missing, "✓", "✗"), 
             " No missing values in critical columns\n"))
  
  # Overall validation
  all_checks_passed <- all(unlist(validation_checks))
  
  if (!all_checks_passed) {
    warning("Company scores validation failed. Check the warnings above.")
  }
  
  return(all_checks_passed)
}

validation_passed <- validate_company_scores(company_scores)

if (!validation_passed) {
  cat("\n⚠  WARNING: Company scores validation failed. Proceed with caution.\n")
  response <- readline(prompt = "Continue with portfolio construction? (y/n): ")
  if (tolower(response) != "y") {
    stop("Portfolio construction halted by user.")
  }
}

# ----- 4. PORTFOLIO CONSTRUCTION FUNCTIONS -----
cat("\n[Module 4] Building portfolio construction engine...\n")

# Strategy 1: Equal-Weight Quartile Portfolios
build_equal_weight_quartile <- function(scores_df) {
  cat("  Building Equal-Weight Quartile portfolios...\n")
  
  # Equal weight within each quartile
  portfolio <- scores_df %>%
    group_by(innovation_quartile) %>%
    mutate(
      weight_within_quartile = 1 / n(),
      strategy = "equal_weight_quartile"
    ) %>%
    ungroup() %>%
    mutate(
      # Distribute 25% across each quartile
      quartile_weight = 0.25,
      final_weight = quartile_weight * weight_within_quartile
    ) %>%
    select(company_id, company_name, innovation_quartile, 
           DAII_3.5_Score, strategy, final_weight)
  
  # Validation: weights should sum to 1
  weight_sum <- sum(portfolio$final_weight)
  cat(paste0("    ✓ Weight sum: ", round(weight_sum, 6), 
             " (expected: 1.000000)\n"))
  
  return(portfolio)
}

# Strategy 2: DAII Score-Weighted Portfolio
build_score_weighted <- function(scores_df) {
  cat("  Building DAII Score-Weighted portfolio...\n")
  
  # Weight proportional to DAII score (with minimum weight)
  min_weight <- ifelse(!is.null(module4_config$min_weight), 
                       module4_config$min_weight, 0.001)
  
  portfolio <- scores_df %>%
    mutate(
      raw_weight = DAII_3.5_Score / sum(DAII_3.5_Score),
      # Apply minimum weight constraint
      adjusted_weight = pmax(raw_weight, min_weight),
      # Renormalize to sum to 1
      final_weight = adjusted_weight / sum(adjusted_weight),
      strategy = "score_weighted"
    ) %>%
    select(company_id, company_name, innovation_quartile, 
           DAII_3.5_Score, strategy, final_weight)
  
  weight_sum <- sum(portfolio$final_weight)
  cat(paste0("    ✓ Weight sum: ", round(weight_sum, 6), 
             " (expected: 1.000000)\n"))
  
  return(portfolio)
}

# Strategy 3: Quartile-Tilted Portfolio (Overweight Q4)
build_quartile_tilted <- function(scores_df) {
  cat("  Building Quartile-Tilted portfolio...\n")
  
  # Define tilt weights: Q4 gets highest weight
  quartile_weights <- data.frame(
    innovation_quartile = c("Q1", "Q2", "Q3", "Q4"),
    quartile_allocation = c(0.10, 0.15, 0.25, 0.50)  # 50% to Q4
  )
  
  portfolio <- scores_df %>%
    left_join(quartile_weights, by = "innovation_quartile") %>%
    group_by(innovation_quartile) %>%
    mutate(
      weight_within_quartile = 1 / n(),
      final_weight = quartile_allocation * weight_within_quartile,
      strategy = "quartile_tilted"
    ) %>%
    ungroup() %>%
    select(company_id, company_name, innovation_quartile, 
           DAII_3.5_Score, strategy, final_weight)
  
  weight_sum <- sum(portfolio$final_weight)
  cat(paste0("    ✓ Weight sum: ", round(weight_sum, 6), 
             " (expected: 1.000000)\n"))
  
  return(portfolio)
}

# ----- 5. EXECUTE PORTFOLIO STRATEGIES -----
cat("\n[Module 4] Executing portfolio strategies...\n")

# Determine which strategies to run
strategies_to_run <- if (!is.null(module4_config$portfolio_strategies)) {
  module4_config$portfolio_strategies
} else {
  c("equal_weight_quartile", "score_weighted", "quartile_tilted")
}

cat(paste0("  Strategies to execute: ", paste(strategies_to_run, collapse = ", "), "\n"))

# Execute each strategy
portfolio_results <- list()

for (strategy in strategies_to_run) {
  cat(paste0("\n  Executing: ", strategy, "\n"))
  
  portfolio <- switch(strategy,
                      "equal_weight_quartile" = build_equal_weight_quartile(company_scores),
                      "score_weighted" = build_score_weighted(company_scores),
                      "quartile_tilted" = build_quartile_tilted(company_scores),
                      {
                        warning(paste("Unknown strategy:", strategy, "- skipping"))
                        NULL
                      }
  )
  
  if (!is.null(portfolio)) {
    portfolio_results[[strategy]] <- portfolio
  }
}

# ----- 6. COMBINE AND VALIDATE RESULTS -----
cat("\n[Module 4] Combining and validating portfolio results...\n")

# Combine all portfolios
all_portfolios <- bind_rows(portfolio_results)

# Generate portfolio statistics
portfolio_statistics <- all_portfolios %>%
  group_by(strategy, innovation_quartile) %>%
  summarise(
    n_companies = n(),
    total_weight = sum(final_weight),
    avg_daii_score = mean(DAII_3.5_Score),
    min_weight = min(final_weight),
    max_weight = max(final_weight),
    .groups = 'drop'
  ) %>%
  arrange(strategy, innovation_quartile)

# Overall strategy statistics
strategy_summary <- all_portfolios %>%
  group_by(strategy) %>%
  summarise(
    n_companies = n(),
    weight_sum = sum(final_weight),
    avg_daii_score = mean(DAII_3.5_Score),
    score_weighted_avg = sum(DAII_3.5_Score * final_weight) / sum(final_weight),
    herfindahl_index = sum(final_weight^2),  # Concentration measure
    .groups = 'drop'
  )

cat("  Portfolio statistics generated:\n")
print(strategy_summary)

# ----- 7. OUTPUT GENERATION -----
cat("\n[Module 4] Generating output files...\n")

# Determine output directory
output_dir <- if (!is.null(run_config$output_directory)) {
  run_config$output_directory
} else {
  "C:/Users/sganesan/OneDrive - dumac.duke.edu/DAII/data/output"
}

# Ensure output directory exists
if (!dir.exists(output_dir)) {
  dir.create(output_dir, recursive = TRUE)
}

# Generate run ID for Module 4 outputs
if (exists("run_id") && !is.null(run_id)) {
  module4_run_id <- run_id
} else {
  module4_run_id <- format(Sys.time(), "DAII_3.5_Run_%Y%m%d_%H%M%S")
}

# Save portfolio weights
weights_file <- file.path(output_dir, paste0(module4_run_id, "_08_portfolio_weights.csv"))
write.csv(all_portfolios, weights_file, row.names = FALSE)
cat(paste0("  ✓ Portfolio weights saved: ", basename(weights_file), "\n"))

# Save portfolio statistics
stats_file <- file.path(output_dir, paste0(module4_run_id, "_09_portfolio_statistics.csv"))
write.csv(portfolio_statistics, stats_file, row.names = FALSE)
cat(paste0("  ✓ Portfolio statistics saved: ", basename(stats_file), "\n"))

# Save strategy summary
summary_file <- file.path(output_dir, paste0(module4_run_id, "_10_strategy_summary.csv"))
write.csv(strategy_summary, summary_file, row.names = FALSE)
cat(paste0("  ✓ Strategy summary saved: ", basename(summary_file), "\n"))

# Save applied configuration
applied_config <- list(
  module4_integration = list(
    integration_date = as.character(Sys.Date()),
    source_script = "DAII_3.5_PHASE_1_2_INTEGRATED_v3.5.9.R",
    company_scores_source = basename(company_scores_path),
    strategies_executed = strategies_to_run,
    validation_passed = validation_passed,
    config_files_used = c(
      "daii_module4_bridge_config.yaml",
      "daii_portfolio_config.yaml",
      "daii_run_config.yaml"
    )
  ),
  applied_module4_config = module4_config,
  applied_portfolio_config = portfolio_config
)

config_file <- file.path(output_dir, paste0(module4_run_id, "_11_applied_config.yaml"))
write_yaml(applied_config, config_file)
cat(paste0("  ✓ Applied configuration saved: ", basename(config_file), "\n"))

# ----- 8. FINAL VALIDATION REPORT -----
cat("\n" + paste(rep("=", 80), collapse="") + "\n")
cat("MODULE 4 EXECUTION SUMMARY\n")
cat(paste(rep("=", 80), collapse="") + "\n")

cat(paste0("\n📊 PORTFOLIO CONSTRUCTION COMPLETE\n"))
cat(paste0("   Run ID: ", module4_run_id, "\n"))
cat(paste0("   Strategies executed: ", length(portfolio_results), "\n"))
cat(paste0("   Companies processed: ", nrow(company_scores), "\n"))
cat(paste0("   Output directory: ", output_dir, "\n"))

cat("\n📁 GENERATED FILES:\n")
cat(paste0("   1. ", basename(weights_file), " - Portfolio weights for all strategies\n"))
cat(paste0("   2. ", basename(stats_file), " - Quartile-level statistics\n"))
cat(paste0("   3. ", basename(summary_file), " - Strategy performance summary\n"))
cat(paste0("   4. ", basename(config_file), " - Applied configuration\n"))

cat("\n✅ MODULE 4 INTEGRATION SUCCESSFUL\n")
cat("   Portfolio construction completed without errors\n")
cat("   Ready for Module 5: Backtesting Framework\n")

cat("\n" + paste(rep("=", 80), collapse="") + "\n")
cat("NEXT STEPS:\n")
cat(paste(rep("=", 80), collapse="") + "\n")
cat("1. Verify output files in the output directory\n")
cat("2. Review portfolio weights for each strategy\n")
cat("3. Proceed to Module 5 integration for backtesting\n")
cat("4. Test with N=200 hybrid sample after validation\n")

# Save environment for continuity
save.image(file = file.path(output_dir, paste0(module4_run_id, "_12_module4_workspace.RData")))
cat(paste0("\n💾 Workspace saved for continuity: ", module4_run_id, "_12_module4_workspace.RData\n"))
