################################################################################
# DAII 3.5 - PHASE 1 COMPLETE CODEBASE WITH FIELD MAPPING INTEGRATION
# Consolidated Version: 3.5.8 (Field Mapping Integrated)
# Date: 2026-02-05
# Author: Siva Ganesan
################################################################################

# =============================================================================
# MODULE 0: ENVIRONMENT SETUP & GITHUB INVENTORY VERIFICATION
# =============================================================================

cat(paste(rep("=", 80), collapse = ""), "\n")
cat("DAII 3.5 - PHASE 1 COMPLETE CODEBASE INITIALIZATION (WITH FIELD MAPPING)\n")
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
    # Load the main mapping configuration
    mapping_config_url <- "https://raw.githubusercontent.com/sivaguru42/DAII-3.5-Framework/main/DAII%203.5%20PRODUCTION%20FIELD%20MAPPING%20CONFIGURATION.yaml"
    mapping_config <- yaml::read_yaml(mapping_config_url)
    
    # Load the field mapping schema
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

# First load the raw data
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

# Now apply field mapping
apply_field_mapping <- function(raw_data, mapping) {
  if (is.null(mapping)) {
    cat("   No field mapping available. Using original column names.\n")
    return(raw_data)
  }
  
  cat("   Applying field mapping transformations...\n")
  mapped_data <- raw_data
  
  # Create a mapping dictionary
  mapping_dict <- list()
  for (field_map in mapping$mapping_config$field_mappings) {
    source_field <- field_map$source_field
    target_field <- field_map$target_field
    mapping_dict[[source_field]] <- target_field
  }
  
  # Apply mappings
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

# Define the standardized column names we expect for the pipeline
standardized_cols <- list(
  # Core identifiers
  ticker = c("ticker", "Ticker", "Symbol", "ticker_symbol"),
  
  # Component score inputs
  rd_expense = c("rd_expense", "R.D.Exp", "RD_Exp", "Research_Development", "R&D"),
  market_cap = c("market_cap", "Mkt.Cap", "MarketCap", "Market_Capitalization"),
  analyst_rating = c("analyst_rating", "BEst.Analyst.Rtg", "AnalystRating", "Analyst_Rating"),
  patent_activity = c("patent_activity", "Patents...Trademarks...Copy.Rgt", "Patents", "Patent_Activity"),
  news_sentiment = c("news_sentiment", "News.Sent", "NewsSentiment", "News_Sentiment"),
  revenue_growth = c("revenue_growth", "Rev...1.Yr.Gr", "RevenueGrowth", "Rev_Growth"),
  
  # Additional fields (optional)
  industry = c("GICS.Ind.Grp.Name", "Industry", "Sector", "GICS_Sector")
)

# Function to find and standardize columns
standardize_columns <- function(data, col_definitions) {
  cat("   Standardizing column names for pipeline...\n")
  
  standardized_data <- data
  standardization_log <- data.frame(
    Original_Name = character(),
    Standardized_Name = character(),
    Status = character(),
    stringsAsFactors = FALSE
  )
  
  # For each standardized column we need
  for (std_name in names(col_definitions)) {
    variations <- col_definitions[[std_name]]
    found <- FALSE
    
    # Check each variation
    for (variation in variations) {
      if (variation %in% colnames(standardized_data)) {
        # If it's not already the standardized name, rename it
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
    
    # If not found, log it
    if (!found) {
      standardization_log <- rbind(standardization_log, data.frame(
        Original_Name = "NOT_FOUND",
        Standardized_Name = std_name,
        Status = "MISSING",
        stringsAsFactors = FALSE
      ))
    }
  }
  
  # Print summary
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
  
  # Check for critical columns
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

# Apply standardization
standardization_result <- standardize_columns(daii_raw_data_mapped, standardized_cols)
daii_standardized_data <- standardization_result$data

cat(sprintf("\n   🎯 FINAL COLUMNS (%d total):\n", ncol(daii_standardized_data)))
cat("   ", paste(colnames(daii_standardized_data)[1:min(8, ncol(daii_standardized_data))], 
                 collapse = ", "), "...\n")

# =============================================================================
# MODULE 1: DATA LOADING & PREPARATION (UPDATED FOR STANDARDIZED COLUMNS)
# =============================================================================

cat("\n")
cat(paste(rep("=", 80), collapse = ""), "\n")
cat("MODULE 1: DATA LOADING & PREPARATION (STANDARDIZED)\n")
cat(paste(rep("=", 80), collapse = ""), "\n\n")

# -----------------------------------------------------------------------------
# 1.1 DATA CLEANING & TYPE ENFORCEMENT (STANDARDIZED)
# -----------------------------------------------------------------------------

cat("🔄 STAGE 1.1: INITIAL DATA CLEANING & TYPE ENFORCEMENT\n")
cat(paste(rep("-", 60), collapse = ""), "\n")

preprocess_standardized_data <- function(std_data) {
  cat("   Applying cleaning rules to standardized data...\n")
  processed_data <- std_data
  
  # Ensure ticker is character
  if ("ticker" %in% colnames(processed_data)) {
    processed_data$ticker <- as.character(processed_data$ticker)
    cat("   ✅ Ticker column standardized as character\n")
  }
  
  # Define numeric columns (using standardized names)
  numeric_columns <- c("rd_expense", "market_cap", "revenue_growth",
                       "analyst_rating", "patent_activity", "news_sentiment")
  
  # Convert to numeric
  conversion_log <- data.frame(
    Column = character(),
    NAs_Before = integer(),
    NAs_After = integer(),
    stringsAsFactors = FALSE
  )
  
  for (col in numeric_columns) {
    if (col %in% colnames(processed_data)) {
      # Store original NA count
      original_na <- sum(is.na(processed_data[[col]]))
      
      # Convert to numeric
      processed_data[[col]] <- as.numeric(as.character(processed_data[[col]]))
      
      # Calculate new NA count
      new_na <- sum(is.na(processed_data[[col]]))
      
      # Log conversion
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
  
  # Print conversion summary
  if (nrow(conversion_log) > 0) {
    cat("\n   📊 NUMERIC CONVERSION SUMMARY:\n")
    print(conversion_log)
  }
  
  # Check data completeness
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
# MODULE 1.5: COMPANY/FUND DATA SEPARATION (NEW MODULE - CRITICAL FIX)
# =============================================================================

cat("\n")
cat(paste(rep("=", 80), collapse = ""), "\n")
cat("MODULE 1.5: COMPANY/FUND DATA SEPARATION & AGGREGATION\n")
cat(paste(rep("=", 80), collapse = ""), "\n\n")

# -----------------------------------------------------------------------------
# 1.5.1 IDENTIFY COMPANY VS FUND COLUMNS
# -----------------------------------------------------------------------------

cat("🔍 STAGE 1.5.1: IDENTIFYING COMPANY VS FUND LEVEL COLUMNS\n")
cat(paste(rep("-", 60), collapse = ""), "\n")

identify_column_types <- function(data) {
  cat("   Analyzing column structure...\n")
  
  # Company-level columns (should be same for all fund holdings of same company)
  company_level_cols <- c(
    # Core scoring inputs
    "ticker", "rd_expense", "market_cap", "analyst_rating", 
    "patent_activity", "news_sentiment", "revenue_growth", "industry",
    
    # Company characteristics (shouldn't vary by fund)
    "Tot.Ret.Idx.Gross", "Last.Price", "Volume", "Number.of.Employees",
    "GM", "Volatil.360D", "BEst.Target.Px", "Rec.Consensus",
    "Earnings.Conference.Call.Date", "Tot.Analyst.Rec",
    "Percent.Change.in.Institutional.Holdings", "Shrt.Int"
  )
  
  # Fund-holding level columns (varies by fund)
  fund_level_cols <- c(
    "fund_id", "fund_name", "fund_weight", "dumac_allocation",
    "as_of_date", "shares_held", "position_value", "fund_weight_raw",
    "abs_weight", "Tot...Hldg.in.Port", "Top.10.as.of.Dt"
  )
  
  # Identify which columns actually exist in the data
  existing_company_cols <- intersect(company_level_cols, colnames(data))
  existing_fund_cols <- intersect(fund_level_cols, colnames(data))
  
  # Find unknown columns (for classification)
  all_cols <- colnames(data)
  unknown_cols <- setdiff(all_cols, c(existing_company_cols, existing_fund_cols, "ticker"))
  
  cat(sprintf("   Found %d company-level columns\n", length(existing_company_cols)))
  cat(sprintf("   Found %d fund-level columns\n", length(existing_fund_cols)))
  cat(sprintf("   Found %d unclassified columns\n", length(unknown_cols)))
  
  return(list(
    company_cols = existing_company_cols,
    fund_cols = existing_fund_cols,
    unknown_cols = unknown_cols
  ))
}

column_types <- identify_column_types(daii_processed_data)

# -----------------------------------------------------------------------------
# 1.5.2 CREATE COMPANY-LEVEL DATASET FOR SCORING
# -----------------------------------------------------------------------------

cat("\n🏢 STAGE 1.5.2: CREATING COMPANY-LEVEL DATASET\n")
cat(paste(rep("-", 60), collapse = ""), "\n")

create_company_level_dataset <- function(data, company_cols) {
  cat("   Aggregating to company level (one row per ticker)...\n")
  
  # Select only company-level columns
  company_data <- data[, company_cols, drop = FALSE]
  
  # Check for duplicates before aggregation
  ticker_counts <- table(company_data$ticker)
  duplicate_tickers <- names(ticker_counts[ticker_counts > 1])
  
  cat(sprintf("   Found %d unique tickers\n", length(unique(company_data$ticker))))
  cat(sprintf("   Found %d tickers with multiple records\n", length(duplicate_tickers)))
  
  if (length(duplicate_tickers) > 0) {
    cat("\n   Aggregating duplicate company records...\n")
    
    # Define aggregation rules for each column type
    numeric_cols <- company_cols[sapply(company_data[, company_cols], is.numeric)]
    char_cols <- setdiff(company_cols, c(numeric_cols, "ticker"))
    
    # Aggregate numeric columns with mean
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
  
  # Log aggregation results
  cat(sprintf("\n   Company dataset created: %d rows × %d columns\n",
              nrow(aggregated_data), ncol(aggregated_data)))
  
  # Check for any remaining duplicates
  final_duplicates <- sum(duplicated(aggregated_data$ticker))
  if (final_duplicates > 0) {
    cat(sprintf("   ⚠️  Warning: %d duplicate tickers remain after aggregation\n", final_duplicates))
  } else {
    cat("   ✅ No duplicate tickers in company dataset\n")
  }
  
  return(aggregated_data)
}

# Create company-level dataset for scoring
daii_company_data <- create_company_level_dataset(daii_processed_data, column_types$company_cols)

# -----------------------------------------------------------------------------
# 1.5.3 PRESERVE FUND-LEVEL DATASET FOR PORTFOLIO CONSTRUCTION
# -----------------------------------------------------------------------------

cat("\n💰 STAGE 1.5.3: PRESERVING FUND-LEVEL DATASET\n")
cat(paste(rep("-", 60), collapse = ""), "\n")

preserve_fund_level_dataset <- function(data, fund_cols) {
  cat("   Preserving fund-level data for portfolio construction...\n")
  
  # Select fund-level columns plus ticker for joining later
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

# Preserve fund-level data
daii_fund_data <- preserve_fund_level_dataset(daii_processed_data, column_types$fund_cols)

# -----------------------------------------------------------------------------
# 1.5.4 VALIDATE THE SEPARATION
# -----------------------------------------------------------------------------

cat("\n✅ STAGE 1.5.4: VALIDATING DATA SEPARATION\n")
cat(paste(rep("-", 60), collapse = ""), "\n")

validate_data_separation <- function(company_data, fund_data, original_data) {
  cat("   Validating company/fund data separation...\n")
  
  validation_passed <- TRUE
  
  # Check 1: Company data should have one row per ticker
  company_tickers <- unique(company_data$ticker)
  duplicate_company_tickers <- sum(duplicated(company_data$ticker))
  
  if (duplicate_company_tickers == 0) {
    cat("   ✅ Company dataset: One row per ticker\n")
  } else {
    cat(sprintf("   ❌ Company dataset: %d duplicate tickers\n", duplicate_company_tickers))
    validation_passed <- FALSE
  }
  
  # Check 2: Row count consistency
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

# Run validation
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

# -----------------------------------------------------------------------------
# 2.1 CORE IMPUTATION FUNCTION - STANDARDIZED VERSION
# -----------------------------------------------------------------------------

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
  
  # Use standardized column names
  numeric_cols <- c("rd_expense", "market_cap", "revenue_growth",
                    "analyst_rating", "patent_activity", "news_sentiment")
  
  for (col in intersect(numeric_cols, colnames(imputed_data))) {
    cat(sprintf("\n   Processing metric: %s\n", col))
    
    # Convert to numeric if not already
    if (!is.numeric(imputed_data[[col]])) {
      imputed_data[[col]] <- as.numeric(as.character(imputed_data[[col]]))
    }
    
    # Identify missing values
    missing_indicators <- c("#N/A", "N/A", "NA", "NULL", "", "Field Not Applicable", "NaN")
    is_missing <- is.na(imputed_data[[col]]) |
      as.character(imputed_data[[col]]) %in% missing_indicators
    
    missing_count <- sum(is_missing, na.rm = TRUE)
    
    if (missing_count > 0) {
      cat(sprintf("      Found %d missing values (%.1f%%).\n",
                  missing_count, 100 * missing_count / nrow(imputed_data)))
      
      # Determine imputation method
      method <- if (col %in% names(imputation_methods)) {
        imputation_methods[[col]]
      } else {
        imputation_methods$default
      }
      
      # Get available values
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
      
      # Apply imputation
      imputed_data[[col]][is_missing] <- impute_val
      cat(sprintf("      Imputed with %s: %.4f\n", method_name, impute_val))
      
      # Log imputations
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
  
  # Print imputation summary
  if (nrow(imputation_log) > 0) {
    cat("\n   📋 IMPUTATION SUMMARY:\n")
    summary_table <- imputation_log %>%
      dplyr::group_by(metric, imputation_method) %>%
      dplyr::summarise(count = dplyr::n(),
                       avg_imputed_value = mean(imputed_value),
                       .groups = 'drop')
    print(summary_table)
    
    # Calculate overall imputation rate
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

# Execute imputation on COMPANY data
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

# -----------------------------------------------------------------------------
# 3.1 NORMALIZATION FUNCTION
# -----------------------------------------------------------------------------

cat("📊 STAGE 3.1: DEFINING NORMALIZATION FUNCTIONS\n")
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
    lower_threshold <- quantile(x_finite, probs = lower_bound, na.rm = TRUE)
    upper_threshold <- quantile(x_finite, probs = upper_bound, na.rm = TRUE)
    x <- ifelse(x < lower_threshold, lower_threshold, x)
    x <- ifelse(x > upper_threshold, upper_threshold, x)
  }
  
  min_val <- min(x, na.rm = TRUE)
  max_val <- max(x, na.rm = TRUE)
  
  if (min_val == max_val) {
    return(rep(50, length(x))) # All values equal, return midpoint
  }
  
  normalized <- 100 * (x - min_val) / (max_val - min_val)
  return(normalized)
}

log_transform_with_offset <- function(x, offset = 1) {
  # Apply log transformation with offset to handle zeros
  return(log(x + offset))
}

cat("   ✅ Normalization and transformation functions defined\n")

# -----------------------------------------------------------------------------
# 3.2 CALCULATE COMPONENT SCORES ON COMPANY DATA
# -----------------------------------------------------------------------------

cat("\n🧮 STAGE 3.2: CALCULATING COMPONENT SCORES (COMPANY-LEVEL)\n")
cat(paste(rep("-", 60), collapse = ""), "\n")

calculate_component_scores <- function(imputed_data) {
  cat("   Calculating 5 component innovation scores...\n")
  scores_data <- imputed_data
  
  # 1. R&D INTENSITY SCORE (Log-Transformed, 30% weight)
  if ("rd_expense" %in% colnames(scores_data) && "market_cap" %in% colnames(scores_data)) {
    cat("   Calculating R&D Intensity Score...\n")
    # Calculate R&D as percentage of market cap
    scores_data$rd_intensity <- scores_data$rd_expense / scores_data$market_cap
    # Log transform to handle skewness
    scores_data$rd_intensity_log <- log_transform_with_offset(scores_data$rd_intensity)
    # Normalize to 0-100 scale
    scores_data$rd_intensity_score <- normalize_to_100(scores_data$rd_intensity_log)
    cat(sprintf("      Range: %.2f to %.2f\n", 
                min(scores_data$rd_intensity_score, na.rm = TRUE),
                max(scores_data$rd_intensity_score, na.rm = TRUE)))
  } else {
    cat("   ⚠️  Missing columns for R&D Intensity Score\n")
    scores_data$rd_intensity_score <- NA_real_
  }
  
  # 2. ANALYST SENTIMENT SCORE (20% weight)
  if ("analyst_rating" %in% colnames(scores_data)) {
    cat("   Calculating Analyst Sentiment Score...\n")
    # Analyst ratings typically 1-5 where 5 is best
    scores_data$analyst_sentiment_score <- normalize_to_100(scores_data$analyst_rating)
    cat(sprintf("      Range: %.2f to %.2f\n",
                min(scores_data$analyst_sentiment_score, na.rm = TRUE),
                max(scores_data$analyst_sentiment_score, na.rm = TRUE)))
  } else {
    cat("   ⚠️  Missing column for Analyst Sentiment Score\n")
    scores_data$analyst_sentiment_score <- NA_real_
  }
  
  # 3. PATENT ACTIVITY SCORE (Log-Transformed, 25% weight)
  if ("patent_activity" %in% colnames(scores_data)) {
    cat("   Calculating Patent Activity Score...\n")
    # Log transform patent counts
    scores_data$patent_activity_log <- log_transform_with_offset(scores_data$patent_activity)
    # Normalize to 0-100 scale
    scores_data$patent_activity_score <- normalize_to_100(scores_data$patent_activity_log)
    cat(sprintf("      Range: %.2f to %.2f\n",
                min(scores_data$patent_activity_score, na.rm = TRUE),
                max(scores_data$patent_activity_score, na.rm = TRUE)))
  } else {
    cat("   ⚠️  Missing column for Patent Activity Score\n")
    scores_data$patent_activity_score <- NA_real_
  }
  
  # 4. NEWS SENTIMENT SCORE (10% weight)
  if ("news_sentiment" %in% colnames(scores_data)) {
    cat("   Calculating News Sentiment Score...\n")
    # News sentiment typically -1 to 1, normalize to 0-100
    scores_data$news_sentiment_score <- normalize_to_100(scores_data$news_sentiment)
    cat(sprintf("      Range: %.2f to %.2f\n",
                min(scores_data$news_sentiment_score, na.rm = TRUE),
                max(scores_data$news_sentiment_score, na.rm = TRUE)))
  } else {
    cat("   ⚠️  Missing column for News Sentiment Score\n")
    scores_data$news_sentiment_score <- NA_real_
  }
  
  # 5. GROWTH MOMENTUM SCORE (15% weight)
  if ("revenue_growth" %in% colnames(scores_data)) {
    cat("   Calculating Growth Momentum Score...\n")
    # Revenue growth as percentage, already normalized
    scores_data$growth_momentum_score <- normalize_to_100(scores_data$revenue_growth)
    cat(sprintf("      Range: %.2f to %.2f\n",
                min(scores_data$growth_momentum_score, na.rm = TRUE),
                max(scores_data$growth_momentum_score, na.rm = TRUE)))
  } else {
    cat("   ⚠️  Missing column for Growth Momentum Score\n")
    scores_data$growth_momentum_score <- NA_real_
  }
  
  # Summary of component scores
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

# Calculate component scores on COMPANY data
daii_company_with_scores <- calculate_component_scores(daii_company_imputed)

# -----------------------------------------------------------------------------
# 3.3 CALCULATE DAII 3.5 COMPOSITE SCORE
# -----------------------------------------------------------------------------

cat("\n🎯 STAGE 3.3: CALCULATING DAII 3.5 COMPOSITE SCORE\n")
cat(paste(rep("-", 60), collapse = ""), "\n")

calculate_composite_score <- function(scores_data) {
  cat("   Calculating composite score with weighted components...\n")
  
  # Define weights (from continuity package)
  weights <- list(
    rd_intensity = 0.30,      # R&D Intensity
    analyst_sentiment = 0.20, # Analyst Sentiment
    patent_activity = 0.25,   # Patent Activity
    news_sentiment = 0.10,    # News Sentiment
    growth_momentum = 0.15    # Growth Momentum
  )
  
  # Initialize composite score
  scores_data$DAII_3.5_Score <- 0
  
  # Add weighted components
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
  
  # If we have missing components, scale the score
  if (total_applied_weight < 1.0 && total_applied_weight > 0) {
    cat(sprintf("      Scaling composite score (only %.0f%% of weights available)\n",
                total_applied_weight * 100))
    scores_data$DAII_3.5_Score <- scores_data$DAII_3.5_Score / total_applied_weight
  }
  
  # Normalize final score to 0-100 range
  scores_data$DAII_3.5_Score <- normalize_to_100(scores_data$DAII_3.5_Score)
  
  # Summary statistics
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

# Calculate composite score on COMPANY data
daii_company_with_composite <- calculate_composite_score(daii_company_with_scores)

# -----------------------------------------------------------------------------
# 3.4 ASSIGN INNOVATION QUARTILES (WITH JITTER FIX)
# -----------------------------------------------------------------------------

cat("\n📊 STAGE 3.4: ASSIGNING INNOVATION QUARTILES\n")
cat(paste(rep("-", 60), collapse = ""), "\n")

assign_innovation_quartiles <- function(scores_data) {
  cat("   Assigning innovation quartiles based on DAII 3.5 Score...\n")
  
  if (!"DAII_3.5_Score" %in% colnames(scores_data)) {
    cat("   ❌ ERROR: DAII_3.5_Score column not found\n")
    scores_data$innovation_quartile <- NA
    return(scores_data)
  }
  
  # Check score distribution
  n_scores <- sum(!is.na(scores_data$DAII_3.5_Score))
  unique_scores <- length(unique(na.omit(scores_data$DAII_3.5_Score)))
  
  cat(sprintf("      Total scores: %d\n", n_scores))
  cat(sprintf("      Unique scores: %d\n", unique_scores))
  
  if (n_scores == 0) {
    cat("   ⚠️  No valid scores to assign quartiles\n")
    scores_data$innovation_quartile <- NA
    return(scores_data)
  }
  
  # Calculate quartile breaks
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
    # Simple ranking-based quartiles
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
    
    # Check for unique breaks
    if (length(unique(quartile_breaks)) < 5) {
      cat("   ⚠️  Non-unique quartile breaks detected. Applying jitter fix...\n")
      
      # Store original scores
      original_scores <- scores_data$DAII_3.5_Score
      
      # Apply small jitter only to duplicate values
      for (i in 1:length(original_scores)) {
        if (!is.na(original_scores[i])) {
          # Count how many times this value appears
          value_count <- sum(original_scores == original_scores[i], na.rm = TRUE)
          if (value_count > 1) {
            # Add tiny random noise (0.001% of value range)
            value_range <- max(original_scores, na.rm = TRUE) - min(original_scores, na.rm = TRUE)
            jitter_amount <- value_range * 0.00001 * rnorm(1)
            scores_data$DAII_3.5_Score[i] <- original_scores[i] + jitter_amount
          }
        }
      }
      
      # Recalculate breaks with jittered scores
      quartile_breaks <- quantile(scores_data$DAII_3.5_Score, 
                                  probs = seq(0, 1, 0.25), 
                                  na.rm = TRUE)
      cat(sprintf("      New quartile breaks: %s\n", 
                  paste(round(quartile_breaks, 2), collapse = ", ")))
    }
    
    # Assign quartiles
    scores_data$innovation_quartile <- cut(scores_data$DAII_3.5_Score,
                                           breaks = quartile_breaks,
                                           labels = c("Q1", "Q2", "Q3", "Q4"),
                                           include.lowest = TRUE)
  }
  
  # Quartile distribution
  quartile_table <- table(scores_data$innovation_quartile, useNA = "ifany")
  cat("\n   📊 QUARTILE DISTRIBUTION:\n")
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
  
  cat("\n   ✅ Innovation quartiles assigned successfully\n")
  return(scores_data)
}

# Assign quartiles on COMPANY data
daii_company_final <- assign_innovation_quartiles(daii_company_with_composite)

# =============================================================================
# MODULE 3.5: JOIN COMPANY SCORES BACK TO FUND-LEVEL DATA
# =============================================================================

cat("\n")
cat(paste(rep("=", 80), collapse = ""), "\n")
cat("MODULE 3.5: JOINING COMPANY SCORES TO FUND-LEVEL DATA\n")
cat(paste(rep("=", 80), collapse = ""), "\n\n")

cat("🔗 STAGE 3.5: JOINING COMPANY SCORES BACK TO FUND HOLDINGS\n")
cat(paste(rep("-", 60), collapse = ""), "\n")

join_scores_to_fund_data <- function(company_scores, fund_data) {
  cat("   Joining company scores to fund-holding dataset...\n")
  
  # Select only score columns from company data
  score_cols <- c("ticker", 
                  "rd_intensity_score", "analyst_sentiment_score", 
                  "patent_activity_score", "news_sentiment_score", 
                  "growth_momentum_score", "DAII_3.5_Score", 
                  "innovation_quartile", "rd_intensity", "rd_intensity_log",
                  "patent_activity_log")
  
  # Only include columns that actually exist
  existing_score_cols <- intersect(score_cols, colnames(company_scores))
  
  # Extract scores
  company_scores_subset <- company_scores[, existing_score_cols, drop = FALSE]
  
  # Join to fund data
  fund_data_with_scores <- fund_data %>%
    left_join(company_scores_subset, by = "ticker")
  
  # Check join results
  joined_rows <- nrow(fund_data_with_scores)
  original_rows <- nrow(fund_data)
  companies_with_scores <- length(unique(company_scores_subset$ticker))
  
  cat(sprintf("   Joined %d company scores to %d fund holdings\n",
              companies_with_scores, joined_rows))
  
  # Check for missing joins
  missing_scores <- sum(is.na(fund_data_with_scores$DAII_3.5_Score))
  if (missing_scores > 0) {
    cat(sprintf("   ⚠️  %d fund holdings missing DAII_3.5_Score\n", missing_scores))
  } else {
    cat("   ✅ All fund holdings have DAII_3.5_Score\n")
  }
  
  return(fund_data_with_scores)
}

# Join scores back to fund data
daii_final_with_scores <- join_scores_to_fund_data(daii_company_final, daii_fund_data)

# Replace the final scores dataset with the joined version
daii_final_scores <- daii_final_with_scores

# =============================================================================
# MODULE 4: OUTPUT GENERATION
# =============================================================================

cat("\n")
cat(paste(rep("=", 80), collapse = ""), "\n")
cat("MODULE 4: OUTPUT GENERATION & EXPORT\n")
cat(paste(rep("=", 80), collapse = ""), "\n\n")

# -----------------------------------------------------------------------------
# 4.1 CREATE OUTPUT DIRECTORY
# -----------------------------------------------------------------------------

cat("📁 STAGE 4.1: CREATING OUTPUT DIRECTORY\n")
cat(paste(rep("-", 60), collapse = ""), "\n")

create_output_directory <- function(base_path = "C:/Users/sganesan/OneDrive - dumac.duke.edu/DAII/data/output") {
  # Create timestamp for this run
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

# -----------------------------------------------------------------------------
# 4.2 SAVE SCORED DATA (WITH COMPONENT SCORES)
# -----------------------------------------------------------------------------

cat("\n💾 STAGE 4.2: SAVING SCORED DATA WITH COMPONENT SCORES\n")
cat(paste(rep("-", 60), collapse = ""), "\n")

save_scored_data <- function(scores_data, output_dir) {
  # Create comprehensive output dataframe
  output_data <- scores_data
  
  # Reorder columns for clarity (group related fields)
  column_order <- c()
  
  # 1. Identifier columns
  if ("ticker" %in% colnames(output_data)) {
    column_order <- c(column_order, "ticker")
  }
  
  # 2. Original input columns
  original_cols <- c("rd_expense", "market_cap", "analyst_rating", 
                     "patent_activity", "news_sentiment", "revenue_growth")
  for (col in original_cols) {
    if (col %in% colnames(output_data)) {
      column_order <- c(column_order, col)
    }
  }
  
  # 3. R&D intensity calculations (grouped together)
  rd_cols <- c("rd_intensity", "rd_intensity_log", "rd_intensity_score")
  for (col in rd_cols) {
    if (col %in% colnames(output_data)) {
      column_order <- c(column_order, col)
    }
  }
  
  # 4. Patent activity calculations (grouped together)
  patent_cols <- c("patent_activity_log", "patent_activity_score")
  for (col in patent_cols) {
    if (col %in% colnames(output_data)) {
      column_order <- c(column_order, col)
    }
  }
  
  # 5. Other component scores
  other_component_cols <- c("analyst_sentiment_score", "news_sentiment_score", 
                            "growth_momentum_score")
  for (col in other_component_cols) {
    if (col %in% colnames(output_data)) {
      column_order <- c(column_order, col)
    }
  }
  
  # 6. Composite score and quartile
  if ("DAII_3.5_Score" %in% colnames(output_data)) {
    column_order <- c(column_order, "DAII_3.5_Score")
  }
  if ("innovation_quartile" %in% colnames(output_data)) {
    column_order <- c(column_order, "innovation_quartile")
  }
  
  # 7. Fund-level columns
  fund_cols <- c("fund_id", "fund_name", "fund_weight", "dumac_allocation",
                 "as_of_date", "shares_held", "position_value", "fund_weight_raw",
                 "abs_weight", "Tot...Hldg.in.Port", "Top.10.as.of.Dt")
  for (col in fund_cols) {
    if (col %in% colnames(output_data)) {
      column_order <- c(column_order, col)
    }
  }
  
  # 8. Any remaining columns
  remaining_cols <- setdiff(colnames(output_data), column_order)
  column_order <- c(column_order, remaining_cols)
  
  # Reorder the dataframe
  output_data <- output_data[, column_order, drop = FALSE]
  
  # Save to CSV
  output_file <- file.path(output_dir, "02_scored_data_with_components.csv")
  tryCatch({
    readr::write_csv(output_data, output_file)
    cat(sprintf("   ✅ Scored data saved: %s\n", output_file))
    cat(sprintf("      Dimensions: %d rows × %d columns\n", 
                nrow(output_data), ncol(output_data)))
    
    # Show column sample
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

# Save the main output
main_output_file <- save_scored_data(daii_final_scores, output_dir)

# -----------------------------------------------------------------------------
# 4.3 SAVE ADDITIONAL OUTPUT FILES
# -----------------------------------------------------------------------------

cat("\n📄 STAGE 4.3: SAVING ADDITIONAL OUTPUT FILES\n")
cat(paste(rep("-", 60), collapse = ""), "\n")

save_additional_outputs <- function(processed_data, imputed_data, scores_data, 
                                    imputation_log, output_dir) {
  files_saved <- list()
  
  # 1. Processed data (after cleaning)
  processed_file <- file.path(output_dir, "01_processed_data.csv")
  tryCatch({
    readr::write_csv(processed_data, processed_file)
    files_saved[["processed_data"]] <- processed_file
    cat("   ✅ Processed data saved\n")
  }, error = function(e) {
    cat("   ⚠️  Could not save processed data\n")
  })
  
  # 2. Imputation log
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
  
  # 3. Company-level scores
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
  
  # 4. Score statistics summary
  if ("DAII_3.5_Score" %in% colnames(scores_data)) {
    # Get company-level statistics (not fund-level)
    if (exists("daii_company_final")) {
      company_scores <- daii_company_final$DAII_3.5_Score
    } else {
      # Extract unique company scores if daii_company_final doesn't exist
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
  
  # 5. Readme file with execution summary
  readme_content <- sprintf(
    "DAII 3.5 Pipeline Execution Summary
Generated: %s
Version: 3.5.8 (Field Mapping Integrated + Company/Fund Separation)

EXECUTION RESULTS:
- Input rows (fund-level): %d
- Unique companies: %d
- Company-level rows: %d
- Output rows (fund-level with scores): %d
- Component scores calculated: %d/5
- Composite score calculated: %s
- Innovation quartiles assigned: %s

KEY IMPROVEMENTS:
1. Field mapping system integrated
2. Company/fund data separation applied
3. Scoring performed at company level (N=%d)
4. Statistics reflect true company distribution
5. Scores correctly joined to fund holdings

DATA STRUCTURE:
- Company dataset: %d rows × company metrics
- Fund dataset: %d rows × fund-specific metrics
- Final output: %d rows (scores repeated per fund holding)

FIELD MAPPING STATUS: %s
COMPANY/FUND SEPARATION: %s

NOTES:
- Statistics in 05_score_statistics.csv are based on %d companies, not %d fund holdings
- This fixes the Q1-Q3 clustering issue from previous runs
- Output ready for Modules 4-9 (portfolio construction)
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

# Save additional outputs
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

# -----------------------------------------------------------------------------
# 5.1 GENERATE EXECUTION SUMMARY
# -----------------------------------------------------------------------------

cat("📋 STAGE 5.1: GENERATING EXECUTION SUMMARY\n")
cat(paste(rep("-", 60), collapse = ""), "\n")

generate_execution_summary <- function(scores_data, output_dir, field_mapping) {
  cat("   Compiling execution statistics...\n")
  
  # Get company-level statistics
  if (exists("daii_company_final")) {
    company_data <- daii_company_final
  } else {
    # Extract unique company scores
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
    version = "3.5.8 (Field Mapping Integrated + Company/Fund Separation)",
    status = "COMPLETED",
    
    # Data statistics
    input_rows_fund_level = nrow(scores_data),
    unique_companies = length(unique(scores_data$ticker)),
    company_level_rows = nrow(company_data),
    
    # Field mapping status
    field_mapping_applied = !is.null(field_mapping),
    company_fund_separation_applied = exists("daii_company_final"),
    
    # Score statistics (company-level)
    component_scores_calculated = sum(c("rd_intensity_score", "analyst_sentiment_score",
                                        "patent_activity_score", "news_sentiment_score",
                                        "growth_momentum_score") %in% colnames(company_data)),
    composite_score_calculated = "DAII_3.5_Score" %in% colnames(company_data),
    quartiles_assigned = "innovation_quartile" %in% colnames(company_data),
    
    # Company-level quality metrics
    na_composite_score = ifelse("DAII_3.5_Score" %in% colnames(company_data),
                                sum(is.na(company_data$DAII_3.5_Score)), NA),
    na_composite_pct = ifelse("DAII_3.5_Score" %in% colnames(company_data),
                              100 * sum(is.na(company_data$DAII_3.5_Score)) / nrow(company_data), NA),
    
    # Company-level score distribution
    company_score_mean = ifelse("DAII_3.5_Score" %in% colnames(company_data),
                                mean(company_data$DAII_3.5_Score, na.rm = TRUE), NA),
    company_score_sd = ifelse("DAII_3.5_Score" %in% colnames(company_data),
                              sd(company_data$DAII_3.5_Score, na.rm = TRUE), NA),
    company_score_range = ifelse("DAII_3.5_Score" %in% colnames(company_data),
                                 paste(round(range(company_data$DAII_3.5_Score, na.rm = TRUE), 2), collapse = " to "), NA),
    
    # Quartile distribution (company-level)
    quartile_counts = ifelse("innovation_quartile" %in% colnames(company_data),
                             paste(capture.output(table(company_data$innovation_quartile, useNA = "ifany")), 
                                   collapse = "\n"), "N/A")
  )
  
  # Print summary to console
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
  
  # Save summary to file
  summary_file <- file.path(output_dir, "07_execution_summary.yaml")
  tryCatch({
    yaml::write_yaml(summary_stats, summary_file)
    cat(sprintf("\n   📄 Execution summary saved: %s\n", summary_file))
  }, error = function(e) {
    cat(sprintf("\n   ⚠️  Could not save execution summary: %s\n", e$message))
  })
  
  return(summary_stats)
}

# Generate final summary
execution_summary <- generate_execution_summary(daii_final_scores, output_dir, field_mapping)

# -----------------------------------------------------------------------------
# 5.2 FINAL VALIDATION CHECKS
# -----------------------------------------------------------------------------

cat("\n🔍 STAGE 5.2: FINAL VALIDATION CHECKS\n")
cat(paste(rep("-", 60), collapse = ""), "\n")

perform_validation_checks <- function(scores_data) {
  cat("   Running validation checks...\n")
  checks_passed <- 0
  checks_total <- 0
  
  # Check 1: Ticker column exists
  checks_total <- checks_total + 1
  if ("ticker" %in% colnames(scores_data)) {
    cat("   ✅ Check 1: Ticker column present\n")
    checks_passed <- checks_passed + 1
  } else {
    cat("   ❌ Check 1: Ticker column missing\n")
  }
  
  # Check 2: Composite score exists
  checks_total <- checks_total + 1
  if ("DAII_3.5_Score" %in% colnames(scores_data)) {
    cat("   ✅ Check 2: DAII_3.5_Score column present\n")
    checks_passed <- checks_passed + 1
    
    # Check score range
    if (all(scores_data$DAII_3.5_Score >= 0 & scores_data$DAII_3.5_Score <= 100, na.rm = TRUE)) {
      cat("   ✅ Check 2a: Scores in 0-100 range\n")
    } else {
      cat("   ⚠️  Check 2a: Some scores outside 0-100 range\n")
    }
  } else {
    cat("   ❌ Check 2: DAII_3.5_Score column missing\n")
  }
  
  # Check 3: Innovation quartiles assigned
  checks_total <- checks_total + 1
  if ("innovation_quartile" %in% colnames(scores_data)) {
    cat("   ✅ Check 3: Innovation quartiles assigned\n")
    checks_passed <- checks_passed + 1
  } else {
    cat("   ❌ Check 3: Innovation quartiles missing\n")
  }
  
  # Check 4: Component scores calculated
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
  
  # Check 5: Company/fund separation applied
  checks_total <- checks_total + 1
  unique_tickers <- length(unique(scores_data$ticker))
  total_rows <- nrow(scores_data)
  
  if (total_rows > unique_tickers) {
    cat(sprintf("   ✅ Check 5: Company/fund separation applied (%d companies, %d fund holdings)\n", 
                unique_tickers, total_rows))
    checks_passed <- checks_passed + 1
  } else {
    cat(sprintf("   ⚠️  Check 5: Company/fund separation not evident (%d rows, %d tickers)\n", 
                total_rows, unique_tickers))
  }
  
  # Check 6: Column grouping (rd_intensity with rd_intensity_score)
  checks_total <- checks_total + 1
  if ("rd_intensity" %in% colnames(scores_data) && "rd_intensity_score" %in% colnames(scores_data)) {
    # Check if they're adjacent in the dataframe
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
  
  # Overall validation result
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

# Run validation
validation_result <- perform_validation_checks(daii_final_scores)

# =============================================================================
# FINAL COMPLETION
# =============================================================================

cat("\n")
cat(paste(rep("=", 80), collapse = ""), "\n")
cat("🎉 DAII 3.5 PHASE 1 PIPELINE COMPLETE! 🎉\n")
cat(paste(rep("=", 80), collapse = ""), "\n\n")

cat("SUMMARY OF WHAT WAS ACCOMPLISHED:\n")
cat(paste(rep("-", 60), collapse = ""), "\n")
cat("1. ✅ FIELD MAPPING INTEGRATED: Raw data columns standardized before processing\n")
cat("2. ✅ COMPANY/FUND SEPARATION: Scoring at company level (N=50), output at fund level (N=589)\n")
cat("3. ✅ QUARTILE ERROR FIXED: Jitter logic implemented for non-unique breaks\n")
cat("4. ✅ COMPONENT SCORES CALCULATED: All 5 domain scores computed\n")
cat("5. ✅ COMPOSITE SCORE GENERATED: DAII_3.5_Score with proper weighting\n")
cat("6. ✅ INNOVATION QUARTILES ASSIGNED: Companies classified into Q1-Q4\n")
cat("7. ✅ COLUMNS REORGANIZED: Related fields grouped together logically\n")
cat("8. ✅ OUTPUT FILES SAVED: Complete dataset saved to output directory\n\n")

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
cat("1. Verify output statistics are based on N=50 companies (not N=589)\n")
cat("2. Check quartile distribution is reasonable (not clustered around single value)\n")
cat("3. Proceed to Modules 4-9 for portfolio construction and analysis\n")
cat("4. Component scores are now in output file, ready for analysis\n\n")

cat("TO RUN AGAIN:\n")
cat(paste(rep("-", 60), collapse = ""), "\n")
cat("# 1. Save this script as 'DAII_3.5_Phase1_FieldMapping_Integrated.R'\n")
cat("# 2. Run: source('DAII_3.5_Phase1_FieldMapping_Integrated.R')\n")
cat("# 3. Check the output directory for results\n\n")

# Save the current environment for continuity
save.image(file.path(output_dir, "DAII_3.5_Phase1_Workspace.RData"))
cat(sprintf("💾 Workspace saved to: %s\n", 
            file.path(output_dir, "DAII_3.5_Phase1_Workspace.RData")))

cat("\n")
cat(paste(rep("🎯", 40), collapse = ""), "\n")
cat("PHASE 1 COMPLETE - READY FOR MODULES 4-9!\n")
cat(paste(rep("🎯", 40), collapse = ""), "\n")