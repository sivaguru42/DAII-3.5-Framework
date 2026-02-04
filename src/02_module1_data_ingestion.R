# ============================================================
# SCRIPT 2: MODULE 1 - DATA INGESTION & PREPROCESSING
# ============================================================
# Purpose: Execute complete Module 1 pipeline on N=50 test dataset
# Run this script AFTER Script 1
# ============================================================

# Clear workspace
rm(list = ls())

# ============================================================
# 1. SET WORKING DIRECTORIES
# ============================================================

# Define paths
data_dir <- "C:/Users/sganesan/OneDrive - dumac.duke.edu/DAII/data/raw"
scripts_dir <- "C:/Users/sganesan/OneDrive - dumac.duke.edu/DAII/R/scripts"

# Set working directory to data directory
setwd(data_dir)
cat("📁 Working directory set to:", getwd(), "\n\n")

# ============================================================
# 2. LOAD REQUIRED PACKAGES
# ============================================================

cat("📦 LOADING REQUIRED PACKAGES...\n")

required_packages <- c("readr", "dplyr", "tidyr", "lubridate", "ggplot2")
for (pkg in required_packages) {
  if (!require(pkg, character.only = TRUE, quietly = TRUE)) {
    install.packages(pkg, dependencies = TRUE)
    library(pkg, character.only = TRUE)
    cat(sprintf("  Installed and loaded: %s\n", pkg))
  } else {
    cat(sprintf("  Loaded: %s\n", pkg))
  }
}

cat("✅ All packages loaded\n\n")

# ============================================================
# 3. LOAD TEST DATASET
# ============================================================

cat("📥 LOADING N=50 TEST DATASET...\n")

test_file <- "DAII_3_5_N50_Test_Dataset.csv"

if (!file.exists(test_file)) {
  stop(paste("❌ ERROR: Test dataset not found at:", test_file, 
             "\nPlease run Script 1 first to create the test dataset."))
}

test_data <- read_csv(test_file, col_types = cols(), show_col_types = FALSE)

# Display basic info
cat("✅ Dataset loaded successfully\n")
cat(sprintf("   • File: %s\n", test_file))
cat(sprintf("   • Rows: %d\n", nrow(test_data)))
cat(sprintf("   • Columns: %d\n", ncol(test_data)))

# Show dataset structure
cat("\n📋 DATASET STRUCTURE:\n")
print(str(test_data))

# Show column names
cat("\n📝 COLUMN NAMES:\n")
print(names(test_data))

# Check the format of as_of_date
cat("\n📅 CHECKING DATE COLUMN FORMAT...\n")
if ("as_of_date" %in% names(test_data)) {
  # Show sample values
  sample_dates <- head(test_data$as_of_date, 3)
  cat("   Sample date values:\n")
  for (i in 1:length(sample_dates)) {
    cat(sprintf("     [%d] %s (type: %s)\n", i, sample_dates[i], typeof(sample_dates[i])))
  }
  
  # Try to parse the date with different formats
  test_date <- sample_dates[1]
  
  # Common date formats
  date_formats <- c(
    "%Y-%m-%d",    # 2024-01-15
    "%m/%d/%Y",    # 01/15/2024
    "%d/%m/%Y",    # 15/01/2024
    "%Y%m%d",      # 20240115
    "%b %d, %Y",   # Jan 15, 2024
    "%B %d, %Y",   # January 15, 2024
    "%d-%b-%Y",    # 15-Jan-2024
    "%d-%B-%Y"     # 15-January-2024
  )
  
  cat("   Attempting to parse dates...\n")
  parsed_date <- NULL
  used_format <- NULL
  
  for (fmt in date_formats) {
    parsed <- try(as.Date(test_date, format = fmt), silent = TRUE)
    if (!inherits(parsed, "try-error") && !is.na(parsed)) {
      parsed_date <- parsed
      used_format <- fmt
      cat(sprintf("   ✓ Successfully parsed with format: %s\n", fmt))
      break
    }
  }
  
  if (is.null(parsed_date)) {
    # Try lubridate's smart parsing
    cat("   Trying lubridate's smart parsing...\n")
    parsed <- try(lubridate::parse_date_time(test_date, orders = c("ymd", "mdy", "dmy", "ymd HMS", "mdy HMS", "dmy HMS")), silent = TRUE)
    if (!inherits(parsed, "try-error") && !is.na(parsed)) {
      parsed_date <- as.Date(parsed)
      used_format <- "lubridate smart parsing"
      cat("   ✓ Successfully parsed with lubridate\n")
    }
  }
  
  if (!is.null(parsed_date)) {
    # Rename and convert the entire column
    test_data <- test_data %>% rename(Date = as_of_date)
    cat("✅ Renamed 'as_of_date' to 'Date'\n")
    
    if (!is.null(used_format) && used_format != "lubridate smart parsing") {
      # Use the found format
      test_data$Date <- as.Date(test_data$Date, format = used_format)
      cat(sprintf("   Converted Date column using format: %s\n", used_format))
    } else {
      # Use lubridate
      test_data$Date <- as.Date(lubridate::parse_date_time(test_data$Date, orders = c("ymd", "mdy", "dmy")))
      cat("   Converted Date column using lubridate\n")
    }
    
    cat(sprintf("   • Date range: %s to %s\n", 
                min(test_data$Date, na.rm = TRUE), max(test_data$Date, na.rm = TRUE)))
    cat(sprintf("   • Unique dates: %d\n", length(unique(test_data$Date))))
  } else {
    cat("❌ WARNING: Could not parse date format. Keeping as character.\n")
    # Still rename but don't convert
    test_data <- test_data %>% rename(Date = as_of_date)
  }
} else {
  cat("⚠️  WARNING: 'as_of_date' column not found in dataset.\n")
}

# Rename other columns for easier processing (using the same mapping as before)
column_rename_map <- list(
  "Tot.Ret.Idx.Gross" = "TotalReturnIndex",
  "Last.Price" = "LastPrice",
  "Mkt.Cap" = "MarketCap",
  "R.D.Exp" = "RDExpense",
  "Patents...Trademarks...Copy.Rgt" = "IntangibleAssets",
  "Number.of.Employees" = "Employees",
  "GM" = "GrossMargin",
  "Rev...1.Yr.Gr" = "RevenueGrowth1Y",
  "Volatil.360D" = "Volatility360D",
  "BEst.Analyst.Rtg" = "AnalystRating",
  "GICS.Ind.Grp.Name" = "IndustryGroup",
  "Tot...Hldg.in.Port" = "TotalPortfolioHolding",
  "Top.10.as.of.Dt" = "Top10Date",
  "BEst.Target.Px" = "TargetPrice",
  "Rec.Consensus" = "RecommendationConsensus",
  "Earnings.Conference.Call.Date" = "EarningsCallDate",
  "Tot.Analyst.Rec" = "TotalAnalystRecs",
  "News.Sent" = "NewsSentiment",
  "Percent.Change.in.Institutional.Holdings" = "InstHoldingsChangePct",
  "Shrt.Int" = "ShortInterest"
)

cat("\n🔄 RENAMING COLUMNS FOR BETTER READABILITY...\n")
renamed_count <- 0
for (old_name in names(column_rename_map)) {
  if (old_name %in% names(test_data)) {
    new_name <- column_rename_map[[old_name]]
    test_data <- test_data %>% rename(!!new_name := !!old_name)
    renamed_count <- renamed_count + 1
    if (renamed_count <= 5) {  # Only show first 5 to avoid clutter
      cat(sprintf("   Renamed '%s' to '%s'\n", old_name, new_name))
    }
  }
}
if (renamed_count > 5) {
  cat(sprintf("   ... and %d more columns renamed\n", renamed_count - 5))
}

cat(sprintf("   • Companies: %d\n", length(unique(test_data$Ticker))))

# Show final column names
cat("\n📝 FINAL COLUMN NAMES AFTER RENAMING:\n")
print(names(test_data))

# ============================================================
# 4. MODULE 1 FUNCTION DEFINITIONS
# ============================================================

cat("\n🔧 DEFINING MODULE 1 FUNCTIONS...\n")

# Function 1: Data Cleaning - ADAPTED FOR YOUR DATASET
clean_financial_data <- function(raw_data) {
  cat("   Step 1.1: Removing duplicates...\n")
  cleaned <- raw_data %>%
    distinct()  # Remove exact duplicates
  
  rows_removed <- nrow(raw_data) - nrow(cleaned)
  if (rows_removed > 0) {
    cat(sprintf("   Removed %d duplicate rows\n", rows_removed))
  }
  
  cat("   Step 1.2: Sorting by Ticker and Date...\n")
  
  # Check if Date column exists and is Date type
  if (!"Date" %in% names(cleaned)) {
    cat("⚠️  WARNING: Date column not found. Sorting by Ticker only.\n")
    # Sort just by Ticker
    cleaned <- cleaned[order(cleaned$Ticker), ]
  } else {
    # Convert to Date if it's character
    if (is.character(cleaned$Date)) {
      cat("   Converting Date column from character to Date...\n")
      cleaned$Date <- as.Date(lubridate::parse_date_time(cleaned$Date, orders = c("ymd", "mdy", "dmy")))
    }
    
    # Sort the data
    if (inherits(cleaned$Date, "Date")) {
      cleaned <- cleaned[order(cleaned$Ticker, cleaned$Date), ]
    } else {
      cat("⚠️  WARNING: Date column is not Date type. Sorting by Ticker only.\n")
      cleaned <- cleaned[order(cleaned$Ticker), ]
    }
  }
  
  cat("   Step 1.3: Checking for invalid values...\n")
  
  # Identify numeric columns
  numeric_cols <- names(cleaned)[sapply(cleaned, is.numeric)]
  
  # Convert negative values to positive for certain columns that shouldn't be negative
  positive_cols <- c("MarketCap", "LastPrice", "Volume", "RDExpense", "Employees", 
                     "GrossMargin", "TargetPrice", "TotalReturnIndex")
  
  for (col in intersect(positive_cols, numeric_cols)) {
    if (col %in% names(cleaned)) {
      neg_count <- sum(cleaned[[col]] < 0, na.rm = TRUE)
      if (neg_count > 0) {
        cat(sprintf("   Converting %d negative values in %s to positive...\n", neg_count, col))
        cleaned[[col]][cleaned[[col]] < 0] <- abs(cleaned[[col]][cleaned[[col]] < 0])
      }
    }
  }
  
  cat(sprintf("   Result: %d → %d rows after cleaning\n", 
              nrow(raw_data), nrow(cleaned)))
  
  return(cleaned)
}

# Function 2: Missing Value Imputation (Simplified)
impute_missing_values <- function(data) {
  cat("   Step 2.1: Analyzing missing values...\n")
  
  # Count missing values before imputation
  missing_before <- sapply(data, function(x) sum(is.na(x)))
  total_missing_before <- sum(missing_before)
  
  if (total_missing_before == 0) {
    cat("   No missing values found. Skipping imputation.\n")
    return(data)
  }
  
  cat("   Missing values before imputation:\n")
  missing_cols <- names(missing_before)[missing_before > 0]
  for (col in missing_cols) {
    cat(sprintf("     • %s: %d missing\n", col, missing_before[col]))
  }
  
  cat("   Step 2.2: Imputing missing values...\n")
  imputed <- data
  
  # Simple imputation: forward fill by Ticker group
  if ("Ticker" %in% names(imputed)) {
    tickers <- unique(imputed$Ticker)
    imputed_rows <- 0
    
    for (ticker in tickers) {
      ticker_idx <- which(imputed$Ticker == ticker)
      if (length(ticker_idx) > 1) {  # Only impute if we have multiple rows
        ticker_data <- imputed[ticker_idx, ]
        
        # Forward fill for each column
        for (col in missing_cols) {
          if (col %in% names(ticker_data) && is.numeric(ticker_data[[col]])) {
            na_idx <- is.na(ticker_data[[col]])
            if (any(na_idx)) {
              # Simple forward fill
              for (i in which(na_idx)) {
                if (i > 1 && !is.na(ticker_data[i-1, col])) {
                  ticker_data[i, col] <- ticker_data[i-1, col]
                  imputed_rows <- imputed_rows + 1
                }
              }
            }
          }
        }
        imputed[ticker_idx, ] <- ticker_data
      }
    }
    
    cat(sprintf("   Result: Imputed %d missing values using forward fill\n", imputed_rows))
  }
  
  return(imputed)
}

# Function 3: Outlier Detection & Treatment
handle_outliers <- function(data, threshold = 3) {
  cat("   Step 3.1: Detecting outliers using IQR method...\n")
  
  # Identify numeric columns (excluding identifier columns)
  numeric_cols <- names(data)[sapply(data, is.numeric)]
  exclude_cols <- c("Ticker", "fund_id")
  numeric_cols <- setdiff(numeric_cols, exclude_cols)
  
  if (length(numeric_cols) == 0) {
    cat("   No numeric columns found for outlier detection\n")
    return(data)
  }
  
  # Create a copy for outlier treatment
  treated <- data
  total_outliers <- 0
  
  for (col in numeric_cols) {
    # Skip columns with too few non-NA values
    non_na_values <- sum(!is.na(data[[col]]))
    if (non_na_values < 10) next
    
    # Calculate IQR
    q1 <- quantile(data[[col]], 0.25, na.rm = TRUE)
    q3 <- quantile(data[[col]], 0.75, na.rm = TRUE)
    iqr <- q3 - q1
    
    # Skip if IQR is 0 or NA
    if (iqr == 0 || is.na(iqr)) next
    
    # Define bounds
    lower_bound <- q1 - threshold * iqr
    upper_bound <- q3 + threshold * iqr
    
    # Count outliers
    outliers <- sum(data[[col]] < lower_bound | data[[col]] > upper_bound, na.rm = TRUE)
    
    if (outliers > 0) {
      cat(sprintf("     • %s: %d outliers detected\n", col, outliers))
      total_outliers <- total_outliers + outliers
      
      # Winsorize outliers (cap at bounds)
      treated[[col]] <- ifelse(
        treated[[col]] < lower_bound, lower_bound,
        ifelse(treated[[col]] > upper_bound, upper_bound, treated[[col]])
      )
    }
  }
  
  cat(sprintf("   Step 3.2: Treated %d total outliers using winsorization\n", total_outliers))
  
  return(treated)
}

# Function 4: Data Validation - ADAPTED FOR YOUR DATASET
validate_financial_data <- function(data) {
  cat("   Step 4.1: Running validation checks...\n")
  
  # Initialize results
  validation_results <- list()
  
  # Check 1: No duplicate rows
  validation_results$no_duplicates <- nrow(data) == nrow(unique(data))
  
  # Check 2: Date monotonicity per company (only if Date exists and is Date type)
  validation_results$dates_monotonic <- NA
  if ("Ticker" %in% names(data) && "Date" %in% names(data) && inherits(data$Date, "Date")) {
    validation_results$dates_monotonic <- TRUE
    tickers <- unique(data$Ticker)
    for (ticker in tickers) {
      ticker_data <- data[data$Ticker == ticker, ]
      if (nrow(ticker_data) > 1) {
        # Sort by Date first
        ticker_data <- ticker_data[order(ticker_data$Date), ]
        date_diff <- diff(ticker_data$Date)
        if (any(date_diff < 0)) {
          validation_results$dates_monotonic <- FALSE
          cat(sprintf("     ⚠️  Non-monotonic dates found for ticker: %s\n", ticker))
          break
        }
      }
    }
  }
  
  # Check 3: Required columns present (adapted for your dataset)
  required_cols <- c("Ticker", "Date", "LastPrice", "MarketCap", "Volume")
  missing_cols <- setdiff(required_cols, names(data))
  validation_results$required_columns <- length(missing_cols) == 0
  
  if (length(missing_cols) > 0) {
    cat(sprintf("     ⚠️  Missing required columns: %s\n", paste(missing_cols, collapse = ", ")))
  }
  
  # Check 4: No negative values in critical columns (if they exist)
  positive_check_cols <- c("MarketCap", "LastPrice", "Volume", "Employees")
  for (col in positive_check_cols) {
    if (col %in% names(data) && is.numeric(data[[col]])) {
      validation_results[[paste0("no_negative_", col)]] <- all(data[[col]] >= 0, na.rm = TRUE)
    } else {
      validation_results[[paste0("no_negative_", col)]] <- NA
    }
  }
  
  # Check 5: Data types are correct
  if ("Date" %in% names(data)) {
    validation_results$date_is_date <- inherits(data$Date, "Date")
  } else {
    validation_results$date_is_date <- NA
  }
  
  if ("LastPrice" %in% names(data)) {
    validation_results$lastprice_is_numeric <- is.numeric(data$LastPrice)
  } else {
    validation_results$lastprice_is_numeric <- NA
  }
  
  # Print validation summary
  cat("\n   Validation Results:\n")
  cat(sprintf("     • No duplicates: %s\n", 
              ifelse(validation_results$no_duplicates, "✅ PASS", "❌ FAIL")))
  
  if (!is.na(validation_results$dates_monotonic)) {
    cat(sprintf("     • Dates monotonic: %s\n", 
                ifelse(validation_results$dates_monotonic, "✅ PASS", "❌ FAIL")))
  }
  
  cat(sprintf("     • Required columns: %s\n", 
              ifelse(validation_results$required_columns, "✅ PASS", "❌ FAIL")))
  
  # Print positive value checks
  for (col in positive_check_cols) {
    result_name <- paste0("no_negative_", col)
    if (result_name %in% names(validation_results)) {
      result <- validation_results[[result_name]]
      if (!is.na(result)) {
        cat(sprintf("     • No negative %s: %s\n", 
                    col, ifelse(result, "✅ PASS", "❌ FAIL")))
      }
    }
  }
  
  return(validation_results)
}

# Function 5: Save Processed Data
save_processed_data <- function(data, filename) {
  cat(sprintf("   Step 5.1: Saving processed data to %s...\n", filename))
  write_csv(data, filename)
  size_mb <- round(file.info(filename)$size / (1024 * 1024), 3)
  cat(sprintf("   Result: Saved %.3f MB\n", size_mb))
}

# Function 6: Generate Data Quality Report
generate_data_quality_report <- function(data, original_data, filename) {
  cat("   Step 6.1: Generating data quality report...\n")
  
  # Create report content
  report_lines <- c(
    "DATA QUALITY REPORT - MODULE 1",
    paste("Generated:", Sys.time()),
    paste("Original dataset:", nrow(original_data), "rows,", ncol(original_data), "columns"),
    paste("Processed dataset:", nrow(data), "rows,", ncol(data), "columns"),
    "",
    "DATA QUALITY METRICS:",
    paste("1. Duplicate rows removed:", nrow(original_data) - nrow(unique(original_data))),
    paste("2. Missing values imputed:", sum(is.na(original_data)) - sum(is.na(data))),
    paste("3. Date range:", ifelse("Date" %in% names(data) && inherits(data$Date, "Date"), 
                                   paste(min(data$Date, na.rm = TRUE), "to", max(data$Date, na.rm = TRUE)), 
                                   "Date column not found or not in Date format")),
    paste("4. Companies analyzed:", ifelse("Ticker" %in% names(data), 
                                           length(unique(data$Ticker)), "N/A")),
    "",
    "COLUMN STATISTICS:"
  )
  
  # Add column statistics for numeric columns
  stats_lines <- character()
  
  # Check and add statistics for each numeric column
  numeric_cols <- names(data)[sapply(data, is.numeric)]
  for (col in numeric_cols) {
    if (!col %in% c("Ticker", "fund_id") && col %in% names(data)) {
      stats_lines <- c(stats_lines,
                       paste(col, ":", sep = ""),
                       paste("  Min:", min(data[[col]], na.rm = TRUE)),
                       paste("  Mean:", mean(data[[col]], na.rm = TRUE)),
                       paste("  Max:", max(data[[col]], na.rm = TRUE)),
                       paste("  SD:", sd(data[[col]], na.rm = TRUE)),
                       paste("  NAs:", sum(is.na(data[[col]]))),
                       ""
      )
    }
  }
  
  # Combine all lines
  report <- c(report_lines, "", stats_lines)
  
  writeLines(report, filename)
  cat(sprintf("   Result: Report saved to %s\n", filename))
}

# ============================================================
# 5. EXECUTE MODULE 1 PIPELINE
# ============================================================

cat("\n")
cat(rep("=", 70), "\n", sep = "")
cat("🚀 EXECUTING MODULE 1: DATA INGESTION & PREPROCESSING\n")
cat(rep("=", 70), "\n\n", sep = "")

# Step 1: Data Cleaning
cat("STEP 1: DATA CLEANING\n")
cat(rep("-", 50), "\n", sep = "")
cleaned_data <- clean_financial_data(test_data)

# Step 2: Missing Value Imputation
cat("\nSTEP 2: MISSING VALUE IMPUTATION\n")
cat(rep("-", 50), "\n", sep = "")
imputed_data <- impute_missing_values(cleaned_data)

# Step 3: Outlier Handling
cat("\nSTEP 3: OUTLIER DETECTION & TREATMENT\n")
cat(rep("-", 50), "\n", sep = "")
outlier_treated <- handle_outliers(imputed_data, threshold = 3)

# Step 4: Data Validation
cat("\nSTEP 4: DATA VALIDATION\n")
cat(rep("-", 50), "\n", sep = "")
validation_results <- validate_financial_data(outlier_treated)

# Step 5: Save Processed Data
cat("\nSTEP 5: SAVE PROCESSED DATA\n")
cat(rep("-", 50), "\n", sep = "")
processed_filename <- "module1_processed_data.csv"
save_processed_data(outlier_treated, processed_filename)

# Step 6: Generate Data Quality Report
cat("\nSTEP 6: GENERATE DATA QUALITY REPORT\n")
cat(rep("-", 50), "\n", sep = "")
report_filename <- "module1_data_quality_report.txt"
generate_data_quality_report(outlier_treated, test_data, report_filename)

# ============================================================
# 6. CREATE EXECUTION SUMMARY
# ============================================================

cat("\n")
cat(rep("=", 70), "\n", sep = "")
cat("📊 MODULE 1 EXECUTION SUMMARY\n")
cat(rep("=", 70), "\n\n", sep = "")

# Get date range for summary
date_range <- if ("Date" %in% names(outlier_treated) && inherits(outlier_treated$Date, "Date")) {
  paste(min(outlier_treated$Date, na.rm = TRUE), "to", max(outlier_treated$Date, na.rm = TRUE))
} else if ("Date" %in% names(outlier_treated)) {
  paste("Date exists but is not Date format:", class(outlier_treated$Date))
} else {
  "Date column not found"
}

# Create summary object
module1_summary <- list(
  execution_timestamp = Sys.time(),
  input_file = test_file,
  output_file = processed_filename,
  report_file = report_filename,
  validation_results = validation_results,
  dataset_metrics = list(
    original_rows = nrow(test_data),
    processed_rows = nrow(outlier_treated),
    original_columns = ncol(test_data),
    processed_columns = ncol(outlier_treated),
    companies_processed = ifelse("Ticker" %in% names(outlier_treated), 
                                 length(unique(outlier_treated$Ticker)), NA),
    date_range = date_range,
    numeric_columns_processed = length(names(outlier_treated)[sapply(outlier_treated, is.numeric)])
  ),
  processing_steps = c(
    "1. Data Cleaning",
    "2. Missing Value Imputation", 
    "3. Outlier Detection & Treatment",
    "4. Data Validation",
    "5. Save Processed Data",
    "6. Generate Data Quality Report"
  ),
  dataset_notes = "Processed portfolio holdings dataset with financial metrics"
)

# Save summary
summary_filename <- "module1_execution_summary.rds"
saveRDS(module1_summary, summary_filename)
cat(sprintf("📄 Execution summary saved: %s\n", summary_filename))

# ============================================================
# 7. FINAL OUTPUT
# ============================================================

cat("\n")
cat(rep("=", 70), "\n", sep = "")
cat("✅ MODULE 1 COMPLETED SUCCESSFULLY!\n")
cat(rep("=", 70), "\n\n", sep = "")

cat("📁 OUTPUT FILES CREATED:\n")
output_files <- c(processed_filename, report_filename, summary_filename)
for (file in output_files) {
  if (file.exists(file)) {
    size_mb <- round(file.info(file)$size / (1024 * 1024), 3)
    cat(sprintf("  • %-40s %.3f MB\n", file, size_mb))
  }
}

cat("\n📊 DATASET TRANSFORMATION SUMMARY:\n")
cat(sprintf("  • Input rows: %d → Output rows: %d\n", 
            nrow(test_data), nrow(outlier_treated)))
cat(sprintf("  • Input columns: %d → Output columns: %d\n", 
            ncol(test_data), ncol(outlier_treated)))
cat(sprintf("  • Companies processed: %d\n", 
            ifelse("Ticker" %in% names(outlier_treated), 
                   length(unique(outlier_treated$Ticker)), 0)))
cat(sprintf("  • Date range: %s\n", date_range))
cat(sprintf("  • Numeric columns processed: %d\n", 
            length(names(outlier_treated)[sapply(outlier_treated, is.numeric)])))

cat("\n🔍 SAMPLE OF PROCESSED DATA (first 3 rows):\n")
print(head(outlier_treated, 3))

cat("\n")
cat(rep("=", 70), "\n", sep = "")
cat("🚀 READY FOR MODULE 2: SENTIMENT ANALYSIS & NLP PROCESSING\n")
# ============================================================
# SCRIPT 3: MODULE 2 - SENTIMENT ANALYSIS & NLP PROCESSING
# ============================================================
# Purpose: Generate synthetic sentiment data and analyze
# Run this script AFTER Script 2
# ============================================================

# Clear workspace
rm(list = ls())

# ============================================================
# 1. SET WORKING DIRECTORIES
# ============================================================

data_dir <- "C:/Users/sganesan/OneDrive - dumac.duke.edu/DAII/data/raw"
scripts_dir <- "C:/Users/sganesan/OneDrive - dumac.duke.edu/DAII/R/scripts"

setwd(data_dir)
cat("📁 Working directory set to:", getwd(), "\n\n")

# ============================================================
# 2. LOAD REQUIRED PACKAGES
# ============================================================

cat("📦 LOADING REQUIRED PACKAGES...\n")

required_packages <- c("readr", "dplyr", "tidyr", "lubridate", "stringr")
for (pkg in required_packages) {
  if (!require(pkg, character.only = TRUE, quietly = TRUE)) {
    install.packages(pkg, dependencies = TRUE)
    library(pkg, character.only = TRUE)
    cat(sprintf("  Installed and loaded: %s\n", pkg))
  } else {
    cat(sprintf("  Loaded: %s\n", pkg))
  }
}

cat("✅ All packages loaded\n\n")

# ============================================================
# 3. LOAD MODULE 1 PROCESSED DATA
# ============================================================

cat("📥 LOADING MODULE 1 PROCESSED DATA...\n")

processed_file <- "module1_processed_data.csv"

if (!file.exists(processed_file)) {
  stop(paste("❌ ERROR: Processed data not found at:", processed_file, 
             "\nPlease run Script 2 first."))
}

processed_data <- read_csv(processed_file, col_types = cols(), show_col_types = FALSE)

cat("✅ Processed data loaded successfully\n")
cat(sprintf("   • Rows: %d\n", nrow(processed_data)))
cat(sprintf("   • Columns: %d\n", ncol(processed_data)))
cat(sprintf("   • Companies: %d\n", length(unique(processed_data$Ticker))))

# Show column names to understand structure
cat("\n📋 COLUMNS AVAILABLE FOR SENTIMENT ANALYSIS:\n")
print(names(processed_data))

# ============================================================
# 4. GENERATE SYNTHETIC SENTIMENT DATA
# ============================================================

cat("\n")
cat(rep("=", 70), "\n", sep = "")
cat("🧠 GENERATING SYNTHETIC SENTIMENT DATA\n")
cat(rep("=", 70), "\n\n", sep = "")

generate_sentiment_data <- function(financial_data, seed = 123) {
  set.seed(seed)
  
  cat("Step 1: Creating sentiment observations for each date-ticker pair...\n")
  
  # Create base sentiment dataset
  sentiment_base <- financial_data %>%
    select(Ticker, Date) %>%
    distinct()
  
  cat(sprintf("   • Creating %d sentiment observations\n", nrow(sentiment_base)))
  
  # Generate synthetic sentiment metrics
  sentiment_data <- sentiment_base %>%
    mutate(
      # News sentiment (-1 to 1 scale)
      news_sentiment = rnorm(n(), mean = 0.2, sd = 0.3),
      
      # Social media sentiment
      social_media_sentiment = rnorm(n(), mean = 0.1, sd = 0.4),
      
      # Earnings call sentiment (higher during earnings months)
      # Since we only have one date (2024-12-31), we'll use month 12
      earnings_call_sentiment = case_when(
        month(Date) %in% c(1, 4, 7, 10) ~ rnorm(n(), mean = 0.3, sd = 0.2),
        TRUE ~ rnorm(n(), mean = 0, sd = 0.1)
      ),
      
      # Sentiment volatility
      sentiment_volatility = abs(rnorm(n(), mean = 0.15, sd = 0.05)),
      
      # Sentiment momentum
      sentiment_momentum = rnorm(n(), mean = 0, sd = 0.1),
      
      # Volume metrics
      news_volume = sample(10:1000, n(), replace = TRUE),
      social_mentions = sample(50:5000, n(), replace = TRUE),
      
      # Binary indicators
      positive_news = as.integer(news_sentiment > 0.2),
      negative_news = as.integer(news_sentiment < -0.2),
      neutral_news = as.integer(news_sentiment >= -0.2 & news_sentiment <= 0.2),
      
      # Composite sentiment score (weighted average)
      composite_sentiment = 0.4*news_sentiment + 0.3*social_media_sentiment + 0.3*earnings_call_sentiment,
      
      # Sentiment categories
      sentiment_category = case_when(
        composite_sentiment > 0.3 ~ "Very Positive",
        composite_sentiment > 0.1 ~ "Positive",
        composite_sentiment > -0.1 ~ "Neutral",
        composite_sentiment > -0.3 ~ "Negative",
        TRUE ~ "Very Negative"
      )
    )
  
  cat("Step 2: Adding industry-based sentiment variations...\n")
  
  # Get industry information from financial data
  if ("IndustryGroup" %in% colnames(financial_data)) {
    industry_info <- financial_data %>%
      select(Ticker, IndustryGroup) %>%
      distinct()
    
    sentiment_data <- sentiment_data %>%
      left_join(industry_info, by = "Ticker")
    
    # Add industry-based sentiment adjustments
    sentiment_data <- sentiment_data %>%
      mutate(
        industry_adjustment = case_when(
          grepl("Technology|Software|Semiconductors", IndustryGroup, ignore.case = TRUE) ~ 0.1,
          grepl("Health Care|Pharmaceuticals|Biotechnology", IndustryGroup, ignore.case = TRUE) ~ 0.05,
          grepl("Financial|Banks|Insurance", IndustryGroup, ignore.case = TRUE) ~ -0.05,
          grepl("Energy|Oil|Gas", IndustryGroup, ignore.case = TRUE) ~ -0.1,
          grepl("Consumer|Retail", IndustryGroup, ignore.case = TRUE) ~ 0,
          TRUE ~ 0
        ),
        adjusted_sentiment = composite_sentiment + industry_adjustment
      )
  }
  
  cat("✅ Synthetic sentiment data generated\n")
  cat(sprintf("   • Added %d sentiment metrics\n", ncol(sentiment_data) - 3))  # Excluding Ticker, Date, IndustryGroup
  
  return(sentiment_data)
}

# Generate sentiment data
sentiment_data <- generate_sentiment_data(processed_data)

# ============================================================
# 5. MERGE SENTIMENT WITH FINANCIAL DATA
# ============================================================

cat("\n🔗 MERGING SENTIMENT WITH FINANCIAL DATA...\n")

combined_data <- processed_data %>%
  left_join(sentiment_data, by = c("Ticker", "Date"))

# Save combined dataset
combined_filename <- "data_with_sentiment.csv"
write_csv(combined_data, combined_filename)

cat(sprintf("✅ Combined dataset created: %d rows × %d columns\n", 
            nrow(combined_data), ncol(combined_data)))
cat(sprintf("   • Saved: %s\n", combined_filename))
cat(sprintf("   • File size: %.3f MB\n", 
            file.info(combined_filename)$size / (1024 * 1024)))

# ============================================================
# 6. SENTIMENT ANALYSIS REPORT
# ============================================================

cat("\n📊 GENERATING SENTIMENT ANALYSIS REPORT...\n")

# Company-level sentiment summary
company_sentiment <- combined_data %>%
  group_by(Ticker) %>%
  summarise(
    avg_sentiment = mean(composite_sentiment, na.rm = TRUE),
    sentiment_volatility = sd(composite_sentiment, na.rm = TRUE),
    positive_news_count = sum(positive_news, na.rm = TRUE),
    negative_news_count = sum(negative_news, na.rm = TRUE),
    neutral_news_count = sum(neutral_news, na.rm = TRUE),
    total_news = n(),
    .groups = "drop"
  ) %>%
  mutate(
    sentiment_category = case_when(
      avg_sentiment > 0.3 ~ "Very Positive",
      avg_sentiment > 0.1 ~ "Positive",
      avg_sentiment > -0.1 ~ "Neutral",
      avg_sentiment > -0.3 ~ "Negative",
      TRUE ~ "Very Negative"
    ),
    positivity_ratio = positive_news_count / total_news
  )

# Add industry information if available
if ("IndustryGroup" %in% colnames(combined_data)) {
  industry_info <- combined_data %>%
    select(Ticker, IndustryGroup) %>%
    distinct()
  
  company_sentiment <- company_sentiment %>%
    left_join(industry_info, by = "Ticker")
}

# Save sentiment report
sentiment_report_file <- "sentiment_analysis_report.csv"
write_csv(company_sentiment, sentiment_report_file)

cat(sprintf("✅ Sentiment analysis report created for %d companies\n", 
            nrow(company_sentiment)))
cat(sprintf("   • Saved: %s\n", sentiment_report_file))

# ============================================================
# 7. VISUALIZATION USING BASE R GRAPHICS
# ============================================================

cat("\n🎨 CREATING SENTIMENT VISUALIZATIONS (Base R)...\n")

# Visualization 1: Sentiment Distribution (Bar Plot)
png("sentiment_distribution.png", width = 800, height = 600, res = 100)

# Create table of sentiment categories
sentiment_counts <- table(company_sentiment$sentiment_category)

# Order categories properly
category_order <- c("Very Positive", "Positive", "Neutral", "Negative", "Very Negative")
sentiment_counts <- sentiment_counts[category_order[category_order %in% names(sentiment_counts)]]

# Create bar plot with colors
colors <- c("darkgreen", "lightgreen", "lightblue", "orange", "red")
colors <- colors[1:length(sentiment_counts)]

par(mar = c(7, 5, 4, 2))  # Adjust margins for longer x-axis labels

barplot(sentiment_counts,
        main = "Distribution of Company Sentiment Categories",
        xlab = "",
        ylab = "Number of Companies",
        col = colors,
        ylim = c(0, max(sentiment_counts) * 1.2),
        cex.names = 1.1,
        cex.axis = 1.1,
        cex.main = 1.3)

title(xlab = "Sentiment Category", line = 5, cex.lab = 1.2)

# Add value labels on top of bars
text(x = barplot(sentiment_counts, plot = FALSE), 
     y = sentiment_counts + 1, 
     labels = sentiment_counts, 
     pos = 3, 
     cex = 1.1)

# Add subtitle
mtext(paste("Based on", nrow(company_sentiment), "companies"), 
      side = 3, line = 0.5, cex = 1.0)

dev.off()
cat("   • sentiment_distribution.png created\n")

# Visualization 2: Sentiment vs Market Cap Scatter Plot
if ("MarketCap" %in% colnames(combined_data)) {
  # Get average market cap and sentiment per company
  marketcap_sentiment <- combined_data %>%
    group_by(Ticker) %>%
    summarise(
      avg_marketcap = mean(MarketCap, na.rm = TRUE) / 1e9,  # Convert to billions
      avg_sentiment = mean(composite_sentiment, na.rm = TRUE),
      .groups = "drop"
    )
  
  png("sentiment_vs_marketcap.png", width = 1000, height = 700, res = 100)
  
  # Set up the plot
  par(mar = c(5, 5, 4, 5))
  
  # Create scatter plot with colored points
  plot(marketcap_sentiment$avg_sentiment, 
       marketcap_sentiment$avg_marketcap,
       xlab = "Average Sentiment Score",
       ylab = "Average Market Cap (Billions)",
       main = "Sentiment vs. Market Capitalization",
       pch = 19,
       cex = 1.2,
       col = "steelblue",
       cex.lab = 1.2,
       cex.axis = 1.1,
       cex.main = 1.4,
       xlim = range(marketcap_sentiment$avg_sentiment, na.rm = TRUE) * 1.1,
       ylim = range(marketcap_sentiment$avg_marketcap, na.rm = TRUE) * 1.1)
  
  # Add grid
  grid(col = "gray", lty = "dotted")
  
  # Add regression line
  if (nrow(marketcap_sentiment) > 1) {
    lm_fit <- lm(avg_marketcap ~ avg_sentiment, data = marketcap_sentiment)
    abline(lm_fit, col = "red", lwd = 2, lty = "dashed")
    
    # Add R-squared value
    r2 <- summary(lm_fit)$r.squared
    legend("topright", 
           legend = paste("R² =", round(r2, 3)),
           bty = "n",
           cex = 1.1)
  }
  
  # Add company count
  mtext(paste("Based on", nrow(marketcap_sentiment), "companies"), 
        side = 3, line = 0.5, cex = 1.0)
  
  dev.off()
  cat("   • sentiment_vs_marketcap.png created\n")
}

# Visualization 3: Sentiment Score Distribution (Histogram)
png("sentiment_histogram.png", width = 800, height = 600, res = 100)

hist(company_sentiment$avg_sentiment,
     breaks = 20,
     main = "Distribution of Average Sentiment Scores",
     xlab = "Average Sentiment Score",
     ylab = "Number of Companies",
     col = "lightblue",
     border = "darkblue",
     cex.lab = 1.2,
     cex.axis = 1.1,
     cex.main = 1.3)

# Add mean line
abline(v = mean(company_sentiment$avg_sentiment, na.rm = TRUE), 
       col = "red", lwd = 2, lty = "dashed")

# Add mean value text
mean_val <- mean(company_sentiment$avg_sentiment, na.rm = TRUE)
text(x = mean_val, 
     y = par("usr")[4] * 0.9, 
     labels = paste("Mean =", round(mean_val, 3)), 
     pos = ifelse(mean_val > 0, 2, 4), 
     col = "red", 
     cex = 1.1)

dev.off()
cat("   • sentiment_histogram.png created\n")

cat("✅ All visualizations created using Base R graphics\n")

# ============================================================
# 8. MODULE 2 EXECUTION SUMMARY
# ============================================================

cat("\n")
cat(rep("=", 70), "\n", sep = "")
cat("📊 MODULE 2 EXECUTION SUMMARY\n")
cat(rep("=", 70), "\n\n", sep = "")

# Create summary
module2_summary <- list(
  execution_timestamp = Sys.time(),
  input_file = "module1_processed_data.csv",
  output_files = c(
    "data_with_sentiment.csv",
    "sentiment_analysis_report.csv",
    "sentiment_distribution.png",
    "sentiment_histogram.png",
    if ("MarketCap" %in% colnames(combined_data)) "sentiment_vs_marketcap.png" else NULL
  ),
  dataset_metrics = list(
    rows_with_sentiment = nrow(combined_data),
    columns_with_sentiment = ncol(combined_data),
    companies_analyzed = length(unique(company_sentiment$Ticker)),
    sentiment_categories = table(company_sentiment$sentiment_category)
  ),
  sentiment_statistics = list(
    avg_composite_sentiment = mean(combined_data$composite_sentiment, na.rm = TRUE),
    sentiment_volatility = sd(combined_data$composite_sentiment, na.rm = TRUE),
    positive_news_total = sum(combined_data$positive_news, na.rm = TRUE),
    negative_news_total = sum(combined_data$negative_news, na.rm = TRUE)
  ),
  visualization_method = "Base R graphics (no ggplot2 dependency)"
)

# Remove NULL values from output files
module2_summary$output_files <- module2_summary$output_files[!sapply(module2_summary$output_files, is.null)]

# Save summary
module2_summary_file <- "module2_execution_summary.rds"
saveRDS(module2_summary, module2_summary_file)

cat("📄 Module 2 execution summary saved\n\n")

# ============================================================
# 9. FINAL OUTPUT
# ============================================================

cat("📁 MODULE 2 OUTPUT FILES:\n")
output_files <- module2_summary$output_files
output_files <- c(output_files, "module2_execution_summary.rds")

for (file in output_files) {
  if (file.exists(file)) {
    size_kb <- ifelse(grepl("\\.png$", file), 
                      round(file.info(file)$size / 1024, 2),
                      round(file.info(file)$size / 1024, 2))
    cat(sprintf("  • %-40s %.2f KB\n", file, size_kb))
  }
}

cat("\n📊 SENTIMENT ANALYSIS INSIGHTS:\n")
cat(sprintf("  • Companies analyzed: %d\n", nrow(company_sentiment)))
cat(sprintf("  • Average sentiment score: %.3f\n", mean(company_sentiment$avg_sentiment, na.rm = TRUE)))
cat(sprintf("  • Most common sentiment category: %s\n", 
            names(which.max(table(company_sentiment$sentiment_category)))))

cat("\n🔍 SAMPLE OF SENTIMENT DATA (first 5 rows):\n")
print(head(company_sentiment, 5))

cat("\n🔍 SAMPLE OF COMBINED DATA (first 3 rows):\n")
print(head(combined_data, 3))

cat("\n")
cat(rep("=", 70), "\n", sep = "")
cat("✅ MODULE 2 COMPLETED SUCCESSFULLY!\n")
cat(rep("=", 70), "\n\n", sep = "")

cat("🚀 READY FOR MODULE 3: TECHNICAL INDICATORS & TIME SERIES ANALYSIS\n")