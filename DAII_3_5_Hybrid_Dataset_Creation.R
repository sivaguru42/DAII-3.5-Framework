# ============================================================
# DAII 3.5 FRAMEWORK - COMPLETE HYBRID DATASET CREATION SCRIPT
# ============================================================
# Author: Data Science Team
# Date: Created on [Date]
# Description: Creates hybrid dataset combining real fundamentals
#              with synthetic time series and macro data
# ============================================================

cat("🚀 STARTING DAII 3.5 HYBRID DATASET CREATION\n")
cat(rep("=", 70), "\n\n", sep = "")

# ============================================================
# STEP 0: LOAD REQUIRED PACKAGES
# ============================================================
cat("📦 STEP 0: LOADING REQUIRED PACKAGES\n")

required_packages <- c("dplyr", "lubridate", "tidyr", "zoo", "ggplot2", "purrr")
new_packages <- required_packages[!(required_packages %in% installed.packages()[,"Package"])]
if(length(new_packages)) install.packages(new_packages)
invisible(lapply(required_packages, require, character.only = TRUE))

cat("✅ Packages loaded successfully\n\n")

# ============================================================
# STEP 1: LOAD AND PREPARE REAL DATA
# ============================================================
cat("📥 STEP 1: LOADING REAL DATA\n")
cat(rep("-", 70), "\n", sep = "")

# Define file path
real_data_path <- "C:/Users/sganesan/OneDrive - dumac.duke.edu/DAII/data/raw/N200_FINAL_StrategicDUMACPortfolioDistribution_BBergUploadRawData_Integrated_Data.csv"

# Load real data
if(file.exists(real_data_path)) {
  cat("📂 Loading data from:", real_data_path, "\n")
  real_data <- read.csv(real_data_path, stringsAsFactors = FALSE)
  cat("✅ Real data loaded successfully\n")
  cat("   Dimensions:", dim(real_data), "\n")
  cat("   Columns:", ncol(real_data), "\n")
  cat("   Rows:", nrow(real_data), "\n")
} else {
  cat("⚠️ Integrated data file not found at:", real_data_path, "\n")
  
  # Try to find any CSV file in the directory
  data_dir <- dirname(real_data_path)
  csv_files <- list.files(data_dir, pattern = "\\.csv$", full.names = TRUE)
  
  if(length(csv_files) > 0) {
    cat("📂 Found CSV files in directory. Loading first file:", basename(csv_files[1]), "\n")
    real_data <- read.csv(csv_files[1], stringsAsFactors = FALSE)
    cat("✅ Data loaded from alternative file\n")
  } else {
    cat("❌ No CSV files found. Creating sample real_data for demonstration.\n")
    # Create sample real_data for demonstration
    real_data <- data.frame(
      company_id = paste0("COMPANY_", 1:217),
      ticker = paste0("TICK", sprintf("%03d", 1:217)),
      sector = sample(c("Technology", "Healthcare", "Financials", "Consumer", "Industrial"), 
                      217, replace = TRUE),
      market_cap = runif(217, 1e9, 1e11),
      pe_ratio = runif(217, 10, 30),
      fund_id = sample(paste0("FUND_", LETTERS[1:10]), 217, replace = TRUE),
      stringsAsFactors = FALSE
    )
  }
}

# Display data structure
cat("\n📊 REAL DATA STRUCTURE:\n")
str(real_data, max.level = 1)
cat("\n📋 FIRST 3 ROWS:\n")
print(head(real_data, 3))

# Identify key columns
cat("\n🔍 IDENTIFYING KEY COLUMNS...\n")

# Find ticker column
ticker_candidates <- c("ticker", "Ticker", "symbol", "Symbol", "company_id", "CompanyID")
ticker_col <- NULL
for(candidate in ticker_candidates) {
  if(candidate %in% colnames(real_data)) {
    ticker_col <- candidate
    break
  }
}

if(is.null(ticker_col)) {
  cat("⚠️ No standard ticker column found. Using first character column as ticker.\n")
  char_cols <- colnames(real_data)[sapply(real_data, is.character)]
  if(length(char_cols) > 0) {
    ticker_col <- char_cols[1]
  } else {
    cat("❌ No character columns found. Creating synthetic tickers.\n")
    real_data$ticker <- paste0("TICK", sprintf("%03d", 1:nrow(real_data)))
    ticker_col <- "ticker"
  }
}
cat("   Ticker column:", ticker_col, "\n")

# Find fund column
fund_candidates <- c("fund_id", "Fund_ID", "fund", "Fund", "portfolio", "Portfolio")
fund_col <- NULL
for(candidate in fund_candidates) {
  if(candidate %in% colnames(real_data)) {
    fund_col <- candidate
    break
  }
}

if(is.null(fund_col)) {
  cat("⚠️ No fund column found. Will use default fund ID.\n")
} else {
  cat("   Fund column:", fund_col, "\n")
}

# Clean real_data
cat("\n🧹 CLEANING REAL DATA...\n")

# Standardize column names
real_data_clean <- real_data

# Ensure ticker column exists and is character
real_data_clean$Ticker <- as.character(real_data_clean[[ticker_col]])

# Remove any invalid tickers
real_data_clean <- real_data_clean %>%
  filter(!is.na(Ticker), Ticker != "", !grepl("#N/A|N/A|NA|null", Ticker, ignore.case = TRUE))

# Add fund_id if available
if(!is.null(fund_col)) {
  real_data_clean$fund_id <- as.character(real_data_clean[[fund_col]])
} else {
  real_data_clean$fund_id <- "DEFAULT_FUND_001"
}

# Get unique companies
all_companies <- unique(real_data_clean$Ticker)
cat("✅ Real data cleaned\n")
cat("   Unique companies:", length(all_companies), "\n")
cat("   Unique funds:", length(unique(real_data_clean$fund_id)), "\n\n")

# ============================================================
# STEP 2: GENERATE SYNTHETIC TIME SERIES
# ============================================================
cat("📈 STEP 2: GENERATING SYNTHETIC TIME SERIES\n")
cat(rep("-", 70), "\n", sep = "")

# Define date range
start_date <- as.Date("2019-01-01")
end_date <- as.Date("2023-12-31")
all_dates <- seq.Date(from = start_date, to = end_date, by = "day")
# Keep only business days (Mon-Fri)
all_dates <- all_dates[!weekdays(all_dates) %in% c("Saturday", "Sunday")]

cat("   Date range:", as.character(start_date), "to", as.character(end_date), "\n")
cat("   Trading days:", length(all_dates), "\n")
cat("   Companies to generate:", length(all_companies), "\n")

# Function to generate synthetic price series for one company
generate_company_series <- function(ticker, start_price = 100, dates = all_dates) {
  n_days <- length(dates)
  
  # Generate random walk with drift and volatility
  set.seed(which(all_companies == ticker))  # Seed based on ticker position for reproducibility
  
  # Base parameters with some variability by ticker
  drift <- rnorm(1, mean = 0.0003, sd = 0.0001)  # Daily drift
  volatility <- rnorm(1, mean = 0.02, sd = 0.005)  # Daily volatility
  
  # Generate log returns
  log_returns <- rnorm(n_days - 1, mean = drift, sd = volatility)
  
  # Calculate prices
  prices <- cumprod(c(start_price, exp(log_returns)))
  
  # Add some autocorrelation and seasonality
  for(i in 2:n_days) {
    # Add momentum effect
    if(i > 5) {
      momentum <- mean(log_returns[(i-5):(i-1)]) * 0.3
      log_returns[i-1] <- log_returns[i-1] + momentum
    }
    
    # Monthly seasonality
    month_day <- as.numeric(format(dates[i], "%d"))
    if(month_day <= 5) {
      log_returns[i-1] <- log_returns[i-1] * 1.1  # First week boost
    }
  }
  
  # Recalculate prices with adjusted returns
  prices <- cumprod(c(start_price, exp(log_returns)))
  
  # Generate volumes (correlated with absolute returns)
  volumes <- runif(n_days, 100000, 10000000) * (1 + abs(c(0, log_returns)) * 10)
  
  # Create data frame
  data.frame(
    Ticker = ticker,
    Date = dates,
    Open = prices * runif(n_days, 0.99, 1.01),
    High = prices * runif(n_days, 1.005, 1.02),
    Low = prices * runif(n_days, 0.98, 0.995),
    Close = prices,
    Volume = round(volumes),
    stringsAsFactors = FALSE
  )
}

# Generate time series for all companies
cat("\n⚙️  GENERATING TIME SERIES (this may take a moment)...\n")

# Use parallel processing if available, otherwise sequential
if(requireNamespace("parallel", quietly = TRUE)) {
  library(parallel)
  n_cores <- detectCores() - 1
  cat("   Using parallel processing with", n_cores, "cores\n")
  
  cl <- makeCluster(n_cores)
  clusterExport(cl, c("all_companies", "all_dates", "generate_company_series"))
  clusterEvalQ(cl, library(dplyr))
  
  combined_ts_list <- parLapply(cl, all_companies, function(ticker) {
    generate_company_series(ticker, dates = all_dates)
  })
  
  stopCluster(cl)
  combined_ts <- bind_rows(combined_ts_list)
} else {
  cat("   Using sequential processing\n")
  combined_ts_list <- list()
  for(i in seq_along(all_companies)) {
    if(i %% 20 == 0) cat("   Processed", i, "of", length(all_companies), "companies\n")
    combined_ts_list[[i]] <- generate_company_series(all_companies[i], dates = all_dates)
  }
  combined_ts <- bind_rows(combined_ts_list)
}

cat("✅ Synthetic time series generated\n")
cat("   Total observations:", nrow(combined_ts), "\n")
cat("   Unique companies:", length(unique(combined_ts$Ticker)), "\n")
cat("   Date range:", as.character(min(combined_ts$Date)), "to", 
    as.character(max(combined_ts$Date)), "\n\n")

# ============================================================
# STEP 3: GENERATE MACROECONOMIC TIME SERIES
# ============================================================
cat("🌍 STEP 3: GENERATING MACROECONOMIC TIME SERIES\n")
cat(rep("-", 70), "\n", sep = "")

generate_macro_series <- function(dates = all_dates) {
  n_days <- length(dates)
  
  # Set seed for reproducibility
  set.seed(123)
  
  # Generate N200 Index (simulating S&P 500 type index)
  n200_returns <- rnorm(n_days - 1, mean = 0.0004, sd = 0.012)
  
  # Add autocorrelation and trends
  for(i in 2:length(n200_returns)) {
    # Momentum effect
    if(i > 5) n200_returns[i] <- n200_returns[i] + 0.3 * mean(n200_returns[(i-5):(i-1)])
    
    # Volatility clustering
    if(i > 1 && abs(n200_returns[i-1]) > 0.02) {
      n200_returns[i] <- n200_returns[i] * 1.5
    }
  }
  
  # Calculate N200 price levels (starting at 4000)
  n200_prices <- cumprod(c(4000, exp(n200_returns)))
  
  # Generate VIX (volatility index)
  vix_base <- 15 + 5 * sin(2 * pi * (1:n_days) / 252)  # Annual cycle
  vix <- vix_base + abs(n200_returns) * 100 * 5  # Scale returns to VIX
  
  # Generate Treasury yields (10-year)
  treasury_base <- 2.5 + 0.5 * sin(2 * pi * (1:n_days) / 252 * 2)  # Semi-annual cycle
  treasury_yield <- treasury_base + cumsum(rnorm(n_days, 0, 0.01))
  
  # Generate inflation expectations
  inflation <- 2.0 + 0.3 * sin(2 * pi * (1:n_days) / 252) + cumsum(rnorm(n_days, 0, 0.005))
  
  # Create data frame
  data.frame(
    Date = dates,
    N200_Index_Close = round(n200_prices, 2),
    VIX_Close = round(pmax(10, pmin(40, vix)), 2),
    Treasury_10Y_Yield = round(pmax(0.5, pmin(5.0, treasury_yield)), 3),
    Inflation_Expectation = round(pmax(0, pmin(10, inflation)), 2),
    USD_Index = round(100 + cumsum(rnorm(n_days, 0, 0.02)), 2),
    Oil_Price = round(70 + 10 * sin(2 * pi * (1:n_days) / 126) + cumsum(rnorm(n_days, 0, 0.1)), 2),
    Gold_Price = round(1800 + 50 * sin(2 * pi * (1:n_days) / 252) + cumsum(rnorm(n_days, 0, 0.05)), 2),
    stringsAsFactors = FALSE
  )
}

macro_data <- generate_macro_series(dates = all_dates)

cat("✅ Macroeconomic data generated\n")
cat("   Observations:", nrow(macro_data), "\n")
cat("   Variables:", ncol(macro_data) - 1, "\n")
cat("   Date range:", as.character(min(macro_data$Date)), "to", 
    as.character(max(macro_data$Date)), "\n\n")

# ============================================================
# STEP 4: CREATE PANEL DATASET
# ============================================================
cat("🧩 STEP 4: CREATING PANEL DATASET\n")
cat(rep("-", 70), "\n", sep = "")

# Create base panel (all company-date combinations)
cat("   Creating base panel (all company-date combinations)...\n")
base_panel <- expand.grid(
  Ticker = all_companies,
  Date = all_dates,
  stringsAsFactors = FALSE
)

cat("   Base panel created:", nrow(base_panel), "rows\n")

# Merge with fund assignments
cat("   Merging with fund assignments...\n")

# Get the most recent fund assignment for each company
fund_assignments <- real_data_clean %>%
  group_by(Ticker) %>%
  slice(1) %>%  # Take first row for each ticker
  ungroup() %>%
  select(Ticker, fund_id)

# Merge with base panel
panel_data <- base_panel %>%
  left_join(fund_assignments, by = "Ticker")

cat("   Panel after fund merge:", nrow(panel_data), "rows\n")

# Fill any missing fund IDs
if(any(is.na(panel_data$fund_id))) {
  panel_data$fund_id[is.na(panel_data$fund_id)] <- "DEFAULT_FUND_001"
  cat("   Filled missing fund IDs\n")
}

# Merge with synthetic time series
cat("   Merging with synthetic time series...\n")
panel_data <- panel_data %>%
  left_join(combined_ts, by = c("Ticker", "Date"))

cat("   Panel after time series merge:", nrow(panel_data), "rows\n")
cat("   Rows with Close data:", sum(!is.na(panel_data$Close)), "\n")

# Merge with macro data
cat("   Merging with macro data...\n")
panel_data <- panel_data %>%
  left_join(macro_data, by = "Date")

cat("   Panel after macro merge:", nrow(panel_data), "rows\n")

# Identify fundamental columns (excluding metadata columns)
metadata_cols <- c(ticker_col, fund_col, "Ticker", "fund_id")
fundamental_cols <- setdiff(colnames(real_data_clean), metadata_cols)

if(length(fundamental_cols) > 0) {
  cat("   Found", length(fundamental_cols), "fundamental variables\n")
  
  # Get the most recent fundamentals for each company
  fundamentals_latest <- real_data_clean %>%
    group_by(Ticker) %>%
    slice(1) %>%  # Take first row for each ticker
    ungroup() %>%
    select(Ticker, all_of(fundamental_cols))
  
  # Merge fundamentals into panel
  panel_data <- panel_data %>%
    left_join(fundamentals_latest, by = "Ticker")
  
  cat("   Fundamental variables added\n")
} else {
  cat("   ⚠️ No fundamental variables to add\n")
}

# Calculate derived variables
cat("   Calculating derived variables...\n")

# Add market cap simulation
set.seed(123)
market_caps <- data.frame(
  Ticker = all_companies,
  base_market_cap = runif(length(all_companies), 1e9, 1e11)  # $1B to $100B
)

# Calculate derived variables
panel_data <- panel_data %>%
  left_join(market_caps, by = "Ticker") %>%
  group_by(Ticker) %>%
  arrange(Date) %>%
  mutate(
    market_cap = base_market_cap * (Close / 100),  # Scale by price relative to $100
    volume_value = Volume * Close,                 # Dollar volume
    log_return = log(Close / lag(Close)),          # Daily log returns
    volatility_30d = zoo::rollapply(log_return, width = 30, FUN = sd, 
                                    fill = NA, align = "right", na.rm = TRUE),
    relative_strength = (Close - zoo::rollapply(Close, width = 30, FUN = mean, 
                                                fill = NA, align = "right")) / 
      zoo::rollapply(Close, width = 30, FUN = sd, 
                     fill = NA, align = "right"),
    beta_to_n200 = {
      # Calculate beta relative to N200 index
      if(sum(!is.na(log_return)) > 30) {
        n200_returns <- log(macro_data$N200_Index_Close / lag(macro_data$N200_Index_Close))
        aligned_returns <- na.omit(cbind(log_return, n200_returns[1:length(log_return)]))
        if(nrow(aligned_returns) > 30) {
          cov(aligned_returns[,1], aligned_returns[,2], use = "complete.obs") / 
            var(aligned_returns[,2], na.rm = TRUE)
        } else NA
      } else NA
    }
  ) %>%
  ungroup()

cat("   Derived variables calculated\n")

# Add sector if available in real_data_clean
if("sector" %in% colnames(real_data_clean) || "Sector" %in% colnames(real_data_clean)) {
  sector_col <- ifelse("sector" %in% colnames(real_data_clean), "sector", "Sector")
  sector_info <- real_data_clean %>%
    select(Ticker, !!sym(sector_col)) %>%
    distinct()
  colnames(sector_info)[2] <- "Sector"
  
  panel_data <- panel_data %>%
    left_join(sector_info, by = "Ticker")
  cat("   Added sector information\n")
}

cat("\n✅ PANEL DATASET CREATED SUCCESSFULLY!\n")
cat(rep("=", 70), "\n", sep = "")
cat("📊 FINAL PANEL DATASET STATISTICS:\n")
cat("   Total rows:", format(nrow(panel_data), big.mark = ","), "\n")
cat("   Total columns:", ncol(panel_data), "\n")
cat("   Date range:", as.character(min(panel_data$Date, na.rm = TRUE)), "to", 
    as.character(max(panel_data$Date, na.rm = TRUE)), "\n")
cat("   Unique companies:", length(unique(panel_data$Ticker)), "\n")
cat("   Unique funds:", length(unique(panel_data$fund_id)), "\n")
cat("   Trading days:", length(unique(panel_data$Date)), "\n")
cat("   Rows with Close data:", sum(!is.na(panel_data$Close)), "\n")
cat("   Complete cases:", sum(complete.cases(panel_data)), "\n")
cat(rep("=", 70), "\n\n", sep = "")

# ============================================================
# STEP 5: SAVE ALL DATA
# ============================================================
cat("💾 STEP 5: SAVING ALL DATA FILES\n")
cat(rep("-", 70), "\n", sep = "")

# Create directory structure
dir.create("DAII_3.5_Framework", showWarnings = FALSE, recursive = TRUE)
dir.create("DAII_3.5_Framework/data", showWarnings = FALSE, recursive = TRUE)
dir.create("DAII_3.5_Framework/scripts", showWarnings = FALSE, recursive = TRUE)
dir.create("DAII_3.5_Framework/tests", showWarnings = FALSE, recursive = TRUE)

# Save the complete panel dataset
saveRDS(panel_data, "DAII_3.5_Framework/data/N200_Hybrid_Panel_Data.rds")
cat("✅ Saved panel dataset: DAII_3.5_Framework/data/N200_Hybrid_Panel_Data.rds\n")

# Save sample as CSV
write.csv(head(panel_data, 100000), 
          "DAII_3.5_Framework/data/N200_Hybrid_Panel_Data_Sample.csv", 
          row.names = FALSE)
cat("✅ Saved sample CSV: DAII_3.5_Framework/data/N200_Hybrid_Panel_Data_Sample.csv\n")

# Save individual components
saveRDS(combined_ts, "DAII_3.5_Framework/data/N200_Time_Series_All.rds")
saveRDS(macro_data, "DAII_3.5_Framework/data/N200_Macro_Data.rds")
saveRDS(real_data_clean, "DAII_3.5_Framework/data/N200_Fundamentals_Clean.rds")

cat("✅ Saved individual data components\n")

# ============================================================
# STEP 6: CREATE VALIDATION REPORT
# ============================================================
cat("\n📊 STEP 6: CREATING VALIDATION REPORT\n")
cat(rep("-", 70), "\n", sep = "")

# Create validation summary
validation_summary <- list(
  creation_timestamp = Sys.time(),
  dataset_stats = list(
    total_rows = nrow(panel_data),
    total_columns = ncol(panel_data),
    unique_companies = length(unique(panel_data$Ticker)),
    unique_funds = length(unique(panel_data$fund_id)),
    trading_days = length(unique(panel_data$Date)),
    date_range = as.character(range(panel_data$Date, na.rm = TRUE)),
    complete_cases = sum(complete.cases(panel_data)),
    missing_close = sum(is.na(panel_data$Close))
  ),
  data_quality = list(
    column_types = sapply(panel_data, class),
    missing_values = colSums(is.na(panel_data)),
    missing_percentage = round(colSums(is.na(panel_data)) / nrow(panel_data) * 100, 2)
  )
)

# Save validation summary
saveRDS(validation_summary, "DAII_3.5_Framework/tests/validation_summary.rds")

# Create a readable report
sink("DAII_3.5_Framework/validation_report.txt")
cat("DAII 3.5 HYBRID DATASET - VALIDATION REPORT\n")
cat("============================================\n\n")
cat("Creation Date:", as.character(Sys.Date()), "\n")
cat("Creation Time:", format(Sys.time(), "%H:%M:%S"), "\n\n")

cat("DATASET STATISTICS:\n")
cat("-------------------\n")
cat("Total Observations:", format(validation_summary$dataset_stats$total_rows, big.mark = ","), "\n")
cat("Variables:", validation_summary$dataset_stats$total_columns, "\n")
cat("Unique Companies:", validation_summary$dataset_stats$unique_companies, "\n")
cat("Unique Funds:", validation_summary$dataset_stats$unique_funds, "\n")
cat("Trading Days:", validation_summary$dataset_stats$trading_days, "\n")
cat("Date Range:", validation_summary$dataset_stats$date_range[1], "to", 
    validation_summary$dataset_stats$date_range[2], "\n")
cat("Complete Cases:", format(validation_summary$dataset_stats$complete_cases, big.mark = ","), "\n")
cat("Missing Close Prices:", format(validation_summary$dataset_stats$missing_close, big.mark = ","), "\n\n")

cat("DATA QUALITY CHECK:\n")
cat("------------------\n")
cat("Columns with >20% missing values:\n")
high_missing <- validation_summary$data_quality$missing_percentage[validation_summary$data_quality$missing_percentage > 20]
if(length(high_missing) > 0) {
  for(col in names(high_missing)) {
    cat(sprintf("  %-25s: %6.1f%% missing\n", col, high_missing[col]))
  }
} else {
  cat("  None (all columns have <20% missing values)\n")
}
sink()

cat("✅ Validation report created: DAII_3.5_Framework/validation_report.txt\n")

# ============================================================
# COMPLETION MESSAGE
# ============================================================
cat("\n")
cat(rep("=", 70), "\n", sep = "")
cat("🎉 DAII 3.5 HYBRID DATASET CREATION - COMPLETE! 🎉\n")
cat(rep("=", 70), "\n\n", sep = "")

cat("📁 FILES SAVED TO DAII_3.5_Framework/:\n")
cat("  data/N200_Hybrid_Panel_Data.rds          # Complete panel dataset\n")
cat("  data/N200_Hybrid_Panel_Data_Sample.csv   # Sample for inspection\n")
cat("  data/N200_Time_Series_All.rds           # Synthetic time series\n")
cat("  data/N200_Macro_Data.rds                # Macroeconomic data\n")
cat("  data/N200_Fundamentals_Clean.rds        # Cleaned fundamentals\n")
cat("  tests/validation_summary.rds            # Validation data\n")
cat("  validation_report.txt                   # Readable validation report\n\n")

cat("🚀 NEXT STEPS:\n")
cat("  1. Load the dataset: panel_data <- readRDS('DAII_3.5_Framework/data/N200_Hybrid_Panel_Data.rds')\n")
cat("  2. Explore the data: summary(panel_data)\n")
cat("  3. Begin analysis and modeling\n\n")

cat("📊 FINAL SUMMARY:\n")
cat("  Total panel observations:", format(nrow(panel_data), big.mark = ","), "\n")
cat("  Time period:", as.character(min(panel_data$Date, na.rm = TRUE)), "to", 
    as.character(max(panel_data$Date, na.rm = TRUE)), "\n")
cat("  Companies:", length(unique(panel_data$Ticker)), "\n")
cat("  Funds:", length(unique(panel_data$fund_id)), "\n")
cat("  Variables available:", ncol(panel_data), "\n\n")

# Show final sample
cat("📋 FINAL DATASET SAMPLE (first 3 rows):\n")
print(head(panel_data, 3))

cat("\n✅ Script execution complete! Ready for analysis.\n")