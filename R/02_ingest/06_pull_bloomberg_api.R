# ============================================================================
# BLOOMBERG DEVELOPMENT PULL - ONE-TIME CACHE
# Version: 1.0 | Date: 2026-02-20
# Purpose: Pull 15 years of monthly prices for all 217 companies
# Authentication: App Identity (daii-dev-app) via Bloomberg API Server
# Output: Cached RDS file for pipeline development
# ============================================================================

# ----------------------------------------------------------------------------
# 1. LOAD REQUIRED PACKAGES
# ----------------------------------------------------------------------------
library(Rblpapi)
library(dplyr)
library(DBI)
library(odbc)

# ----------------------------------------------------------------------------
# 2. SETUP BLOOMBERG CONNECTION WITH APP IDENTITY
# ----------------------------------------------------------------------------

#' Connect to Bloomberg API Server using App Identity
#' 
#' Required Information from Engineering:
#'   - Bloomberg API Server Host (e.g., "api.bloomberg.com")
#'   - API Server Port (usually 8194)
#'   - Authentication Method (Client Credentials)
#'   
connect_bloomberg_app <- function() {
  
  # Load credentials from environment variables (SECURE METHOD)
  client_id <- Sys.getenv("BLOOMBERG_CLIENT_ID")
  client_secret <- Sys.getenv("BLOOMBERG_CLIENT_SECRET")
  tenant_id <- Sys.getenv("BLOOMBERG_TENANT_ID")
  
  # If not set in environment, prompt user (for development only)
  if(client_id == "") {
    cat("\n⚠️  Bloomberg credentials not found in environment.\n")
    cat("Please enter your Bloomberg API credentials:\n")
    client_id <- readline("Client ID: ")
    client_secret <- readline("Client Secret: ")
    tenant_id <- readline("Tenant ID: ")
  }
  
  # Connection parameters - YOU MUST UPDATE THESE FROM ENGINEERING
  conn_params <- list(
    host = "api.bloomberg.com",           # ← UPDATE THIS
    port = 8194L,                          # ← UPDATE THIS
    auth_type = "windows",                  # or "app" depending on setup
    app_name = "daii-dev-app",
    client_id = client_id,
    client_secret = client_secret,
    tenant_id = tenant_id
  )
  
  cat("🔌 Connecting to Bloomberg API Server...\n")
  
  tryCatch({
    # Initialize Bloomberg connection
    blpConnect(
      host = conn_params$host,
      port = conn_params$port,
      auth_type = conn_params$auth_type,
      app_name = conn_params$app_name,
      client_id = conn_params$client_id,
      client_secret = conn_params$client_secret,
      tenant_id = conn_params$tenant_id
    )
    
    cat("✅ Connected to Bloomberg API Server using App Identity\n")
    return(TRUE)
    
  }, error = function(e) {
    stop("❌ Bloomberg connection failed: ", e$message)
  })
}

# ----------------------------------------------------------------------------
# 3. GET COMPANY TICKER LIST
# ----------------------------------------------------------------------------

get_company_tickers <- function() {
  
  # Try to load from company resolution table
  map_file <- "C:\\Users\\sganesan\\OneDrive - dumac.duke.edu\\DAII\\data\\raw\\company_repno_mapping.rds"
  
  if(file.exists(map_file)) {
    company_map <- readRDS(map_file)
    tickers <- company_map$Ticker
    cat(sprintf("✅ Loaded %d tickers from company map\n", length(tickers)))
  } else {
    # Fallback - run company resolution first
    cat("⚠️ Company map not found. Run R_ingest_build_company_resolution.r first\n")
    cat("Using sample tickers for testing...\n")
    tickers <- c(
      "NVDA US Equity", "MSFT US Equity", "AAPL US Equity",
      "GOOGL US Equity", "META US Equity", "TSLA US Equity"
    )
  }
  
  # Ensure proper format (add " Equity" suffix if missing)
  tickers <- ifelse(grepl(" Equity$", tickers), 
                    tickers, 
                    paste0(tickers, " Equity"))
  
  return(tickers)
}

# ----------------------------------------------------------------------------
# 4. PULL MONTHLY PRICE DATA
# ----------------------------------------------------------------------------

pull_monthly_prices <- function(tickers, 
                                start_date = "2010-01-01",
                                end_date = Sys.Date()) {
  
  cat(sprintf("\n📊 Pulling monthly prices for %d tickers...\n", length(tickers)))
  cat(sprintf("   Date range: %s to %s\n", start_date, end_date))
  cat("   This will take approximately 15-20 minutes...\n\n")
  
  # Progress tracking
  total <- length(tickers)
  results <- list()
  
  for(i in seq_along(tickers)) {
    ticker <- tickers[i]
    cat(sprintf("   [%d/%d] %s... ", i, total, ticker))
    
    tryCatch({
      # Pull monthly data for this ticker
      data <- bdh(
        securities = ticker,
        fields = c("PX_LAST"),
        start.date = as.Date(start_date),
        end.date = end_date,
        options = c("periodicitySelection" = "MONTHLY")
      )
      
      # Add ticker column
      data$ticker <- ticker
      results[[ticker]] <- data
      
      cat(sprintf("✅ %d rows\n", nrow(data)))
      
    }, error = function(e) {
      cat(sprintf("❌ Failed: %s\n", e$message))
    })
    
    # Small delay to be kind to the API
    Sys.sleep(0.5)
  }
  
  # Combine all results
  if(length(results) > 0) {
    all_data <- bind_rows(results)
    cat(sprintf("\n✅ Successfully pulled data for %d/%d tickers\n", 
                length(results), total))
    return(all_data)
  } else {
    stop("❌ No data successfully pulled")
  }
}

# ----------------------------------------------------------------------------
# 5. VALIDATE AND SAVE DATA
# ----------------------------------------------------------------------------

validate_and_save <- function(data) {
  
  cat("\n🔍 Validating pulled data...\n")
  
  # Basic validation
  cat(sprintf("   Total rows: %d\n", nrow(data)))
  cat(sprintf("   Unique tickers: %d\n", length(unique(data$ticker))))
  cat(sprintf("   Date range: %s to %s\n", 
              min(data$date), max(data$date)))
  cat(sprintf("   Missing values: %d\n", sum(is.na(data$PX_LAST))))
  
  # Calculate coverage
  expected_rows <- length(unique(data$ticker)) * 12 * 15  # approx
  coverage <- round(nrow(data) / expected_rows * 100, 1)
  cat(sprintf("   Coverage: %s%% of expected\n", coverage))
  
  # Save RDS
  save_path <- "C:\\Users\\sganesan\\OneDrive - dumac.duke.edu\\DAII\\data\\raw\\bloomberg_monthly_prices.rds"
  saveRDS(data, save_path)
  cat(sprintf("\n✅ Saved RDS to: %s\n", save_path))
  
  # Also save CSV for easy viewing
  csv_path <- "C:\\Users\\sganesan\\OneDrive - dumac.duke.edu\\DAII\\data\\raw\\bloomberg_monthly_prices.csv"
  write.csv(data, csv_path, row.names = FALSE)
  cat(sprintf("✅ Saved CSV to: %s\n", csv_path))
  
  return(data)
}

# ----------------------------------------------------------------------------
# 6. MAIN EXECUTION
# ----------------------------------------------------------------------------

cat(paste(rep("=", 80), collapse = ""), "\n")
cat("🚀 BLOOMBERG DEVELOPMENT PULL - ONE-TIME CACHE\n")
cat(paste(rep("=", 80), collapse = ""), "\n\n")

# Step 1: Connect to Bloomberg
connect_bloomberg_app()

# Step 2: Get ticker list
tickers <- get_company_tickers()

# Step 3: Pull monthly prices
monthly_data <- pull_monthly_prices(tickers)

# Step 4: Validate and save
final_data <- validate_and_save(monthly_data)

# Step 5: Summary
cat("\n", paste(rep("=", 80), collapse = ""), "\n")
cat("✅ BLOOMBERG PULL COMPLETE\n")
cat(paste(rep("=", 80), collapse = ""), "\n")
cat("\n📊 Next steps:\n")
cat("   1. Re-run main pipeline - it will automatically use cached data\n")
cat("   2. Verify volatility calculations in Module 1-3\n")
cat("   3. Proceed to Phase 2 ML training\n")