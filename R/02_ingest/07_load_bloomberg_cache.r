# ============================================================================
# INGEST: Load Bloomberg Cache Data
# Version: 2.0 | Date: 2026-03-11
# Purpose: Load previously reformatted Bloomberg monthly prices from CSV cache
#          This file is already in long format with calculated metrics
# ============================================================================

#' Load Bloomberg cached monthly prices
#'
#' @param cache_path Character. Path to the Bloomberg long-format CSV file.
#' @return A dataframe with columns: date, ticker, price, monthly_return, 
#'         volatility_12m, total_return
#' @export
load_bloomberg_cache <- function(cache_path = NULL) {
  
  # Use default path if not provided
  if (is.null(cache_path)) {
    cache_path <- here::here("data", "01_raw", "bloomberg_prices_long.csv")
  }
  
  message("📂 Loading Bloomberg cached data from: ", cache_path)
  
  # Check if file exists
  if (!file.exists(cache_path)) {
    stop("❌ Bloomberg cache file not found at: ", cache_path, "\n",
         "   Please ensure bloomberg_prices_long.csv exists in data/01_raw/")
  }
  
  # Read the CSV (it's already in the correct long format!)
  bloomberg_prices <- read.csv(cache_path, stringsAsFactors = FALSE) %>%
    dplyr::mutate(date = as.Date(date))
  
  # Validate the data
  expected_cols <- c("date", "ticker", "price", "monthly_return", 
                     "volatility_12m", "total_return")
  missing_cols <- setdiff(expected_cols, names(bloomberg_prices))
  
  if (length(missing_cols) > 0) {
    warning("⚠️ Missing expected columns: ", paste(missing_cols, collapse = ", "))
  }
  
  message(sprintf("   ✅ Loaded %d rows", nrow(bloomberg_prices)))
  message(sprintf("   📅 Date range: %s to %s", 
                  min(bloomberg_prices$date), 
                  max(bloomberg_prices$date)))
  message(sprintf("   🏢 Companies: %d", 
                  length(unique(bloomberg_prices$ticker))))
  
  return(bloomberg_prices)
}

# Optional: If script is run directly, test the function
if (interactive() && !exists("skip_test")) {
  test_data <- load_bloomberg_cache()
  cat("\n📊 Sample of loaded data:\n")
  print(head(test_data))
}