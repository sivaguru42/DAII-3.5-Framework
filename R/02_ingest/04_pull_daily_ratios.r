# ============================================================================
# INGEST: Pull Daily Ratios from Refinitiv
# ============================================================================

source("C:\\Users\\sganesan\\OneDrive - dumac.duke.edu\\DAII\\R\\scripts\\utils\\R_utils_warehouse_connection.r") 

pull_daily_ratios <- function(company_map, start_date = "2024-01-01") {
  message("📈 Pulling daily ratios...")
  
  # Validate input
  if(missing(company_map) || is.null(company_map)) {
    stop("❌ company_map is required")
  }
  
  if(nrow(company_map) == 0) {
    stop("❌ company_map is empty")
  }
  
  # Create ticker list for SQL IN clause
  ticker_list <- paste0("'", paste(company_map$Ticker, collapse = "', '"), "'")
  
  # Build SQL query
  sql <- sprintf("
    SELECT 
        Ticker,
        RepNo,
        as_of_date,
        -- Market Data
        MKTCAP_USD,
        Price_closing_or_last_bid,
        Price_vs_50_Day_Average,
        Price_vs_150_Day_Average,
        Price_vs_200_Day_Average,
        Price_percent_change_52_week,
        Average_volume_10_day,
        Average_volume_3_month,
        -- Valuation
        PE_ratio,
        PB_ratio,
        EV_to_EBITDA,
        -- Risk
        Beta_3Y_weekly,
        Short_Interest_as_percent_of_float,
        -- R&D
        Research_and_Development_Expense_most_recent_fiscal_yearr,
        Research_and_Development_Expense_trailing_12_month
    FROM refinitiv.daily_ratios_and_values
    WHERE Ticker IN (%s)
      AND as_of_date >= '%s'
    ORDER BY Ticker, as_of_date DESC
  ", ticker_list, start_date)
  
  # CHANGED: Use explicit connection instead of query_warehouse()
  message("   Connecting to Refinitiv database...")
  
  conn <- tryCatch({
    connect_refinitiv()  # Connect to Refinitiv database
  }, error = function(e) {
    stop("❌ Failed to connect to Refinitiv database: ", e$message)
  })
  
  # Execute query
  ratios <- tryCatch({
    dbGetQuery(conn, sql)
  }, error = function(e) {
    dbDisconnect(conn)
    stop("❌ Query failed: ", e$message)
  })
  
  # Close connection
  dbDisconnect(conn)
  
  # Check results
  if(nrow(ratios) == 0) {
    warning("⚠️ No daily ratio records found for the specified tickers and date range")
    return(data.frame())
  }
  
  # Summary statistics
  message(sprintf("✅ Pulled %d daily ratio records", nrow(ratios)))
  message(sprintf("   Date range: %s to %s", 
                  min(ratios$as_of_date, na.rm = TRUE), 
                  max(ratios$as_of_date, na.rm = TRUE)))
  message(sprintf("   Companies with data: %d", length(unique(ratios$Ticker))))
  
  # Save to raw data directory (using absolute paths)
  save_path_rds <- "C:\\Users\\sganesan\\OneDrive - dumac.duke.edu\\DAII\\data\\raw\\daily_ratios.rds"
  save_path_csv <- "C:\\Users\\sganesan\\OneDrive - dumac.duke.edu\\DAII\\data\\raw\\daily_ratios.csv"
  
  saveRDS(ratios, save_path_rds)
  write.csv(ratios, save_path_csv, row.names = FALSE)
  
  message("✅ Saved RDS to: ", save_path_rds)
  message("✅ Saved CSV to: ", save_path_csv)
  
  return(ratios)
}

# Optional: Run directly if script is executed for testing
if (interactive() && !exists("skip_run")) {
  message("\n", paste(rep("=", 60), collapse = ""))
  message("🔧 TESTING pull_daily_ratios FUNCTION")
  message(paste(rep("=", 60), collapse = ""))
  
  # Try to load company map if it exists
  map_path <- "C:\\Users\\sganesan\\OneDrive - dumac.duke.edu\\DAII\\data\\reference\\company_repno_mapping.rds"
  
  if(file.exists(map_path)) {
    test_map <- readRDS(map_path)
    test_ratios <- pull_daily_ratios(test_map, start_date = "2024-01-01")
    message("\n📊 Sample of pulled data:")
    print(head(test_ratios[, 1:5]))
  } else {
    message("⚠️ Company map not found. Create a test map first.")
    # Create minimal test map
    test_map <- data.frame(
      Ticker = c("NVDA US", "MSFT US", "AAPL US"),
      RepNo = c(12345, 67890, 54321),
      stringsAsFactors = FALSE
    )
    test_ratios <- pull_daily_ratios(test_map, start_date = "2024-01-01")
  }
}