# ============================================================================
# INGEST: Pull Current Holdings from DUMAC Holdings Database
# ============================================================================

# Load Azure connection utilities
source("C:\\Users\\sganesan\\OneDrive - dumac.duke.edu\\DAII\\R\\scripts\\utils\\R_utils_azure_connection.r")

pull_current_holdings <- function() {
  message("🏦 Pulling current holdings...")
  
  sql <- "
    SELECT 
        Report_o_End_Date as as_of_date,
        Fund_o_Client_Code as fund_id,
        Fund_o_Fund_Legal_Name as fund_name,
        Ticker,
        Long__l___pct_LTP__l_ as fund_weight,
        DUMAC_o_Net as dumac_allocation,
        Positions_o_End_Quantity as shares_held,
        Price_o_End as price
    FROM risk_exposure.ltp_exposure_ts
    WHERE Report_o_End_Date = (SELECT MAX(Report_o_End_Date) FROM risk_exposure.ltp_exposure_ts)
  "
  
  # Connect to Holdings database (DIFFERENT from Refinitiv!)
  conn <- tryCatch({
    connect_holdings()  # ← Note: connect_holdings(), not connect_refinitiv()
  }, error = function(e) {
    stop("❌ Failed to connect to Holdings database: ", e$message)
  })
  
  # Execute query
  holdings <- tryCatch({
    dbGetQuery(conn, sql)
  }, error = function(e) {
    dbDisconnect(conn)
    stop("❌ Query failed: ", e$message)
  })
  
  # Close connection
  dbDisconnect(conn)
  
  # Check results
  if(nrow(holdings) == 0) {
    warning("⚠️ No holdings data found")
    return(data.frame())
  }
  
  message(sprintf("✅ Pulled %d holdings records", nrow(holdings)))
  message(sprintf("   As of date: %s", max(holdings$as_of_date)))
  message(sprintf("   Unique tickers: %d", length(unique(holdings$Ticker))))
  message(sprintf("   Unique funds: %d", length(unique(holdings$fund_id))))
  
  # Save
  save_path_rds <- "C:\\Users\\sganesan\\OneDrive - dumac.duke.edu\\DAII\\data\\raw\\current_holdings.rds"
  save_path_csv <- "C:\\Users\\sganesan\\OneDrive - dumac.duke.edu\\DAII\\data\\raw\\current_holdings.csv"
  
  saveRDS(holdings, save_path_rds)
  write.csv(holdings, save_path_csv, row.names = FALSE)
  
  message("✅ Saved RDS to: ", save_path_rds)
  message("✅ Saved CSV to: ", save_path_csv)
  
  return(holdings)
}

# Optional: Run directly if script is executed for testing
if (interactive() && !exists("skip_run")) {
  message("\n", paste(rep("=", 60), collapse = ""))
  message("🔧 TESTING pull_current_holdings FUNCTION")
  message(paste(rep("=", 60), collapse = ""))
  
  test_holdings <- pull_current_holdings()
  message("\n📊 Sample of pulled data:")
  print(head(test_holdings))
}