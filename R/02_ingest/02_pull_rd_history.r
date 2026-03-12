# ============================================================================
# INGEST: Pull 40-Year R&D History from Refinitiv
# ============================================================================

# Load Azure connection utilities
source("C:\\Users\\sganesan\\OneDrive - dumac.duke.edu\\DAII\\R\\scripts\\utils\\R_utils_azure_connection.r")

pull_rd_history <- function(company_map) {
  message("📊 Pulling 40-year R&D history...")
  
  # Validate input
  if(missing(company_map) || is.null(company_map)) {
    stop("❌ company_map is required")
  }
  
  if(nrow(company_map) == 0) {
    stop("❌ company_map is empty")
  }
  
  # Create RepNo list for SQL IN clause
  repno_list <- paste0("'", paste(company_map$RepNo, collapse = "', '"), "'")
  
  sql <- sprintf("
    SELECT 
        RepNo,
        Statement_PeriodEndDate as fiscal_year_end,
        MAX(CASE WHEN COA = 'ERAD' THEN FinancialValue END) as rd_expense,
        MAX(CASE WHEN COA = 'REVT' THEN FinancialValue END) as revenue,
        MAX(CASE WHEN COA = 'OI' THEN FinancialValue END) as operating_income,
        MAX(CASE WHEN COA = 'NI' THEN FinancialValue END) as net_income
    FROM refinitiv.class20_stdann_itemized_stmt
    WHERE RepNo IN (%s)
      AND Statement_PeriodEndDate BETWEEN '1980-01-01' AND '2025-12-31'
      AND COA IN ('ERAD', 'REVT', 'OI', 'NI')
    GROUP BY RepNo, Statement_PeriodEndDate
    ORDER BY RepNo, Statement_PeriodEndDate DESC
  ", repno_list)
  
  # Connect to Refinitiv database
  conn <- tryCatch({
    connect_refinitiv()
  }, error = function(e) {
    stop("❌ Failed to connect to Refinitiv database: ", e$message)
  })
  
  # Execute query
  rd_history <- tryCatch({
    dbGetQuery(conn, sql)
  }, error = function(e) {
    dbDisconnect(conn)
    stop("❌ Query failed: ", e$message)
  })
  
  # Close connection
  dbDisconnect(conn)
  
  # Check results
  if(nrow(rd_history) == 0) {
    warning("⚠️ No R&D history found for the specified companies")
    return(data.frame())
  }
  
  # Join with ticker from company_map
  rd_history <- rd_history %>%
    left_join(company_map[, c("RepNo", "Ticker")], by = "RepNo")
  
  message(sprintf("✅ Pulled %d rows of historical data", nrow(rd_history)))
  message(sprintf("   Date range: %s to %s", 
                  min(rd_history$fiscal_year_end, na.rm = TRUE), 
                  max(rd_history$fiscal_year_end, na.rm = TRUE)))
  message(sprintf("   Companies with data: %d", length(unique(rd_history$Ticker))))
  
  # Save
  save_path_rds <- "C:\\Users\\sganesan\\OneDrive - dumac.duke.edu\\DAII\\data\\raw\\rd_history_40yr.rds"
  save_path_csv <- "C:\\Users\\sganesan\\OneDrive - dumac.duke.edu\\DAII\\data\\raw\\rd_history_40yr.csv"
  
  saveRDS(rd_history, save_path_rds)
  write.csv(rd_history, save_path_csv, row.names = FALSE)
  
  message("✅ Saved RDS to: ", save_path_rds)
  message("✅ Saved CSV to: ", save_path_csv)
  
  return(rd_history)
}

# Optional: Run directly if script is executed for testing
if (interactive() && !exists("skip_run")) {
  message("\n", paste(rep("=", 60), collapse = ""))
  message("🔧 TESTING pull_rd_history FUNCTION")
  message(paste(rep("=", 60), collapse = ""))
  
  # Try to load company map if it exists
  map_path <- "C:\\Users\\sganesan\\OneDrive - dumac.duke.edu\\DAII\\data\\reference\\company_repno_mapping.rds"
  
  if(file.exists(map_path)) {
    test_map <- readRDS(map_path)
    test_history <- pull_rd_history(test_map)
    message("\n📊 Sample of pulled data:")
    print(head(test_history))
  } else {
    message("⚠️ Company map not found. Run R_ingest_build_company_resolution.r first")
  }
}