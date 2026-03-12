# ============================================================================
# INGEST: Pull 40-Year R&D History
# ============================================================================

source("C:\\Users\\sganesan\\OneDrive - dumac.duke.edu\\DAII\\R\\scripts\\utils\\R_utils_warehouse_connection.r")

pull_rd_history <- function(company_map, chunk_by_decade = TRUE) {
  message("📊 Pulling 40-year R&D history...")
  
  repno_list <- paste0("'", paste(company_map$RepNo, collapse = "', '"), "'")
  
  if(chunk_by_decade) {
    # Break into decades to avoid timeouts
    decades <- list(
      "1980-1989" = "1980-01-01' AND '1989-12-31",
      "1990-1999" = "1990-01-01' AND '1999-12-31",
      "2000-2009" = "2000-01-01' AND '2009-12-31",
      "2010-2019" = "2010-01-01' AND '2019-12-31",
      "2020-2025" = "2020-01-01' AND '2025-12-31"
    )
    
    all_results <- list()
    
    for(decade_name in names(decades)) {
      date_range <- decades[[decade_name]]
      message(sprintf("   Querying %s...", decade_name))
      
      sql <- sprintf("
        SELECT 
            RepNo,
            Statement_PeriodEndDate as fiscal_year_end,
            MAX(CASE WHEN COA = 'ERAD' THEN FinancialValue END) as rd_expense,
            MAX(CASE WHEN COA = 'REVT' THEN FinancialValue END) as revenue
        FROM refinitiv.class20_stdann_itemized_stmt
        WHERE RepNo IN (%s)
          AND Statement_PeriodEndDate BETWEEN %s
          AND COA IN ('ERAD', 'REVT')
        GROUP BY RepNo, Statement_PeriodEndDate
      ", repno_list, date_range)
      
      # Use the robust query function!
      decade_data <- query_refinitiv(sql)
      
      if(nrow(decade_data) > 0) {
        all_results[[decade_name]] <- decade_data
      }
    }
    
    rd_history <- bind_rows(all_results)
    
  } else {
    # Single query with chunked fetching
    sql <- sprintf("...", repno_list)
    rd_history <- query_refinitiv(sql, chunk_size = 10000)
  }
  
  # Join with tickers
  rd_history <- rd_history %>%
    left_join(company_map[, c("RepNo", "Ticker")], by = "RepNo")
  
  message(sprintf("✅ Pulled %d rows", nrow(rd_history)))
  
  save_path <- "C:\\Users\\sganesan\\OneDrive - dumac.duke.edu\\DAII\\data\\raw\\rd_history_40yr.rds"
  saveRDS(rd_history, save_path)
  
  return(rd_history)
}