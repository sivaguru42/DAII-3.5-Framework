# ============================================================================
# INGEST: Pull 40-Year Financial History from Refinitiv
# Version: 4.0 | Date: 2026-03-16
# Description: Pulls annual financials from research database using JSON extraction
# ============================================================================

# Load database connection utilities
source("R/01_utils/database_connection.R")
library(dplyr)
library(tidyr)

#' Pull 40-year financial history for companies
#' @param company_map Dataframe with Ticker and RepNo columns
#' @return Dataframe with financial history
#' @export
pull_financial_history <- function(company_map) {
  
  message("📊 Pulling 40-year financial history...")
  
  # Connect to RESEARCH database (not the default)
  conn <- connect_research()
  
  # Extract RepNos from company_map
  # Use the RepNo column - we need to add this to company_map!
  repno_list <- paste0("'", paste(company_map$RepNo, collapse = "', '"), "'")
  
  # Direct JSON extraction query (recommended by chatbot)
  sql <- sprintf("
    SELECT 
        s.RepNo,
        s.Statement_PeriodEndDate AS fiscal_year_end,
        s.Statement_Type,
        s.Currencies_ConvertedTo AS currency,
        JSON_VALUE(s.FinancialValuesJSON, '$.SREV') AS revenue,
        JSON_VALUE(s.FinancialValuesJSON, '$.ERAD') AS rd_expense,
        JSON_VALUE(s.FinancialValuesJSON, '$.SGRP') AS gross_profit,
        JSON_VALUE(s.FinancialValuesJSON, '$.SSGA') AS sga_expense,
        JSON_VALUE(s.FinancialValuesJSON, '$.SOPI') AS operating_income,
        JSON_VALUE(s.FinancialValuesJSON, '$.EIBT') AS net_income_before_tax,
        JSON_VALUE(s.FinancialValuesJSON, '$.TIAT') AS net_income,
        JSON_VALUE(s.FinancialValuesJSON, '$.SDBF') AS diluted_eps,
        JSON_VALUE(s.FinancialValuesJSON, '$.SBDA') AS normalized_ebitda
    FROM research.refinitiv.class20_stdann_stmt s
    WHERE s.RepNo IN (%s)
      AND s.Statement_Type = 'INC'  -- Income Statement only
      AND s.Statement_PeriodEndDate >= '1980-01-01'
    ORDER BY s.RepNo, s.Statement_PeriodEndDate DESC
  ", repno_list)
  
  message("   Querying research database for financial history...")
  
  raw_data <- tryCatch({
    dbGetQuery(conn, sql)
  }, error = function(e) {
# dbDisconnect(conn)  # REMOVED for persistent connection
    stop("❌ Query failed: ", e$message)
  })
  
# dbDisconnect(conn)  # REMOVED for persistent connection
  
  message(sprintf("   ✅ Pulled %d rows of historical data", nrow(raw_data)))
  
  # Convert JSON values from character to numeric
  financial_history <- raw_data %>%
    mutate(across(c(revenue, rd_expense, gross_profit, sga_expense, 
                    operating_income, net_income_before_tax, net_income,
                    diluted_eps, normalized_ebitda), as.numeric))
  
  # Join with ticker from company_map
  financial_history <- financial_history %>%
    left_join(company_map[, c("RepNo", "Ticker")], by = "RepNo")
  
  message(sprintf("   ✅ Processed %d company-year records", nrow(financial_history)))
  
  # Show sample for NVIDIA
  if (nrow(financial_history) > 0) {
    message("\n📋 Sample data for NVDA (most recent 5 years):")
    nvda_sample <- financial_history %>% 
      filter(Ticker == "NVDA") %>%
      arrange(desc(fiscal_year_end)) %>%
      select(Ticker, fiscal_year_end, revenue, rd_expense, net_income) %>%
      head(5)
    print(nvda_sample)
  }
  
  # Save to raw data folder
  save_path <- "C:/Users/sganesan/DAII-3.5-Framework/data/01_raw/financial_history_40yr.rds"
  saveRDS(financial_history, save_path)
  message("\n   ✅ Saved to: ", save_path)
  
  return(financial_history)
}

# Alias for backward compatibility
pull_rd_history <- pull_financial_history
