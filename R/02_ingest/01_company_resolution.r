# ============================================================================
# UPDATED: R_ingest_build_company_resolution.r
# ============================================================================

source("C:\\Users\\sganesan\\OneDrive - dumac.duke.edu\\DAII\\R\\scripts\\utils\\R_utils_warehouse_connection.r")

build_company_resolution <- function() {
  message("🔍 Building company resolution table...")
  
  conn <- connect_warehouse()  # ONE function for everything!
  
  sql <- "
    SELECT DISTINCT
        Ticker,
        RepNo,
        CompanyName,
        ISIN,
        GICS_Sector,
        GICS_Industry,
        Country
    FROM refinitiv.daily_ratios_and_values
    WHERE Ticker IN ('NVDA US', 'MSFT US', 'AAPL US')
      AND RepNo IS NOT NULL
  "
  
  company_map <- dbGetQuery(conn, sql)
  dbDisconnect(conn)
  
  saveRDS(company_map, "C:\\data\\reference\\company_repno_mapping.rds")
  return(company_map)
}