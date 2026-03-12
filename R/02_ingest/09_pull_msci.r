# /R/ingest/pull_msci_data.R
# Option 2: Use the script_dir variable (better)
source("C:\\Users\\sganesan\\OneDrive - dumac.duke.edu\\DAII\\R\\scripts\\utils\\R_utils_warehouse_connection.r") 

pull_msci_data <- function(company_map) {
  message("📊 Pulling MSCI index data...")
  
  # Get MSCI security codes (you'll need to map these)
  sql <- "
    SELECT 
        m.Security_ID,
        m.Security_Name,
        m.Ticker,
        m.ISIN,
        m.GICS_Sector,
        m.GICS_Industry,
        w.as_of_date,
        w.index_weight,
        s.Price,
        s.Adj_Price,
        s.Daily_Return
    FROM msci.Merged_Main_Security m
    LEFT JOIN msci_derived.historical_acwi_index_weight w
        ON m.Security_ID = w.Security_ID
    LEFT JOIN msci.merged_security s
        ON m.Security_ID = s.Security_ID
    WHERE m.Ticker IN (SELECT Ticker FROM company_map)
      AND w.as_of_date >= '2000-01-01'
  "
  
  msci_data <- query_warehouse(sql)
  
  message(sprintf("✅ Pulled %d MSCI records", nrow(msci_data)))
  
  saveRDS(msci_data, "data/processed/msci_data.rds")
  
  return(msci_data)
}