# ============================================================================
# INGEST: Build Company Resolution Table (Ticker → RepNo)
# Version: 7.5 | Date: 2026-03-24
# Description: Maps tickers to Refinitiv RepNos with TRBC industry data
# ============================================================================

source("R/01_utils/database_connection.r")

build_company_resolution <- function(tickers = NULL) {
  message("🔍 Building company resolution table...")
  
  if(is.null(tickers)) {
    if(file.exists("data/00_reference/company_map_217.rds")) {
      company_map_ref <- readRDS("data/00_reference/company_map_217.rds")
      tickers <- company_map_ref$ticker
      message("   Using full company map with ", length(tickers), " tickers")
    } else {
      stop("No tickers provided and company_map_217.rds not found")
    }
  }
  
  conn <- connect_research()
  on.exit(dbDisconnect(conn), add = TRUE)
  
  ticker_list <- paste0("'", tickers, "'", collapse = ", ")
  
  sql <- sprintf("
    SELECT DISTINCT
        i.Xref_Ticker                       AS Ticker,
        i.RepNo                             AS RepNo,
        c.CompanyName                       AS CompanyName,
        i.Xref_ISIN                         AS ISIN,
        i.ExchangeCode                      AS Exchange,
        i.ExchangeCountry                   AS Country,
        trbc.Description                    AS TRBC_Industry
    FROM refinitiv.class20_refinfo_issue i
    JOIN refinitiv.class20_refinfo_comp c ON c.RepNo = i.RepNo
    LEFT JOIN refinitiv.class20_refinfo_comp_indycls trbc
        ON trbc.RepNo = i.RepNo AND trbc.Type = 'TRBC'
    WHERE i.Xref_Ticker IN (%s)
      AND i.IssueID = 1
      AND c.FilingStatus = 'Filing'
      AND c.LatestInformation_of_InterimFinancials >= '2024-01-01'
    ORDER BY i.Xref_Ticker
  ", ticker_list)
  
  message("   Querying database for company mappings...")
  
  company_map <- dbGetQuery(conn, sql)
  
  message(sprintf("   ✅ Found %d companies with RepNo mapping", nrow(company_map)))
  
  return(company_map)
}
