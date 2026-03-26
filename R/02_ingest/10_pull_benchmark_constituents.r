# ============================================================================
# BENCHMARK CONSTITUENTS QUERY (New File)
# File: R/02_ingest/10_pull_benchmark_constituents.r
# ============================================================================

source("R/01_utils/database_connection.r")

#' Fetch benchmark constituents from Refinitiv
#' @param benchmark_name Name of benchmark ("SP500", "RUSSELL3000", "MSCI_ACWI", "NASDAQ100")
#' @return Vector of tickers
fetch_benchmark_constituents <- function(benchmark_name) {
  
  message("📊 Fetching ", benchmark_name, " constituents...")
  
  conn <- connect_research()
  on.exit(dbDisconnect(conn), add = TRUE)
  
  # Map benchmark names to Refinitiv identifiers
  benchmark_map <- list(
    "SP500" = list(
      name = "S&P 500",
      ric = ".SPX",
      query = "SELECT Xref_Ticker FROM refinitiv.class20_refinfo_issue WHERE Index_Name = 'S&P 500'"
    ),
    "RUSSELL3000" = list(
      name = "Russell 3000",
      ric = ".RUA",
      query = "SELECT Xref_Ticker FROM refinitiv.class20_refinfo_issue WHERE Index_Name = 'Russell 3000'"
    ),
    "MSCI_ACWI" = list(
      name = "MSCI ACWI",
      ric = ".MXWD",
      query = "SELECT Xref_Ticker FROM refinitiv.class20_refinfo_issue WHERE Index_Name = 'MSCI ACWI'"
    ),
    "NASDAQ100" = list(
      name = "NASDAQ 100",
      ric = ".NDX",
      query = "SELECT Xref_Ticker FROM refinitiv.class20_refinfo_issue WHERE Index_Name = 'NASDAQ 100'"
    )
  )
  
  if(!benchmark_name %in% names(benchmark_map)) {
    stop("Unknown benchmark: ", benchmark_name, 
         ". Available: ", paste(names(benchmark_map), collapse = ", "))
  }
  
  # Query constituents
  sql <- benchmark_map[[benchmark_name]]$query
  
  result <- dbGetQuery(conn, sql)
  
  if(nrow(result) == 0) {
    # Fallback: Use alternative table
    sql_alt <- sprintf("
      SELECT DISTINCT Xref_Ticker 
      FROM refinitiv.class20_refinfo_issue 
      WHERE Xref_Ticker IN (
        SELECT Xref_Ticker 
        FROM refinitiv.class20_refinfo_comp_indycls 
        WHERE Code = '%s'
      )
    ", benchmark_map[[benchmark_name]]$ric)
    
    result <- dbGetQuery(conn, sql_alt)
  }
  
  tickers <- unique(result$Xref_Ticker)
  message(sprintf("   ✅ Found %d constituents for %s", length(tickers), benchmark_map[[benchmark_name]]$name))
  
  return(tickers)
}

#' Get all benchmarks at once
get_all_benchmark_constituents <- function() {
  
  benchmarks <- c("SP500", "RUSSELL3000", "MSCI_ACWI", "NASDAQ100")
  results <- list()
  
  for(b in benchmarks) {
    results[[b]] <- fetch_benchmark_constituents(b)
  }
  
  return(results)
}