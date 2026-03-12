# ============================================================================
# UTILITY: Database Connection Functions with Azure AD Authentication
# ============================================================================

library(DBI)
library(odbc)

# Load credentials from environment variables
get_azure_credentials <- function() {
  list(
    client_id = Sys.getenv("DUMAC_CLIENT_ID"),
    client_secret = Sys.getenv("DUMAC_CLIENT_SECRET"),
    tenant_id = Sys.getenv("DUMAC_TENANT_ID")
  )
}

connect_warehouse <- function() {
  creds <- get_azure_credentials()
  
  # Validate credentials exist
  if(any(sapply(creds, function(x) x == ""))) {
    stop("❌ Azure credentials not set in environment variables.\n",
         "Please set:\n",
         "  DUMAC_CLIENT_ID\n",
         "  DUMAC_CLIENT_SECRET\n",
         "  DUMAC_TENANT_ID")
  }
  
  tryCatch({
    conn <- dbConnect(
      odbc(),
      Driver = "ODBC Driver 17 for SQL Server",  # You may need to install this
      Server = "dumac-prd-db-eus.database.windows.net",
      Database = "research",
      Authentication = "ActiveDirectoryServicePrincipal",
      UID = creds$client_id,
      PWD = creds$client_secret,
      Tenant = creds$tenant_id,
      Encrypt = "yes",
      TrustServerCertificate = "no",
      ConnectionTimeout = 30
    )
    message("✅ Connected to DUMAC Warehouse using App Identity")
    return(conn)
  }, error = function(e) {
    stop("❌ Warehouse connection failed: ", e$message)
  })
}

query_warehouse <- function(sql, max_retries = 3) {
  for(i in 1:max_retries) {
    tryCatch({
      conn <- connect_warehouse()
      result <- dbGetQuery(conn, sql)
      dbDisconnect(conn)
      return(result)
    }, error = function(e) {
      if(i == max_retries) stop(paste("Query failed after", max_retries, "retries:", e$message))
      message(sprintf("Query attempt %d failed, retrying in %d seconds...", i, 5*i))
      Sys.sleep(5 * i)
    })
  }
}

test_connection <- function() {
  tryCatch({
    conn <- connect_warehouse()
    result <- dbGetQuery(conn, "SELECT TOP 1 * FROM risk_exposure.ltp_exposure_ts")
    dbDisconnect(conn)
    message("✅ Warehouse connection successful")
    message("   Sample data retrieved: ", nrow(result), " rows")
    return(TRUE)
  }, error = function(e) {
    message("❌ Warehouse connection failed: ", e$message)
    return(FALSE)
  })
}