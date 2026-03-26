# ============================================================================
# DATABASE CONNECTION – Singleton with Validation
# Version: 12.0 | Date: 2026-03-23
# ============================================================================

library(odbc)
library(DBI)

# Private connection cache
.connection_cache <- new.env(parent = emptyenv())

# Check if connection is valid
is_valid_connection <- function(conn) {
  if(is.null(conn)) return(FALSE)
  tryCatch({
    dbIsValid(conn)
  }, error = function(e) {
    FALSE
  })
}

# Get or create Research connection
connect_research <- function() {
  # Check if existing connection is still valid
  if(!is.null(.connection_cache$research) && is_valid_connection(.connection_cache$research)) {
    message("   Reusing existing RESEARCH connection")
    return(.connection_cache$research)
  }
  
  # Create new connection
  message("🔌 Connecting to RESEARCH database...")
  
  .connection_cache$research <- dbConnect(
    odbc::odbc(),
    Driver = "ODBC Driver 18 for SQL Server",
    Server = "dumac-prd-db-eus.dumac.local",
    Database = "research",
    UID = "sganesan@dumac.duke.edu",
    Authentication = "ActiveDirectoryInteractive",
    Encrypt = "yes",
    TrustServerCertificate = "yes"
  )
  message("✅ Connected to RESEARCH database")
  
  return(.connection_cache$research)
}

# Get or create Warehouse connection
connect_warehouse <- function() {
  # Check if existing connection is still valid
  if(!is.null(.connection_cache$warehouse) && is_valid_connection(.connection_cache$warehouse)) {
    message("   Reusing existing WAREHOUSE connection")
    return(.connection_cache$warehouse)
  }
  
  # Create new connection
  message("🔌 Connecting to WAREHOUSE database...")
  
  .connection_cache$warehouse <- dbConnect(
    odbc::odbc(),
    Driver = "ODBC Driver 18 for SQL Server",
    Server = "dumacmx-prd-db-eus.dumac.local",
    Database = "dumacmx-prd-db-eus",
    UID = "sganesan@dumac.duke.edu",
    Authentication = "ActiveDirectoryInteractive",
    Encrypt = "yes",
    TrustServerCertificate = "yes"
  )
  message("✅ Connected to WAREHOUSE database")
  
  return(.connection_cache$warehouse)
}

# Close all connections
close_all_connections <- function() {
  if(!is.null(.connection_cache$research)) {
    tryCatch({
      dbDisconnect(.connection_cache$research)
    }, error = function(e) {})
    .connection_cache$research <- NULL
    message("Closed RESEARCH connection")
  }
  if(!is.null(.connection_cache$warehouse)) {
    tryCatch({
      dbDisconnect(.connection_cache$warehouse)
    }, error = function(e) {})
    .connection_cache$warehouse <- NULL
    message("Closed WAREHOUSE connection")
  }
}

# Test connections
test_connections <- function() {
  cat("\n", paste(rep("=", 60), collapse = ""), "\n")
  cat("🔐 TESTING CONNECTIONS\n")
  cat(paste(rep("=", 60), collapse = ""), "\n")
  
  cat("\n1. Testing Research DB...\n")
  conn <- tryCatch({
    connect_research()
  }, error = function(e) {
    cat("   ❌ Failed: ", e$message, "\n")
    return(NULL)
  })
  
  if(!is.null(conn)) {
    result <- dbGetQuery(conn, "SELECT GETDATE() AS server_time")
    cat("   ✅ Connected - Server time:", result$server_time[1], "\n")
  }
  
  cat("\n2. Testing Warehouse DB...\n")
  conn <- tryCatch({
    connect_warehouse()
  }, error = function(e) {
    cat("   ❌ Failed: ", e$message, "\n")
    return(NULL)
  })
  
  if(!is.null(conn)) {
    result <- dbGetQuery(conn, "SELECT GETDATE() AS server_time")
    cat("   ✅ Connected - Server time:", result$server_time[1], "\n")
  }
  
  cat("\n", paste(rep("=", 60), collapse = ""), "\n")
  cat("✅ TEST COMPLETE\n")
}

# Run test if interactive
if(interactive()) {
  test_connections()
}