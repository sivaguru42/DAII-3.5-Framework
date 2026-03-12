# ============================================================================
# SECURE DATABASE CONNECTION UTILITY FOR BOTH DUMAC DATABASES
# Version: 2.0 | Date: 2026-02-20
# Features: 
#   - Multiple authentication methods (app identity, managed identity, interactive)
#   - No hardcoded passwords
#   - Works for both Holdings and Refinitiv databases
# ============================================================================

library(odbc)
library(DBI)
library(AzureAuth)
library(AzureGraph)
library(dplyr)

# ----------------------------------------------------------------------------
# DEFAULT AZURE CREDENTIAL - R EQUIVALENT
# Tries multiple auth methods in sequence
# ----------------------------------------------------------------------------

get_azure_token_auto <- function(scope = "https://database.windows.net/.default",
                                  tenant = "dumac.duke.edu") {
  
  cat("🔐 Attempting to get Azure token...\n")
  
  # Method 1: Environment variables (CI/CD, GitHub Actions, etc.)
  if (Sys.getenv("AZURE_CLIENT_ID") != "" && 
      Sys.getenv("AZURE_CLIENT_SECRET") != "") {
    cat("   Method 1: Using environment variables (client credentials)\n")
    return(
      get_azure_token(
        resource = scope,
        tenant = tenant,
        app = Sys.getenv("AZURE_CLIENT_ID"),
        password = Sys.getenv("AZURE_CLIENT_SECRET"),
        auth_type = "client_credentials"
      )
    )
  }
  
  # Method 2: Managed Identity (Azure VM, App Service, etc.)
  tryCatch({
    cat("   Method 2: Attempting managed identity...\n")
    return(
      get_azure_token(
        resource = scope,
        tenant = tenant,
        auth_type = "managed"
      )
    )
  }, error = function(e) {
    cat("   ⚠️ Managed identity failed:", e$message, "\n")
  })
  
  # Method 3: Device code (interactive - no password)
  tryCatch({
    cat("   Method 3: Device code authentication (follow instructions)\n")
    return(
      get_azure_token(
        resource = scope,
        tenant = tenant,
        app = Sys.getenv("AZURE_APP_ID", "your-app-id-here"),  # Optional
        auth_type = "device_code"
      )
    )
  }, error = function(e) {
    cat("   ⚠️ Device code failed:", e$message, "\n")
  })
  
  # Method 4: Interactive browser (if RStudio/desktop)
  tryCatch({
    cat("   Method 4: Interactive browser authentication\n")
    return(
      get_azure_token(
        resource = scope,
        tenant = tenant,
        auth_type = "authorization_code"
      )
    )
  }, error = function(e) {
    cat("   ⚠️ Interactive auth failed:", e$message, "\n")
  })
  
  stop("❌ All authentication methods failed. Check your Azure setup.")
}

# ----------------------------------------------------------------------------
# EXTRACT JWT FROM TOKEN OBJECT
# ----------------------------------------------------------------------------

extract_jwt <- function(token) {
  if (inherits(token, "AzureToken")) {
    return(token$credentials$access_token)
  } else if (is.character(token)) {
    return(token)  # Assume it's already a JWT string
  } else {
    stop("❌ Invalid token object")
  }
}

# ----------------------------------------------------------------------------
# CONNECTION FUNCTION FOR BOTH DATABASES
# ----------------------------------------------------------------------------

#' Connect to Azure SQL Database
#' @param database_name Which database to connect to ("holdings" or "refinitiv")
#' @return Database connection object

connect_azure_sql <- function(database_name = "holdings") {
  
  # Database configurations
  db_config <- list(
    holdings = list(
      server = "dumacmx-prd-db-eus.database.windows.net,1433",
      database = "dumacmx-prd-db-eus"  # Update with actual holdings DB name
    ),
    refinitiv = list(
      server = "dumacmx-prd-db-eus.database.windows.net,1433",
      database = "refinitiv"  # Update with actual Refinitiv DB name
    )
  )
  
  if (!database_name %in% names(db_config)) {
    stop("❌ Invalid database_name. Choose 'holdings' or 'refinitiv'")
  }
  
  config <- db_config[[database_name]]
  
  cat(sprintf("\n🔌 Connecting to %s database...\n", database_name))
  
  # Get token using auto method (no hardcoded secrets)
  token <- get_azure_token_auto()
  jwt <- extract_jwt(token)
  
  # Create connection
  tryCatch({
    conn <- DBI::dbConnect(
      odbc::odbc(),
      driver = "ODBC Driver 18 for SQL Server",
      server = config$server,
      database = config$database,
      Encrypt = "yes",
      TrustServerCertificate = "Yes",
      attributes = list("azure_token" = jwt)
    )
    
    cat(sprintf("✅ Connected to %s database\n", database_name))
    
    # Test connection
    test <- dbGetQuery(conn, "SELECT 1 as test")
    if(nrow(test) == 1) {
      cat("   Connection test: OK\n")
    }
    
    return(conn)
    
  }, error = function(e) {
    stop("❌ Connection failed: ", e$message)
  })
}

# ----------------------------------------------------------------------------
# CONVENIENCE FUNCTIONS FOR SPECIFIC DATABASES
# ----------------------------------------------------------------------------

connect_holdings <- function() {
  connect_azure_sql("holdings")
}

connect_refinitiv <- function() {
  connect_azure_sql("refinitiv")
}

# ----------------------------------------------------------------------------
# QUERY EXECUTION WITH ERROR HANDLING
# ----------------------------------------------------------------------------

execute_query <- function(conn, sql, max_retries = 3) {
  for (i in 1:max_retries) {
    tryCatch({
      result <- dbGetQuery(conn, sql)
      return(result)
    }, error = function(e) {
      if (i == max_retries) stop("Query failed: ", e$message)
      cat(sprintf("   Query attempt %d failed, retrying...\n", i))
      Sys.sleep(2 ^ i)  # Exponential backoff
    })
  }
}

# ----------------------------------------------------------------------------
# EXAMPLE USAGE
# ----------------------------------------------------------------------------

if (interactive()) {
  cat("\n", paste(rep("=", 60), collapse = ""), "\n")
  cat("🔐 TESTING AZURE DATABASE CONNECTIONS\n")
  cat(paste(rep("=", 60), collapse = ""), "\n")
  
  # Test holdings database
  conn1 <- tryCatch({
    connect_holdings()
  }, error = function(e) {
    cat("❌ Holdings connection failed:", e$message, "\n")
    NULL
  })
  
  if (!is.null(conn1)) {
    # Test query
    result <- execute_query(conn1, "SELECT TOP 5 * FROM INFORMATION_SCHEMA.TABLES")
    print(result)
    dbDisconnect(conn1)
  }
  
  cat("\n")
  
  # Test refinitiv database
  conn2 <- tryCatch({
    connect_refinitiv()
  }, error = function(e) {
    cat("❌ Refinitiv connection failed:", e$message, "\n")
    NULL
  })
  
  if (!is.null(conn2)) {
    # Test query
    result <- execute_query(conn2, "SELECT TOP 5 * FROM INFORMATION_SCHEMA.TABLES")
    print(result)
    dbDisconnect(conn2)
  }
}

# ----------------------------------------------------------------------------
# SETUP INSTRUCTIONS FOR DIFFERENT ENVIRONMENTS
# ----------------------------------------------------------------------------

setup_instructions <- "
📋 AZURE AUTHENTICATION SETUP

1. For LOCAL DEVELOPMENT (RStudio on your machine):
   - Option A (Interactive): Just run the code - browser will open for login
   - Option B (Device code): Uses https://microsoft.com/devicelogin
   - No passwords stored!

2. For CI/CD / AUTOMATED (GitHub Actions, Jenkins):
   Set these environment variables:
   - AZURE_CLIENT_ID = <your-app-client-id>
   - AZURE_CLIENT_SECRET = <your-app-secret>
   - AZURE_TENANT_ID = dumac.duke.edu

3. For AZURE SERVICES (VM, App Service, Functions):
   - Enable Managed Identity
   - Code will auto-detect and use it
   - No credentials needed!

4. For PRODUCTION DEPLOYMENT:
   - Use Managed Identity if running in Azure
   - Use Environment Variables if running elsewhere
   - Never hardcode secrets!
"

# Uncomment to view instructions
# cat(setup_instructions)