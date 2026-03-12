# ============================================================================
# UTILITY: DUMAC Data Warehouse Connection with Robust Query Handling
# Version: 3.0 | Date: 2026-02-22
# Features: 
#   - Single connection for all schemas (refinitiv.*, risk_exposure.*)
#   - Azure AD authentication with app identity
#   - Robust query execution with retries, timeouts, and error handling
# ============================================================================

library(odbc)
library(DBI)
library(AzureAuth)
library(dplyr)

# ----------------------------------------------------------------------------
# CONNECTION FUNCTION
# ----------------------------------------------------------------------------

#' Connect to DUMAC Data Warehouse
#' @return Database connection object
connect_warehouse <- function() {
  
  # Load credentials from environment variables
  client_id <- Sys.getenv("DUMAC_CLIENT_ID")
  client_secret <- Sys.getenv("DUMAC_CLIENT_SECRET")
  
  if(client_id == "" || client_secret == "") {
    stop("❌ DUMAC_CLIENT_ID and DUMAC_CLIENT_SECRET must be set in .Renviron")
  }
  
  tryCatch({
    conn <- dbConnect(
      odbc::odbc(),
      driver = "ODBC Driver 18 for SQL Server",
      server = "dumacmx-prd-db-eus.database.windows.net,1433",
      database = "dumacmx-prd-db-eus",
      Encrypt = "yes",
      TrustServerCertificate = "Yes",
      authentication = "ActiveDirectoryServicePrincipal",
      uid = client_id,
      pwd = client_secret
    )
    
    message("✅ Connected to DUMAC Data Warehouse")
    return(conn)
    
  }, error = function(e) {
    stop("❌ Warehouse connection failed: ", e$message)
  })
}

# ============================================================================
# ROBUST QUERY FUNCTION WITH ALL CONTINGENCIES - INSERT HERE
# ============================================================================

#' Robust query execution with retries, timeouts, and error handling
#' @param conn Database connection object
#' @param sql SQL query string
#' @param params Optional list of parameters for parameterized query
#' @param schema Which schema being queried ("refinitiv" or "risk_exposure")
#' @param max_retries Maximum number of retry attempts (default: 3)
#' @param chunk_size Optional chunk size for large result sets
#' @param timeout_seconds Query timeout in seconds (default: 300)
#' @return Query result as data frame

robust_query <- function(conn, 
                         sql, 
                         params = NULL,
                         schema = c("refinitiv", "risk_exposure"),
                         max_retries = 3,
                         chunk_size = NULL,
                         timeout_seconds = 300) {
  
  schema <- match.arg(schema)
  
  # Set timeout for this query
  setTimeLimit(elapsed = timeout_seconds, transient = TRUE)
  on.exit(setTimeLimit(cpu = Inf, elapsed = Inf, transient = FALSE))
  
  for (attempt in 1:max_retries) {
    tryCatch({
      
      # Use parameterized query if params provided 
      if (!is.null(params)) {
        result <- dbGetQuery(conn, sql, params = params)
      } 
      # Handle large results in chunks if requested 
      else if (!is.null(chunk_size)) {
        message(sprintf("   Fetching large result in chunks of %d rows...", chunk_size))
        res <- dbSendQuery(conn, sql)
        all_chunks <- list()
        rows_fetched <- 0
        
        while (!dbHasCompleted(res)) {
          chunk <- dbFetch(res, n = chunk_size)
          if (nrow(chunk) > 0) {
            all_chunks <- c(all_chunks, list(chunk))
            rows_fetched <- rows_fetched + nrow(chunk)
            message(sprintf("      Fetched %d rows so far...", rows_fetched))
          }
        }
        dbClearResult(res)
        result <- bind_rows(all_chunks)
        message(sprintf("   ✅ Complete: %d total rows", rows_fetched))
      }
      # Standard query
      else {
        result <- dbGetQuery(conn, sql)
      }
      
      # Convert date columns automatically 
      date_cols <- grep("date|as_of|period|_date", names(result), 
                        ignore.case = TRUE, value = TRUE)
      for (col in date_cols) {
        tryCatch({
          result[[col]] <- as.Date(result[[col]])
        }, error = function(e) {
          # Silently skip if conversion fails
        })
      }
      
      # Handle NULLs - convert to NA (R will handle)
      # No action needed - R already uses NA for NULLs
      
      message(sprintf("   ✅ Query successful: %d rows returned", nrow(result)))
      return(result)
      
    }, error = function(e) {
      error_msg <- conditionMessage(e)
      
      # Create logs directory if it doesn't exist
      log_dir <- "C:\\Users\\sganesan\\OneDrive - dumac.duke.edu\\DAII\\logs"
      if(!dir.exists(log_dir)) dir.create(log_dir, recursive = TRUE)
      
      # Log full error to file
      log_file <- file.path(log_dir, paste0("db_error_", Sys.Date(), ".log"))
      writeLines(sprintf("[%s] Schema: %s, Attempt %d\nQuery: %s\nError: %s\n", 
                         Sys.time(), schema, attempt, 
                         substr(sql, 1, 200),  # First 200 chars of query
                         error_msg), 
                 log_file, append = TRUE)
      
      # Handle specific error types
      if (grepl("timeout|timed out|Gateway Time-out", error_msg, ignore.case = TRUE)) {
        if (attempt < max_retries) {
          wait_time <- 10 * attempt  # Exponential backoff: 10s, 20s, 30s
          message(sprintf("   ⚠️ Timeout, retrying in %ds (attempt %d/%d)...", 
                          wait_time, attempt + 1, max_retries))
          Sys.sleep(wait_time)
          return(NULL)  # Continue to next attempt
        }
      }
      
      if (grepl("connection|network|broken|reset", error_msg, ignore.case = TRUE)) {
        if (attempt < max_retries) {
          wait_time <- 5 * attempt
          message(sprintf("   ⚠️ Connection issue, retrying in %ds...", wait_time))
          
          # Reconnect before retry
          tryCatch({
            dbDisconnect(conn)
          }, error = function(e) {})
          
          # Assign new connection in parent frame
          assign("conn", connect_warehouse(), envir = parent.frame())
          
          Sys.sleep(wait_time)
          return(NULL)
        }
      }
      
      if (grepl("Invalid column name|Invalid identifier", error_msg, ignore.case = TRUE)) {
        # Extract column name from error if possible
        col_match <- regexpr("Invalid column name '([^']+)'", error_msg)
        if (col_match > -1) {
          bad_col <- regmatches(error_msg, col_match)
          stop("❌ Schema mismatch: ", bad_col, " - check field mappings for ", schema, " schema")
        } else {
          stop("❌ Schema mismatch in ", schema, " schema: ", error_msg)
        }
      }
      
      # On last attempt, throw error
      if (attempt == max_retries) {
        stop("❌ Query failed after ", max_retries, " attempts: ", error_msg)
      }
      
      # Default retry with backoff
      wait_time <- 2 ^ attempt
      message(sprintf("   ⚠️ Attempt %d failed, retrying in %ds...", attempt, wait_time))
      Sys.sleep(wait_time)
    })
  }
}

# ============================================================================
# CONVENIENCE WRAPPERS FOR SPECIFIC SCHEMAS
# ============================================================================

#' Query Refinitiv schema with robust handling
#' @param sql SQL query
#' @param ... Additional arguments passed to robust_query
#' @return Query result
query_refinitiv <- function(sql, ...) {
  conn <- connect_warehouse()
  on.exit(dbDisconnect(conn))
  
  robust_query(conn, sql, schema = "refinitiv", ...)
}

#' Query Risk Exposure schema with robust handling
#' @param sql SQL query
#' @param ... Additional arguments passed to robust_query
#' @return Query result
query_holdings <- function(sql, ...) {
  conn <- connect_warehouse()
  on.exit(dbDisconnect(conn))
  
  robust_query(conn, sql, schema = "risk_exposure", ...)
}

# ============================================================================
# TEST FUNCTION
# ============================================================================

#' Test warehouse connection and query capabilities
test_warehouse <- function() {
  cat("\n", paste(rep("=", 60), collapse = ""), "\n")
  cat("🔐 TESTING DUMAC DATA WAREHOUSE CONNECTION\n")
  cat(paste(rep("=", 60), collapse = ""), "\n")
  
  # Test connection
  cat("\n1. Testing connection...\n")
  conn <- tryCatch({
    connect_warehouse()
  }, error = function(e) {
    cat("   ❌ Connection failed: ", e$message, "\n")
    return(NULL)
  })
  
  if(is.null(conn)) return(FALSE)
  
  # Test simple query
  cat("\n2. Testing simple query...\n")
  test_result <- tryCatch({
    dbGetQuery(conn, "SELECT TOP 1 * FROM refinitiv.daily_ratios_and_values")
  }, error = function(e) {
    cat("   ❌ Query failed: ", e$message, "\n")
    dbDisconnect(conn)
    return(NULL)
  })
  
  if(!is.null(test_result)) {
    cat("   ✅ Success - retrieved", ncol(test_result), "columns\n")
  }
  
  # Test robust query
  cat("\n3. Testing robust query function...\n")
  test_sql <- "SELECT TOP 10 Ticker, as_of_date, MKTCAP_USD 
               FROM refinitiv.daily_ratios_and_values 
               WHERE as_of_date = (SELECT MAX(as_of_date) FROM refinitiv.daily_ratios_and_values)"
  
  test_robust <- tryCatch({
    query_refinitiv(test_sql)
  }, error = function(e) {
    cat("   ❌ Robust query failed: ", e$message, "\n")
    return(NULL)
  })
  
  if(!is.null(test_robust)) {
    cat("   ✅ Success - retrieved", nrow(test_robust), "rows\n")
    print(head(test_robust))
  }
  
  dbDisconnect(conn)
  
  cat("\n", paste(rep("=", 60), collapse = ""), "\n")
  cat("✅ WAREHOUSE TEST COMPLETE\n")
  cat(paste(rep("=", 60), collapse = ""), "\n")
  
  return(TRUE)
}

# Run test if script is executed directly
if (interactive() && !exists("skip_warehouse_test")) {
  test_warehouse()
}