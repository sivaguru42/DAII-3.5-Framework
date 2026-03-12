# ============================================================================
# UTILITY: Azure AD Authentication for External APIs
# ============================================================================

library(httr)
library(jsonlite)

# Load credentials from environment variables
get_azure_credentials <- function() {
  list(
    client_id = Sys.getenv("DUMAC_CLIENT_ID"),
    client_secret = Sys.getenv("DUMAC_CLIENT_SECRET"),
    tenant_id = Sys.getenv("DUMAC_TENANT_ID")
  )
}

#' Get OAuth2 access token for Azure AD protected APIs
#' @param scope The API scope required (default for Power BI)
#' @return Access token string
get_access_token <- function(scope = "https://analysis.windows.net/powerbi/api/.default") {
  creds <- get_azure_credentials()
  
  # Validate credentials exist
  if(any(sapply(creds, function(x) x == ""))) {
    stop("❌ Azure credentials not set in environment variables")
  }
  
  token_url <- sprintf(
    "https://login.microsoftonline.com/%s/oauth2/v2.0/token",
    creds$tenant_id
  )
  
  body <- list(
    client_id = creds$client_id,
    client_secret = creds$client_secret,
    scope = scope,
    grant_type = "client_credentials"
  )
  
  tryCatch({
    response <- POST(token_url, body = body, encode = "form")
    
    if(status_code(response) == 200) {
      token_content <- content(response)
      message("✅ Access token obtained successfully")
      return(token_content$access_token)
    } else {
      error_content <- content(response)
      stop("HTTP ", status_code(response), ": ", 
           error_content$error_description %||% error_content$error)
    }
  }, error = function(e) {
    stop("❌ Failed to get access token: ", e$message)
  })
}

#' Make an authenticated API call using Azure AD token
#' @param url The API endpoint URL
#' @param method HTTP method (GET, POST, etc.)
#' @param body Optional request body
#' @param scope Optional API scope
#' @return API response
call_authenticated_api <- function(url, method = "GET", body = NULL, scope = NULL) {
  token <- get_access_token(scope)
  
  tryCatch({
    response <- VERB(
      method,
      url,
      add_headers(Authorization = paste("Bearer", token)),
      body = body,
      encode = "json"
    )
    
    if(status_code(response) >= 400) {
      warning("API returned HTTP ", status_code(response))
    }
    
    return(response)
  }, error = function(e) {
    stop("❌ API call failed: ", e$message)
  })
}

#' Example: Call Power BI REST API
#' @param group_id Power BI workspace ID
#' @return List of datasets in the workspace
call_powerbi_api <- function(group_id) {
  url <- sprintf("https://api.powerbi.com/v1.0/myorg/groups/%s/datasets", group_id)
  response <- call_authenticated_api(url)
  
  if(status_code(response) == 200) {
    return(content(response))
  } else {
    stop("Power BI API call failed")
  }
}