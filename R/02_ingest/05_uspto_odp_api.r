# ============================================================================
# USPTO ODP API – PRODUCTION MODULE (CONFIRMED WORKING)
# File: R/02_ingest/05_uspto_odp_api.r
# Version: 2.0 | Date: 2026-03-26
# ============================================================================

library(httr)
library(jsonlite)
library(dplyr)

# Configuration
USPTO_API_KEY <- Sys.getenv("PATENTSVIEW_API_KEY")
if(USPTO_API_KEY == "") USPTO_API_KEY <- "tqazgisppzzuyskpntfzqiolhzrrpn"

USPTO_API_URL <- "https://api.uspto.gov/api/v1/patent/applications/search"

# AI CPC codes for filtering
AI_CPC_CODES <- c("G06N", "G06K", "G06F", "G10L", "G05B", "G16H")

# Rate limiting (be respectful to the API)
RATE_LIMIT_DELAY <- 0.5  # seconds between requests

#' Search USPTO patents by assignee name
#' @param assignee_name Company name
#' @return List with total patent count and metadata
search_uspto_patents <- function(assignee_name) {
  
  # Clean assignee name
  clean_name <- gsub(" Inc$| Corp$| Ltd$| LLC$| PLC$| Co$| Company$", "", assignee_name)
  clean_name <- trimws(clean_name)
  
  # Build request body
  body <- list(
    filters = list(
      list(
        name = "assignmentBag.assigneeBag.assigneeNameText",
        value = list(clean_name)
      )
    )
  )
  
  # Make request
  response <- POST(
    USPTO_API_URL,
    add_headers(
      "accept" = "application/json",
      "X-API-KEY" = USPTO_API_KEY,
      "Content-Type" = "application/json"
    ),
    body = toJSON(body, auto_unbox = TRUE),
    encode = "json",
    timeout(30)
  )
  
  if(status_code(response) != 200) {
    return(list(total = 0, source = paste0("USPTO_ODP_", status_code(response))))
  }
  
  data <- fromJSON(content(response, "text"), flatten = TRUE)
  total <- ifelse(is.null(data$count), 0, data$count)
  
  return(list(
    total = total,
    source = "USPTO_ODP",
    timestamp = Sys.time()
  ))
}

#' Search AI-related patents with CPC filter
#' @param assignee_name Company name
#' @param cpc_codes Vector of CPC codes
#' @return Integer count of AI patents
search_uspto_ai_patents <- function(assignee_name, cpc_codes = AI_CPC_CODES) {
  
  clean_name <- gsub(" Inc$| Corp$| Ltd$| LLC$| PLC$| Co$| Company$", "", assignee_name)
  clean_name <- trimws(clean_name)
  
  # Build filters for assignee AND CPC codes
  body <- list(
    filters = list(
      list(
        name = "assignmentBag.assigneeBag.assigneeNameText",
        value = list(clean_name)
      ),
      list(
        name = "applicationMetaData.cpcClassificationBag",
        value = cpc_codes
      )
    )
  )
  
  response <- POST(
    USPTO_API_URL,
    add_headers(
      "accept" = "application/json",
      "X-API-KEY" = USPTO_API_KEY,
      "Content-Type" = "application/json"
    ),
    body = toJSON(body, auto_unbox = TRUE),
    encode = "json",
    timeout(30)
  )
  
  if(status_code(response) != 200) {
    return(0)
  }
  
  data <- fromJSON(content(response, "text"), flatten = TRUE)
  return(ifelse(is.null(data$count), 0, data$count))
}

#' Batch search for multiple companies
#' @param companies Dataframe with ticker and company_name columns
#' @param delay_seconds Delay between requests
#' @param get_ai_counts Whether to fetch AI patent counts
#' @return Dataframe with patent counts
batch_search_uspto <- function(companies, delay_seconds = RATE_LIMIT_DELAY, 
                               get_ai_counts = TRUE) {
  
  message(sprintf("\n🔍 Searching USPTO ODP for %d companies...", nrow(companies)))
  message(sprintf("   Rate limit: %.1f sec between requests", delay_seconds))
  
  results <- list()
  start_time <- Sys.time()
  
  for(i in 1:nrow(companies)) {
    ticker <- companies$ticker[i]
    company_name <- companies$company_name[i]
    
    cat(sprintf("   [%d/%d] %s... ", i, nrow(companies), ticker))
    
    # Get total patents
    total_result <- search_uspto_patents(company_name)
    
    # Get AI patents if requested
    if(get_ai_counts && total_result$total > 0) {
      ai_count <- search_uspto_ai_patents(company_name)
      Sys.sleep(0.3)  # Small delay between the two calls
    } else {
      ai_count <- 0
    }
    
    results[[i]] <- data.frame(
      ticker = ticker,
      company_name = company_name,
      total_patents = total_result$total,
      ai_patents = ai_count,
      patent_source = total_result$source,
      timestamp = total_result$timestamp,
      stringsAsFactors = FALSE
    )
    
    cat(sprintf("✅ %d patents (AI: %d)\n", total_result$total, ai_count))
    
    # Rate limiting
    Sys.sleep(delay_seconds)
  }
  
  elapsed <- difftime(Sys.time(), start_time, units = "mins")
  message(sprintf("\n   ✅ Completed in %.1f minutes", elapsed))
  
  return(bind_rows(results))
}

#' Test the API with a sample of companies
test_uspto_api <- function() {
  
  test_companies <- data.frame(
    ticker = c("NVDA", "MSFT", "AAPL", "GOOGL", "AMZN"),
    company_name = c("NVIDIA Corp", "Microsoft Corp", "Apple Inc", 
                     "Alphabet Inc", "Amazon.com Inc"),
    stringsAsFactors = FALSE
  )
  
  cat("\n🧪 TESTING USPTO ODP API\n")
  cat(paste(rep("=", 60), collapse = ""), "\n\n")
  
  # Test individual queries
  cat("Individual Tests:\n")
  for(i in 1:nrow(test_companies)) {
    result <- search_uspto_patents(test_companies$company_name[i])
    cat(sprintf("   %s: %d patents\n", test_companies$ticker[i], result$total))
  }
  
  # Test batch search
  cat("\nBatch Test:\n")
  batch_result <- batch_search_uspto(test_companies, get_ai_counts = TRUE)
  
  cat("\n📊 Results:\n")
  print(batch_result)
  
  return(batch_result)
}