# ============================================================================
# INGEST: PatentsView API Integration (No Database Connection)
# ============================================================================

library(httr)
library(jsonlite)
library(dplyr)

# PatentsView API Key
PATENTSVIEW_KEY <- "tqazgisppzzuyskpntfzqiolhzrrpn"

#' Get AI patent data for a company
#' @param company_name Character string to search
#' @return List with patent count and metadata
get_ai_patents <- function(company_name) {
  
  # Rate limiting - be kind to the API
  Sys.sleep(0.5)
  
  query <- list(
    q = list(
      `_and` = list(
        list(assignee_organization = list(`_contains` = company_name)),
        list(cpc_group_id = list(`_begins` = "G06N"))  # AI/ML classification
      )
    ),
    f = c("patent_id", "patent_title", "patent_date"),
    o = list(per_page = 1, page = 1),
    k = PATENTSVIEW_KEY
  )
  
  response <- tryCatch({
    POST(
      url = "https://api.patentsview.org/patents/query",
      body = toJSON(query, auto_unbox = TRUE),
      encode = "json",
      add_headers("Content-Type" = "application/json"),
      timeout(30)
    )
  }, error = function(e) {
    warning("API request failed: ", e$message)
    return(NULL)
  })
  
  if(!is.null(response) && status_code(response) == 200) {
    result <- content(response, as = "parsed")
    return(list(
      count = result$count %||% 0,
      status = "success"
    ))
  } else {
    return(list(
      count = 0,
      status = ifelse(is.null(response), "connection error", sprintf("HTTP %d", status_code(response)))
    ))
  }
}

#' Batch process all companies with rate limiting
#' @param company_names Vector of company names
#' @return Dataframe with patent counts
batch_get_patents <- function(company_names) {
  
  if(missing(company_names) || length(company_names) == 0) {
    stop("❌ company_names is required")
  }
  
  results <- data.frame(
    company_name = character(),
    patent_count = integer(),
    status = character(),
    stringsAsFactors = FALSE
  )
  
  cat(sprintf("📊 Querying PatentsView API for %d companies...\n", length(company_names)))
  cat("   Rate limit: 500/hour → 7.2 seconds between calls\n\n")
  
  for(i in seq_along(company_names)) {
    name <- company_names[i]
    cat(sprintf("   [%d/%d] Querying %s... ", i, length(company_names), name))
    
    result <- get_ai_patents(name)
    
    results <- rbind(results, data.frame(
      company_name = name,
      patent_count = result$count,
      status = result$status,
      stringsAsFactors = FALSE
    ))
    
    cat(sprintf("✅ %d patents\n", result$count))
    
    # Rate limiting: 500/hour = 7.2 seconds between calls
    if(i < length(company_names)) {
      Sys.sleep(8)  # Conservative 8 seconds
    }
  }
  
  cat(sprintf("\n✅ Completed. Success rate: %.1f%%\n", 
              100 * sum(results$status == "success") / nrow(results)))
  
  return(results)
}

#' Main function to pull patent data for all companies
#' @param company_map Dataframe with ticker and company_name columns
#' @return Dataframe with patent data joined to tickers
pull_patent_data <- function(company_map) {
  
  message("🔬 Pulling AI patent data from PatentsView...")
  
  # Validate input
  if(missing(company_map) || is.null(company_map)) {
    stop("❌ company_map is required")
  }
  
  if(!"company_name" %in% names(company_map)) {
    # If company_name not in map, create from ticker
    company_map$company_name <- gsub(" US$", "", company_map$Ticker)
  }
  
  # Get unique company names
  companies <- unique(company_map$company_name)
  
  # Batch query PatentsView
  patent_results <- batch_get_patents(companies)
  
  # Join back to tickers
  patent_data <- company_map %>%
    select(Ticker, company_name) %>%
    left_join(patent_results, by = "company_name") %>%
    select(Ticker, patent_count, status)
  
  message(sprintf("✅ Patent data pulled for %d companies", nrow(patent_data)))
  message(sprintf("   Companies with patents: %d", sum(patent_data$patent_count > 0)))
  
  # Save
  save_path_rds <- "C:\\Users\\sganesan\\OneDrive - dumac.duke.edu\\DAII\\data\\raw\\patent_data.rds"
  save_path_csv <- "C:\\Users\\sganesan\\OneDrive - dumac.duke.edu\\DAII\\data\\raw\\patent_data.csv"
  
  saveRDS(patent_data, save_path_rds)
  write.csv(patent_data, save_path_csv, row.names = FALSE)
  
  message("✅ Saved RDS to: ", save_path_rds)
  message("✅ Saved CSV to: ", save_path_csv)
  
  return(patent_data)
}

# Optional: Run directly if script is executed for testing
if (interactive() && !exists("skip_run")) {
  message("\n", paste(rep("=", 60), collapse = ""))
  message("🔧 TESTING PatentsView API")
  message(paste(rep("=", 60), collapse = ""))
  
  # Test with a few companies
  test_companies <- c("NVIDIA", "Microsoft", "Apple", "Google", "Meta")
  test_results <- batch_get_patents(test_companies)
  print(test_results)
}