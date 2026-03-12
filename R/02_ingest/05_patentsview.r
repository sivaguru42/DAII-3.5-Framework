# ============================================================================
# INGEST: PatentsView API Integration (ENHANCED CPC FILTERING)
# Version: 2.0 | Date: 2026-03-12
# Purpose: Pull AI patent data for all companies with sub-theme detection
# API Key: tqazgisppzzuyskpntfzqiolhzrrpn
# ============================================================================

library(httr)
library(jsonlite)
library(dplyr)

PATENTSVIEW_KEY <- "tqazgisppzzuyskpntfzqiolhzrrpn"

#' Get AI patent data for a company with expanded CPC filtering
#' @param company_name Character string to search
#' @param cpc_codes Vector of CPC codes to include (default: core AI/ML codes)
#' @return List with patent count by sub-theme and total
get_ai_patents <- function(company_name, cpc_codes = NULL) {
  
  # Default expanded CPC codes for AI sub-themes
  if (is.null(cpc_codes)) {
    cpc_codes <- c(
      "G06N",  # Core AI/ML
      "G06K",  # Pattern recognition / Computer vision
      "G10L",  # Speech recognition
      "G05B",  # Control systems / Robotics
      "G16H",  # Healthcare AI
      "G06F"   # Neural networks / Deep learning (subset)
    )
  }
  
  # Rate limiting - be kind to the API
  Sys.sleep(0.5)
  
  # Build OR query for multiple CPC codes
  cpc_conditions <- lapply(cpc_codes, function(code) {
    list(cpc_group_id = list(`_begins` = code))
  })
  
  query <- list(
    q = list(
      `_and` = list(
        list(assignee_organization = list(`_contains` = company_name)),
        list(`_or` = cpc_conditions)  # ANY of the AI CPC codes
      )
    ),
    f = c("patent_id", "patent_title", "patent_date", "cpc_group_id"),
    o = list(per_page = 100, page = 1),
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
    
    # Parse results by CPC code for sub-theme analysis
    patents <- result$patents %||% list()
    
    # Count by CPC code (first 4 characters for group)
    cpc_counts <- table(sapply(patents, function(p) {
      substr(p$cpc_group_id[1], 1, 4) %||% "OTHER"
    }))
    
    return(list(
      total_count = result$count %||% 0,
      cpc_breakdown = as.list(cpc_counts),
      patents = patents,
      status = "success"
    ))
  } else {
    return(list(
      total_count = 0,
      cpc_breakdown = list(),
      patents = list(),
      status = ifelse(is.null(response), "connection error", 
                      sprintf("HTTP %d", status_code(response)))
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
      patent_count = result$total_count,
      status = result$status,
      stringsAsFactors = FALSE
    ))
    
    cat(sprintf("✅ %d patents\n", result$total_count))
    
    # Rate limiting: 500/hour = 7.2 seconds between calls
    if(i < length(company_names)) {
      Sys.sleep(8)  # Conservative 8 seconds
    }
  }
  
  cat(sprintf("\n✅ Complete. Success rate: %.1f%%\n", 
              100 * sum(results$status == "success") / nrow(results)))
  
  return(results)
}

#' Enhanced batch processing with sub-theme tracking
#' @param company_names Vector of company names
#' @return Dataframe with patent counts by sub-theme
batch_get_patents_enhanced <- function(company_names) {
  
  if(missing(company_names) || length(company_names) == 0) {
    stop("❌ company_names is required")
  }
  
  # Define CPC codes for sub-themes
  cpc_themes <- list(
    "G06N" = "Core_AI",
    "G06K" = "Computer_Vision",
    "G10L" = "Speech",
    "G05B" = "Robotics",
    "G16H" = "Healthcare_AI",
    "G06F" = "Neural_Networks"
  )
  
  results <- data.frame(
    company_name = character(),
    total_patents = integer(),
    Core_AI = integer(),
    Computer_Vision = integer(),
    Speech = integer(),
    Robotics = integer(),
    Healthcare_AI = integer(),
    Neural_Networks = integer(),
    status = character(),
    stringsAsFactors = FALSE
  )
  
  cat(sprintf("📊 Querying PatentsView API for %d companies (expanded CPC codes)...\n", 
              length(company_names)))
  cat("   Sub-themes: Core AI, Computer Vision, Speech, Robotics, Healthcare AI\n\n")
  
  for(i in seq_along(company_names)) {
    name <- company_names[i]
    cat(sprintf("   [%d/%d] Querying %s... ", i, length(company_names), name))
    
    result <- get_ai_patents(name)
    
    # Build row with sub-theme breakdowns
    row <- data.frame(
      company_name = name,
      total_patents = result$total_count,
      Core_AI = result$cpc_breakdown$G06N %||% 0,
      Computer_Vision = result$cpc_breakdown$G06K %||% 0,
      Speech = result$cpc_breakdown$G10L %||% 0,
      Robotics = result$cpc_breakdown$G05B %||% 0,
      Healthcare_AI = result$cpc_breakdown$G16H %||% 0,
      Neural_Networks = result$cpc_breakdown$G06F %||% 0,
      status = result$status,
      stringsAsFactors = FALSE
    )
    
    results <- rbind(results, row)
    
    cat(sprintf("✅ %d total patents\n", result$total_count))
    
    # Rate limiting: 500/hour = 7.2 seconds between calls
    if(i < length(company_names)) {
      Sys.sleep(8)  # Conservative 8 seconds
    }
  }
  
  cat(sprintf("\n✅ Complete. Success rate: %.1f%%\n", 
              100 * sum(results$status == "success") / nrow(results)))
  
  return(results)
}

#' Main function to pull patent data for all companies
#' @param company_map Dataframe with ticker and company_name columns
#' @return Dataframe with patent data joined to tickers
pull_patent_data <- function(company_map) {
  
  message("🔬 Pulling AI patent data from PatentsView (enhanced CPC filtering)...")
  
  # Validate input
  if(missing(company_map) || is.null(company_map)) {
    stop("❌ company_map is required")
  }
  
  # Create search names
  company_map <- company_map %>%
    mutate(
      search_name = gsub(" US$| Equity$", "", Ticker),
      search_name = gsub(" [A-Z]{2}$", "", search_name)
    )
  
  # Get unique company names
  companies <- unique(company_map$search_name)
  
  # Batch query with enhanced CPC filtering
  patent_results <- batch_get_patents_enhanced(companies)
  
  # Join back to tickers
  patent_data <- company_map %>%
    select(Ticker, search_name) %>%
    left_join(patent_results, by = c("search_name" = "company_name")) %>%
    select(-search_name)
  
  message(sprintf("✅ Patent data pulled for %d companies", nrow(patent_data)))
  message(sprintf("   Companies with patents: %d", sum(patent_data$total_patents > 0)))
  message("\n📊 Sub-theme breakdown:")
  message(sprintf("   Core AI: %d", sum(patent_data$Core_AI > 0)))
  message(sprintf("   Computer Vision: %d", sum(patent_data$Computer_Vision > 0)))
  message(sprintf("   Speech: %d", sum(patent_data$Speech > 0)))
  message(sprintf("   Robotics: %d", sum(patent_data$Robotics > 0)))
  message(sprintf("   Healthcare AI: %d", sum(patent_data$Healthcare_AI > 0)))
  
  # Save
  save_path_rds <- "C:/Users/sganesan/DAII-3.5-Framework/data/01_raw/patent_data_enhanced.rds"
  save_path_csv <- "C:/Users/sganesan/DAII-3.5-Framework/data/01_raw/patent_data_enhanced.csv"
  
  saveRDS(patent_data, save_path_rds)
  write.csv(patent_data, save_path_csv, row.names = FALSE)
  
  message("\n✅ Saved enhanced patent data to: ", save_path_rds)
  
  return(patent_data)
}

# Optional: Run directly if script is executed for testing
if (interactive() && !exists("skip_run")) {
  message("\n", paste(rep("=", 60), collapse = ""))
  message("🔧 TESTING ENHANCED PatentsView API")
  message(paste(rep("=", 60), collapse = ""))
  
  # Test with a few companies
  test_companies <- c("NVIDIA", "Microsoft", "Apple", "Google", "Meta")
  test_results <- batch_get_patents_enhanced(test_companies)
  print(test_results)
}