# ============================================================================
# INGEST: International Patent Data (Robust Version)
# Version: 7.0 | Date: 2026-03-24
# ============================================================================

library(dplyr)

pull_international_patents <- function(company_map, use_cache = TRUE, manual_data_path = NULL) {
  
  message("🌍 Pulling international patent data...")
  
  cache_path <- here::here("data", "01_raw", "patent_data_international.rds")
  
  if(use_cache && file.exists(cache_path)) {
    message("   Using cached international patent data")
    return(readRDS(cache_path))
  }
  
  # Find ticker column
  ticker_col <- NULL
  if("ticker" %in% names(company_map)) {
    ticker_col <- "ticker"
  } else if("Ticker" %in% names(company_map)) {
    ticker_col <- "Ticker"
  } else {
    message("   No ticker column found")
    return(data.frame())
  }
  
  # Find country column
  country_col <- NULL
  if("Country" %in% names(company_map)) {
    country_col <- "Country"
  } else if("ExchangeCountry" %in% names(company_map)) {
    country_col <- "ExchangeCountry"
  }
  
  if(is.null(country_col)) {
    message("   No country column found")
    return(data.frame())
  }
  
  # Find name column
  name_col <- ticker_col
  if("company_name" %in% names(company_map)) {
    name_col <- "company_name"
  } else if("CompanyName" %in% names(company_map)) {
    name_col <- "CompanyName"
  }
  
  # Get the actual data
  tickers <- company_map[[ticker_col]]
  countries <- company_map[[country_col]]
  names <- company_map[[name_col]]
  
  # Filter for non-US companies
  us_indicators <- c("usa", "united states", "us")
  non_us_idx <- which(!tolower(countries) %in% us_indicators & !is.na(countries))
  
  if(length(non_us_idx) == 0) {
    message("   No international companies found")
    return(data.frame())
  }
  
  message(sprintf("   Found %d international companies", length(non_us_idx)))
  
  # Build template using simple vectors
  ticker_vec <- character(length(non_us_idx))
  name_vec <- character(length(non_us_idx))
  country_vec <- character(length(non_us_idx))
  url_vec <- character(length(non_us_idx))
  
  for(i in seq_along(non_us_idx)) {
    idx <- non_us_idx[i]
    ticker_vec[i] <- as.character(tickers[idx])
    name_vec[i] <- as.character(names[idx])
    country_vec[i] <- as.character(countries[idx])
    
    # Clean name for URL
    clean_name <- gsub(" Inc$| Corp$| Ltd$| LLC$| PLC$| Co$| Company$", "", name_vec[i])
    clean_name <- trimws(clean_name)
    clean_name <- URLencode(clean_name, reserved = TRUE)
    
    url_vec[i] <- paste0(
      "https://patentscope.wipo.int/search/en/result.jsf?",
      "query=AN:(", clean_name,
      ") AND (CPC:G06N OR CPC:G06K OR CPC:G06F OR CPC:G10L OR CPC:G05B OR CPC:G16H)"
    )
  }
  
  # Create template data frame
  template <- data.frame(
    ticker = ticker_vec,
    company_name = name_vec,
    country = country_vec,
    total_patents = NA,
    ai_patents = NA,
    source = "WIPO_MANUAL",
    notes = "Fill from PATENTSCOPE search",
    search_url = url_vec,
    stringsAsFactors = FALSE
  )
  
  # Save template
  template_file <- here::here("data", "01_raw", "wipo_manual_template.csv")
  write.csv(template, template_file, row.names = FALSE)
  message(sprintf("   Manual template saved to: %s", template_file))
  message("   Instructions: Open search_url in browser, download results, count AI patents")
  
  return(data.frame())
}

merge_patent_data <- function(uspto_data, international_data) {
  message("🔗 Merging USPTO and international patent data...")
  if(nrow(international_data) == 0) return(uspto_data)
  
  uspto_data$ticker <- as.character(uspto_data$ticker)
  international_data$ticker <- as.character(international_data$ticker)
  
  combined <- uspto_data %>%
    left_join(international_data %>% select(ticker, total_patents, ai_patents), 
              by = "ticker", suffix = c("", "_int")) %>%
    mutate(
      total_patents = ifelse(is.na(total_patents_int), total_patents, total_patents + total_patents_int),
      ai_patents = ifelse(is.na(ai_patents_int), ai_patents, ai_patents + ai_patents_int)
    ) %>%
    select(-ends_with("_int"))
  
  return(combined)
}

