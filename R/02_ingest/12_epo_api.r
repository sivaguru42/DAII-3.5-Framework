# ============================================================================
# EUROPEAN PATENT DATA – SIMPLIFIED WORKING VERSION
# Version: 6.0 | Date: 2026-03-24
# ============================================================================

library(dplyr)

# ============================================================================
# MANUAL PATENT DATA FOR KEY EUROPEAN COMPANIES
# ============================================================================

european_patent_data <- data.frame(
  ticker = c(
    "ASML", "SAP", "SIEMENS", "NVO", "NVS", "AZN", "SNY", "GSK",
    "NESTLE", "UL", "BP", "SHEL", "TTE", "RHHBY", "SAN", "BAYN",
    "AIR", "SAF", "DGE", "UN", "ABBN", "ROG", "NOVN", "LIN"
  ),
  company_name = c(
    "ASML Holding NV", "SAP SE", "Siemens AG", "Novo Nordisk", "Novartis",
    "AstraZeneca", "Sanofi", "GSK plc", "Nestle SA", "Unilever PLC",
    "BP p.l.c.", "Shell plc", "TotalEnergies SE", "Roche Holding",
    "Samsung Electronics", "Bayer AG", "Airbus SE", "Safran SA",
    "Diageo plc", "Unilever NV", "ABB Ltd", "Roche Holding", "Novartis",
    "Linde plc"
  ),
  total_patents = c(
    5600, 11078, 45000, 3200, 5800, 4200, 3800, 2100,
    2800, 3200, 2500, 2800, 3100, 4900, 5800, 4200,
    2100, 1800, 1200, 2100, 3500, 4900, 5800, 3200
  ),
  ai_patents = c(
    95, 3320, 11250, 320, 580, 420, 380, 210,
    140, 160, 125, 140, 155, 245, 290, 210,
    105, 90, 60, 105, 175, 245, 290, 160
  ),
  source = "MANUAL_EPO",
  last_updated = as.character(Sys.Date()),
  notes = "Initial estimate",
  stringsAsFactors = FALSE
)

pull_european_patents <- function(company_map, use_cache = TRUE, force_refresh = FALSE) {
  
  message("Pulling European patent data...")
  
  cache_path <- here::here("data", "01_raw", "european_patents.rds")
  
  if(use_cache && file.exists(cache_path) && !force_refresh) {
    message("   Using cached European patent data")
    return(readRDS(cache_path))
  }
  
  european_countries <- c("DEU", "GBR", "FRA", "NLD", "CHE", "SWE", "FIN", "ITA", "ESP", "AUT", "BEL", "DNK", "NOR", "IRL")
  
  # Filter European companies
  european_companies <- company_map %>%
    filter(Country %in% european_countries) %>%
    select(Ticker, CompanyName, Country)
  
  message(sprintf("   Found %d European companies", nrow(european_companies)))
  
  # Rename columns for join
  names(european_companies)[names(european_companies) == "Ticker"] <- "ticker"
  names(european_companies)[names(european_companies) == "CompanyName"] <- "company_name"
  
  # Join with manual data
  result <- merge(european_companies, european_patent_data, by = "ticker", all.x = TRUE)
  
  # Fill NAs
  result$total_patents[is.na(result$total_patents)] <- 0
  result$ai_patents[is.na(result$ai_patents)] <- 0
  result$source[is.na(result$source)] <- "NO_DATA"
  result$last_updated[is.na(result$last_updated)] <- as.character(Sys.Date())
  result$notes[is.na(result$notes)] <- "No data"
  
  # Rename back
  names(result)[names(result) == "ticker"] <- "Ticker"
  names(result)[names(result) == "company_name"] <- "CompanyName"
  
  saveRDS(result, cache_path)
  message(sprintf("   Saved data for %d companies", nrow(result)))
  
  return(result)
}

update_european_patent <- function(ticker, total_patents, ai_patents, notes = "") {
  
  idx <- which(european_patent_data$ticker == ticker)
  
  if(length(idx) > 0) {
    european_patent_data$total_patents[idx] <<- total_patents
    european_patent_data$ai_patents[idx] <<- ai_patents
    european_patent_data$last_updated[idx] <<- as.character(Sys.Date())
    european_patent_data$notes[idx] <<- notes
    message(sprintf("Updated %s: %d patents, %d AI", ticker, total_patents, ai_patents))
  } else {
    new_row <- data.frame(
      ticker = ticker,
      company_name = ticker,
      total_patents = total_patents,
      ai_patents = ai_patents,
      source = "MANUAL_EPO",
      last_updated = as.character(Sys.Date()),
      notes = notes,
      stringsAsFactors = FALSE
    )
    european_patent_data <<- bind_rows(european_patent_data, new_row)
    message(sprintf("Added %s: %d patents, %d AI", ticker, total_patents, ai_patents))
  }
}

is_quarterly_update_time <- function() {
  current_month <- as.numeric(format(Sys.Date(), "%m"))
  return(current_month %in% c(3, 6, 9, 12))
}

remind_quarterly_update <- function() {
  cat("
", paste(rep("=", 60), collapse = ""), "
")
  cat("EUROPEAN PATENT DATA QUARTERLY UPDATE REMINDER
")
  cat(paste(rep("=", 60), collapse = ""), "

")
  
  if(is_quarterly_update_time()) {
    cat("This is a quarterly update month
")
    cat("Use update_european_patent function

")
    cat("Current data:
")
    print(european_patent_data[, c("ticker", "total_patents", "ai_patents", "last_updated")])
  } else {
    next_month <- c("March", "June", "September", "December")[which(c(3,6,9,12) > as.numeric(format(Sys.Date(), "%m")))[1]]
    cat("Next quarterly update will be in", next_month, "
")
  }
}

test_european_patents <- function() {
  cat("Testing European patent module

")
  cat("Current data:
")
  print(european_patent_data[, c("ticker", "total_patents", "ai_patents", "last_updated")])
  remind_quarterly_update()
  cat("
Module ready
")
}

