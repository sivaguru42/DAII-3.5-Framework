# ============================================================================
# PATENT DATA: USPTO Bulk Data Processor (INTEGRATED)
# Version: 25.0 | Date: 2026-03-23
# Description: Uses USPTO bulk files with fixed assignee patterns
# ============================================================================

library(data.table)
library(dplyr)

# File paths
BULK_DIR <- here::here("data", "01_raw", "uspto_bulk/")
ASSIGNEE_FILE <- file.path(BULK_DIR, "g_assignee_disambiguated.tsv")
CPC_FILE <- file.path(BULK_DIR, "g_cpc_current.tsv")

# AI CPC codes
AI_CPC_CODES <- c("G06N", "G06K", "G06F", "G10L", "G05B", "G16H")

# Fixed assignee patterns (from our successful test)
ASSIGNEE_PATTERNS <- data.frame(
  ticker = c("AAPL", "AMD", "AMZN", "GOOGL", "INTC", "META", "MSFT", "NVDA", "TSLA", "V"),
  pattern = c(
    "Apple Inc\\.",
    "Advanced Micro Devices",
    "Amazon\\.com, Inc\\.|AMAZON TECHNOLOGIES",
    "Google LLC|GOOGLE TECHNOLOGY HOLDINGS",
    "Intel Corp\\.",
    "Meta Platforms|Facebook",
    "Microsoft Corp\\.",
    "NVIDIA Corp\\.",
    "Tesla Motors|Tesla, Inc\\.",
    "Visa International|Visa U\\.S\\.A"
  ),
  stringsAsFactors = FALSE
)

#' Process USPTO bulk data for companies
pull_patent_data <- function(company_map, use_cache = TRUE, force_refresh = FALSE) {
  
  message("🔬 Pulling AI patent data from USPTO bulk files...")
  
  cache_path <- here::here("data", "01_raw", "patent_data.rds")
  
  if(use_cache && file.exists(cache_path) && !force_refresh) {
    message("   Using cached patent data from: ", cache_path)
    patent_data <- readRDS(cache_path)
    message(sprintf("   ✅ Loaded patent data for %d companies", nrow(patent_data)))
    return(patent_data)
  }
  
  # Check files exist
  if(!file.exists(ASSIGNEE_FILE)) stop("Assignee file not found")
  if(!file.exists(CPC_FILE)) stop("CPC file not found")
  
  # Load data
  message("   Loading assignee data...")
  assignee_data <- fread(ASSIGNEE_FILE, sep = "\t", fill = TRUE, nrows = 15000000)
  
  # Load CPC data
  message("   Loading CPC data...")
  cpc_data <- fread(CPC_FILE, sep = "\t", fill = TRUE, nrows = 30000000)
  cpc_data[, cpc_full := paste0(cpc_section, cpc_class, cpc_subclass, cpc_group)]
  ai_pattern <- paste(AI_CPC_CODES, collapse = "|")
  
  # Process each company
  results <- list()
  total <- nrow(company_map)
  
  for(i in 1:total) {
    ticker <- company_map$ticker[i]
    company_name <- company_map$company_name[i]
    
    # Get pattern for this ticker
    pattern_row <- ASSIGNEE_PATTERNS[ASSIGNEE_PATTERNS$ticker == ticker, ]
    if(nrow(pattern_row) > 0) {
      search_pattern <- pattern_row$pattern[1]
    } else {
      # Fallback: clean company name
      search_pattern <- gsub(" Inc$| Corp$| Ltd$| LLC$| PLC$| Co$| Company$", "", company_name)
      search_pattern <- trimws(search_pattern)
      search_pattern <- toupper(search_pattern)
    }
    
    cat(sprintf("   [%d/%d] %s... ", i, total, ticker))
    
    matched <- assignee_data[grepl(search_pattern, disambig_assignee_organization, ignore.case = TRUE), ]
    
    if(nrow(matched) == 0) {
      results[[i]] <- data.frame(ticker = ticker, company_name = company_name, total_patents = 0, ai_patents = 0)
      cat("0 patents\n")
      next
    }
    
    patent_ids <- unique(matched$patent_id)
    patent_cpc <- cpc_data[patent_id %in% patent_ids, ]
    total_patents <- length(patent_ids)
    
    ai_patents <- patent_cpc %>%
      filter(grepl(ai_pattern, cpc_full, ignore.case = TRUE)) %>%
      pull(patent_id) %>%
      unique() %>%
      length()
    
    results[[i]] <- data.frame(ticker = ticker, company_name = company_name, total_patents = total_patents, ai_patents = ai_patents)
    cat(sprintf("%d patents, %d AI\n", total_patents, ai_patents))
  }
  
  patent_data <- bind_rows(results)
  
  saveRDS(patent_data, cache_path)
  message("   ✅ Saved to cache: ", cache_path)
  
  return(patent_data)
}