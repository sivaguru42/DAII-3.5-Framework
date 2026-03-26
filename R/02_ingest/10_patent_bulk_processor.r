# ============================================================================
# PROCESS PATENT DATA FOR ALL 216 COMPANIES
# ============================================================================

library(data.table)
library(dplyr)

# File paths
bulk_dir <- "C:/Users/sganesan/DAII-3.5-Framework/data/01_raw/uspto_bulk/"
assignee_file <- file.path(bulk_dir, "g_assignee_disambiguated.tsv")
cpc_file <- file.path(bulk_dir, "g_cpc_current.tsv")

# AI CPC codes
ai_codes <- c("G06N", "G06K", "G06F", "G10L", "G05B", "G16H")
ai_pattern <- paste(ai_codes, collapse = "|")

# Fixed assignee patterns (from our successful test)
assignee_patterns <- data.frame(
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

# Load assignee data
message("Loading assignee data...")
assignee_data <- fread(assignee_file, sep = "\t", fill = TRUE, nrows = 15000000)
message("  Loaded ", nrow(assignee_data), " assignee records")

# Load CPC data
message("Loading CPC data...")
cpc_data <- fread(cpc_file, sep = "\t", fill = TRUE, nrows = 30000000)
cpc_data[, cpc_full := paste0(cpc_section, cpc_class, cpc_subclass, cpc_group)]
message("  Loaded ", nrow(cpc_data), " CPC records")

# Process each company
message("\nProcessing ", nrow(company_map), " companies...\n")

results <- list()
total <- nrow(company_map)

for(i in 1:total) {
  ticker <- company_map$ticker[i]
  company_name <- company_map$company_name[i]
  
  # Get custom pattern if available
  pattern_row <- assignee_patterns[assignee_patterns$ticker == ticker, ]
  if(nrow(pattern_row) > 0) {
    search_pattern <- pattern_row$pattern[1]
  } else {
    # Fallback: use company name or ticker
    search_pattern <- gsub(" Inc$| Corp$| Ltd$| LLC$| PLC$| Co$| Company$", "", company_name)
    search_pattern <- trimws(search_pattern)
    search_pattern <- toupper(search_pattern)
  }
  
  cat(sprintf("[%d/%d] %s... ", i, total, ticker))
  
  # Find assignees matching this company
  matched <- assignee_data[grepl(search_pattern, disambig_assignee_organization, ignore.case = TRUE), ]
  
  if(nrow(matched) == 0) {
    results[[i]] <- data.frame(ticker = ticker, company_name = company_name, total_patents = 0, ai_patents = 0)
    cat("0 patents\n")
    next
  }
  
  # Get patent IDs
  patent_ids <- unique(matched$patent_id)
  
  # Find CPC codes for these patents
  patent_cpc <- cpc_data[patent_id %in% patent_ids, ]
  
  total_patents <- length(patent_ids)
  
  # Count AI patents
  ai_patents <- patent_cpc %>%
    filter(grepl(ai_pattern, cpc_full, ignore.case = TRUE)) %>%
    pull(patent_id) %>%
    unique() %>%
    length()
  
  results[[i]] <- data.frame(ticker = ticker, company_name = company_name, total_patents = total_patents, ai_patents = ai_patents)
  cat(sprintf("%d patents, %d AI\n", total_patents, ai_patents))
}

# Combine results
patent_data <- bind_rows(results)

# Save to cache
saveRDS(patent_data, "data/01_raw/patent_data.rds")
write.csv(patent_data, "data/01_raw/patent_data.csv", row.names = FALSE)

cat("\n✅ Patent data saved for", nrow(patent_data), "companies\n")
cat("   Total patents:", sum(patent_data$total_patents), "\n")
cat("   Total AI patents:", sum(patent_data$ai_patents), "\n")
cat("   Companies with patents:", sum(patent_data$total_patents > 0), "\n")

# Show top 20
cat("\n📊 Top 20 companies by total patents:\n")
top_20 <- patent_data %>%
  arrange(desc(total_patents)) %>%
  head(20) %>%
  select(ticker, total_patents, ai_patents)
print(top_20)