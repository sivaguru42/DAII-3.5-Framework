
# ============================================================================
# WIPO MANUAL DATA ENTRY HELPER
# Run this after completing manual lookups
# ============================================================================

# Load tracking sheet
tracking <- read.csv("data/01_raw/wipo_tracking.csv")

# Show pending items
pending <- tracking[tracking$Status == "Pending", ]
if(nrow(pending) > 0) {
  cat("📋 PENDING COMPANIES:
")
  print(pending[, c("Priority", "Ticker", "Company", "Status")])
} else {
  cat("✅ All companies completed!
")
}

# Create data entry template
manual_data <- tracking[tracking$Status == "Completed", 
                        c("Ticker", "Company", "Total_Patents", "AI_Patents")]

# Remove rows with NA
manual_data <- manual_data[!is.na(manual_data$Total_Patents), ]

# Save final data
if(nrow(manual_data) > 0) {
  write.csv(manual_data, "data/01_raw/wipo_manual_data.csv", row.names = FALSE)
  cat(sprintf("
✅ Saved %d completed companies to wipo_manual_data.csv
", nrow(manual_data)))
  
  # Merge with USPTO data
  source("R/02_ingest/11_pull_international_patents.r")
  uspto_data <- readRDS("data/01_raw/patent_data.rds")
  combined <- merge_patent_data(uspto_data, manual_data)
  saveRDS(combined, "data/01_raw/patent_data.rds")
  cat("✅ Merged with USPTO data
")
} else {
  cat("
⚠️ No completed companies yet. Update tracking sheet first.
")
}

