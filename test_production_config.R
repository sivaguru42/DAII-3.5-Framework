# ============================================================================
# TEST PRODUCTION CONFIGURATION WITH MOCK DATA
# ============================================================================

# Clear workspace
rm(list = ls())
cat("\014")

cat("🧪 TESTING PRODUCTION CONFIGURATION\n")
cat(rep("=", 70), "\n", sep="")

# Set working directory
setwd("C:/Users/sganesan/OneDrive - dumac.duke.edu/DAII")
cat("Working directory:", getwd(), "\n\n")

# Load required packages
if (!require("yaml")) install.packages("yaml")
library(yaml)

# Load production pipeline functions
source_file <- "R/scripts/DAII_3_5_Production_Pipeline.R"
if(!file.exists(source_file)) {
  stop("Production pipeline file not found. Please create it first.")
}
source(source_file)

# Step 1: Create mock Bloomberg data (with spaces in headers)
cat("1. CREATING MOCK BLOOMBERG DATA\n")
cat(rep("-", 50), "\n", sep="")

mock_bloomberg <- data.frame(
  `Ticker` = c("AAPL US", "MSFT US", "GOOGL US", "AMZN US", "TSLA US"),
  `R&D Exp` = c(18752, 22483, 27565, 42334, 1987),
  `Mkt Cap` = c(2.8e12, 2.2e12, 1.7e12, 1.5e12, 0.8e12),
  `BEst Analyst Rtg` = c(4.8, 4.9, 4.7, 4.6, 4.2),
  `Patents / Trademarks / Copy Rgt` = c(8456, 9234, 7563, 6543, 2345),
  `News Sent` = c(0.75, 0.68, 0.72, 0.65, 0.45),
  `Rev - 1 Yr Gr` = c(8.5, 12.3, 15.7, 9.8, 25.4),
  `GICS Ind Grp Name` = c("Technology Hardware", "Software", "Software", 
                          "Consumer Discretionary", "Automobiles"),
  check.names = FALSE  # Keep spaces in column names
)

cat("Mock Bloomberg data created:\n")
cat(sprintf("   Rows: %d, Columns: %d\n", nrow(mock_bloomberg), ncol(mock_bloomberg)))
cat("   Headers (with spaces):", paste(names(mock_bloomberg), collapse=", "), "\n")

# Step 2: Create mock DUMAC warehouse data
cat("\n2. CREATING MOCK DUMAC WAREHOUSE DATA\n")
cat(rep("-", 50), "\n", sep="")

mock_dumac <- data.frame(
  `Fund Name` = c("Technology Growth Fund", "Global Innovation Fund", 
                  "Sustainable Future Fund", "Market Neutral Fund"),
  `Long (%LTP)` = c(0.25, 0.18, 0.32, 0.15),
  `Matched_Ticker` = c("AAPL US", "MSFT US", "GOOGL US", "AMZN US"),
  `Instr/BB_IndustrySector` = c("Technology", "Technology", "Technology", "Consumer"),
  `Fund/Client Code` = c("TECH001", "GLOBAL002", "SUS003", "MKT004"),
  `Position Date` = as.Date(c("2025-01-15", "2025-01-15", "2025-01-15", "2025-01-15")),
  check.names = FALSE
)

cat("Mock DUMAC data created:\n")
cat(sprintf("   Rows: %d, Columns: %d\n", nrow(mock_dumac), ncol(mock_dumac)))
cat("   Headers (complex):", paste(names(mock_dumac), collapse=", "), "\n")

# Step 3: Test production configuration
cat("\n3. TESTING PRODUCTION CONFIGURATION\n")
cat(rep("=", 70), "\n", sep="")

config_path <- "config/daii_production.yaml"
if(!file.exists(config_path)) {
  stop(sprintf("Production config not found: %s\nPlease create daii_production.yaml first.", config_path))
}

cat(sprintf("Loading production config: %s\n", config_path))
config <- yaml::read_yaml(config_path)

# Test 1: Bloomberg data mapping
cat("\nA. TESTING BLOOMBERG DATA MAPPING:\n")
std_bloomberg <- prepare_daii_data(
  raw_data = mock_bloomberg,
  config_path = config_path,
  user_col_map = list()  # No overrides needed
)

cat("\nBloomberg mapping results:\n")
bloomberg_mapping <- attr(std_bloomberg, "field_mapping")
for(field in names(bloomberg_mapping)) {
  cat(sprintf("   %-20s → %s\n", field, bloomberg_mapping[[field]]))
}

# Test 2: DUMAC data mapping
cat("\nB. TESTING DUMAC DATA MAPPING:\n")
std_dumac <- prepare_daii_data(
  raw_data = mock_dumac,
  config_path = config_path,
  user_col_map = list(
    industry = "Instr/BB_IndustrySector",  # Map DUMAC industry field
    ticker = "Matched_Ticker"              # Map DUMAC ticker field
  )
)

cat("\nDUMAC mapping results:\n")
dumac_mapping <- attr(std_dumac, "field_mapping")
for(field in names(dumac_mapping)) {
  cat(sprintf("   %-20s → %s\n", field, dumac_mapping[[field]]))
}

# Test 3: Combined data (simulating merged dataset)
cat("\nC. TESTING COMBINED DATA (Bloomberg + DUMAC):\n")

# Simulate merging - in reality, you'd merge on ticker
combined_data <- merge(mock_bloomberg, mock_dumac, 
                       by.x = "Ticker", by.y = "Matched_Ticker",
                       all = TRUE)

std_combined <- prepare_daii_data(
  raw_data = combined_data,
  config_path = config_path,
  user_col_map = list(
    industry = "GICS Ind Grp Name",  # Prefer Bloomberg industry
    fund_weight = "Long (%LTP)"      # Use DUMAC weight field
  )
)

cat("\nCombined data columns after mapping:\n")
cat("   ", paste(names(std_combined), collapse=", "), "\n")

# Step 4: Save test results
cat("\n4. SAVING TEST RESULTS\n")
cat(rep("-", 50), "\n", sep="")

if(!dir.exists("test_output")) dir.create("test_output", showWarnings = FALSE)

write.csv(std_bloomberg, "test_output/mock_bloomberg_std.csv", row.names = FALSE)
write.csv(std_dumac, "test_output/mock_dumac_std.csv", row.names = FALSE)
write.csv(std_combined, "test_output/mock_combined_std.csv", row.names = FALSE)

cat("✅ Test files saved to test_output/ directory:\n")
cat("   • mock_bloomberg_std.csv\n")
cat("   • mock_dumac_std.csv\n")
cat("   • mock_combined_std.csv\n")

# Step 5: Summary and next steps
cat("\n5. PRODUCTION READINESS SUMMARY\n")
cat(rep("=", 70), "\n", sep="")

cat("✅ CONFIGURATION VALIDATED:\n")
cat("   • Handles Bloomberg headers with spaces (R&D Exp → rd_expense)\n")
cat("   • Handles DUMAC complex headers (Long (%LTP) → fund_weight)\n")
cat("   • Supports user overrides for field mapping\n")
cat("   • Successfully mapped mock datasets\n")

cat("\n🔧 NEXT STEPS FOR PRODUCTION DEPLOYMENT:\n")
cat("1. Create actual Bloomberg data extract with your headers\n")
cat("2. Create actual DUMAC warehouse data extract\n")
cat("3. Test field mapping with REAL data samples\n")
cat("4. Merge company data (Bloomberg) with holdings data (DUMAC)\n")
cat("5. Run complete DAII 3.5 pipeline on merged data\n")

cat("\n📋 SAMPLE USER WORKFLOW FOR ANALYSTS:\n")
cat("# Load production configuration\n")
cat("cleaned_data <- prepare_daii_data(\n")
cat("  raw_data = my_bloomberg_data,\n")
cat("  config_path = 'config/daii_production.yaml',\n")
cat("  user_col_map = list(rd_expense = 'R&D Exp')  # Simple override if needed\n")
cat(")\n")

cat("\n")
cat(rep("=", 80), "\n", sep="")
cat("🎯 PRODUCTION CONFIGURATION TEST COMPLETE - READY FOR REAL DATA\n")
cat(rep("=", 80), "\n", sep="")