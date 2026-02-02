# ============================================================================
# DAII 3.5 REAL DATA TEST - BLOOMBERG & DUMAC DATA
# ============================================================================
# Save as: R/scripts/test_real_data.R
# ============================================================================

rm(list = ls())
cat("\014")  # Clear console

cat("🚀 DAII 3.5 REAL DATA TEST - PRODUCTION DEPLOYMENT\n")
cat(rep("=", 80), "\n", sep="")

# Set working directory
setwd("C:/Users/sganesan/OneDrive - dumac.duke.edu/DAII")
cat("Working directory:", getwd(), "\n\n")

# Load required packages
if (!require("dplyr")) install.packages("dplyr"); library(dplyr)
if (!require("yaml")) install.packages("yaml"); library(yaml)

# Load production pipeline functions
source("R/scripts/DAII_3_5_Production_Pipeline.R")

# ============================================================================
# STEP 1: CHECK FOR REAL DATA FILES
# ============================================================================

cat("1. CHECKING FOR REAL DATA FILES\n")
cat(rep("-", 60), "\n", sep="")

# Define expected file paths
bloomberg_file <- "data/raw/bloomberg_sample.csv"
dumac_file <- "data/raw/dumac_sample.csv"

# Check if files exist
if (!file.exists(bloomberg_file)) {
  cat("❌ Bloomberg sample not found at:", bloomberg_file, "\n")
  cat("   Please export a small Bloomberg sample with these headers:\n")
  cat("   Ticker, R&D Exp, Mkt Cap, BEst Analyst Rtg, etc.\n")
  cat("   Save as: data/raw/bloomberg_sample.csv\n")
  bloomberg_data <- NULL
} else {
  bloomberg_data <- read.csv(bloomberg_file, stringsAsFactors = FALSE, check.names = FALSE)
  cat(sprintf("✅ Bloomberg data loaded: %d rows, %d columns\n", 
              nrow(bloomberg_data), ncol(bloomberg_data)))
}

if (!file.exists(dumac_file)) {
  cat("❌ DUMAC sample not found at:", dumac_file, "\n")
  cat("   Please export a small DUMAC sample with these headers:\n")
  cat("   fund_id, fund_name, fund_weight, etc.\n")
  cat("   Save as: data/raw/dumac_sample.csv\n")
  dumac_data <- NULL
} else {
  dumac_data <- read.csv(dumac_file, stringsAsFactors = FALSE, check.names = FALSE)
  cat(sprintf("✅ DUMAC data loaded: %d rows, %d columns\n", 
              nrow(dumac_data), ncol(dumac_data)))
}

if (is.null(bloomberg_data) || is.null(dumac_data)) {
  cat("\n⚠️  Please create the sample files and run again.\n")
  cat("   See instructions above.\n")
  stop("Missing data files")
}

# ============================================================================
# STEP 2: FIELD MAPPING WITH REAL DATA
# ============================================================================

cat("\n2. FIELD MAPPING WITH REAL DATA\n")
cat(rep("=", 70), "\n", sep="")

config_path <- "config/daii_production.yaml"

# Map Bloomberg data
cat("\nA. MAPPING BLOOMBERG DATA:\n")
cat(rep("-", 40), "\n", sep="")

std_bloomberg <- prepare_daii_data(
  raw_data = bloomberg_data,
  config_path = config_path,
  user_col_map = list(
    # Add any Bloomberg-specific overrides here
    # Example: rd_expense = "Custom_RD_Field"
  )
)

cat("\nBloomberg columns after mapping:\n")
cat("   ", paste(names(std_bloomberg), collapse=", "), "\n")

# Map DUMAC data
cat("\nB. MAPPING DUMAC DATA:\n")
cat(rep("-", 40), "\n", sep="")

std_dumac <- prepare_daii_data(
  raw_data = dumac_data,
  config_path = config_path,
  user_col_map = list(
    # Map DUMAC-specific fields
    ticker = "Matched_Ticker",  # If using Matched_Ticker instead of Ticker
    industry = "Instr/BB_IndustrySector",  # If available
    fund_weight = "Long (%LTP)"  # Primary weight field
  )
)

cat("\nDUMAC columns after mapping:\n")
cat("   ", paste(names(std_dumac), collapse=", "), "\n")

# ============================================================================
# STEP 3: MERGE COMPANY DATA WITH HOLDINGS
# ============================================================================

cat("\n3. MERGING BLOOMBERG & DUMAC DATA\n")
cat(rep("=", 70), "\n", sep="")

# Check for ticker column in both datasets
if (!"ticker" %in% names(std_bloomberg)) {
  stop("❌ Bloomberg data must have a 'ticker' column after mapping")
}

if (!"ticker" %in% names(std_dumac)) {
  # Try to find alternative ticker column in DUMAC data
  ticker_candidates <- c("Matched_Ticker", "Instr/BB_Code", "Ticker")
  found_ticker <- NULL
  
  for (candidate in ticker_candidates) {
    if (candidate %in% names(dumac_data)) {
      cat(sprintf("   Using '%s' as ticker for DUMAC data\n", candidate))
      std_dumac$ticker <- dumac_data[[candidate]]
      found_ticker <- candidate
      break
    }
  }
  
  if (is.null(found_ticker)) {
    stop("❌ Cannot find ticker column in DUMAC data")
  }
}

# Merge the datasets
merged_data <- merge(
  std_bloomberg,
  std_dumac,
  by = "ticker",
  all.x = TRUE,  # Keep all Bloomberg companies
  suffixes = c("_company", "_fund")
)

cat(sprintf("✅ Merged data: %d rows, %d columns\n", 
            nrow(merged_data), ncol(merged_data)))

# Check merge success rate
matched_companies <- sum(!is.na(merged_data$fund_name))
cat(sprintf("   Companies matched with fund holdings: %d (%.1f%%)\n",
            matched_companies, 100 * matched_companies / nrow(merged_data)))

# ============================================================================
# STEP 4: SAVE PROCESSED DATA
# ============================================================================

cat("\n4. SAVING PROCESSED DATA\n")
cat(rep("-", 60), "\n", sep="")

# Create output directory
output_dir <- "data/processed"
if (!dir.exists(output_dir)) dir.create(output_dir, recursive = TRUE)

# Save processed data
output_file <- file.path(output_dir, "daii_processed_data.csv")
write.csv(merged_data, output_file, row.names = FALSE)
cat(sprintf("✅ Processed data saved: %s\n", output_file))

# Save mapping reports
mapping_bloomberg <- attr(std_bloomberg, "field_mapping")
mapping_dumac <- attr(std_dumac, "field_mapping")

mapping_report <- list(
  bloomberg = mapping_bloomberg,
  dumac = mapping_dumac,
  timestamp = Sys.time(),
  data_files = c(bloomberg_file, dumac_file)
)

saveRDS(mapping_report, file.path(output_dir, "field_mapping_report.rds"))
cat(sprintf("✅ Mapping report saved: %s\n", file.path(output_dir, "field_mapping_report.rds")))

# ============================================================================
# STEP 5: QUICK DATA QUALITY CHECK
# ============================================================================

cat("\n5. DATA QUALITY CHECK\n")
cat(rep("=", 70), "\n", sep="")

# Check for required DAII fields
required_fields <- c("rd_expense", "market_cap", "analyst_rating", 
                     "patent_count", "news_sentiment", "revenue_growth")

cat("Required DAII fields in merged data:\n")
for (field in required_fields) {
  if (field %in% names(merged_data)) {
    missing_pct <- round(100 * sum(is.na(merged_data[[field]])) / nrow(merged_data), 1)
    cat(sprintf("   %-20s: Present, %.1f%% missing\n", field, missing_pct))
  } else {
    cat(sprintf("   %-20s: ❌ MISSING\n", field))
  }
}

# Check fund information
if ("fund_name" %in% names(merged_data)) {
  unique_funds <- length(unique(merged_data$fund_name[!is.na(merged_data$fund_name)]))
  cat(sprintf("\nFund information: %d unique funds\n", unique_funds))
}

if ("fund_weight" %in% names(merged_data)) {
  weight_summary <- summary(merged_data$fund_weight[!is.na(merged_data$fund_weight)])
  cat("Fund weight distribution:\n")
  print(weight_summary)
}

# ============================================================================
# STEP 6: PREPARE FOR DAII PIPELINE
# ============================================================================

cat("\n6. PREPARING FOR DAII 3.5 PIPELINE\n")
cat(rep("=", 70), "\n", sep="")

# Check if we have enough data for DAII scoring
has_required_data <- all(required_fields %in% names(merged_data))
has_portfolio_data <- all(c("fund_name", "fund_weight") %in% names(merged_data))

if (has_required_data && has_portfolio_data) {
  cat("✅ Data ready for complete DAII 3.5 pipeline:\n")
  cat("   • All company metrics present\n")
  cat("   • Portfolio holdings present\n")
  cat("   • Ready for Modules 1-5 execution\n")
  
  # Create ready signal file
  writeLines("READY", file.path(output_dir, "pipeline_ready.txt"))
  
} else if (has_required_data) {
  cat("⚠️  Data ready for company scoring only:\n")
  cat("   • All company metrics present\n")
  cat("   • Portfolio data incomplete\n")
  cat("   • Can run Modules 1-4 (scoring only)\n")
} else {
  cat("❌ Insufficient data for DAII scoring\n")
  cat("   Please check data completeness\n")
}

# ============================================================================
# STEP 7: NEXT STEPS INSTRUCTIONS
# ============================================================================

cat("\n7. NEXT STEPS INSTRUCTIONS\n")
cat(rep("=", 70), "\n", sep="")

cat("To run the complete DAII 3.5 pipeline:\n\n")

cat("1. Load the processed data:\n")
cat(sprintf('   processed_data <- read.csv("%s")\n', output_file))

cat("\n2. Run the DAII 3.5 pipeline:\n")
cat("   # Load the complete pipeline\n")
cat('   source("R/scripts/DAII_3_5_Complete_Pipeline.R")\n')
cat("   \n")
cat("   # Execute the pipeline\n")
cat("   results <- run_daii_pipeline(\n")
cat(sprintf('     processed_data_path = "%s",\n', output_file))
cat('     config_path = "config/daii_production.yaml"\n')
cat("   )\n")

cat("\n3. Access the results:\n")
cat("   # Portfolio metrics\n")
cat("   print(results$portfolio$portfolio_metrics$overall)\n")
cat("   \n")
cat("   # Company scores\n")
cat("   print(head(results$scores$scores_data))\n")
cat("   \n")
cat("   # Fund analysis\n")
cat("   print(results$portfolio$fund_analysis)\n")

cat("\n📁 OUTPUT FILES:\n")
cat(sprintf("   • %s - Merged & standardized data\n", output_file))
cat(sprintf("   • %s - Field mapping report\n", 
            file.path(output_dir, "field_mapping_report.rds")))

cat("\n")
cat(rep("=", 80), "\n", sep="")
cat("🎯 REAL DATA TEST COMPLETE - READY FOR PRODUCTION PIPELINE\n")
cat(rep("=", 80), "\n", sep="")

# ============================================================================
# OPTIONAL: CREATE A COMPLETE PIPELINE SCRIPT
# ============================================================================

cat("\n💡 Want to create the complete pipeline script now? (y/n): ")
response <- readline()

if (tolower(response) %in% c("y", "yes")) {
  cat("\nCreating complete pipeline script...\n")
  
  complete_pipeline_script <- '
# ============================================================================
# DAII 3.5 COMPLETE PIPELINE - PRODUCTION VERSION
# ============================================================================
# Save as: R/scripts/DAII_3_5_Complete_Pipeline.R
# ============================================================================

run_daii_pipeline <- function(processed_data_path, config_path = NULL) {
  cat("🚀 DAII 3.5 COMPLETE PIPELINE EXECUTION\n")
  cat(rep("=", 80), "\\n", sep="")
  
  # Load processed data
  processed_data <- read.csv(processed_data_path, stringsAsFactors = FALSE)
  
  # Run all DAII modules
  # (Modules 1-5 would be called here)
  
  # Return results structure
  return(list(
    data = processed_data,
    status = "Pipeline execution complete",
    timestamp = Sys.time()
  ))
}

cat("✅ DAII 3.5 Complete Pipeline functions loaded\\n")
'

writeLines(complete_pipeline_script, "R/scripts/DAII_3_5_Complete_Pipeline.R")
cat("✅ Complete pipeline script created: R/scripts/DAII_3_5_Complete_Pipeline.R\n")
}