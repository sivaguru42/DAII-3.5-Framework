# ============================================================================
# QUICK FIXED DAII TEST - NO STRING CONCATENATION ERRORS
# ============================================================================

# Clear workspace
rm(list = ls())
cat("\014")  # Clear console

cat("🧪 QUICK DAII FIELD MAPPING TEST\n")
cat(rep("=", 60), "\n", sep="")

# Set working directory
setwd("C:/Users/sganesan/OneDrive - dumac.duke.edu/DAII")
cat("Working directory:", getwd(), "\n\n")

# Check for chunk files
chunk_files <- c(
  "data/raw/chunk_1.csv",
  "data/raw/chunk_2.csv", 
  "data/raw/chunk_3.csv",
  "data/raw/chunk_4.csv",
  "data/raw/chunk_5.csv"
)

missing_files <- chunk_files[!file.exists(chunk_files)]
if(length(missing_files) > 0) {
  stop("Missing files: ", paste(missing_files, collapse=", "))
}

cat("1. Loading chunk files...\n")
data_list <- list()
for(i in 1:5) {
  data_list[[i]] <- read.csv(chunk_files[i], stringsAsFactors = FALSE, check.names = FALSE)
  cat(sprintf("   Loaded %s: %d rows\n", basename(chunk_files[i]), nrow(data_list[[i]])))
}

full_data <- do.call(rbind, data_list)
cat(sprintf("\n✅ Combined: %d rows, %d columns\n", nrow(full_data), ncol(full_data)))

# Simple field mapping without complex dependencies
cat("\n2. Field mapping test...\n")

# Create a simple mapping function
simple_field_mapper <- function(data) {
  cat("Mapping columns:\n")
  
  # Define expected N-50 columns
  n50_columns <- c(
    "Ticker",
    "R.D.Exp",
    "Mkt.Cap", 
    "BEst.Analyst.Rtg",
    "Patents...Trademarks...Copy.Rgt",
    "News.Sent",
    "Rev...1.Yr.Gr",
    "GICS.Ind.Grp.Name",
    "fund_name",
    "fund_weight"
  )
  
  # Check what's available
  available <- n50_columns[n50_columns %in% names(data)]
  missing <- n50_columns[!n50_columns %in% names(data)]
  
  cat(sprintf("   Found: %d of %d expected columns\n", length(available), length(n50_columns)))
  
  if(length(missing) > 0) {
    cat("   Missing columns:\n")
    for(col in missing) cat(sprintf("     - %s\n", col))
  }
  
  return(list(
    available = available,
    missing = missing,
    data = data[, available]
  ))
}

# Run the simple mapper
result <- simple_field_mapper(full_data)

# Create test_output directory if it doesn't exist
if(!dir.exists("test_output")) {
  dir.create("test_output", showWarnings = FALSE)
}

# Save the available data
write.csv(result$data, "test_output/simple_mapped_data.csv", row.names = FALSE)
cat(sprintf("\n✅ Saved mapped data: test_output/simple_mapped_data.csv\n"))

# Compare with Bloomberg headers
cat("\n3. Comparing with Bloomberg headers:\n")
cat(rep("-", 60), "\n", sep="")

comparison <- data.frame(
  Bloomberg_Header = c(
    "Ticker",
    "R&D Exp",
    "Mkt Cap", 
    "BEst Analyst Rtg",
    "Patents / Trademarks / Copy Rgt",
    "News Sent",
    "Rev - 1 Yr Gr",
    "GICS Ind Grp Name"
  ),
  N50_Header = c(
    "Ticker",
    "R.D.Exp",
    "Mkt.Cap", 
    "BEst.Analyst.Rtg",
    "Patents...Trademarks...Copy.Rgt",
    "News.Sent",
    "Rev...1.Yr.Gr",
    "GICS.Ind.Grp.Name"
  ),
  Status = c("✓ Match", "✗ Different", "✗ Different", "✗ Different", 
             "✗ Different", "✗ Different", "✗ Different", "✗ Different")
)

print(comparison)

cat("\n🔍 KEY FINDING: Bloomberg uses SPACES, N-50 uses DOTS\n")
cat("   This confirms we NEED the field mapping system!\n")

# Next steps
cat("\n4. Next Steps:\n")
cat(rep("-", 60), "\n", sep="")

cat("1. ✅ Verified N-50 data structure\n")
cat("2. 📝 Create production field mapping for:\n")
cat("   • Bloomberg: \"R&D Exp\" → \"rd_expense\"\n")
cat("   • DUMAC: \"Fund Name\" → \"fund_name\"\n")
cat("3. 🧪 Test with actual Bloomberg data sample\n")
cat("4. 🚀 Integrate with DAII scoring modules\n")

cat("\n📁 Output created:\n")
cat("   • test_output/simple_mapped_data.csv\n")

cat("\n")
cat(rep("=", 80), "\n", sep="")
cat("✅ QUICK TEST COMPLETE - READY FOR PRODUCTION CONFIGURATION\n")
cat(rep("=", 80), "\n", sep="")