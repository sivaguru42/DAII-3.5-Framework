
# ============================================================================
# VERIFY SAMPLE DATA CREATION
# ============================================================================

cat("🧪 VERIFYING SAMPLE DATA FILES\n")
cat(rep("=", 60), "\n", sep="")

# Check files exist
files_to_check <- c(
  "data/raw/bloomberg_sample.csv",
  "data/raw/dumac_sample.csv",
  "data/raw/dumac_extended_sample.csv",
  "config/daii_production.yaml"
)

all_exist <- TRUE
for(file in files_to_check) {
  if(file.exists(file)) {
    file_size <- file.info(file)$size / 1024  # Size in KB
    cat(sprintf("✅ %-40s (%.1f KB)\n", basename(file), file_size))
  } else {
    cat(sprintf("❌ %-40s (MISSING)\n", basename(file)))
    all_exist <- FALSE
  }
}

if(all_exist) {
  cat("\n🎯 ALL FILES CREATED SUCCESSFULLY!\n")
  cat("\nNext steps:\n")
  cat("1. Run the field mapping test:\n")
  cat("   source(\"R/scripts/test_real_data.R\")\n")
  cat("\n2. Or run the complete pipeline test:\n")
  cat("   source(\"R/scripts/test_production_config.R\")\n")
} else {
  cat("\n⚠️  Some files are missing. Please re-run create_sample_data.R\n")
}

