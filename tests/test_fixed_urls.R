
# Test script for DAII 3.5 with fixed URLs
cat("Testing fixed DAII 3.5 script...\n")
cat("Time:", Sys.time(), "\n\n")

# Set to GitHub clone directory
setwd("C:/Users/sganesan/DAII-3.5-Framework")

# Load required packages
if (!require("httr")) install.packages("httr")
library(httr)

# Test the fixed URLs
test_urls <- c(
  "https://raw.githubusercontent.com/sivaguru42/DAII-3.5-Framework/main/data/raw/DAII_3_5_N50_Test_Dataset.csv",
  "https://raw.githubusercontent.com/sivaguru42/DAII-3.5-Framework/main/data/raw/bloomberg_sample.csv",
  "https://raw.githubusercontent.com/sivaguru42/DAII-3.5-Framework/main/data/raw/dumac_sample.csv"
)

for (url in test_urls) {
  cat("Testing:", basename(url), "... ")
  response <- tryCatch({
    HEAD(url)
  }, error = function(e) NULL)
  
  if (!is.null(response) && response$status_code == 200) {
    cat("✅\n")
  } else {
    cat("❌ (HTTP", ifelse(is.null(response), "error", response$status_code), ")\n")
  }
}

# Check if the fixed script exists
fixed_script <- "src/consolidated/DAII_3.5_Phase1_Complete_Codebase_FIXED.R"
if (file.exists(fixed_script)) {
  cat("\n✅ Fixed script exists:", fixed_script, "\n")
  cat("You can now run: source(\"", fixed_script, "\")\n", sep = "")
} else {
  cat("\n❌ Fixed script not found\n")
}

