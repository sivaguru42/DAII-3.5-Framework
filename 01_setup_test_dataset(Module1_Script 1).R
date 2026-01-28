# ============================================================
# SCRIPT 1: SETUP & TEST DATASET CREATION - ULTIMATE FIX
# ============================================================
# Purpose: Download and reassemble N200 dataset, create N=50 test dataset
# Run this script FIRST
# Fixed: Forces consistent column types across all chunks
# ============================================================

# Clear workspace
rm(list = ls())

# ============================================================
# 1. SET WORKING DIRECTORIES
# ============================================================

# Define paths (Windows format - use forward slashes for R)
data_dir <- "C:/Users/sganesan/OneDrive - dumac.duke.edu/DAII/data/raw"
scripts_dir <- "C:/Users/sganesan/OneDrive - dumac.duke.edu/DAII/R/scripts"

# Create directories if they don't exist
dir.create(data_dir, recursive = TRUE, showWarnings = FALSE)
dir.create(scripts_dir, recursive = TRUE, showWarnings = FALSE)

# Set working directory to data directory
setwd(data_dir)
cat("📁 Working directory set to:", getwd(), "\n\n")

# ============================================================
# 2. INSTALL REQUIRED PACKAGES
# ============================================================

cat("📦 INSTALLING REQUIRED PACKAGES...\n")

required_packages <- c("readr", "dplyr", "tidyr", "lubridate", "httr", "stringr")
new_packages <- required_packages[!(required_packages %in% installed.packages()[,"Package"])]

if(length(new_packages) > 0) {
  cat("Installing:", paste(new_packages, collapse = ", "), "\n")
  install.packages(new_packages, dependencies = TRUE)
} else {
  cat("All required packages are already installed\n")
}

# Load packages
library(readr)
library(dplyr)
library(tidyr)
library(lubridate)
library(httr)
library(stringr)

cat("✅ Packages loaded successfully\n\n")

# ============================================================
# 3. DOWNLOAD CSV CHUNKS FROM GITHUB
# ============================================================

cat("🌐 DOWNLOADING N200 DATASET CHUNKS FROM GITHUB...\n")

github_urls <- list(
  chunk1 = "https://raw.githubusercontent.com/sivaguru42/DAII-3.5-Framework/refs/heads/main/N200_chunk_01.csv",
  chunk2 = "https://raw.githubusercontent.com/sivaguru42/DAII-3.5-Framework/refs/heads/main/N200_chunk_02.csv",
  chunk3 = "https://raw.githubusercontent.com/sivaguru42/DAII-3.5-Framework/refs/heads/main/N200_chunk_03.csv",
  chunk4 = "https://raw.githubusercontent.com/sivaguru42/DAII-3.5-Framework/refs/heads/main/N200_chunk_04.csv"
)

download_file <- function(url, filename) {
  cat(sprintf("  Downloading %s... ", filename))
  tryCatch({
    response <- GET(url)
    writeBin(content(response, "raw"), filename)
    size_kb <- round(file.info(filename)$size / 1024, 2)
    cat(sprintf("✅ (%.2f KB)\n", size_kb))
    return(TRUE)
  }, error = function(e) {
    cat(sprintf("❌ Error: %s\n", e$message))
    return(FALSE)
  })
}

# Download all chunks
for (i in 1:4) {
  url <- github_urls[[paste0("chunk", i)]]
  filename <- paste0("N200_chunk_0", i, ".csv")
  download_file(url, filename)
}

cat("✅ All CSV chunks downloaded\n\n")

# ============================================================
# 4. REASSEMBLE N200 DATASET WITH AGGRESSIVE TYPE STANDARDIZATION
# ============================================================

cat("🔧 REASSEMBLING N200 DATASET FROM 4 CHUNKS...\n")

chunk_files <- c("N200_chunk_01.csv", "N200_chunk_02.csv",
                 "N200_chunk_03.csv", "N200_chunk_04.csv")

# Verify all files exist
missing_files <- chunk_files[!file.exists(chunk_files)]
if (length(missing_files) > 0) {
  stop(paste("Missing files:", paste(missing_files, collapse = ", ")))
}

# Read first chunk to understand structure
cat("🔍 Analyzing first chunk to understand structure...\n")
first_chunk <- read_csv(chunk_files[1], show_col_types = FALSE)
cat(sprintf("  First chunk: %d rows × %d columns\n", nrow(first_chunk), ncol(first_chunk)))

# Get column names and sample data types
col_names <- names(first_chunk)
cat("  Column names in dataset:\n")
for (i in seq_along(col_names)) {
  cat(sprintf("    %2d. %s\n", i, col_names[i]))
}

# Define a robust standardization function
standardize_dataframe <- function(df, chunk_num) {
  cat(sprintf("  Processing chunk %d...\n", chunk_num))
  
  # Store original dimensions
  orig_rows <- nrow(df)
  orig_cols <- ncol(df)
  
  # Clean column names
  names(df) <- gsub("[^a-zA-Z0-9._]", ".", names(df))
  names(df) <- trimws(names(df))
  
  # List of known character columns (should NOT be converted to numeric)
  char_cols <- c("Ticker", "Currency", "Company", "Sector", "Industry")
  
  # List of known date columns
  date_cols <- c("Date", "Report.Date", "Fiscal.Year.End")
  
  # Convert date columns
  for (col in date_cols) {
    if (col %in% names(df)) {
      df[[col]] <- as.Date(df[[col]], format = "%Y-%m-%d")
    }
  }
  
  # Ensure character columns stay as character
  for (col in char_cols) {
    if (col %in% names(df)) {
      df[[col]] <- as.character(df[[col]])
    }
  }
  
  # Now, for ALL other columns, try to convert to numeric
  conversion_report <- list()
  for (col in names(df)) {
    # Skip columns we've already processed
    if (col %in% c(char_cols, date_cols)) next
    
    # Store original type and NA count
    orig_type <- class(df[[col]])
    orig_na <- sum(is.na(df[[col]]))
    
    # Try conversion to numeric
    converted <- suppressWarnings(as.numeric(df[[col]]))
    
    # Check if conversion was successful (not all NAs unless original was all NAs)
    if (!all(is.na(converted)) || all(is.na(df[[col]]))) {
      df[[col]] <- converted
      new_na <- sum(is.na(df[[col]]))
      if (orig_type != "numeric") {
        conversion_report[[col]] <- list(
          from = orig_type,
          to = "numeric",
          new_nas = new_na - orig_na
        )
      }
    }
    # If conversion failed (all NAs and original wasn't all NAs), leave as is
  }
  
  # Report conversions
  if (length(conversion_report) > 0) {
    cat("    Converted columns:\n")
    for (col in names(conversion_report)) {
      info <- conversion_report[[col]]
      cat(sprintf("      • %s: %s → %s (added %d NAs)\n", 
                  col, info$from, info$to, info$new_nas))
    }
  } else {
    cat("    No type conversions needed\n")
  }
  
  cat(sprintf("    Final: %d rows × %d columns\n", nrow(df), ncol(df)))
  return(df)
}

# Read and standardize all chunks
all_data <- list()
for (i in seq_along(chunk_files)) {
  cat(sprintf("\n  Reading chunk %d/%d: %s\n", i, length(chunk_files), chunk_files[i]))
  
  # Read with minimal assumptions
  chunk_data <- read_csv(
    chunk_files[i], 
    col_types = cols(.default = col_character()),  # Read everything as text
    show_col_types = FALSE,
    na = c("", "NA", "N/A", "null", "NULL", "#N/A", "#VALUE!", "#DIV/0!", "NaN")
  )
  
  cat(sprintf("    Initial: %d rows × %d columns\n", nrow(chunk_data), ncol(chunk_data)))
  
  # Apply standardization
  chunk_data <- standardize_dataframe(chunk_data, i)
  all_data[[i]] <- chunk_data
}

# EMERGENCY FIX: Ensure all chunks have exactly the same column types
cat("\n⚠️  EMERGENCY TYPE UNIFICATION ACROSS ALL CHUNKS...\n")

# Get all column names across all chunks
all_cols <- unique(unlist(lapply(all_data, names)))
cat(sprintf("  Total unique columns: %d\n", length(all_cols)))

# Create a reference type mapping from the first chunk
type_mapping <- list()
for (col in all_cols) {
  if (col %in% names(all_data[[1]])) {
    type_mapping[[col]] <- class(all_data[[1]][[col]])
  }
}

# Force all chunks to match the first chunk's types
for (i in 2:length(all_data)) {
  for (col in all_cols) {
    if (col %in% names(all_data[[i]])) {
      target_type <- type_mapping[[col]]
      current_type <- class(all_data[[i]][[col]])
      
      if (!is.null(target_type) && current_type != target_type) {
        cat(sprintf("    Chunk %d: Converting %s from %s to %s\n", 
                    i, col, current_type, target_type))
        
        if (target_type == "numeric") {
          all_data[[i]][[col]] <- as.numeric(all_data[[i]][[col]])
        } else if (target_type == "character") {
          all_data[[i]][[col]] <- as.character(all_data[[i]][[col]])
        } else if (target_type == "Date") {
          all_data[[i]][[col]] <- as.Date(all_data[[i]][[col]])
        }
      }
    }
  }
}

# Now combine all chunks
cat("\n🔗 Combining all chunks with unified types...\n")
n200_data <- bind_rows(all_data)

cat(sprintf("✅ Combined dataset: %d rows × %d columns\n", 
            nrow(n200_data), ncol(n200_data)))

# Save reassembled dataset
output_file_full <- "N200_FULL_DATASET.csv"
write_csv(n200_data, output_file_full)
cat(sprintf("   Saved: %s/%s\n", data_dir, output_file_full))

# ============================================================
# 5. CREATE N=50 TEST DATASET
# ============================================================

cat("\n🎯 CREATING N=50 TEST DATASET...\n")

# Set seed for reproducibility
set.seed(123)

# Get unique tickers
unique_tickers <- unique(n200_data$Ticker)
cat(sprintf("  Total companies in N200: %d\n", length(unique_tickers)))

# Randomly select 50 companies
test_companies <- sample(unique_tickers, 50)
cat(sprintf("  Selected %d companies for test dataset\n", length(test_companies)))

# Create test dataset
n50_test_data <- n200_data %>%
  filter(Ticker %in% test_companies)

# Save test dataset
output_file_test <- "DAII_3_5_N50_Test_Dataset.csv"
write_csv(n50_test_data, output_file_test)

cat(sprintf("\n✅ N=50 TEST DATASET CREATED SUCCESSFULLY!\n"))
cat(sprintf("   File: %s/%s\n", data_dir, output_file_test))
cat(sprintf("   Dimensions: %d rows × %d columns\n", 
            nrow(n50_test_data), ncol(n50_test_data)))

# Date range with NA handling
date_range <- range(n50_test_data$Date, na.rm = TRUE)
cat(sprintf("   Date range: %s to %s\n", date_range[1], date_range[2]))
cat(sprintf("   Companies: %d unique tickers\n", length(unique(n50_test_data$Ticker))))

# Show sample
cat("\n📋 SAMPLE OF TEST DATASET (first 3 rows):\n")
print(head(n50_test_data, 3))

# ============================================================
# 6. DATA QUALITY REPORT
# ============================================================

cat("\n📊 DATA QUALITY REPORT...\n")

# Check column types
type_summary <- data.frame(
  Column = names(n50_test_data),
  Type = sapply(n50_test_data, function(x) paste(class(x), collapse = ", ")),
  NonNA_Count = sapply(n50_test_data, function(x) sum(!is.na(x))),
  NA_Count = sapply(n50_test_data, function(x) sum(is.na(x))),
  NA_Percent = round(sapply(n50_test_data, function(x) sum(is.na(x))/length(x)*100), 1),
  stringsAsFactors = FALSE
)

cat("  Column Type Summary:\n")
print(type_summary, row.names = FALSE)

# Check for any remaining character columns that should be numeric
problem_cols <- type_summary %>%
  filter(Type == "character" & 
           !Column %in% c("Ticker", "Currency", "Company", "Sector", "Industry") &
           NA_Percent < 100)

if (nrow(problem_cols) > 0) {
  cat("\n⚠️  WARNING: Character columns that might need numeric conversion:\n")
  print(problem_cols, row.names = FALSE)
} else {
  cat("\n✅ All numeric columns properly converted\n")
}

# ============================================================
# 7. FILE VERIFICATION AND CLEANUP
# ============================================================

cat("\n", rep("=", 70), "\n", sep = "")
cat("✅ SCRIPT 1 COMPLETED SUCCESSFULLY!\n\n")

cat("📁 FILES CREATED:\n")
files_created <- c("N200_FULL_DATASET.csv", "DAII_3_5_N50_Test_Dataset.csv")
for (file in files_created) {
  if (file.exists(file)) {
    size_kb <- round(file.info(file)$size / 1024, 2)
    size_mb <- round(size_kb / 1024, 3)
    cat(sprintf("  • %-40s %8.2f KB  (%6.3f MB)\n", file, size_kb, size_mb))
  }
}

# Optional: Delete chunk files to save space
cat("\n🧹 OPTIONAL CLEANUP (delete chunk files):\n")
chunk_sizes <- sapply(chunk_files, function(f) {
  if (file.exists(f)) round(file.info(f)$size / 1024, 2) else 0
})
total_chunk_size <- sum(chunk_sizes)
cat(sprintf("  Chunk files total: %.2f KB (%.3f MB)\n", total_chunk_size, total_chunk_size/1024))
cat("  Delete with: file.remove(chunk_files)\n")

# ============================================================
# 8. PREPARE FOR NEXT SCRIPT
# ============================================================

cat("\n🚀 NEXT STEPS:\n")
cat("   1. Verify the test dataset was created successfully\n")
cat("   2. Check the data quality report above\n")
cat("   3. Run Script 2: 02_module1_data_ingestion.R\n\n")

cat("📋 DATASET SUMMARY:\n")
cat(sprintf("   N200 Full Dataset:      %d rows, %d columns, %d companies\n", 
            nrow(n200_data), ncol(n200_data), length(unique(n200_data$Ticker))))
cat(sprintf("   N50 Test Dataset:       %d rows, %d columns, %d companies\n", 
            nrow(n50_test_data), ncol(n50_test_data), length(unique(n50_test_data$Ticker))))

cat("\n✅ READY FOR MODULE 1 DATA INGESTION!\n")