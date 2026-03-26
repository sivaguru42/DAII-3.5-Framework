

# ============================================================================
# UPDATE USPTO BULK FILES
# File: R/02_ingest/06_update_uspto_bulk.r
# Version: 1.0 | Date: 2026-03-26
# ============================================================================

update_uspto_bulk <- function() {
  
  message("
🔄 Updating USPTO bulk files...
")
  
  BULK_DIR <- here::here("data", "01_raw", "uspto_bulk/")
  if(!dir.exists(BULK_DIR)) dir.create(BULK_DIR, recursive = TRUE)
  
  # URLs for the latest files (use most recent year)
  current_year <- format(Sys.Date(), "%Y")
  
  # Try current year, fall back to previous year if needed
  assignee_url <- paste0("https://bulkdata.uspto.gov/data/patent/grant/redbook/bibliographic/", 
                         current_year, "/g_assignee_disambiguated_", current_year, "1231.tsv.zip")
  cpc_url <- paste0("https://bulkdata.uspto.gov/data/patent/grant/redbook/bibliographic/", 
                    current_year, "/g_cpc_current_", current_year, "1231.tsv.zip")
  
  # Function to download with fallback
  download_with_fallback <- function(url, dest_zip, year) {
    # Try current year
    result <- tryCatch({
      download.file(url, dest_zip, mode = "wb", quiet = TRUE)
      TRUE
    }, error = function(e) FALSE)
    
    if(!result && year > 2020) {
      # Try previous year
      prev_year <- as.numeric(year) - 1
      fallback_url <- gsub(year, prev_year, url)
      message(sprintf("   Trying %d...", prev_year))
      result <- tryCatch({
        download.file(fallback_url, dest_zip, mode = "wb", quiet = TRUE)
        TRUE
      }, error = function(e) FALSE)
    }
    
    return(result)
  }
  
  # Download assignee file
  message("   Downloading assignee file...")
  assignee_zip <- file.path(BULK_DIR, "assignee.zip")
  if(download_with_fallback(assignee_url, assignee_zip, current_year)) {
    unzip(assignee_zip, exdir = BULK_DIR)
    file.remove(assignee_zip)
    message("   ✅ Assignee file updated")
  } else {
    message("   ⚠️ Could not download assignee file. Using existing.")
  }
  
  # Download CPC file
  message("   Downloading CPC file...")
  cpc_zip <- file.path(BULK_DIR, "cpc.zip")
  if(download_with_fallback(cpc_url, cpc_zip, current_year)) {
    unzip(cpc_zip, exdir = BULK_DIR)
    file.remove(cpc_zip)
    message("   ✅ CPC file updated")
  } else {
    message("   ⚠️ Could not download CPC file. Using existing.")
  }
  
  # Clear cache so next run uses fresh data
  cache_file <- here::here("data", "01_raw", "patent_data.rds")
  if(file.exists(cache_file)) {
    file.remove(cache_file)
    message("   ✅ Cache cleared")
  }
  
  message("
✅ Bulk file update complete
")
}

