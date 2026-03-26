# ============================================================================
# WIPO BULK DATA – QUARTERLY DOWNLOAD MODULE
# File: R/02_ingest/13_wipo_bulk.r
# ============================================================================

library(dplyr)
library(xml2)
library(httr)

#' Download WIPO PCT data (quarterly)
#' @param year Year to download
#' @param quarter Quarter (1-4)
#' @param data_dir Directory to save files
#' @param force_download Force re-download even if file exists
#' @return Path to downloaded file
download_wipo_bulk <- function(year = format(Sys.Date(), "%Y"), 
                               quarter = ceiling(as.numeric(format(Sys.Date(), "%m")) / 3),
                               data_dir = "data/01_raw/wipo_bulk/",
                               force_download = FALSE) {
  
  # Create directory if needed
  if(!dir.exists(data_dir)) dir.create(data_dir, recursive = TRUE)
  
  # WIPO FTP URLs (these are the official bulk data locations)
  # Note: FTP access may require login; alternative is HTTP download
  base_url <- "https://www3.wipo.int/patentscope/data/pct_data/"
  
  # Quarterly files pattern
  file_pattern <- paste0("pct_data_", year, "_Q", quarter, ".zip")
  url <- paste0(base_url, file_pattern)
  dest_file <- file.path(data_dir, file_pattern)
  
  # Check if file exists
  if(file.exists(dest_file) && !force_download) {
    message("   File already exists: ", file_pattern)
    return(dest_file)
  }
  
  # Download
  message("   Downloading WIPO bulk data for ", year, " Q", quarter, "...")
  result <- tryCatch({
    download.file(url, dest_file, mode = "wb", quiet = FALSE)
    TRUE
  }, error = function(e) {
    message("   ⚠️ Download failed: ", e$message)
    message("   Try manual download from: ", url)
    return(FALSE)
  })
  
  if(!result) return(NULL)
  
  # Extract
  message("   Extracting...")
  unzip(dest_file, exdir = data_dir)
  message("   ✅ Extracted to: ", data_dir)
  
  return(dest_file)
}

#' Process WIPO bulk XML files for specific companies
#' @param companies Dataframe with ticker and company_name
#' @param data_dir Directory with extracted bulk data
#' @param ai_codes Vector of AI IPC/CPC codes
#' @return Dataframe with patent counts
process_wipo_bulk <- function(companies, 
                              data_dir = "data/01_raw/wipo_bulk/",
                              ai_codes = c("G06N", "G06K", "G06F", "G10L", "G05B", "G16H")) {
  
  message("   Processing WIPO bulk data...")
  
  # Find all XML files
  xml_files <- list.files(data_dir, pattern = "\\.xml$", full.names = TRUE, recursive = TRUE)
  message("   Found ", length(xml_files), " XML files")
  
  # For large datasets, we'll need a more efficient approach
  # This is a simplified version that searches for company names in file contents
  # In production, you'd want to parse the XML properly
  
  results <- data.frame()
  
  for(i in 1:nrow(companies)) {
    ticker <- companies$ticker[i]
    company_name <- companies$company_name[i]
    
    # Search for company name in XML files
    # This is simplified – full implementation requires proper XML parsing
    # For now, we'll use grep on file names as a placeholder
    clean_name <- gsub(" Inc$| Corp$| Ltd$| LLC$| PLC$| Co$| Company$", "", company_name)
    clean_name <- trimws(clean_name)
    
    # Count files containing company name (simplified)
    matching_files <- grep(clean_name, xml_files, ignore.case = TRUE, value = TRUE)
    total_patents <- length(matching_files)
    
    # Estimate AI patents (simplified)
    ai_patents <- round(total_patents * 0.25)  # Placeholder
    
    results <- rbind(results, data.frame(
      ticker = ticker,
      company_name = company_name,
      total_patents = total_patents,
      ai_patents = ai_patents,
      source = "WIPO_BULK",
      stringsAsFactors = FALSE
    ))
    
    if(i %% 10 == 0) cat("   Processed", i, "companies\n")
  }
  
  return(results)
}

#' Check if WIPO data is available for a given quarter
#' @param year Year
#' @param quarter Quarter (1-4)
#' @return TRUE if data exists
check_wipo_availability <- function(year = format(Sys.Date(), "%Y"), 
                                    quarter = ceiling(as.numeric(format(Sys.Date(), "%m")) / 3)) {
  
  url <- paste0("https://www3.wipo.int/patentscope/data/pct_data/pct_data_", year, "_Q", quarter, ".zip")
  
  response <- HEAD(url)
  return(status_code(response) == 200)
}