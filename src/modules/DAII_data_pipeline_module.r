# ============================================================================
# MODULE 1: DATA PIPELINE - Loading & Preprocessing
# ============================================================================
# 
# PURPOSE: Load, validate, and prepare raw data for DAII calculation
# 
# DESIGN PRINCIPLES:
# 1. Robustness: Handle various data formats and missing patterns
# 2. Transparency: Track all data transformations
# 3. Reproducibility: Version control for data inputs
# 4. Scalability: Handle from 200 to 2,000+ companies
# 
# STATISTICAL CONCEPTS:
# - Data validation: Range checks, type validation, completeness assessment
# - Missing data patterns: MCAR, MAR, MNAR assessment
# - Data cleaning: Outlier detection, consistency checks
# 
# ============================================================================

initialize_daii_environment <- function(working_dir = NULL) {
  #' Initialize DAII 3.5 Analysis Environment
  #' 
  #' @param working_dir Path to working directory (optional)
  #' @return List of environment settings
  #' 
  #' CONCEPT: Environment setup ensures reproducibility across sessions
  
  cat("🎯 INITIALIZING DAII 3.5 ANALYSIS ENVIRONMENT\n")
  cat(paste(rep("=", 70), collapse = ""), "\n")
  
  # Set working directory
  if(!is.null(working_dir)) {
    setwd(working_dir)
    cat(sprintf("📂 Working directory: %s\n", getwd()))
  }
  
  # Create directory structure
  dirs_to_create <- c(
    "01_Raw_Data",
    "02_Imputed_Data", 
    "03_Intermediate",
    "04_Results",
    "05_Visualizations",
    "06_Validation",
    "07_Comparison",
    "00_DUMAC_Data"
  )
  
  for(dir in dirs_to_create) {
    if(!dir.exists(dir)) {
      dir.create(dir, showWarnings = FALSE)
      cat(sprintf("📁 Created directory: %s/\n", dir))
    }
  }
  
  # Load required packages
  required_packages <- c(
    "dplyr",        # Data manipulation
    "tidyr",        # Data tidying
    "ggplot2",      # Visualization
    "corrplot",     # Correlation visualization
    "gridExtra",    # Multiple plots
    "psych",        # Psychometric analysis
    "reshape2",     # Data reshaping
    "openxlsx",     # Excel output
    "rmarkdown",    # Reporting
    "knitr"         # Dynamic reporting
  )
  
  cat("\n📦 LOADING REQUIRED PACKAGES:\n")
  for(pkg in required_packages) {
    if(!require(pkg, character.only = TRUE)) {
      install.packages(pkg, dependencies = TRUE)
      library(pkg, character.only = TRUE)
    }
    cat(sprintf("✅ %s\n", pkg))
  }
  
  # Return environment settings
  return(list(
    working_dir = getwd(),
    directories = dirs_to_create,
    packages = required_packages,
    timestamp = format(Sys.time(), "%Y-%m-%d %H:%M:%S")
  ))
}

load_and_validate_data <- function(data_path, 
                                   expected_cols = NULL,
                                   min_companies = 100,
                                   max_missing_pct = 0.5) {
  #' Load and Validate Input Data
  #' 
  #' @param data_path Path to input CSV file
  #' @param expected_cols Expected column names (optional)
  #' @param min_companies Minimum number of companies required
  #' @param max_missing_pct Maximum allowed missing data percentage
  #' @return List containing validated data and validation report
  #' 
  #' STATISTICAL CONCEPT: Data quality assessment before processing
  
  cat("\n📥 LOADING AND VALIDATING INPUT DATA\n")
  cat(paste(rep("-", 50), collapse = ""), "\n")
  
  # Load data
  if(!file.exists(data_path)) {
    stop(sprintf("❌ Data file not found: %s", data_path))
  }
  
  data <- read.csv(data_path, stringsAsFactors = FALSE, check.names = FALSE)
  cat(sprintf("✅ Data loaded: %d rows, %d columns\n", 
              nrow(data), ncol(data)))
  
  # Identify ticker column (first column assumed)
  ticker_col <- names(data)[1]
  cat(sprintf("📊 Ticker column identified: %s\n", ticker_col))
  
  # Basic validation
  validation_report <- list(
    file_info = list(
      path = data_path,
      size_bytes = file.size(data_path),
      load_time = Sys.time()
    ),
    structure = list(
      total_rows = nrow(data),
      total_cols = ncol(data),
      ticker_col = ticker_col,
      unique_companies = length(unique(data[[ticker_col]]))
    ),
    missing_data = list(),
    data_types = list()
  )
  
  # Check minimum companies requirement
  if(validation_report$structure$unique_companies < min_companies) {
    warning(sprintf("⚠️ Only %d unique companies (minimum %d required)",
                    validation_report$structure$unique_companies, min_companies))
  }
  
  # Validate expected columns if provided
  if(!is.null(expected_cols)) {
    missing_cols <- setdiff(expected_cols, names(data))
    if(length(missing_cols) > 0) {
      warning(sprintf("⚠️ Missing expected columns: %s", 
                      paste(missing_cols, collapse = ", ")))
    }
  }
  
  # Analyze data types
  for(col in names(data)) {
    validation_report$data_types[[col]] <- class(data[[col]])
  }
  
  # Analyze missing data patterns
  numeric_cols <- grep("R\\.D\\.Exp|Mkt\\.Cap|Rev|BEst\\.Analyst|Patents|News", 
                       names(data), value = TRUE, ignore.case = TRUE)
  
  missing_summary <- data.frame(
    Column = character(),
    Missing_Count = integer(),
    Missing_Percent = numeric(),
    stringsAsFactors = FALSE
  )
  
  for(col in numeric_cols) {
    if(col %in% names(data)) {
      # Count various missing indicators
      missing_patterns <- c("#N/A", "N/A", "NA", "NULL", "", "Field Not Applicable")
      missing_count <- sum(
        is.na(data[[col]]) | 
        data[[col]] %in% missing_patterns |
        grepl(paste(missing_patterns, collapse = "|"), data[[col]], ignore.case = TRUE)
      )
      
      missing_pct <- round(100 * missing_count / nrow(data), 1)
      
      missing_summary <- rbind(missing_summary, data.frame(
        Column = col,
        Missing_Count = missing_count,
        Missing_Percent = missing_pct
      ))
      
      # Flag high missing percentages
      if(missing_pct > max_missing_pct * 100) {
        warning(sprintf("⚠️ High missing data in %s: %.1f%%", col, missing_pct))
      }
    }
  }
  
  validation_report$missing_data$summary <- missing_summary
  
  # Print validation summary
  cat("\n📋 DATA VALIDATION SUMMARY:\n")
  cat(sprintf(" • Total holdings (rows): %d\n", nrow(data)))
  cat(sprintf(" • Unique companies: %d\n", validation_report$structure$unique_companies))
  cat(sprintf(" • Columns requiring imputation: %d\n", 
              sum(missing_summary$Missing_Percent > 0)))
  
  if(nrow(missing_summary) > 0) {
    cat("\n📊 MISSING DATA ANALYSIS:\n")
    print(missing_summary)
  }
  
  # Detect holdings structure
  fund_col <- grep("fund", names(data), ignore.case = TRUE, value = TRUE)[1]
  if(!is.null(fund_col)) {
    unique_funds <- length(unique(data[[fund_col]]))
    cat(sprintf("\n🏦 PORTFOLIO STRUCTURE DETECTED:\n"))
    cat(sprintf(" • Fund column: %s\n", fund_col))
    cat(sprintf(" • Unique funds: %d\n", unique_funds))
    
    # Analyze holdings distribution
    holdings_per_company <- table(data[[ticker_col]])
    cat(sprintf(" • Average holdings per company: %.1f\n", 
                mean(as.numeric(holdings_per_company))))
    cat(sprintf(" • Max holdings per company: %d\n", 
                max(as.numeric(holdings_per_company))))
    
    validation_report$portfolio_structure <- list(
      fund_column = fund_col,
      unique_funds = unique_funds,
      holdings_distribution = summary(as.numeric(holdings_per_company))
    )
  }
  
  return(list(
    data = data,
    ticker_col = ticker_col,
    validation_report = validation_report
  ))
}

extract_unique_companies <- function(data, ticker_col, 
                                     exclude_cols = NULL,
                                     min_components = 3) {
  #' Extract Unique Companies for Scoring
  #' 
  #' @param data Full holdings data
  #' @param ticker_col Ticker column name
  #' @param exclude_cols Columns to exclude (fund-specific)
  #' @param min_components Minimum component columns required
  #' @return Unique company-level data
  #' 
  #' DESIGN PRINCIPLE: Innovation measured at company level, not holding level
  
  cat("\n🏢 EXTRACTING UNIQUE COMPANIES FOR SCORING\n")
  cat(paste(rep("-", 50), collapse = ""), "\n")
  
  # Identify fund-specific columns to exclude
  if(is.null(exclude_cols)) {
    exclude_cols <- grep("fund|weight|allocation|shares|position|date", 
                         names(data), ignore.case = TRUE, value = TRUE)
  }
  
  # Keep company-level columns only
  company_cols <- setdiff(names(data), exclude_cols)
  
  # Extract unique companies (remove duplicate rows for same company)
  unique_companies <- data[!duplicated(data[[ticker_col]]), company_cols]
  
  cat(sprintf("✅ Extracted %d unique companies from %d holdings\n", 
              nrow(unique_companies), nrow(data)))
  
  # Verify required component columns exist
  required_component_cols <- c(
    "R.D.Exp", "Mkt.Cap", "Rev...1.Yr.Gr", 
    "BEst.Analyst.Rtg", "Patents...Trademarks...Copy.Rgt", "News.Sent"
  )
  
  missing_components <- setdiff(required_component_cols, names(unique_companies))
  if(length(missing_components) > 0) {
    warning(sprintf("⚠️ Missing component columns: %s", 
                    paste(missing_components, collapse = ", ")))
  }
  
  # Check data completeness for scoring
  component_check <- list()
  for(col in intersect(required_component_cols, names(unique_companies))) {
    non_missing <- sum(!is.na(unique_companies[[col]]) & 
                       !grepl("#N/A|N/A", unique_companies[[col]]))
    component_check[[col]] <- round(100 * non_missing / nrow(unique_companies), 1)
  }
  
  cat("\n📊 COMPANY-LEVEL DATA COMPLETENESS:\n")
  for(col in names(component_check)) {
    cat(sprintf(" • %-35s: %6.1f%% complete\n", col, component_check[[col]]))
  }
  
  return(list(
    companies = unique_companies,
    company_cols = company_cols,
    component_completeness = component_check
  ))
}