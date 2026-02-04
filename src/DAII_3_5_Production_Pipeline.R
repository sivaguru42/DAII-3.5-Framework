# ============================================================================
# DAII 3.5 PRODUCTION PIPELINE - CORE FUNCTIONS
# ============================================================================
# Save to: R/scripts/DAII_3_5_Production_Pipeline.R
# ============================================================================

# Load required packages
load_daii_packages <- function() {
  cat("📦 LOADING DAII 3.5 PACKAGES\n")
  packages <- c("dplyr", "tidyr", "yaml")
  
  for(pkg in packages) {
    if(!require(pkg, character.only = TRUE, quietly = TRUE)) {
      cat(sprintf("   Installing: %s\n", pkg))
      install.packages(pkg, dependencies = TRUE)
      library(pkg, character.only = TRUE)
    }
    cat(sprintf("✅ %s\n", pkg))
  }
}

# Initialize environment for testing
initialize_test_environment <- function() {
  cat("🎯 INITIALIZING DAII TEST ENVIRONMENT\n")
  cat(paste(rep("=", 60), collapse = ""), "\n")
  
  # Create necessary directories
  dirs_to_create <- c(
    "test_output",
    "config",
    "data/processed",
    "data/intermediate"
  )
  
  for(dir in dirs_to_create) {
    if(!dir.exists(dir)) {
      dir.create(dir, recursive = TRUE, showWarnings = FALSE)
      cat(sprintf("📁 Created: %s/\n", dir))
    }
  }
  
  # Set paths
  paths <- list(
    data_raw = "data/raw",
    data_processed = "data/processed",
    scripts = "R/scripts",
    config = "config",
    test_output = "test_output"
  )
  
  return(paths)
}

# Core field mapping function
prepare_daii_data <- function(raw_data, config_path = NULL, user_col_map = list()) {
  cat("🧹 DAII FIELD MAPPING & STANDARDIZATION\n")
  cat(paste(rep("=", 70), collapse = ""), "\n")
  
  # Load or create default config
  if(!is.null(config_path) && file.exists(config_path)) {
    config <- yaml::read_yaml(config_path)
    cat(sprintf("   Loaded config: %s\n", basename(config_path)))
  } else {
    # DEFAULT CONFIG for N-50 test data
    config <- list(
      daii_fields = list(
        ticker = list(expected = "Ticker", alternatives = c("Symbol")),
        rd_expense = list(expected = "R.D.Exp", alternatives = c("R&D Exp")),
        market_cap = list(expected = "Mkt.Cap", alternatives = c("Mkt Cap")),
        analyst_rating = list(expected = "BEst.Analyst.Rtg", alternatives = c("BEst Analyst Rtg")),
        patent_count = list(expected = "Patents...Trademarks...Copy.Rgt", 
                            alternatives = c("Patents / Trademarks / Copy Rgt")),
        news_sentiment = list(expected = "News.Sent", alternatives = c("News Sent")),
        revenue_growth = list(expected = "Rev...1.Yr.Gr", alternatives = c("Rev - 1 Yr Gr")),
        industry = list(expected = "GICS.Ind.Grp.Name", alternatives = c("GICS Ind Grp Name", "Industry")),
        fund_name = list(expected = "fund_name", alternatives = c("fund_name", "Fund Name")),
        fund_weight = list(expected = "fund_weight", alternatives = c("fund_weight", "Weight"))
      )
    )
    cat("   Using default configuration for N-50 test\n")
  }
  
  required_fields <- names(config$daii_fields)
  final_map <- list()
  source_log <- list()
  missing_fields <- c()
  
  cat("\n🔍 MAPPING LOGICAL FIELDS TO SOURCE COLUMNS:\n")
  
  # Process each field
  for(field in required_fields) {
    config_entry <- config$daii_fields[[field]]
    found_col <- NA
    method <- "not found"
    
    # Priority 1: User override
    if(!is.null(user_col_map[[field]])) {
      user_spec <- user_col_map[[field]]
      if(user_spec %in% names(raw_data)) {
        found_col <- user_spec
        method <- sprintf("user override: '%s'", user_spec)
      }
    }
    
    # Priority 2: Expected name
    if(is.na(found_col) && config_entry$expected %in% names(raw_data)) {
      found_col <- config_entry$expected
      method <- sprintf("expected: '%s'", found_col)
    }
    
    # Priority 3: Alternative names
    if(is.na(found_col)) {
      for(alt in config_entry$alternatives) {
        if(alt %in% names(raw_data)) {
          found_col <- alt
          method <- sprintf("alternative: '%s'", alt)
          break
        }
      }
    }
    
    # Record result
    if(!is.na(found_col)) {
      final_map[[found_col]] <- field
      source_log[[field]] <- method
      cat(sprintf("   ✓ %-25s → %s\n", field, method))
    } else {
      missing_fields <- c(missing_fields, field)
      cat(sprintf("   ✗ %-25s → NOT FOUND\n", field))
    }
  }
  
  # Create standardized dataframe
  cat("\n📦 CREATING STANDARDIZED DATAFRAME\n")
  
  if(length(final_map) == 0) {
    stop("❌ CRITICAL: No DAII fields could be mapped from the input data")
  }
  
  cols_to_keep <- names(final_map)
  clean_data <- raw_data[, cols_to_keep, drop = FALSE]
  names(clean_data) <- unlist(final_map[cols_to_keep])
  
  # Store metadata
  attr(clean_data, "field_mapping") <- source_log
  attr(clean_data, "missing_fields") <- missing_fields
  attr(clean_data, "original_columns") <- cols_to_keep
  
  # Report summary
  cat(sprintf("   Selected %d of %d required fields\n", 
              length(cols_to_keep), length(required_fields)))
  
  if(length(missing_fields) > 0) {
    cat("\n⚠️  MISSING FIELDS:\n")
    for(mf in missing_fields) {
      cat(sprintf("   - %s\n", mf))
    }
  }
  
  cat("\n✅ DATA STANDARDIZATION COMPLETE\n")
  return(clean_data)
}

# Data validation function
validate_standardized_data <- function(std_data) {
  cat("\n📊 VALIDATING STANDARDIZED DATA\n")
  cat(paste(rep("-", 50), collapse = ""), "\n")
  
  # Check for required fields
  daii_vars <- c("rd_expense", "market_cap", "analyst_rating", 
                 "patent_count", "news_sentiment", "revenue_growth")
  
  missing_summary <- data.frame(
    Variable = daii_vars,
    Present = sapply(daii_vars, function(x) x %in% names(std_data)),
    Missing_Count = sapply(daii_vars, function(x) {
      if(x %in% names(std_data)) sum(is.na(std_data[[x]])) else NA
    }),
    Missing_Percent = sapply(daii_vars, function(x) {
      if(x %in% names(std_data)) round(100*sum(is.na(std_data[[x]]))/nrow(std_data), 1) else NA
    })
  )
  
  cat(sprintf("Dataset: %d rows, %d columns\n", nrow(std_data), ncol(std_data)))
  
  if("ticker" %in% names(std_data)) {
    unique_companies <- length(unique(std_data$ticker))
    cat(sprintf("Unique companies: %d\n", unique_companies))
  }
  
  cat("\nMissing Data Analysis:\n")
  print(missing_summary)
  
  return(list(
    data = std_data,
    validation_report = list(missing_summary = missing_summary)
  ))
}

cat("✅ DAII 3.5 Production Pipeline functions loaded\n")