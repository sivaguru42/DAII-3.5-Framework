# ============================================================================
# DAII 3.5 COMPLETE PIPELINE - MODULES 1-3 (ALL FIXES INTEGRATED)
# ============================================================================
# Load required packages
required_packages <- c("dplyr", "tidyr", "yaml", "caret", "stringr", "ggplot2")
new_packages <- required_packages[!(required_packages %in% installed.packages()[,"Package"])]
if(length(new_packages)) install.packages(new_packages)
invisible(lapply(required_packages, library, character.only = TRUE))

# ============================================================================
# SET WORKING DIRECTORY STRUCTURE
# ============================================================================

# Base directory
base_dir <- "C:/Users/sganesan/OneDrive - dumac.duke.edu/DAII"

# Define all directory paths
dir_paths <- list(
  r_scripts = file.path(base_dir, "R", "scripts"),
  config = file.path(base_dir, "config"),
  data_raw = file.path(base_dir, "data", "raw"),
  data_processed = file.path(base_dir, "data", "processed"),
  data_output = file.path(base_dir, "data", "output"),
  logs = file.path(base_dir, "logs")
)

# Create all directories if they don't exist
create_directories <- function() {
  cat("📁 SETTING UP DIRECTORY STRUCTURE\n")
  cat(paste(rep("=", 60), collapse = ""), "\n")
  
  for (dir_name in names(dir_paths)) {
    dir_path <- dir_paths[[dir_name]]
    if (!dir.exists(dir_path)) {
      dir.create(dir_path, recursive = TRUE, showWarnings = FALSE)
      cat(sprintf("   Created: %-15s -> %s\n", dir_name, dir_path))
    } else {
      cat(sprintf("   Exists:  %-15s -> %s\n", dir_name, dir_path))
    }
  }
  cat("✅ Directory structure verified\n\n")
}

# Initialize directories
create_directories()

# Set working directory to R scripts folder
setwd(dir_paths$r_scripts)
cat("Working directory set to:", getwd(), "\n\n")

# ============================================================================
# DATA STRUCTURE DIAGNOSTIC FUNCTION
# ============================================================================

check_data_structure <- function(data) {
  cat("\n🔍 DATA STRUCTURE DIAGNOSTIC\n")
  cat(paste(rep("=", 60), collapse = ""), "\n")
  
  cat("   Dataset dimensions:", nrow(data), "rows ×", ncol(data), "columns\n")
  
  # Check for duplicate industry names
  if ("gics_industry" %in% names(data)) {
    industry_counts <- table(data$gics_industry)
    duplicates <- industry_counts[industry_counts > 1]
    
    if (length(duplicates) > 0) {
      cat("\n   ⚠️  DUPLICATE INDUSTRY NAMES FOUND:\n")
      for (ind in names(duplicates)[1:min(5, length(duplicates))]) {
        cat(sprintf("     %-30s: %d occurrences\n", ind, duplicates[ind]))
      }
      if (length(duplicates) > 5) {
        cat(sprintf("     ... and %d more\n", length(duplicates) - 5))
      }
    } else {
      cat("\n   ✓ No duplicate industry names\n")
    }
  }
  
  # Check for required DAII fields
  required_fields <- c("ticker", "rd_expense", "total_assets", "analyst_coverage",
                       "patent_applications", "news_volume", "revenue_growth")
  
  cat("\n   REQUIRED FIELDS CHECK:\n")
  for (field in required_fields) {
    if (field %in% names(data)) {
      missing_pct <- round(sum(is.na(data[[field]])) / nrow(data) * 100, 1)
      cat(sprintf("     ✓ %-25s: Present (%d%% missing)\n", field, missing_pct))
    } else {
      cat(sprintf("     ✗ %-25s: NOT FOUND\n", field))
    }
  }
  
  cat("\n   FIRST 5 ROWS:\n")
  print(head(data[, 1:min(6, ncol(data))], 5))
  
  return(data)
}

# ============================================================================
# CONFIGURATION FILES - SAVED TO CONFIG DIRECTORY
# ============================================================================

# Configuration 1: Field Mapping Configuration
field_mapping_config <- '
daii_fields:
  ticker:
    expected: "ticker"
    alternatives: ["Symbol", "Ticker", "Company_Symbol"]
  company_name:
    expected: "company_name"
    alternatives: ["Name", "company", "issuer_name", "Company.Name"]
  gics_industry:
    expected: "gics_industry"
    alternatives: ["Industry", "sector", "GICS_Sector", "GICS.Ind.Grp.Name"]
  rd_expense:
    expected: "rd_expense"
    alternatives: ["R.D.Exp", "research_development", "R_D_Expense"]
  total_assets:
    expected: "total_assets"
    alternatives: ["Tot_Assets", "assets", "TotalAssets"]
  market_cap:
    expected: "market_cap"
    alternatives: ["MKTCAP", "MarketCap", "mkt_cap"]
  analyst_coverage:
    expected: "analyst_coverage"
    alternatives: ["Analyst.Coverage", "analyst_count", "analysts", "AnalystCount"]
  patent_applications:
    expected: "patent_applications"
    alternatives: ["Patent.Applications", "patents", "patent_count", "Patents"]
  news_volume:
    expected: "news_volume"
    alternatives: ["News.Volume", "news_count", "news_mentions", "NewsCount"]
  revenue_growth:
    expected: "revenue_growth"
    alternatives: ["Rev_Growth", "revenue_growth", "growth_rate", "Growth"]
'

# Save configuration files to config directory
save_config_files <- function() {
  cat("💾 SAVING CONFIGURATION FILES\n")
  cat(paste(rep("-", 50), collapse = ""), "\n")
  
  configs <- list(
    "daii_field_mapping.yaml" = field_mapping_config,
    "daii_imputation_config.yaml" = 'imputation:\n  strategy: "tiered"\n  tiers:\n    - method: "industry_median"\n      fields: ["rd_expense", "analyst_coverage", "patent_applications"]\n    - method: "median"\n      fields: ["total_assets", "market_cap"]\n    - method: "mean"\n      fields: ["news_volume", "revenue_growth"]\n  validation:\n    missing_threshold: 0.3\n    outlier_method: "iqr"\n    outlier_threshold: 1.5',
    "daii_scoring_config.yaml" = 'scoring:\n  weights:\n    rd_intensity: 0.30\n    analyst_intensity: 0.20\n    patent_intensity: 0.25\n    news_intensity: 0.10\n    growth_intensity: 0.15\n  transformations:\n    rd_intensity: "log"\n    analyst_intensity: "sqrt"\n    patent_intensity: "log"\n    news_intensity: "sqrt"\n    growth_intensity: "raw"\n  quartiles:\n    method: "rank"\n    ties.method: "first"\n    labels: ["Low", "Moderate", "High", "Very High"]',
    "daii_module4_bridge_config.yaml" = 'column_mapping:\n  rd_intensity_score: "R_D_Score"\n  analyst_intensity_score: "Analyst_Score"\n  patent_intensity_score: "Patent_Score"\n  news_intensity_score: "News_Score"\n  growth_intensity_score: "Growth_Score"\naggregation:\n  group_by: ["gics_industry", "ticker"]\n  metrics: ["mean", "median", "count"]'
  )
  
  for (filename in names(configs)) {
    filepath <- file.path(dir_paths$config, filename)
    writeLines(configs[[filename]], filepath)
    cat(sprintf("   Saved: %s\n", filename))
  }
  cat("✅ Configuration files saved to:", dir_paths$config, "\n\n")
}

save_config_files()

# ============================================================================
# CORE PIPELINE FUNCTIONS (ALL FIXES INTEGRATED)
# ============================================================================

# Field Mapping Engine (Pre-Module 1)
load_daii_config <- function(config_path = NULL) {
  if (!is.null(config_path) && file.exists(config_path)) {
    config <- yaml::yaml.load_file(config_path)
  } else {
    config <- yaml::yaml.load(field_mapping_config)
  }
  return(config)
}

prepare_daii_data <- function(raw_data, config_path = NULL, user_col_map = list(), auto_map = TRUE) {
  cat("🧹 PREPARING DAII INPUT DATA\n")
  cat(paste(rep("=", 60), collapse = ""), "\n")
  
  # Show raw data structure first
  cat("   Raw data structure:\n")
  cat(sprintf("     Rows: %d, Columns: %d\n", nrow(raw_data), ncol(raw_data)))
  cat("     First few column names:", paste(head(names(raw_data), 5), collapse = ", "), "...\n")
  
  # Load configuration
  if (is.null(config_path)) {
    config_path <- file.path(dir_paths$config, "daii_field_mapping.yaml")
  }
  config <- load_daii_config(config_path)
  required_fields <- names(config$daii_fields)
  
  # Initialize mapping logic
  final_map <- list()
  source_cols <- list()
  missing_fields <- c()
  
  cat("\n🔍 MAPPING LOGICAL FIELDS TO SOURCE COLUMNS:\n")
  
  for (field in required_fields) {
    config_entry <- config$daii_fields[[field]]
    found_col <- NA
    
    # Priority 1: User-specified override
    if (!is.null(user_col_map[[field]])) {
      user_spec <- user_col_map[[field]]
      if (user_spec %in% names(raw_data)) {
        found_col <- user_spec
        source_cols[[field]] <- paste0("user override: '", found_col, "'")
        cat(sprintf("   ✓ %-25s -> %s\n", field, source_cols[[field]]))
      }
    }
    
    # Priority 2: Expected column name from config
    if (is.na(found_col) && config_entry$expected %in% names(raw_data)) {
      found_col <- config_entry$expected
      source_cols[[field]] <- paste0("expected: '", found_col, "'")
      cat(sprintf("   ✓ %-25s -> %s\n", field, source_cols[[field]]))
    }
    
    # Priority 3: Alternative column names
    if (is.na(found_col) && length(config_entry$alternatives) > 0) {
      for (alt in config_entry$alternatives) {
        if (alt %in% names(raw_data)) {
          found_col <- alt
          source_cols[[field]] <- paste0("alternative: '", found_col, "'")
          cat(sprintf("   ✓ %-25s -> %s\n", field, source_cols[[field]]))
          break
        }
      }
    }
    
    # Priority 4: Fuzzy/auto matching
    if (is.na(found_col) && auto_map) {
      pattern <- gsub("_", ".", field)
      matches <- grep(pattern, names(raw_data), ignore.case = TRUE, value = TRUE)
      if (length(matches) == 1) {
        found_col <- matches[1]
        source_cols[[field]] <- paste0("auto-matched: '", found_col, "'")
        cat(sprintf("   ~ %-25s -> %s\n", field, source_cols[[field]]))
      }
    }
    
    # Store result or flag as missing
    if (!is.na(found_col)) {
      final_map[[found_col]] <- field
    } else {
      missing_fields <- c(missing_fields, field)
      cat(sprintf("   ✗ %-25s -> NOT FOUND\n", field))
    }
  }
  
  # Create standardized dataframe
  cat("\n📦 CREATING STANDARDIZED DATAFRAME\n")
  cols_to_keep <- names(final_map)
  
  if (length(cols_to_keep) == 0) {
    cat("⚠️  No standard DAII fields found. Using first 10 columns with raw names.\n")
    cols_to_keep <- names(raw_data)[1:min(10, ncol(raw_data))]
    clean_data <- raw_data[, cols_to_keep, drop = FALSE]
  } else {
    clean_data <- raw_data[, cols_to_keep, drop = FALSE]
    names(clean_data) <- unlist(final_map[cols_to_keep])
  }
  
  # Report and return
  cat(sprintf("   Selected %d of %d required fields.\n", 
              length(cols_to_keep), length(required_fields)))
  
  if (length(missing_fields) > 0) {
    cat("\n⚠️  MISSING FIELDS:\n")
    cat(paste("   -", missing_fields, collapse = "\n"), "\n")
  }
  
  # Add metadata
  attr(clean_data, "daii_field_mapping") <- source_cols
  attr(clean_data, "missing_fields") <- missing_fields
  attr(clean_data, "original_colnames") <- names(raw_data)
  
  cat("\n✅ DATA PREPARATION COMPLETE\n")
  return(clean_data)
}

# Module 1: Data Validation
validate_daii_data <- function(data) {
  cat("\n📊 MODULE 1: DATA VALIDATION\n")
  cat(paste(rep("-", 50), collapse = ""), "\n")
  
  validation_report <- list(
    timestamp = Sys.time(),
    n_companies = nrow(data),
    n_variables = ncol(data),
    column_names = names(data),
    missing_summary = list(),
    validation_checks = list()
  )
  
  # Check for required columns
  required_cols <- c("ticker", "rd_expense", "total_assets", "analyst_coverage", 
                     "patent_applications", "news_volume", "revenue_growth")
  
  # Check which required columns are present
  present_cols <- required_cols[required_cols %in% names(data)]
  missing_req <- setdiff(required_cols, names(data))
  
  validation_report$validation_checks$required_columns <- list(
    required = required_cols,
    present = present_cols,
    missing = missing_req,
    status = ifelse(length(missing_req) == 0, "PASS", "WARNING")
  )
  
  # Calculate missing data percentages
  if (ncol(data) > 0) {
    missing_pct <- colSums(is.na(data)) / nrow(data) * 100
    validation_report$missing_summary <- missing_pct[missing_pct > 0]
  }
  
  cat("   Data dimensions:", nrow(data), "companies ×", ncol(data), "variables\n")
  cat("   Column names:", paste(names(data), collapse = ", "), "\n")
  
  cat("   Required columns check:\n")
  cat(sprintf("     Present: %s\n", paste(present_cols, collapse = ", ")))
  if (length(missing_req) > 0) {
    cat(sprintf("     Missing: %s\n", paste(missing_req, collapse = ", ")))
    cat("   ⚠️  Warning: Some required columns are missing. Scoring may be incomplete.\n")
  }
  
  cat("   Missing data analysis:\n")
  if (length(validation_report$missing_summary) > 0) {
    for (col in names(validation_report$missing_summary)) {
      cat(sprintf("     %-25s: %6.2f%% missing\n", 
                  col, validation_report$missing_summary[col]))
    }
  } else {
    cat("     No missing data found.\n")
  }
  
  return(validation_report)
}

# Module 2: Imputation Engine (FIXED VERSION - No duplicate industry error)
impute_daii_data <- function(data, config_path = NULL) {
  cat("\n🔄 MODULE 2: INTELLIGENT IMPUTATION ENGINE\n")
  cat(paste(rep("-", 50), collapse = ""), "\n")
  
  # Load imputation configuration
  if (is.null(config_path)) {
    config_path <- file.path(dir_paths$config, "daii_imputation_config.yaml")
  }
  config <- yaml::yaml.load_file(config_path)$imputation
  
  imputed_data <- data
  imputation_log <- list()
  
  # Check if we have industry information
  has_industry <- "gics_industry" %in% names(data)
  
  for (tier in config$tiers) {
    method <- tier$method
    fields <- tier$fields
    
    for (field in fields) {
      if (field %in% names(imputed_data)) {
        missing_idx <- is.na(imputed_data[[field]])
        
        if (any(missing_idx)) {
          if (method == "industry_median" && has_industry) {
            cat(sprintf("   Processing %s with industry_median...\n", field))
            
            # Industry-specific imputation - FIXED: Handle duplicates
            industry_medians <- imputed_data %>%
              group_by(gics_industry) %>%
              summarise(
                median_val = median(.data[[field]], na.rm = TRUE),
                .groups = "drop"
              ) %>%
              distinct(gics_industry, .keep_all = TRUE)  # Remove duplicates
            
            # Create a lookup table
            median_lookup <- setNames(industry_medians$median_val, industry_medians$gics_industry)
            
            for (i in which(missing_idx)) {
              industry <- imputed_data$gics_industry[i]
              
              # Check if industry exists in lookup
              if (!is.na(industry) && industry %in% names(median_lookup)) {
                industry_val <- median_lookup[[industry]]
                
                # Check if industry_val is valid (single value, not NA)
                if (length(industry_val) == 1 && !is.na(industry_val)) {
                  imputed_data[i, field] <- industry_val
                  imputation_type <- "industry_median"
                } else {
                  # Fall back to overall median
                  overall_median <- median(imputed_data[[field]], na.rm = TRUE)
                  imputed_data[i, field] <- overall_median
                  imputation_type <- "overall_median"
                }
              } else {
                # Industry not found or NA, use overall median
                overall_median <- median(imputed_data[[field]], na.rm = TRUE)
                imputed_data[i, field] <- overall_median
                imputation_type <- "overall_median"
              }
            }
          } else if (method == "median") {
            imputed_data[missing_idx, field] <- median(imputed_data[[field]], na.rm = TRUE)
            imputation_type <- "median"
          } else if (method == "mean") {
            imputed_data[missing_idx, field] <- mean(imputed_data[[field]], na.rm = TRUE)
            imputation_type <- "mean"
          }
          
          # Only log if we actually imputed values
          if (exists("imputation_type")) {
            imputation_log[[field]] <- list(
              method = imputation_type,
              n_imputed = sum(missing_idx),
              pct_imputed = round(sum(missing_idx) / nrow(data) * 100, 2)
            )
            
            cat(sprintf("   %-25s: %3d values imputed using %s\n",
                        field, sum(missing_idx), imputation_type))
          }
        }
      }
    }
  }
  
  # Verify no missing values remain
  remaining_missing <- colSums(is.na(imputed_data))
  if (any(remaining_missing > 0)) {
    cat("   ⚠️  Some missing values remain after imputation:\n")
    for (col in names(remaining_missing[remaining_missing > 0])) {
      cat(sprintf("     %-25s: %d missing values remain\n", 
                  col, remaining_missing[col]))
    }
  }
  
  attr(imputed_data, "imputation_log") <- imputation_log
  cat("✅ IMPUTATION COMPLETE\n")
  
  return(imputed_data)
}

# Module 3: Scoring Engine (UPDATED TO HANDLE MISSING COLUMNS)
calculate_component_scores <- function(data, config_path = NULL) {
  cat("\n📈 MODULE 3: DAII SCORING ENGINE\n")
  cat(paste(rep("-", 50), collapse = ""), "\n")
  
  # Load scoring configuration
  if (is.null(config_path)) {
    config_path <- file.path(dir_paths$config, "daii_scoring_config.yaml")
  }
  config <- yaml::yaml.load_file(config_path)$scoring
  
  scored_data <- data
  
  # Track which components we can calculate
  available_components <- c()
  missing_components <- c()
  
  # 1. R&D Intensity Score
  if ("rd_expense" %in% names(scored_data) && "total_assets" %in% names(scored_data)) {
    cat("   ✓ Calculating R&D Intensity Score...\n")
    rd_intensity <- scored_data$rd_expense / scored_data$total_assets
    
    if (config$transformations$rd_intensity == "log") {
      rd_intensity <- log1p(rd_intensity * 100)
    }
    
    # Handle cases where all values are the same
    if (length(unique(rd_intensity[!is.na(rd_intensity)])) > 1) {
      rd_intensity_score <- 100 * (rd_intensity - min(rd_intensity, na.rm = TRUE)) / 
        (max(rd_intensity, na.rm = TRUE) - min(rd_intensity, na.rm = TRUE))
    } else {
      rd_intensity_score <- rep(50, length(rd_intensity))
    }
    
    scored_data$rd_intensity_score <- rd_intensity_score
    available_components <- c(available_components, "rd_intensity_score")
  } else {
    missing_components <- c(missing_components, "R&D Intensity")
    cat("   ✗ Cannot calculate R&D Intensity Score\n")
  }
  
  # 2. Analyst Coverage Intensity Score
  if ("analyst_coverage" %in% names(scored_data)) {
    cat("   ✓ Calculating Analyst Coverage Score...\n")
    analyst_intensity <- scored_data$analyst_coverage
    
    if (config$transformations$analyst_intensity == "sqrt") {
      analyst_intensity <- sqrt(analyst_intensity)
    }
    
    if (length(unique(analyst_intensity[!is.na(analyst_intensity)])) > 1) {
      analyst_intensity_score <- 100 * (analyst_intensity - min(analyst_intensity, na.rm = TRUE)) / 
        (max(analyst_intensity, na.rm = TRUE) - min(analyst_intensity, na.rm = TRUE))
    } else {
      analyst_intensity_score <- rep(50, length(analyst_intensity))
    }
    
    scored_data$analyst_intensity_score <- analyst_intensity_score
    available_components <- c(available_components, "analyst_intensity_score")
  } else {
    missing_components <- c(missing_components, "Analyst Coverage")
    cat("   ✗ Cannot calculate Analyst Coverage Score\n")
  }
  
  # 3. Patent Intensity Score
  if ("patent_applications" %in% names(scored_data) && "total_assets" %in% names(scored_data)) {
    cat("   ✓ Calculating Patent Intensity Score...\n")
    patent_intensity <- scored_data$patent_applications / scored_data$total_assets * 1000000
    
    if (config$transformations$patent_intensity == "log") {
      patent_intensity <- log1p(patent_intensity)
    }
    
    if (length(unique(patent_intensity[!is.na(patent_intensity)])) > 1) {
      patent_intensity_score <- 100 * (patent_intensity - min(patent_intensity, na.rm = TRUE)) / 
        (max(patent_intensity, na.rm = TRUE) - min(patent_intensity, na.rm = TRUE))
    } else {
      patent_intensity_score <- rep(50, length(patent_intensity))
    }
    
    scored_data$patent_intensity_score <- patent_intensity_score
    available_components <- c(available_components, "patent_intensity_score")
  } else {
    missing_components <- c(missing_components, "Patent Intensity")
    cat("   ✗ Cannot calculate Patent Intensity Score\n")
  }
  
  # 4. News Intensity Score
  if ("news_volume" %in% names(scored_data)) {
    cat("   ✓ Calculating News Intensity Score...\n")
    news_intensity <- scored_data$news_volume
    
    if (config$transformations$news_intensity == "sqrt") {
      news_intensity <- sqrt(news_intensity)
    }
    
    if (length(unique(news_intensity[!is.na(news_intensity)])) > 1) {
      news_intensity_score <- 100 * (news_intensity - min(news_intensity, na.rm = TRUE)) / 
        (max(news_intensity, na.rm = TRUE) - min(news_intensity, na.rm = TRUE))
    } else {
      news_intensity_score <- rep(50, length(news_intensity))
    }
    
    scored_data$news_intensity_score <- news_intensity_score
    available_components <- c(available_components, "news_intensity_score")
  } else {
    missing_components <- c(missing_components, "News Intensity")
    cat("   ✗ Cannot calculate News Intensity Score\n")
  }
  
  # 5. Growth Intensity Score
  if ("revenue_growth" %in% names(scored_data)) {
    cat("   ✓ Calculating Growth Intensity Score...\n")
    growth_intensity <- scored_data$revenue_growth
    
    if (length(unique(growth_intensity[!is.na(growth_intensity)])) > 1) {
      growth_intensity_score <- 100 * (growth_intensity - min(growth_intensity, na.rm = TRUE)) / 
        (max(growth_intensity, na.rm = TRUE) - min(growth_intensity, na.rm = TRUE))
    } else {
      growth_intensity_score <- rep(50, length(growth_intensity))
    }
    
    scored_data$growth_intensity_score <- growth_intensity_score
    available_components <- c(available_components, "growth_intensity_score")
  } else {
    missing_components <- c(missing_components, "Growth Intensity")
    cat("   ✗ Cannot calculate Growth Intensity Score\n")
  }
  
  # Report on missing components
  if (length(missing_components) > 0) {
    cat("\n   ⚠️  MISSING COMPONENTS:", paste(missing_components, collapse = ", "), "\n")
    cat("   ⚠️  Some scores will not be calculated\n")
  }
  
  # Calculate weighted DAII 3.5 Score if we have at least one component
  if (length(available_components) > 0) {
    cat("\n   Calculating Weighted DAII 3.5 Score...\n")
    
    # Map component names to weights
    component_weights <- list(
      rd_intensity_score = config$weights$rd_intensity,
      analyst_intensity_score = config$weights$analyst_intensity,
      patent_intensity_score = config$weights$patent_intensity,
      news_intensity_score = config$weights$news_intensity,
      growth_intensity_score = config$weights$growth_intensity
    )
    
    # Get weights for available components
    available_weights <- unlist(component_weights[available_components])
    
    # Normalize weights to sum to 1 with available components
    if (sum(available_weights) > 0) {
      available_weights <- available_weights / sum(available_weights)
      
      # Calculate weighted score
      score_matrix <- as.matrix(scored_data[, available_components])
      scored_data$daii_35_score <- score_matrix %*% available_weights
      
      # Create quartile classification
      cat("   Creating Quartile Classification...\n")
      if (length(unique(scored_data$daii_35_score[!is.na(scored_data$daii_35_score)])) > 1) {
        scored_data$innovation_quartile <- cut(
          rank(scored_data$daii_35_score, ties.method = "first"),
          breaks = quantile(rank(scored_data$daii_35_score, ties.method = "first"), 
                            probs = c(0, 0.25, 0.50, 0.75, 1), na.rm = TRUE),
          labels = c("Q4 (Low)", "Q3", "Q2", "Q1 (High)"),
          include.lowest = TRUE
        )
      } else {
        scored_data$innovation_quartile <- rep("Q3", nrow(scored_data))
      }
    } else {
      cat("   ⚠️  Cannot calculate DAII 3.5 Score (no valid weights)\n")
    }
  } else {
    cat("   ⚠️  Cannot calculate DAII 3.5 Score (no component scores available)\n")
  }
  
  # Report summary
  if ("daii_35_score" %in% names(scored_data)) {
    cat(sprintf("\n   DAII 3.5 Score Range: %.2f - %.2f\n",
                min(scored_data$daii_35_score, na.rm = TRUE),
                max(scored_data$daii_35_score, na.rm = TRUE)))
    cat(sprintf("   Components calculated: %s\n", paste(available_components, collapse = ", ")))
  } else {
    cat("   ⚠️  No DAII 3.5 Score calculated\n")
  }
  
  cat("\n✅ SCORING COMPLETE\n")
  
  return(scored_data)
}

# ============================================================================
# MAIN PIPELINE EXECUTION FUNCTION (FIXED - NO INFINITE RECURSION)
# ============================================================================

run_daii_pipeline <- function(raw_data, save_outputs = TRUE, output_suffix = NULL) {
  cat("\n", paste(rep("=", 60), collapse = ""), "\n", sep = "")
  cat("🚀 DAII 3.5 PIPELINE EXECUTION\n")
  cat(paste(rep("=", 60), collapse = ""), "\n\n")
  
  start_time <- Sys.time()
  
  # Step 0: Field Mapping
  cat("STEP 0: FIELD MAPPING & DATA PREPARATION\n")
  mapped_data <- prepare_daii_data(raw_data)
  
  # Step 1: Data Validation
  validation_report <- validate_daii_data(mapped_data)
  
  # Step 2: Imputation
  imputed_data <- impute_daii_data(mapped_data)
  
  # Step 3: Scoring
  scored_data <- calculate_component_scores(imputed_data, config_path = NULL)
  
  # Step 4: Prepare for Module 4 (Column Name Bridge)
  cat("\n🔗 PREPARING FOR MODULE 4 INTEGRATION\n")
  cat(paste(rep("-", 50), collapse = ""), "\n")
  
  scores_for_aggregation <- scored_data
  
  # Load Module 4 bridge config
  module4_config_path <- file.path(dir_paths$config, "daii_module4_bridge_config.yaml")
  if (file.exists(module4_config_path)) {
    module4_config <- yaml::yaml.load_file(module4_config_path)
    column_mapping <- module4_config$column_mapping
    
    # Apply renaming for available columns
    rename_count <- 0
    for (old_name in names(column_mapping)) {
      if (old_name %in% names(scores_for_aggregation)) {
        new_name <- column_mapping[[old_name]]
        names(scores_for_aggregation)[names(scores_for_aggregation) == old_name] <- new_name
        cat(sprintf("   Renamed: %-25s -> %s\n", old_name, new_name))
        rename_count <- rename_count + 1
      }
    }
    
    if (rename_count == 0) {
      cat("   ⚠️  No columns renamed for Module 4\n")
    }
  } else {
    cat("   ⚠️  Module 4 bridge config not found at:", module4_config_path, "\n")
  }
  
  # Calculate execution time
  end_time <- Sys.time()
  execution_time <- difftime(end_time, start_time, units = "secs")
  
  # Final summary
  cat("\n", paste(rep("=", 60), collapse = ""), "\n", sep = "")
  cat("📊 PIPELINE EXECUTION SUMMARY\n")
  cat(paste(rep("=", 60), collapse = ""), "\n")
  cat(sprintf("   Execution Time: %.2f seconds\n", as.numeric(execution_time)))
  cat(sprintf("   Companies Processed: %d\n", nrow(scored_data)))
  
  if ("daii_35_score" %in% names(scored_data)) {
    cat(sprintf("   DAII Score Range: %.2f - %.2f\n",
                min(scored_data$daii_35_score, na.rm = TRUE),
                max(scored_data$daii_35_score, na.rm = TRUE)))
  }
  
  if ("innovation_quartile" %in% names(scored_data)) {
    quartile_dist <- table(scored_data$innovation_quartile)
    cat("   Innovation Quartile Distribution:\n")
    for (q in names(quartile_dist)) {
      cat(sprintf("     %-12s: %d companies\n", q, quartile_dist[q]))
    }
  }
  
  # Save outputs if requested
  if (save_outputs) {
    save_pipeline_outputs(mapped_data, validation_report, imputed_data, 
                          scored_data, scores_for_aggregation, execution_time, output_suffix)
  }
  
  # Return results
  results <- list(
    mapped_data = mapped_data,
    validation_report = validation_report,
    imputed_data = imputed_data,
    scored_data = scored_data,
    scores_for_aggregation = scores_for_aggregation,
    execution_time = execution_time,
    timestamp = Sys.time()
  )
  
  return(results)
}

# ============================================================================
# OUTPUT MANAGEMENT FUNCTIONS
# ============================================================================

save_pipeline_outputs <- function(mapped_data, validation_report, imputed_data, 
                                  scored_data, scores_for_aggregation, execution_time, suffix = NULL) {
  
  # Create timestamp for output files
  timestamp <- format(Sys.time(), "%Y%m%d_%H%M%S")
  if (!is.null(suffix)) {
    output_dir_name <- paste0("pipeline_output_", timestamp, "_", suffix)
  } else {
    output_dir_name <- paste0("pipeline_output_", timestamp)
  }
  
  output_dir <- file.path(dir_paths$data_output, output_dir_name)
  dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)
  
  cat("\n💾 SAVING OUTPUT FILES TO:", output_dir, "\n")
  cat(paste(rep("-", 60), collapse = ""), "\n")
  
  # 1. Save mapped data
  write.csv(mapped_data, 
            file = file.path(output_dir, "01_mapped_data.csv"),
            row.names = FALSE)
  cat("   ✅ 01_mapped_data.csv\n")
  
  # 2. Save scored data
  write.csv(scored_data,
            file = file.path(output_dir, "02_scored_data.csv"),
            row.names = FALSE)
  cat("   ✅ 02_scored_data.csv\n")
  
  # 3. Save Module 4 ready data
  write.csv(scores_for_aggregation,
            file = file.path(output_dir, "03_ready_for_module4.csv"),
            row.names = FALSE)
  cat("   ✅ 03_ready_for_module4.csv\n")
  
  # 4. Save validation report
  sink(file.path(output_dir, "04_validation_report.txt"))
  print(validation_report)
  sink()
  cat("   ✅ 04_validation_report.txt\n")
  
  # 5. Save execution summary
  summary_text <- c(
    paste("DAII 3.5 Pipeline Execution Summary"),
    paste("Timestamp:", Sys.time()),
    paste("Execution Time:", round(as.numeric(execution_time), 2), "seconds"),
    paste("Companies Processed:", nrow(scored_data)),
    paste("Columns in Output:", ncol(scored_data))
  )
  
  if ("daii_35_score" %in% names(scored_data)) {
    summary_text <- c(summary_text,
                      paste("DAII Score Range:", 
                            round(min(scored_data$daii_35_score, na.rm = TRUE), 2), "-",
                            round(max(scored_data$daii_35_score, na.rm = TRUE), 2)))
  }
  
  if ("innovation_quartile" %in% names(scored_data)) {
    quartile_table <- table(scored_data$innovation_quartile)
    summary_text <- c(summary_text, "", "Innovation Quartile Distribution:")
    for (q in names(quartile_table)) {
      summary_text <- c(summary_text, paste("  ", q, ":", quartile_table[q], "companies"))
    }
  }
  
  writeLines(summary_text, file.path(output_dir, "05_execution_summary.txt"))
  cat("   ✅ 05_execution_summary.txt\n")
  
  # 6. Save field mapping log
  if (!is.null(attr(mapped_data, "daii_field_mapping"))) {
    mapping <- attr(mapped_data, "daii_field_mapping")
    mapping_df <- data.frame(
      field = names(mapping),
      source = unlist(mapping),
      stringsAsFactors = FALSE
    )
    write.csv(mapping_df, 
              file = file.path(output_dir, "06_field_mapping.csv"),
              row.names = FALSE)
    cat("   ✅ 06_field_mapping.csv\n")
  }
  
  # 7. Save imputation log
  if (!is.null(attr(imputed_data, "imputation_log"))) {
    imp_log <- attr(imputed_data, "imputation_log")
    if (length(imp_log) > 0) {
      imp_df <- do.call(rbind, lapply(names(imp_log), function(field) {
        data.frame(
          field = field,
          method = imp_log[[field]]$method,
          n_imputed = imp_log[[field]]$n_imputed,
          pct_imputed = imp_log[[field]]$pct_imputed,
          stringsAsFactors = FALSE
        )
      }))
      write.csv(imp_df, 
                file = file.path(output_dir, "07_imputation_log.csv"),
                row.names = FALSE)
      cat("   ✅ 07_imputation_log.csv\n")
    }
  }
  
  # 8. Save log file
  log_file <- file.path(dir_paths$logs, paste0("pipeline_log_", timestamp, ".txt"))
  sink(log_file)
  cat("DAII 3.5 Pipeline Execution Log\n")
  cat("===============================\n")
  cat("Timestamp:", Sys.time(), "\n")
  cat("Output Directory:", output_dir, "\n")
  cat("Execution Time:", round(as.numeric(execution_time), 2), "seconds\n")
  cat("Files Created:", paste(list.files(output_dir), collapse = ", "), "\n")
  sink()
  
  cat("\n📁 Total output files created:", length(list.files(output_dir)), "\n")
  cat("📍 Output location:", output_dir, "\n")
}

# ============================================================================
# TESTING FUNCTIONS (UPDATED WITH DIAGNOSTIC)
# ============================================================================

test_n50_dataset <- function() {
  cat("\n🧪 TEST 1: N=50 HYBRID DATASET\n")
  cat(paste(rep("=", 60), collapse = ""), "\n")
  
  # Download N=50 dataset
  n50_url <- "https://raw.githubusercontent.com/sivaguru42/DAII-3.5-Framework/main/DAII_3_5_N50_Test_Dataset.csv"
  n50_data <- read.csv(n50_url, stringsAsFactors = FALSE)
  
  # Save to raw data directory
  raw_data_path <- file.path(dir_paths$data_raw, "DAII_3_5_N50_Test_Dataset.csv")
  write.csv(n50_data, raw_data_path, row.names = FALSE)
  cat("   Dataset saved to:", raw_data_path, "\n")
  
  cat("   Dataset loaded:", nrow(n50_data), "rows ×", ncol(n50_data), "columns\n")
  
  # Run diagnostic first
  n50_data <- check_data_structure(n50_data)
  
  # Show input data snippet
  cat("\n   INPUT DATA SNIPPET (First 3 rows):\n")
  print(head(n50_data, 3))
  
  # Run pipeline
  cat("\n   RUNNING PIPELINE...\n")
  results <- run_daii_pipeline(n50_data, save_outputs = TRUE, output_suffix = "n50_test")
  
  # Show output data snippet
  if (!is.null(results$scored_data)) {
    cat("\n   OUTPUT DATA SNIPPET (First 3 rows with scores):\n")
    
    # Find which score columns were created
    score_cols <- grep("score|quartile", names(results$scored_data), value = TRUE, ignore.case = TRUE)
    
    if (length(score_cols) > 0) {
      display_cols <- c("ticker", "company_name")
      display_cols <- c(display_cols, score_cols[1:min(4, length(score_cols))])
      display_cols <- display_cols[display_cols %in% names(results$scored_data)]
      
      if (length(display_cols) > 0) {
        print(head(results$scored_data[, display_cols], 3))
      }
    } else {
      cat("   No score columns found in output. Showing first few columns:\n")
      print(head(results$scored_data[, 1:min(6, ncol(results$scored_data))], 3))
    }
  }
  
  # Show summary
  if (!is.null(results$scored_data)) {
    cat("\n   OUTPUT SUMMARY:\n")
    score_cols <- grep("score|quartile", names(results$scored_data), value = TRUE, ignore.case = TRUE)
    
    if (length(score_cols) > 0) {
      cat("   Score columns created:", paste(score_cols, collapse = ", "), "\n")
      
      if ("daii_35_score" %in% score_cols) {
        cat(sprintf("   DAII Score range: %.2f - %.2f\n",
                    min(results$scored_data$daii_35_score, na.rm = TRUE),
                    max(results$scored_data$daii_35_score, na.rm = TRUE)))
      }
    }
  }
  
  return(results)
}

# ============================================================================
# EXECUTE THE PIPELINE AUTOMATICALLY (FIXED STRING CONCATENATION)
# ============================================================================

cat("\n")
cat(paste(rep("=", 70), collapse = ""), "\n")
cat("DAII 3.5 PIPELINE - STARTING EXECUTION\n")
cat(paste(rep("=", 70), collapse = ""), "\n\n")

# Run the N=50 test automatically
results <- test_n50_dataset()

# Show completion message
cat("\n")
cat(paste(rep("=", 70), collapse = ""), "\n")
cat("✅ PIPELINE EXECUTION COMPLETE\n")
cat(paste(rep("=", 70), collapse = ""), "\n")

# Show where to find results
cat("\n📊 RESULTS SUMMARY:\n")
cat("   Output variable: 'results' (contains all pipeline outputs)\n")
cat("   Files saved to:", dir_paths$data_output, "\n")
cat("   Logs saved to:", dir_paths$logs, "\n\n")

# List the latest output directory
output_dirs <- list.dirs(dir_paths$data_output, recursive = FALSE, full.names = TRUE)
if (length(output_dirs) > 0) {
  latest_dir <- sort(output_dirs, decreasing = TRUE)[1]
  cat("📁 Latest output directory:", basename(latest_dir), "\n")
  cat("📄 Files created:", paste(list.files(latest_dir), collapse = ", "), "\n")
}