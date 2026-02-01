# ============================================================================
# DAII 3.5 PRODUCTION PIPELINE WITH FIELD MAPPING SYSTEM
# ============================================================================
# 
# ENTRY POINT: prepare_daii_data() - Handles ANY data source format
# OUTPUT: Complete innovation scoring & portfolio analysis
# 
# ARCHITECTURE:
# 1. Field Mapping → 2. Validation → 3. Imputation → 4. Scoring → 5. Aggregation → 6. Portfolio Integration
# ============================================================================

# Load required packages
cat("📦 LOADING DAII 3.5 PRODUCTION PACKAGES\n")
required_packages <- c("dplyr", "tidyr", "ggplot2", "yaml")
for(pkg in required_packages) {
  if(!require(pkg, character.only = TRUE)) {
    install.packages(pkg, dependencies = TRUE)
    library(pkg, character.only = TRUE)
  }
  cat(sprintf("✅ %s\n", pkg))
}

# ============================================================================
# 0. FIELD MAPPING SYSTEM (NEW ENTRY POINT)
# ============================================================================

load_daii_config <- function(config_path = NULL) {
  #' Load DAII field mapping configuration
  if (!is.null(config_path) && file.exists(config_path)) {
    config <- yaml::read_yaml(config_path)
    cat("   Loaded configuration from:", config_path, "\n")
  } else {
    # DEFAULT CONFIG - Matches your N=50 dataset
    config <- list(
      daii_fields = list(
        ticker = list(expected = "Ticker", 
                      alternatives = c("Symbol", "Securities", "ID_BB_GLOBAL")),
        rd_expense = list(expected = "R.D.Exp", 
                          alternatives = c("RD_Expense", "Research_Development")),
        market_cap = list(expected = "Mkt.Cap", 
                          alternatives = c("MarketCap", "CUR_MKT_CAP")),
        analyst_rating = list(expected = "BEst.Analyst.Rtg", 
                              alternatives = c("ANR", "BEST_ANALYST_RATING")),
        patent_count = list(expected = "Patents...Trademarks...Copy.Rgt", 
                            alternatives = c("Patent_Count", "NO_PATENTS")),
        news_sentiment = list(expected = "News.Sent", 
                              alternatives = c("NEWS_SENTIMENT", "News_Sentiment_Score")),
        revenue_growth = list(expected = "Rev...1.Yr.Gr", 
                              alternatives = c("Sales_Growth", "RTG_SALES_GROWTH")),
        industry = list(expected = "GICS.Ind.Grp.Name", 
                        alternatives = c("Industry", "GICS_INDUSTRY_GROUP_NAME")),
        fund_name = list(expected = "fund_name", 
                         alternatives = c("Fund_Name", "FUND_NAME", "Manager")),
        fund_weight = list(expected = "fund_weight", 
                           alternatives = c("Weight", "FUND_WEIGHT", "Allocation"))
      )
    )
    cat("   Using default configuration\n")
  }
  return(config)
}

prepare_daii_data <- function(raw_data, config_path = NULL, user_col_map = list()) {
  #' MAIN ENTRY POINT: Map raw data to standardized DAII format
  cat("🧹 DAII FIELD MAPPING & STANDARDIZATION\n")
  cat(paste(rep("=", 70), collapse = ""), "\n")
  
  # Load configuration
  config <- load_daii_config(config_path)
  required_fields <- names(config$daii_fields)
  
  # Initialize tracking
  final_map <- list()      # source -> standard mapping
  source_log <- list()     # How each field was found
  missing_fields <- c()
  
  cat("\n🔍 MAPPING LOGICAL FIELDS TO SOURCE COLUMNS:\n")
  cat("   [Priority: User Override → Expected → Alternatives → Auto-match]\n\n")
  
  # Process each field with priority hierarchy
  for (field in required_fields) {
    config_entry <- config$daii_fields[[field]]
    found_col <- NA
    method <- "not found"
    
    # PRIORITY 1: User-Specified Override
    if (!is.null(user_col_map[[field]])) {
      user_spec <- user_col_map[[field]]
      if (user_spec %in% names(raw_data)) {
        found_col <- user_spec
        method <- paste("USER override ('", user_spec, "')", sep = "")
      } else {
        warning(sprintf("User override for '%s' ('%s') not found in data", 
                        field, user_spec))
      }
    }
    
    # PRIORITY 2: Expected column name
    if (is.na(found_col) && config_entry$expected %in% names(raw_data)) {
      found_col <- config_entry$expected
      method <- paste("expected name ('", found_col, "')", sep = "")
    }
    
    # PRIORITY 3: Alternative names
    if (is.na(found_col)) {
      for (alt in config_entry$alternatives) {
        if (alt %in% names(raw_data)) {
          found_col <- alt
          method <- paste("alternative ('", alt, "')", sep = "")
          break
        }
      }
    }
    
    # PRIORITY 4: Case-insensitive partial match (last resort)
    if (is.na(found_col)) {
      # Convert field name to search pattern (rd_expense -> "rd" or "expense")
      patterns <- c(field, strsplit(field, "_")[[1]])
      for (pattern in patterns) {
        matches <- grep(pattern, names(raw_data), ignore.case = TRUE, value = TRUE)
        if (length(matches) == 1) {  # Only use if unambiguous
          found_col <- matches[1]
          method <- paste("auto-match ('", found_col, "')", sep = "")
          break
        }
      }
    }
    
    # Record results
    if (!is.na(found_col)) {
      final_map[[found_col]] <- field
      source_log[[field]] <- method
      cat(sprintf("   ✓ %-25s → %-40s\n", field, method))
    } else {
      missing_fields <- c(missing_fields, field)
      cat(sprintf("   ✗ %-25s → NOT FOUND\n", field))
    }
  }
  
  # Create standardized dataframe
  cat("\n📦 CREATING STANDARDIZED DATAFRAME\n")
  
  if (length(final_map) == 0) {
    stop("CRITICAL ERROR: No DAII fields could be mapped from the input data.")
  }
  
  # Select and rename columns
  cols_to_keep <- names(final_map)
  clean_data <- raw_data[, cols_to_keep, drop = FALSE]
  names(clean_data) <- unlist(final_map[cols_to_keep])
  
  # Add original ticker column as attribute for merging later
  if ("ticker" %in% names(clean_data)) {
    attr(clean_data, "original_ticker_col") <- cols_to_keep[which(names(final_map) == "ticker")]
  }
  
  # Report summary
  cat(sprintf("   Selected %d of %d required fields.\n", 
              length(cols_to_keep), length(required_fields)))
  
  if (length(missing_fields) > 0) {
    cat("\n⚠️  MISSING FIELDS (may cause errors in later modules):\n")
    for (mf in missing_fields) cat(sprintf("   - %s\n", mf))
  }
  
  # Store mapping metadata
  attr(clean_data, "field_mapping") <- source_log
  attr(clean_data, "missing_fields") <- missing_fields
  attr(clean_data, "original_cols") <- cols_to_keep
  
  cat("\n✅ DATA STANDARDIZATION COMPLETE\n")
  return(clean_data)
}

# ============================================================================
# 1. MODIFIED MODULE 1: DATA PIPELINE (For Standardized Data)
# ============================================================================

initialize_daii_environment <- function(working_dir = NULL) {
  # [Same as before, unchanged]
}

validate_standardized_data <- function(std_data, min_companies = 1) {
  #' VALIDATE STANDARDIZED DATA (Replaces load_and_validate_data)
  cat("\n📥 VALIDATING STANDARDIZED DAII DATA\n")
  cat(paste(rep("-", 50), collapse = ""), "\n")
  
  # Known standardized column names
  daii_vars <- c("rd_expense", "market_cap", "analyst_rating", 
                 "patent_count", "news_sentiment", "revenue_growth")
  
  # Basic validation
  validation_report <- list(
    structure = list(
      total_rows = nrow(std_data),
      total_cols = ncol(std_data),
      unique_companies = ifelse("ticker" %in% names(std_data),
                                length(unique(std_data$ticker)),
                                NA)
    ),
    missing_data = list()
  )
  
  cat(sprintf("📊 Dataset: %d rows, %d columns\n", 
              nrow(std_data), ncol(std_data)))
  if ("ticker" %in% names(std_data)) {
    cat(sprintf("🏢 Unique companies: %d\n", validation_report$structure$unique_companies))
  }
  
  # Analyze missing data for DAII components
  missing_summary <- data.frame(
    Variable = daii_vars,
    Description = c("R&D Expenses", "Market Cap", "Analyst Rating", 
                    "Patents/Trademarks", "News Sentiment", "Revenue Growth"),
    Missing_Count = sapply(daii_vars, function(x) {
      if (x %in% names(std_data)) sum(is.na(std_data[[x]])) else NA
    }),
    Missing_Percent = sapply(daii_vars, function(x) {
      if (x %in% names(std_data)) round(100*sum(is.na(std_data[[x]]))/nrow(std_data), 1) else NA
    })
  )
  
  validation_report$missing_data$summary <- missing_summary
  
  cat("\n📋 MISSING DATA ANALYSIS (DAII Components):\n")
  print(missing_summary)
  
  return(list(
    data = std_data,
    validation_report = validation_report
  ))
}

extract_standardized_companies <- function(validated_data) {
  #' EXTRACT UNIQUE COMPANIES FROM STANDARDIZED DATA
  cat("\n🏢 EXTRACTING UNIQUE COMPANIES FOR SCORING\n")
  cat(paste(rep("-", 50), collapse = ""), "\n")
  
  if (!"ticker" %in% names(validated_data$data)) {
    stop("ERROR: No 'ticker' column found in standardized data")
  }
  
  # Identify fund-specific columns to exclude
  exclude_patterns <- c("fund", "weight", "allocation", "shares", "position", "date")
  exclude_cols <- grep(paste(exclude_patterns, collapse = "|"), 
                       names(validated_data$data), ignore.case = TRUE, value = TRUE)
  
  # Keep company-level columns only
  company_cols <- setdiff(names(validated_data$data), exclude_cols)
  
  # Extract unique companies
  unique_companies <- validated_data$data[!duplicated(validated_data$data$ticker), company_cols]
  rownames(unique_companies) <- NULL
  
  cat(sprintf("✅ Extracted %d unique companies from %d holdings\n", 
              nrow(unique_companies), nrow(validated_data$data)))
  
  # Check data completeness
  daii_vars <- c("rd_expense", "market_cap", "analyst_rating", 
                 "patent_count", "news_sentiment", "revenue_growth")
  
  component_check <- list()
  for (col in intersect(daii_vars, names(unique_companies))) {
    non_missing <- sum(!is.na(unique_companies[[col]]))
    component_check[[col]] <- round(100 * non_missing / nrow(unique_companies), 1)
  }
  
  cat("\n📊 COMPANY-LEVEL DATA COMPLETENESS:\n")
  for (col in names(component_check)) {
    cat(sprintf(" • %-25s: %6.1f%% complete\n", col, component_check[[col]]))
  }
  
  return(list(
    companies = unique_companies,
    company_cols = company_cols,
    component_completeness = component_check
  ))
}

# ============================================================================
# 2. MODULE 2: IMPUTATION ENGINE (Modified for Standardized Names)
# ============================================================================

impute_missing_data_std <- function(company_data, industry_col = "industry") {
  #' IMPUTATION FOR STANDARDIZED DATA
  cat("\n🔧 IMPUTING MISSING DATA (Standardized)\n")
  cat(paste(rep("-", 50), collapse = ""), "\n")
  
  # Create a copy
  imputed_data <- company_data
  imputation_flags <- rep("", nrow(imputed_data))
  
  # Define DAII component variables (standardized names)
  daii_components <- c("rd_expense", "market_cap", "analyst_rating",
                       "patent_count", "news_sentiment", "revenue_growth")
  
  cat("📊 PRE-IMPUTATION MISSINGNESS:\n")
  for (var in daii_components) {
    if (var %in% names(imputed_data)) {
      missing_pct <- round(100 * sum(is.na(imputed_data[[var]])) / nrow(imputed_data), 1)
      cat(sprintf(" • %-20s: %6.1f%% missing\n", var, missing_pct))
    }
  }
  
  # Industry-specific imputation
  if (industry_col %in% names(imputed_data)) {
    cat("\n🏭 APPLYING INDUSTRY-SPECIFIC IMPUTATION:\n")
    
    if ("rd_expense" %in% names(imputed_data)) {
      industry_rd <- imputed_data %>%
        group_by_at(industry_col) %>%
        summarise(industry_median = median(rd_expense, na.rm = TRUE))
      
      for (i in 1:nrow(imputed_data)) {
        if (is.na(imputed_data$rd_expense[i]) && !is.na(imputed_data[[industry_col]][i])) {
          industry <- imputed_data[[industry_col]][i]
          industry_val <- industry_rd$industry_median[industry_rd[[industry_col]] == industry]
          if (length(industry_val) > 0 && !is.na(industry_val)) {
            imputed_data$rd_expense[i] <- industry_val
            imputation_flags[i] <- paste0(imputation_flags[i], "R&D_industry;")
          }
        }
      }
    }
  }
  
  # Median imputation for remaining missing values
  cat("\n📈 APPLYING MEDIAN IMPUTATION:\n")
  
  for (var in daii_components) {
    if (var %in% names(imputed_data)) {
      missing_before <- sum(is.na(imputed_data[[var]]))
      if (missing_before > 0) {
        median_val <- median(imputed_data[[var]], na.rm = TRUE)
        imputed_data[[var]][is.na(imputed_data[[var]])] <- median_val
        
        # Update flags for originally missing values
        for (i in which(is.na(company_data[[var]]))) {
          imputation_flags[i] <- paste0(imputation_flags[i], var, "_median;")
        }
        
        missing_after <- sum(is.na(imputed_data[[var]]))
        cat(sprintf(" • %-20s: %d → %d missing (median: %.2f)\n", 
                    var, missing_before, missing_after, median_val))
      }
    }
  }
  
  # Add imputation flags
  imputed_data$imputation_flags <- imputation_flags
  
  return(list(
    imputed_data = imputed_data,
    imputation_flags = imputation_flags
  ))
}

# ============================================================================
# 3. MODULE 3: SCORING ENGINE (Modified for Standardized Names)
# ============================================================================

calculate_component_scores_std <- function(imputed_data) {
  #' SCORING ENGINE FOR STANDARDIZED DATA
  cat("\n📊 CALCULATING DAII 3.5 COMPONENT SCORES\n")
  cat(paste(rep("=", 60), collapse = ""), "\n")
  
  scores_data <- imputed_data
  
  # COMPONENT 1: R&D INTENSITY (30%)
  cat("\n1. 🔬 R&D INTENSITY (30% weight)\n")
  if (all(c("rd_expense", "market_cap") %in% names(scores_data))) {
    scores_data$rd_intensity_raw <- scores_data$rd_expense / scores_data$market_cap
    scores_data$rd_intensity_raw[scores_data$rd_intensity_raw <= 0] <- NA
    scores_data$rd_intensity_log <- log(scores_data$rd_intensity_raw + 1e-10)
    
    rd_min <- min(scores_data$rd_intensity_log, na.rm = TRUE)
    rd_max <- max(scores_data$rd_intensity_log, na.rm = TRUE)
    scores_data$rd_intensity_score <- 100 * (scores_data$rd_intensity_log - rd_min) / (rd_max - rd_min)
    
    cat(sprintf("   • R&D/Mkt Cap range: %.2e to %.2e\n", 
                min(scores_data$rd_intensity_raw, na.rm = TRUE),
                max(scores_data$rd_intensity_raw, na.rm = TRUE)))
  } else {
    warning("Missing R&D or Market Cap data")
    scores_data$rd_intensity_score <- NA
  }
  
  # COMPONENT 2: ANALYST SENTIMENT (20%)
  cat("\n2. 💬 ANALYST SENTIMENT (20% weight)\n")
  if ("analyst_rating" %in% names(scores_data)) {
    analyst_min <- min(scores_data$analyst_rating, na.rm = TRUE)
    analyst_max <- max(scores_data$analyst_rating, na.rm = TRUE)
    scores_data$analyst_sentiment_score <- 100 * (scores_data$analyst_rating - analyst_min) / (analyst_max - analyst_min)
    cat(sprintf("   • Analyst rating range: %.2f to %.2f\n", analyst_min, analyst_max))
  } else {
    scores_data$analyst_sentiment_score <- NA
  }
  
  # COMPONENT 3: PATENT ACTIVITY (25%)
  cat("\n3. 📜 PATENT ACTIVITY (25% weight)\n")
  if ("patent_count" %in% names(scores_data)) {
    scores_data$patent_log <- log(scores_data$patent_count + 1)
    patent_min <- min(scores_data$patent_log, na.rm = TRUE)
    patent_max <- max(scores_data$patent_log, na.rm = TRUE)
    scores_data$patent_activity_score <- 100 * (scores_data$patent_log - patent_min) / (patent_max - patent_min)
    cat(sprintf("   • Patent count range: %.0f to %.0f\n", 
                min(scores_data$patent_count, na.rm = TRUE),
                max(scores_data$patent_count, na.rm = TRUE)))
  } else {
    scores_data$patent_activity_score <- NA
  }
  
  # COMPONENT 4: NEWS SENTIMENT (10%)
  cat("\n4. 📰 NEWS SENTIMENT (10% weight)\n")
  if ("news_sentiment" %in% names(scores_data)) {
    news_min <- min(scores_data$news_sentiment, na.rm = TRUE)
    news_max <- max(scores_data$news_sentiment, na.rm = TRUE)
    scores_data$news_sentiment_score <- 100 * (scores_data$news_sentiment - news_min) / (news_max - news_min)
  } else {
    scores_data$news_sentiment_score <- NA
  }
  
  # COMPONENT 5: GROWTH MOMENTUM (15%)
  cat("\n5. 📈 GROWTH MOMENTUM (15% weight)\n")
  if ("revenue_growth" %in% names(scores_data)) {
    growth_min <- min(scores_data$revenue_growth, na.rm = TRUE)
    growth_max <- max(scores_data$revenue_growth, na.rm = TRUE)
    scores_data$growth_momentum_score <- 100 * (scores_data$revenue_growth - growth_min) / (growth_max - growth_min)
  } else {
    scores_data$growth_momentum_score <- NA
  }
  
  # CALCULATE OVERALL DAII 3.5 SCORE
  cat("\n🧮 CALCULATING OVERALL DAII 3.5 SCORE\n")
  cat(paste(rep("-", 40), collapse = ""), "\n")
  
  weights <- c(rd_intensity = 0.30, analyst_sentiment = 0.20, 
               patent_activity = 0.25, news_sentiment = 0.10, 
               growth_momentum = 0.15)
  
  scores_data$daii_35_score <- 
    scores_data$rd_intensity_score * weights["rd_intensity"] +
    scores_data$analyst_sentiment_score * weights["analyst_sentiment"] +
    scores_data$patent_activity_score * weights["patent_activity"] +
    scores_data$news_sentiment_score * weights["news_sentiment"] +
    scores_data$growth_momentum_score * weights["growth_momentum"]
  
  # Create quartiles (robust version)
  non_missing <- scores_data$daii_35_score[!is.na(scores_data$daii_35_score)]
  if (length(non_missing) >= 4) {
    # Use rank-based approach to handle duplicate values
    ranks <- rank(non_missing, ties.method = "first")
    quartile_breaks <- quantile(ranks, probs = c(0, 0.25, 0.5, 0.75, 1))
    
    quartiles <- cut(ranks, breaks = quartile_breaks,
                     labels = c("Q4 (Low)", "Q3", "Q2", "Q1 (High)"),
                     include.lowest = TRUE)
    
    scores_data$daii_quartile <- NA
    scores_data$daii_quartile[!is.na(scores_data$daii_35_score)] <- quartiles
  } else {
    scores_data$daii_quartile <- NA
  }
  
  cat(sprintf("   • DAII 3.5 Score range: %.1f to %.1f\n", 
              min(scores_data$daii_35_score, na.rm = TRUE),
              max(scores_data$daii_35_score, na.rm = TRUE)))
  
  cat("\n📊 QUARTILE DISTRIBUTION:\n")
  print(table(scores_data$daii_quartile))
  
  return(list(
    scores_data = scores_data,
    weights = weights
  ))
}

# ============================================================================
# 4. MODULE 4 & 5 ADAPTERS
# ============================================================================

adapt_for_module4 <- function(scores_data_std) {
  #' ADAPTER: Convert standardized scores to Module 4 expected format
  # Module 4 expects: R_D_Score, Analyst_Score, Patent_Score, News_Score, Growth_Score
  
  adapted_data <- scores_data_std
  
  # Rename columns if they exist
  col_mapping <- c(
    "rd_intensity_score" = "R_D_Score",
    "analyst_sentiment_score" = "Analyst_Score", 
    "patent_activity_score" = "Patent_Score",
    "news_sentiment_score" = "News_Score",
    "growth_momentum_score" = "Growth_Score"
  )
  
  for (old_name in names(col_mapping)) {
    if (old_name %in% names(adapted_data)) {
      new_name <- col_mapping[old_name]
      names(adapted_data)[names(adapted_data) == old_name] <- new_name
    }
  }
  
  return(adapted_data)
}

adapt_for_module5 <- function(raw_holdings, daii_scores) {
  #' ADAPTER: Prepare data for Module 5 portfolio integration
  # Need to merge with original holdings using original ticker column name
  
  # Get original ticker column name from attributes
  original_ticker <- attr(raw_holdings, "original_ticker_col")
  if (is.null(original_ticker)) {
    original_ticker <- names(raw_holdings)[1]  # Fallback
  }
  
  # Ensure daii_scores has a 'ticker' column
  if (!"ticker" %in% names(daii_scores)) {
    stop("DAII scores must have a 'ticker' column for merging")
  }
  
  # Rename daii_scores ticker to match original holdings
  daii_for_merge <- daii_scores
  names(daii_for_merge)[names(daii_for_merge) == "ticker"] <- original_ticker
  
  return(list(
    holdings = raw_holdings,
    scores = daii_for_merge,
    ticker_col = original_ticker
  ))
}

# ============================================================================
# MAIN EXECUTION FUNCTION
# ============================================================================

run_daii_pipeline <- function(raw_data_path, 
                              user_col_map = list(),
                              config_path = NULL,
                              weight_col = "fund_weight",
                              fund_col = "fund_name") {
  #' COMPLETE DAII 3.5 PIPELINE EXECUTION
  
  cat(paste(rep("=", 80), collapse = ""), "\n")
  cat("🚀 DAII 3.5 PRODUCTION PIPELINE EXECUTION\n")
  cat(paste(rep("=", 80), collapse = ""), "\n")
  
  # Step 0: Load raw data
  cat("\n0️⃣ LOADING RAW DATA\n")
  raw_data <- read.csv(raw_data_path, stringsAsFactors = FALSE, check.names = FALSE)
  cat(sprintf("   Loaded: %d rows, %d columns\n", nrow(raw_data), ncol(raw_data)))
  
  # Step 1: Field Mapping & Standardization
  std_data <- prepare_daii_data(
    raw_data = raw_data,
    config_path = config_path,
    user_col_map = user_col_map
  )
  
  # Step 2: Validation
  validation <- validate_standardized_data(std_data)
  
  # Step 3: Extract unique companies
  companies <- extract_standardized_companies(validation)
  
  # Step 4: Imputation
  imputed <- impute_missing_data_std(companies$companies, industry_col = "industry")
  
  # Step 5: Scoring
  scores <- calculate_component_scores_std(imputed$imputed_data)
  
  # Step 6: Prepare for Module 4 (Aggregation)
  scores_for_module4 <- adapt_for_module4(scores$scores_data)
  
  # Step 7: Run Module 4 (using original function from GitHub)
  # Note: Need to load the actual Module 4 function
  source("DAII_aggregation_framework_module.r")  # Load from GitHub
  aggregated <- calculate_daii_scores(
    scores_data = scores_for_module4,
    ticker_col = "ticker",
    industry_col = "industry"
  )
  
  # Step 8: Prepare for Module 5
  module5_data <- adapt_for_module5(raw_data, aggregated$daii_data)
  
  # Step 9: Run Module 5 (using original function from GitHub)
  source("DAII_portfolio_integration_module.r")  # Load from GitHub
  portfolio <- integrate_with_portfolio(
    holdings_data = module5_data$holdings,
    daii_scores = module5_data$scores,
    ticker_col = module5_data$ticker_col,
    weight_col = weight_col,
    fund_col = fund_col
  )
  
  # FINAL RESULTS
  cat(paste(rep("=", 80), collapse = ""), "\n")
  cat("✅ DAII 3.5 PIPELINE COMPLETE\n")
  cat(paste(rep("=", 80), collapse = ""), "\n")
  
  return(list(
    standardized_data = std_data,
    validation = validation,
    companies = companies,
    imputed = imputed,
    scores = scores,
    aggregated = aggregated,
    portfolio = portfolio,
    field_mapping = attr(std_data, "field_mapping")
  ))
}

# ============================================================================
# EXAMPLE USAGE
# ============================================================================

# Example 1: Using your N=50 dataset (no overrides needed)
# results <- run_daii_pipeline("DAII_3_5_N50_Complete_Dataset.csv")

# Example 2: With Bloomberg data and user overrides
# user_overrides <- list(
#   rd_expense = "Bloomberg_RD_Field",
#   market_cap = "CUR_MKT_CAP",
#   analyst_rating = "BEST_ANALYST_RATING"
# )
# 
# results <- run_daii_pipeline(
#   raw_data_path = "bloomberg_extract_2025.csv",
#   user_col_map = user_overrides,
#   config_path = "config/daii_fields.yaml"
# )

# Example 3: Quick test with minimal configuration
# quick_test <- function() {
#   # Load your N=50 data
#   data <- read.csv("DAII_3_5_N50_Complete_Dataset.csv")
#   
#   # Just test the field mapping
#   std <- prepare_daii_data(data)
#   print(attr(std, "field_mapping"))
#   
#   # Test validation
#   val <- validate_standardized_data(std)
#   print(val$validation_report$missing_data$summary)
# }