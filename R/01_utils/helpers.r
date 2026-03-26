# ============================================================================
# UTILITY: General Helper Functions
# Version: 2.0 | Date: 2026-03-17
# Description: Common utility functions for data cleaning, validation, and
#              pipeline pre-flight checks
# ============================================================================

library(dplyr)
library(tidyr)

# ============================================================================
# PRE-FLIGHT CHECK FUNCTIONS
# ============================================================================

#' Pre-flight check for pipeline execution
#' 
#' Runs comprehensive checks on dataframe before executing pipeline stages
#' to catch missing columns, data quality issues, and anomalies early.
#' 
#' @param df Dataframe to check
#' @param stage Which pipeline stage ("pre_portfolio", "pre_export", "pre_ml")
#' @return Invisible list of issues found
#' @export
pre_flight_check <- function(df, stage = "pre_portfolio") {
  
  cat("\n", paste(rep("=", 60), collapse = ""), "\n")
  cat("🛫 PRE-FLIGHT CHECK:", stage, "\n")
  cat(paste(rep("=", 60), collapse = ""), "\n")
  
  issues <- list()
  
  # Stage-specific column requirements
  if(stage == "pre_portfolio") {
    required <- c("ticker", "innovation_quartile", "ai_quartile", 
                  "volatility", "fund_weight", "in_portfolio")
    critical <- c("ticker", "innovation_quartile", "ai_quartile")
    
  } else if(stage == "pre_export") {
    required <- c("ticker", "ai_score", "innovation_score", 
                  "quality_innovators_weight", "ai_concentrated_weight",
                  "balanced_growth_weight", "anomaly_score", "is_anomaly",
                  "total_fund_weight")  # Added missing column!
    critical <- c("ticker", "ai_score", "innovation_score")
    
  } else if(stage == "pre_ml") {
    required <- c("ticker", "rd_intensity", "patent_activity", "revenue_growth",
                  "volatility", "market_cap", "total_return", "ai_quartile")
    critical <- c("ticker", "ai_quartile")
    
  } else {
    # Generic check - just look for common essential columns
    required <- c("ticker")
    critical <- c("ticker")
  }
  
  # --------------------------------------------------------------------------
  # CHECK 1: Required columns
  # --------------------------------------------------------------------------
  cat("\n📋 Checking required columns...\n")
  missing <- setdiff(required, names(df))
  if(length(missing) > 0) {
    cat("   ❌ Missing columns:", paste(missing, collapse = ", "), "\n")
    issues$missing_cols <- missing
    
    # Suggest fixes for common missing columns
    if("total_fund_weight" %in% missing) {
      cat("     → SUGGESTION: Add with: df$total_fund_weight <- df$fund_weight\n")
    }
    if("quality_innovators_weight" %in% missing) {
      cat("     → SUGGESTION: Run portfolio construction module first\n")
    }
  } else {
    cat("   ✅ All required columns present\n")
  }
  
  # --------------------------------------------------------------------------
  # CHECK 2: NAs in critical columns
  # --------------------------------------------------------------------------
  cat("\n🔍 Checking for NAs in critical columns...\n")
  for(col in intersect(critical, names(df))) {
    na_count <- sum(is.na(df[[col]]))
    na_pct <- round(100 * na_count / nrow(df), 1)
    
    if(na_count > 0) {
      cat(sprintf("   ⚠️ Column '%s' has %d NAs (%.1f%%)\n", col, na_count, na_pct))
      issues$nas[[col]] <- na_count
      
      # Show which companies have NAs (first 5)
      na_companies <- df[is.na(df[[col]]), "ticker", drop = TRUE]
      if(length(na_companies) > 0) {
        cat("      Companies:", paste(head(na_companies, 5), collapse = ", "))
        if(length(na_companies) > 5) cat(" ...")
        cat("\n")
      }
    }
  }
  
  # --------------------------------------------------------------------------
  # CHECK 3: Data types
  # --------------------------------------------------------------------------
  cat("\n📊 Key columns structure:\n")
  cols_to_show <- intersect(c("ticker", critical[1:min(3, length(critical))]), names(df))
  if(length(cols_to_show) > 0) {
    df_subset <- df %>% select(all_of(cols_to_show))
    glimpse_output <- capture.output(glimpse(df_subset))
    cat("   ", glimpse_output[1], "\n")
    cat("   ", glimpse_output[2], "\n")
  }
  
  # --------------------------------------------------------------------------
  # CHECK 4: Value ranges
  # --------------------------------------------------------------------------
  cat("\n📈 Value range checks:\n")
  
  if("rd_intensity" %in% names(df)) {
    r_range <- range(df$rd_intensity, na.rm = TRUE)
    r_mean <- mean(df$rd_intensity, na.rm = TRUE)
    cat(sprintf("   rd_intensity: range [%.3f, %.3f], mean %.3f\n", 
                r_range[1], r_range[2], r_mean))
    
    if(r_range[2] > 1) {
      cat("      ⚠️ Some R&D intensities > 100% - possible data issue\n")
    }
  }
  
  if("ai_score" %in% names(df)) {
    a_range <- range(df$ai_score, na.rm = TRUE)
    cat(sprintf("   ai_score: range [%.3f, %.3f]\n", a_range[1], a_range[2]))
  }
  
  if("fund_weight" %in% names(df) && stage == "pre_portfolio") {
    w_sum <- sum(df$fund_weight, na.rm = TRUE)
    cat(sprintf("   fund_weight sum: %.4f (should be ~1.0)\n", w_sum))
    if(abs(w_sum - 1) > 0.01) {
      cat("      ⚠️ Fund weights don't sum to 1 - will affect portfolios\n")
    }
  }
  
  if("total_fund_weight" %in% names(df) && stage == "pre_export") {
    tw_sum <- sum(df$total_fund_weight, na.rm = TRUE)
    cat(sprintf("   total_fund_weight sum: %.4f\n", tw_sum))
  }
  
  # --------------------------------------------------------------------------
  # SUMMARY
  # --------------------------------------------------------------------------
  cat("\n", paste(rep("=", 60), collapse = ""), "\n")
  
  if(length(issues) == 0) {
    cat("✅✅✅ PRE-FLIGHT PASSED! Ready to proceed.\n")
  } else {
    cat("⚠️⚠️⚠️ PRE-FLIGHT FAILED! Fix issues before proceeding.\n")
    cat("   Missing columns:", paste(issues$missing_cols, collapse = ", "), "\n")
    if(!is.null(issues$nas)) {
      cat("   NAs in critical columns:", paste(names(issues$nas), collapse = ", "), "\n")
    }
  }
  cat(paste(rep("=", 60), collapse = ""), "\n\n")
  
  invisible(issues)
}

#' Quick data summary for any dataframe
#' @param df Dataframe to summarize
#' @param n Number of rows to show
#' @export
quick_summary <- function(df, n = 5) {
  cat("\n📋 DATAFRAME SUMMARY\n")
  cat(paste(rep("-", 40), collapse = ""), "\n")
  cat("Rows:", nrow(df), "| Columns:", ncol(df), "\n")
  cat("\nFirst", n, "rows:\n")
  print(head(df, n))
  cat("\nColumn names:\n")
  print(names(df))
  cat("\nStructure:\n")
  glimpse(df)
}

# ============================================================================
# DATA CLEANING FUNCTIONS
# ============================================================================

#' Safe numeric conversion with warning suppression
#' @param x Character vector to convert
#' @return Numeric vector
safe_numeric <- function(x) {
  result <- suppressWarnings(as.numeric(as.character(x)))
  if(all(is.na(result)) && !all(is.na(x))) {
    warning("All values became NA during numeric conversion")
  }
  return(result)
}

#' Fill missing values with defaults
#' @param df Dataframe
#' @param default_rd Default R&D intensity (default 0.03)
#' @param default_patent Default patent count (default 0)
#' @return Dataframe with NAs filled
fill_missing_defaults <- function(df, default_rd = 0.03, default_patent = 0) {
  df %>%
    mutate(
      rd_intensity = ifelse(is.na(rd_intensity), default_rd, rd_intensity),
      patent_activity = ifelse(is.na(patent_activity), default_patent, patent_activity),
      revenue_growth = ifelse(is.na(revenue_growth), 0.05, revenue_growth)
    )
}

#' Ensure required columns exist with defaults
#' @param df Dataframe
#' @param required_cols Named list of columns and their default values
#' @return Dataframe with missing columns added
ensure_columns <- function(df, required_cols) {
  for(col in names(required_cols)) {
    if(!col %in% names(df)) {
      df[[col]] <- required_cols[[col]]
      message("➕ Added missing column '", col, "' with default value")
    }
  }
  return(df)
}

# ============================================================================
# PIPELINE UTILITIES
# ============================================================================

#' Create timestamped run directory
#' @param base_dir Base output directory
#' @return Path to created directory
create_run_dir <- function(base_dir = "data/03_output") {
  timestamp <- format(Sys.time(), "%Y%m%d_%H%M%S")
  run_dir <- file.path(base_dir, paste0("run_", timestamp))
  dir.create(run_dir, recursive = TRUE, showWarnings = FALSE)
  message("📁 Created run directory: ", run_dir)
  return(run_dir)
}

#' Save checkpoint data during pipeline execution
#' @param df Dataframe to save
#' @param name Checkpoint name
#' @param checkpoint_dir Directory to save checkpoints
save_checkpoint <- function(df, name, checkpoint_dir = "data/02_processed/checkpoints") {
  dir.create(checkpoint_dir, recursive = TRUE, showWarnings = FALSE)
  timestamp <- format(Sys.time(), "%Y%m%d_%H%M%S")
  filename <- file.path(checkpoint_dir, paste0(name, "_", timestamp, ".rds"))
  saveRDS(df, filename)
  message("💾 Saved checkpoint: ", filename)
}

#' Load most recent checkpoint
#' @param pattern Pattern to match checkpoint files
#' @param checkpoint_dir Checkpoint directory
load_latest_checkpoint <- function(pattern, checkpoint_dir = "data/02_processed/checkpoints") {
  files <- list.files(checkpoint_dir, pattern = paste0(pattern, "_.*\\.rds"), full.names = TRUE)
  if(length(files) == 0) return(NULL)
  latest <- files[order(file.info(files)$mtime, decreasing = TRUE)[1]]
  message("📂 Loading latest checkpoint: ", basename(latest))
  readRDS(latest)
}