# ============================================================================
# MODULE 4: Portfolio Construction
# Version: 2.0 | Date: 2026-03-17
# Description: Fixed versions of portfolio functions with proper error handling
#              and missing column management
# ============================================================================

#' Build DUMAC portfolios (FIXED VERSION)
#' 
#' Constructs three portfolios for companies in_portfolio = TRUE:
#' 1. Quality Innovators: Top innovation quartile, below-median volatility
#' 2. AI Concentrated: Top AI quartile
#' 3. Balanced Growth: Innovation >= Q3 AND AI >= Q3
#'
#' @param df Dataframe with columns: ticker, innovation_quartile, ai_quartile, 
#'           volatility, fund_weight, in_portfolio
#' @return Dataframe with portfolio weight columns added
#' @export
build_dumac_portfolios <- function(df) {
  
  message("\n💼 Building DUMAC portfolios...")
  
  # --------------------------------------------------------------------------
  # Input validation
  # --------------------------------------------------------------------------
  required_cols <- c("ticker", "innovation_quartile", "ai_quartile", 
                     "volatility", "fund_weight", "in_portfolio")
  missing_cols <- setdiff(required_cols, names(df))
  
  if(length(missing_cols) > 0) {
    stop("❌ Missing required columns: ", paste(missing_cols, collapse = ", "))
  }
  
  # --------------------------------------------------------------------------
  # Filter to portfolio companies only
  # --------------------------------------------------------------------------
  portfolio_df <- df %>% dplyr::filter(in_portfolio == TRUE)
  
  if (nrow(portfolio_df) == 0) {
    message("   ⚠️ No portfolio companies found. Adding placeholder columns with zeros.")
    df$quality_innovators_weight <- 0
    df$ai_concentrated_weight <- 0
    df$balanced_growth_weight <- 0
    return(df)
  }
  
  message("   Portfolio companies: ", nrow(portfolio_df))
  
  # --------------------------------------------------------------------------
  # Calculate median volatility for portfolio companies
  # --------------------------------------------------------------------------
  vol_median <- stats::median(portfolio_df$volatility, na.rm = TRUE)
  message("   Median volatility: ", round(vol_median, 4))
  
  # --------------------------------------------------------------------------
  # Strategy 1: Quality Innovators
  # --------------------------------------------------------------------------
  qual_innov_mask <- portfolio_df$innovation_quartile == 4 & 
    portfolio_df$volatility < vol_median
  
  if (sum(qual_innov_mask) > 0) {
    total_weight <- sum(portfolio_df$fund_weight[qual_innov_mask], na.rm = TRUE)
    portfolio_df$quality_innovators_weight <- ifelse(
      qual_innov_mask,
      portfolio_df$fund_weight / total_weight,
      0
    )
    message("   Quality Innovators: ", sum(qual_innov_mask), " companies")
  } else {
    portfolio_df$quality_innovators_weight <- 0
    message("   Quality Innovators: 0 companies (no candidates)")
  }
  
  # --------------------------------------------------------------------------
  # Strategy 2: AI Concentrated
  # --------------------------------------------------------------------------
  ai_conc_mask <- portfolio_df$ai_quartile == 4
  
  if (sum(ai_conc_mask) > 0) {
    total_weight <- sum(portfolio_df$fund_weight[ai_conc_mask], na.rm = TRUE)
    portfolio_df$ai_concentrated_weight <- ifelse(
      ai_conc_mask,
      portfolio_df$fund_weight / total_weight,
      0
    )
    message("   AI Concentrated: ", sum(ai_conc_mask), " companies")
  } else {
    portfolio_df$ai_concentrated_weight <- 0
    message("   AI Concentrated: 0 companies (no candidates)")
  }
  
  # --------------------------------------------------------------------------
  # Strategy 3: Balanced Growth
  # --------------------------------------------------------------------------
  balanced_mask <- portfolio_df$innovation_quartile >= 3 & 
    portfolio_df$ai_quartile >= 3
  
  if (sum(balanced_mask) > 0) {
    total_weight <- sum(portfolio_df$fund_weight[balanced_mask], na.rm = TRUE)
    portfolio_df$balanced_growth_weight <- ifelse(
      balanced_mask,
      portfolio_df$fund_weight / total_weight,
      0
    )
    message("   Balanced Growth: ", sum(balanced_mask), " companies")
  } else {
    portfolio_df$balanced_growth_weight <- 0
    message("   Balanced Growth: 0 companies (no candidates)")
  }
  
  # --------------------------------------------------------------------------
  # Merge back with full dataframe
  # --------------------------------------------------------------------------
  # First, ensure main df has weight columns initialized to 0
  df$quality_innovators_weight <- 0
  df$ai_concentrated_weight <- 0
  df$balanced_growth_weight <- 0
  
  # Update values for portfolio companies
  for(ticker_val in portfolio_df$ticker) {
    idx_main <- which(df$ticker == ticker_val)
    idx_port <- which(portfolio_df$ticker == ticker_val)
    
    df$quality_innovators_weight[idx_main] <- portfolio_df$quality_innovators_weight[idx_port]
    df$ai_concentrated_weight[idx_main] <- portfolio_df$ai_concentrated_weight[idx_port]
    df$balanced_growth_weight[idx_main] <- portfolio_df$balanced_growth_weight[idx_port]
  }
  
  # --------------------------------------------------------------------------
  # Final summary
  # --------------------------------------------------------------------------
  cat("\n📊 DUMAC Portfolio Summary:\n")
  cat(sprintf("   Quality Innovators: %d companies, total weight: %.4f\n",
              sum(df$quality_innovators_weight > 0, na.rm = TRUE),
              sum(df$quality_innovators_weight, na.rm = TRUE)))
  cat(sprintf("   AI Concentrated: %d companies, total weight: %.4f\n",
              sum(df$ai_concentrated_weight > 0, na.rm = TRUE),
              sum(df$ai_concentrated_weight, na.rm = TRUE)))
  cat(sprintf("   Balanced Growth: %d companies, total weight: %.4f\n",
              sum(df$balanced_growth_weight > 0, na.rm = TRUE),
              sum(df$balanced_growth_weight, na.rm = TRUE)))
  
  return(df)
}

#' Build discovery portfolios (FIXED VERSION)
#' 
#' Creates discovery universe portfolios for companies NOT in the DUMAC portfolio.
#' Includes tier rankings and discovery weights.
#'
#' @param df Dataframe with columns: ticker, innovation_score, ai_score,
#'           innovation_quartile, ai_quartile, volatility, in_portfolio
#' @return Dataframe with discovery portfolio columns added
#' @export
build_discovery_portfolios <- function(df) {
  
  message("\n🔍 Building discovery portfolios...")
  
  # --------------------------------------------------------------------------
  # Input validation
  # --------------------------------------------------------------------------
  required_cols <- c("ticker", "innovation_score", "ai_score", "innovation_quartile",
                     "ai_quartile", "volatility", "in_portfolio")
  missing_cols <- setdiff(required_cols, names(df))
  
  if(length(missing_cols) > 0) {
    stop("❌ Missing required columns: ", paste(missing_cols, collapse = ", "))
  }
  
  # --------------------------------------------------------------------------
  # Filter to non-portfolio companies
  # --------------------------------------------------------------------------
  discovery_df <- df %>% dplyr::filter(in_portfolio == FALSE)
  
  if (nrow(discovery_df) == 0) {
    message("   ⚠️ No discovery companies found. Adding placeholder columns.")
    df <- df %>%
      dplyr::mutate(
        discovery_tier = "Not in Discovery Universe",
        innovation_rank = NA_integer_,
        ai_rank = NA_integer_,
        combined_rank = NA_integer_,
        discovery_quality_weight = 0,
        discovery_ai_weight = 0,
        discovery_balanced_weight = 0
      )
    return(df)
  }
  
  message("   Discovery companies: ", nrow(discovery_df))
  
  # --------------------------------------------------------------------------
  # Calculate ranks
  # --------------------------------------------------------------------------
  discovery_df <- discovery_df %>%
    dplyr::mutate(
      innovation_rank = rank(-innovation_score, ties.method = "min"),
      ai_rank = rank(-ai_score, ties.method = "min"),
      combined_rank = rank(-(innovation_score + ai_score), ties.method = "min")
    )
  
  # --------------------------------------------------------------------------
  # Discovery tiers (based on combined rank)
  # --------------------------------------------------------------------------
  n_companies <- nrow(discovery_df)
  discovery_df <- discovery_df %>%
    dplyr::mutate(
      discovery_tier = dplyr::case_when(
        combined_rank <= ceiling(n_companies * 0.1) ~ "Tier 1: Top 10% Opportunities",
        combined_rank <= ceiling(n_companies * 0.25) ~ "Tier 2: Top 25% Candidates",
        combined_rank <= ceiling(n_companies * 0.5) ~ "Tier 3: Top 50% Watch List",
        TRUE ~ "Tier 4: Monitor"
      )
    )
  
  # --------------------------------------------------------------------------
  # Discovery weights
  # --------------------------------------------------------------------------
  # Quality Innovators weight (for discovery)
  qual_mask <- discovery_df$innovation_quartile == 4 & 
    discovery_df$volatility < median(discovery_df$volatility, na.rm = TRUE)
  
  if(sum(qual_mask) > 0) {
    total_score <- sum(discovery_df$innovation_score[qual_mask], na.rm = TRUE)
    discovery_df$discovery_quality_weight <- ifelse(
      qual_mask,
      discovery_df$innovation_score / total_score,
      0
    )
  } else {
    discovery_df$discovery_quality_weight <- 0
  }
  
  # AI Concentrated weight (for discovery)
  ai_mask <- discovery_df$ai_quartile == 4
  
  if(sum(ai_mask) > 0) {
    total_score <- sum(discovery_df$ai_score[ai_mask], na.rm = TRUE)
    discovery_df$discovery_ai_weight <- ifelse(
      ai_mask,
      discovery_df$ai_score / total_score,
      0
    )
  } else {
    discovery_df$discovery_ai_weight <- 0
  }
  
  # Balanced Growth weight (for discovery)
  balanced_mask <- discovery_df$innovation_quartile >= 3 & 
    discovery_df$ai_quartile >= 3
  
  if(sum(balanced_mask) > 0) {
    total_score <- sum(discovery_df$innovation_score[balanced_mask] + 
                         discovery_df$ai_score[balanced_mask], na.rm = TRUE)
    discovery_df$discovery_balanced_weight <- ifelse(
      balanced_mask,
      (discovery_df$innovation_score + discovery_df$ai_score) / total_score,
      0
    )
  } else {
    discovery_df$discovery_balanced_weight <- 0
  }
  
  # --------------------------------------------------------------------------
  # Merge back with full dataframe
  # --------------------------------------------------------------------------
  # Initialize columns in main df
  df <- df %>%
    dplyr::mutate(
      discovery_tier = "Not in Discovery Universe",
      innovation_rank = NA_integer_,
      ai_rank = NA_integer_,
      combined_rank = NA_integer_,
      discovery_quality_weight = 0,
      discovery_ai_weight = 0,
      discovery_balanced_weight = 0
    )
  
  # Update for discovery companies
  for(ticker_val in discovery_df$ticker) {
    idx_main <- which(df$ticker == ticker_val)
    idx_disc <- which(discovery_df$ticker == ticker_val)
    
    df$discovery_tier[idx_main] <- discovery_df$discovery_tier[idx_disc]
    df$innovation_rank[idx_main] <- discovery_df$innovation_rank[idx_disc]
    df$ai_rank[idx_main] <- discovery_df$ai_rank[idx_disc]
    df$combined_rank[idx_main] <- discovery_df$combined_rank[idx_disc]
    df$discovery_quality_weight[idx_main] <- discovery_df$discovery_quality_weight[idx_disc]
    df$discovery_ai_weight[idx_main] <- discovery_df$discovery_ai_weight[idx_disc]
    df$discovery_balanced_weight[idx_main] <- discovery_df$discovery_balanced_weight[idx_disc]
  }
  
  # --------------------------------------------------------------------------
  # Summary
  # --------------------------------------------------------------------------
  cat("\n📊 Discovery Portfolio Summary:\n")
  cat(sprintf("   Discovery companies: %d\n", nrow(discovery_df)))
  cat("\n   Tier distribution:\n")
  print(table(df$discovery_tier[df$in_portfolio == FALSE]))
  
  return(df)
}