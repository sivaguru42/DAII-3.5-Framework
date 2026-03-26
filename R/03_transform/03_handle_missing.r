# =============================================================================
# TRANSFORM: Handle Missing Values
# Version: 2.1 | Date: 2026-03-18
# Description: Imputes missing values with reasonable defaults
# =============================================================================

handle_missing_values <- function(df) {
  
  message("   [Transform] Handling missing values...")
  
  result <- df %>%
    dplyr::mutate(
      # R&D expense - if missing, estimate as 3% of market cap
      rd_expense = dplyr::if_else(
        is.na(rd_expense) | rd_expense == 0,
        market_cap * 0.03,  # Changed from MKTCAP_USD to market_cap
        rd_expense
      ),
      
      # Revenue - if missing, estimate as 5x R&D (typical ratio)
      revenue = dplyr::if_else(
        is.na(revenue) | revenue == 0,
        rd_expense * 5,
        revenue
      ),
      
      # Market cap - if missing, use median of portfolio companies
      market_cap = dplyr::if_else(
        is.na(market_cap) | market_cap == 0,
        stats::median(market_cap[in_portfolio], na.rm = TRUE),
        market_cap
      ),
      
      # PE ratio - if missing, use sector median (simplified: overall median)
      pe_ratio_fy = dplyr::if_else(
        is.na(pe_ratio_fy) | pe_ratio_fy == 0,
        stats::median(pe_ratio_fy[in_portfolio], na.rm = TRUE),
        pe_ratio_fy
      ),
      
      # Beta - if missing, use 1.0 (market average)
      beta_5y = dplyr::if_else(
        is.na(beta_5y) | beta_5y == 0,
        1.0,
        beta_5y
      ),
      
      # Patent activity - if missing, set to 0
      patent_activity = dplyr::if_else(
        is.na(patent_activity),
        0,
        patent_activity
      ),
      
      # Total return - if missing, use 10%
      total_return = dplyr::if_else(
        is.na(total_return),
        0.10,
        total_return
      ),
      
      # Revenue growth - if missing, use 5%
      revenue_growth = dplyr::if_else(
        is.na(revenue_growth),
        0.05,
        revenue_growth
      )
    )
  
  message(sprintf("   [Transform] Missing values handled for %d companies", nrow(result)))
  
  return(result)
}

