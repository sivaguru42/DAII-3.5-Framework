# =============================================================================
# TRANSFORM: Calculate Company Features
# Version: 2.5 | Date: 2026-03-18
# Description: Calculates derived features for scoring and analysis
# =============================================================================

calculate_company_features <- function(snapshot) {
  
  message("   [Transform] Calculating company features...")
  
  # Calculate features
  result <- snapshot %>%
    dplyr::mutate(
      # R&D intensity (if missing, use median of portfolio companies)
      rd_intensity = ifelse(!is.na(rd_expense) & !is.na(market_cap) & market_cap > 0,
                            rd_expense / market_cap,
                            NA_real_),
      
      # Portfolio flag - already exists from snapshot
      in_portfolio = in_portfolio,
      
      # Fund weight - from holdings data
      fund_weight = ifelse(in_portfolio & total_net_pct_ltp > 0,
                           total_net_pct_ltp / 100,  # Convert from percentage to decimal
                           0),
      
      # Patent activity flag
      patent_activity = ifelse(!is.na(total_patents) & total_patents > 0,
                               total_patents, 0),
      
      # Revenue growth (placeholder - would need historical data)
      revenue_growth = runif(dplyr::n(), -0.1, 0.3),
      
      # Total return - placeholder for now
      total_return = 0.10,
      
      # Volatility - OPTION: Use 1-year historical volatility (most stable)
      # For core analysis and portfolio construction
      volatility = volatility_260d  # 1-year historical volatility from daily returns
      
      # ALTERNATIVE OPTIONS (uncomment one if preferred):
      # volatility = volatility_90d,   # 3-month volatility - more responsive
      # volatility = volatility_30d,   # 1-month volatility - most responsive
      # volatility = ifelse(!is.na(bloomberg_volatility), bloomberg_volatility, volatility_260d)  # Bloomberg if available
    )
  
  # Fill missing R&D intensity with median of portfolio companies
  portfolio_median_rd <- stats::median(result$rd_intensity[result$in_portfolio], na.rm = TRUE)
  result <- result %>%
    dplyr::mutate(
      rd_intensity = ifelse(is.na(rd_intensity), portfolio_median_rd, rd_intensity)
    )
  
  message(sprintf("   [Transform] Features calculated for %d companies", nrow(result)))
  message(sprintf("   [Transform] Portfolio companies: %d", sum(result$in_portfolio)))
  
  return(result)
}