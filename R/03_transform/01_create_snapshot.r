# =============================================================================
# TRANSFORM: Create Master Company Snapshot
# Version: 3.1 FINAL | Date: 2026-03-23
# =============================================================================

create_company_snapshot <- function(company_map, 
                                    rd_history, 
                                    current_holdings, 
                                    daily_ratios, 
                                    patent_data,
                                    bloomberg_prices = NULL,
                                    msci_data = NULL) {
  
  message("   [Transform] Creating master company snapshot...")
  
  # ---- 0. Ensure consistent column names ----
  if("ticker" %in% names(company_map) && !"Ticker" %in% names(company_map)) {
    company_map <- company_map %>% dplyr::rename(Ticker = ticker)
  }
  if("ticker" %in% names(rd_history) && !"Ticker" %in% names(rd_history)) {
    rd_history <- rd_history %>% dplyr::rename(Ticker = ticker)
  }
  if("ticker" %in% names(patent_data) && !"Ticker" %in% names(patent_data)) {
    patent_data <- patent_data %>% dplyr::rename(Ticker = ticker)
  }
  
  # ---- 1. Get most recent R&D year ----
  latest_rd <- rd_history %>%
    dplyr::group_by(Ticker) %>%
    dplyr::slice_max(fiscal_year_end, n = 1) %>%
    dplyr::ungroup() %>%
    dplyr::select(Ticker, rd_expense, revenue, fiscal_year_end)
  
  # ---- 2. Get most recent daily ratios (includes volatility) ----
  latest_ratios <- daily_ratios %>%
    dplyr::group_by(Ticker) %>%
    dplyr::slice_max(as_of_date, n = 1) %>%
    dplyr::ungroup() %>%
    dplyr::select(Ticker, market_cap, pe_ratio_fy, beta_5y, 
                  volatility_30d, volatility_90d, volatility_260d)
  
  # ---- 3. Start with company_map and join ----
  snapshot <- company_map %>%
    dplyr::left_join(latest_rd, by = "Ticker") %>%
    dplyr::left_join(latest_ratios, by = "Ticker") %>%
    dplyr::left_join(patent_data, by = "Ticker") %>%
    dplyr::mutate(
      in_portfolio = FALSE,
      total_net_exposure_usd = 0,
      total_net_pct_ltp = 0,
      n_funds = 0
    )
  
  message(sprintf("   [Transform] Snapshot created with %d companies.", nrow(snapshot)))
  message(sprintf("   [Transform] Companies in portfolio: %d", sum(snapshot$in_portfolio)))
  
  return(snapshot)
}

