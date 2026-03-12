# =============================================================================
# TRANSFORM: Create Master Company Snapshot
# Version: 2.0 | Date: 2026-03-11
# Description: Joins company map, R&D history, holdings, ratios, patent data,
#              and Bloomberg data to create a single-row-per-company dataframe.
# =============================================================================

#' Creates the initial master company snapshot by joining all data sources.
#'
#' @param company_map Dataframe from 01_company_resolution.R (must have Ticker, RepNo).
#' @param rd_history Dataframe from 02_pull_rd_history.R (must have Ticker, fiscal_year_end).
#' @param current_holdings Dataframe from 03_pull_holdings.R.
#' @param daily_ratios Dataframe from 04_pull_daily_ratios.R (must have as_of_date).
#' @param patent_data Dataframe from 05_patentsview.R (must have Ticker, patent_count).
#' @param bloomberg_prices Dataframe from 07_load_bloomberg_cache.R (optional).
#' @param msci_data Dataframe from 09_pull_msci.R (optional).
#' @return A combined dataframe with one row per company and raw joined data.
create_company_snapshot <- function(company_map, 
                                     rd_history, 
                                     current_holdings, 
                                     daily_ratios, 
                                     patent_data,
                                     bloomberg_prices = NULL,
                                     msci_data = NULL) {

  message("   [Transform] Creating master company snapshot...")

  # ---- 1. Get most recent R&D year for each company ----
  latest_rd <- rd_history %>%
    dplyr::group_by(Ticker) %>%
    dplyr::slice_max(fiscal_year_end, n = 1) %>%
    dplyr::ungroup() %>%
    dplyr::select(Ticker, rd_expense, revenue, fiscal_year_end)

  # ---- 2. Get most recent daily ratios for each company ----
  latest_ratios <- daily_ratios %>%
    dplyr::filter(as_of_date == max(as_of_date, na.rm = TRUE)) %>%
    dplyr::select(Ticker, MKTCAP_USD, PE_ratio, Beta_3Y_weekly)

  # ---- 3. Start with company map and join all sources ----
  snapshot <- company_map %>%
    dplyr::left_join(latest_rd, by = "Ticker") %>%
    dplyr::left_join(current_holdings, by = "Ticker") %>%
    dplyr::left_join(latest_ratios, by = "Ticker") %>%
    dplyr::left_join(patent_data, by = "Ticker")

  # ---- 4. Join Bloomberg data if provided ----
  if (!is.null(bloomberg_prices)) {
    bloomberg_summary <- bloomberg_prices %>%
      dplyr::group_by(ticker) %>%
      dplyr::summarise(
        bloomberg_volatility = dplyr::first(volatility_12m),
        total_return_5yr = dplyr::last(price) / dplyr::first(price) - 1,
        .groups = "drop"
      ) %>%
      dplyr::rename(Ticker = ticker)
    
    snapshot <- snapshot %>%
      dplyr::left_join(bloomberg_summary, by = "Ticker")
  }

  # ---- 5. Join MSCI data if provided ----
  if (!is.null(msci_data)) {
    snapshot <- snapshot %>%
      dplyr::left_join(msci_data, by = "Ticker")
  }

  message(sprintf("   [Transform] Snapshot created with %d companies.", nrow(snapshot)))
  return(snapshot)
}