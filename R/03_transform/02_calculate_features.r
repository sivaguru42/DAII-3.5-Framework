# =============================================================================
# TRANSFORM: Calculate Company Features
# Version: 1.0 | Date: 2026-03-11
# Description: Adds derived features (rd_intensity, in_portfolio, etc.) to the
#              master snapshot.
# =============================================================================

#' Calculates new features based on raw data in the company snapshot.
#'
#' @param snapshot Dataframe output from create_company_snapshot().
#' @return The same dataframe with new calculated columns added.
calculate_company_features <- function(snapshot) {

  message("   [Transform] Calculating company features...")

  snapshot <- snapshot %>%
    dplyr::mutate(
      # Create portfolio flag
      in_portfolio = !is.na(fund_weight),

      # Calculate R&D intensity
      rd_intensity = rd_expense / MKTCAP_USD,

      # Calculate a placeholder fund weight for companies not in portfolio
      fund_weight = dplyr::if_else(is.na(fund_weight), 1 / dplyr::n(), fund_weight)
    )

  message("   [Transform] Features calculated.")
  return(snapshot)
}