# =============================================================================
# TRANSFORM: Handle Missing Values
# Version: 1.0 | Date: 2026-03-11
# Description: Imputes or fills missing values in the company snapshot with
#              sensible defaults.
# =============================================================================

#' Imputes missing values in the company snapshot.
#'
#' @param snapshot Dataframe output from calculate_company_features().
#' @return The same dataframe with NAs filled.
handle_missing_values <- function(snapshot) {

  message("   [Transform] Handling missing values...")

  # Calculate medians from the data for imputation
  median_mcap <- median(snapshot$MKTCAP_USD, na.rm = TRUE)

  snapshot <- snapshot %>%
    dplyr::mutate(
      # For R&D expense: use 3% of market cap if missing, else keep original
      rd_expense = dplyr::if_else(is.na(rd_expense), MKTCAP_USD * 0.03, rd_expense),

      # For patent activity: set missing to 0
      patent_activity = dplyr::if_else(is.na(patent_count), 0, patent_count),

      # Recalculate R&D intensity after imputation
      rd_intensity = rd_expense / MKTCAP_USD
    )

  message("   [Transform] Missing values handled.")
  return(snapshot)
}