# =============================================================================
# TRANSFORM: Join Additional Data Sources
# Version: 2.0 | Date: 2026-03-11
# Description: Joins portfolio weights, discovery data, and other 
#              post-calculation datasets to the main dataframe.
# =============================================================================

#' Join portfolio and discovery data to main dataframe
#'
#' @param df Dataframe with ticker column and core scores.
#' @param dumac_portfolios Dataframe from Module 4.3.
#' @param discovery_portfolios Dataframe from Module 4.4.
#' @param ai_cube Dataframe from Module 5.1.
#' @return Dataframe with all additional columns merged.
join_additional_sources <- function(df, 
                                     dumac_portfolios = NULL,
                                     discovery_portfolios = NULL,
                                     ai_cube = NULL) {

  message("   [Transform] Joining additional data sources...")

  # ---- 1. Join DUMAC portfolios ----
  if (!is.null(dumac_portfolios)) {
    df <- df %>%
      dplyr::left_join(
        dumac_portfolios[, c("ticker", "quality_innovators_weight", 
                             "ai_concentrated_weight", "balanced_growth_weight")],
        by = "ticker"
      )
  }

  # ---- 2. Join Discovery portfolios ----
  if (!is.null(discovery_portfolios)) {
    df <- df %>%
      dplyr::left_join(
        discovery_portfolios[, c("ticker", "discovery_quality_weight", 
                                 "discovery_ai_weight", "discovery_balanced_weight",
                                 "discovery_tier", "innovation_rank", 
                                 "ai_rank", "combined_rank")],
        by = "ticker"
      )
  }

  # ---- 3. Join AI Cube data ----
  if (!is.null(ai_cube)) {
    df <- df %>%
      dplyr::left_join(
        ai_cube[, c("ticker", "strategic_profile", "ai_exposure", "innovation_exposure")],
        by = "ticker"
      )
  }

  # ---- 4. Fill NAs with defaults ----
  df <- df %>%
    dplyr::mutate(
      # Portfolio weights
      quality_innovators_weight = dplyr::if_else(
        is.na(quality_innovators_weight), 0, quality_innovators_weight),
      ai_concentrated_weight = dplyr::if_else(
        is.na(ai_concentrated_weight), 0, ai_concentrated_weight),
      balanced_growth_weight = dplyr::if_else(
        is.na(balanced_growth_weight), 0, balanced_growth_weight),
      
      # Discovery weights
      discovery_quality_weight = dplyr::if_else(
        is.na(discovery_quality_weight), 0, discovery_quality_weight),
      discovery_ai_weight = dplyr::if_else(
        is.na(discovery_ai_weight), 0, discovery_ai_weight),
      discovery_balanced_weight = dplyr::if_else(
        is.na(discovery_balanced_weight), 0, discovery_balanced_weight),
      
      # Discovery tier
      discovery_tier = dplyr::if_else(
        is.na(discovery_tier), "Not in Discovery Universe", discovery_tier)
    )

  message("   [Transform] Additional sources joined successfully")
  return(df)
}