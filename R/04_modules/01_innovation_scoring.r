# =============================================================================
# MODULE 1-3: Innovation Scoring
# Version: 2.0 | Date: 2026-03-11
# Description: Calculates quartiles for R&D, patents, growth, and volatility,
#              then computes innovation scores and labels.
# =============================================================================

#' Calculate innovation scores for all companies
#'
#' @param df Dataframe with columns: rd_intensity, patent_activity, 
#'           revenue_growth, volatility
#' @return Dataframe with added columns: rd_quartile, patent_quartile, 
#'         growth_quartile, volatility_quartile, innovation_score,
#'         innovation_quartile, innovation_label
#' @export
calculate_innovation_scores <- function(df) {

  message("   [Module 1-3] Calculating innovation scores...")

  # Ensure we're working with company-level data
  df <- df %>%
    dplyr::distinct(ticker, .keep_all = TRUE)

  df <- df %>%
    dplyr::mutate(
      # R&D Intensity Quartile (higher is better)
      rd_quartile = dplyr::ntile(rd_intensity, 4),
      
      # Patent Activity Quartile (higher is better)
      patent_quartile = dplyr::ntile(patent_activity, 4),
      
      # Revenue Growth Quartile (higher is better)
      growth_quartile = dplyr::ntile(revenue_growth, 4),
      
      # Volatility Quartile (lower is better - invert)
      volatility_quartile = 5 - dplyr::ntile(volatility, 4),
      
      # Innovation Score (weighted average of quartiles)
      innovation_score = (
        rd_quartile * 0.30 +
        patent_quartile * 0.30 +
        growth_quartile * 0.20 +
        volatility_quartile * 0.20
      ) / 4,  # Normalize to 0-1 scale
      
      # Innovation Quartile (final categorization)
      innovation_quartile = dplyr::ntile(innovation_score, 4),
      
      innovation_label = dplyr::case_when(
        innovation_quartile == 4 ~ "Leader",
        innovation_quartile == 3 ~ "Strong",
        innovation_quartile == 2 ~ "Developing",
        TRUE ~ "Emerging"
      )
    )

  message(sprintf("   [Module 1-3] Innovation scores calculated for %d companies", 
                  nrow(df)))
  message("   Distribution:")
  print(table(df$innovation_label))

  return(df)
}