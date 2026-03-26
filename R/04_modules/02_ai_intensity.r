# ============================================================================
# MODULE 4: AI Intensity Scoring
# Version: 2.0 | Date: 2026-03-17
# Description: Calculates AI scores by applying industry multipliers to
#              innovation scores. FIXED version - robust error handling.
# ============================================================================

#' Calculate AI intensity scores
#'
#' Multiplies innovation scores by industry-specific multipliers to get
#' AI intensity scores.
#'
#' @param df Dataframe with innovation_score and industry columns
#' @param industry_multipliers Dataframe with industry and multiplier columns
#' @return Dataframe with added ai_score, ai_quartile, ai_label columns
#' @export
calculate_ai_intensity <- function(df, industry_multipliers = NULL) {
  
  message("\n🤖 Calculating AI intensity scores...")
  
  # Input validation
  if(!"innovation_score" %in% names(df)) {
    stop("❌ Missing required column: innovation_score")
  }
  
  if(!"industry" %in% names(df)) {
    stop("❌ Missing required column: industry")
  }
  
  # Default industry multipliers if not provided
  if (is.null(industry_multipliers)) {
    industry_multipliers <- data.frame(
      industry = c(
        "Semiconductors & Semiconductor",
        "Software & Services",
        "Media & Entertainment",
        "Pharmaceuticals, Biotechnology",
        "Technology Hardware & Equipment",
        "Automobiles & Components",
        "Financial Services",
        "Capital Goods",
        "Consumer Discretionary",
        "Energy",
        "Materials",
        "Utilities",
        "Unknown"
      ),
      multiplier = c(
        1.5, 1.4, 1.3, 1.2, 1.2, 1.1, 0.8, 0.9, 1.0, 0.7, 0.8, 0.5, 1.0
      )
    )
    message("   Using default industry multipliers")
  }
  
  # Join multipliers and calculate AI scores
  df <- df %>%
    dplyr::left_join(industry_multipliers, by = "industry") %>%
    dplyr::mutate(
      multiplier = ifelse(is.na(multiplier), 1.0, multiplier),
      ai_score = innovation_score * multiplier,
      ai_quartile = dplyr::ntile(ai_score, 4),
      ai_label = dplyr::case_when(
        ai_quartile == 4 ~ "AI Leader",
        ai_quartile == 3 ~ "AI Adopter",
        ai_quartile == 2 ~ "AI Follower",
        TRUE ~ "AI Laggard"
      )
    )
  
  # Summary statistics
  cat("\n📊 AI Score Summary:\n")
  cat(sprintf("   Range: [%.3f, %.3f]\n", 
              min(df$ai_score, na.rm = TRUE), 
              max(df$ai_score, na.rm = TRUE)))
  cat(sprintf("   Mean: %.3f\n", mean(df$ai_score, na.rm = TRUE)))
  
  cat("\n📊 AI Label Distribution:\n")
  print(table(df$ai_label))
  
  return(df)
}