calculate_ai_intensity <- function(df, industry_multipliers) {
  df %>%
    left_join(industry_multipliers, by = "industry") %>%
    mutate(
      multiplier = ifelse(is.na(multiplier), 1.0, multiplier),
      ai_score = innovation_score * multiplier,
      ai_quartile = ntile(ai_score, 4),
      ai_label = case_when(
        ai_quartile == 4 ~ "AI Leader",
        ai_quartile == 3 ~ "AI Adopter",
        ai_quartile == 2 ~ "AI Follower",
        TRUE ~ "AI Laggard"
      )
    )
}