
# FIXED QUARTILE CALCULATION
quartile_breaks <- quantile(scores_data$DAII_3.5_Score, 
                           probs = seq(0, 1, 0.25), 
                           na.rm = TRUE)

# Check for unique breaks (need 5 unique values for 4 quartiles)
if (length(unique(quartile_breaks)) < 5) {
  cat("Warning: Non-unique quartile breaks detected. Applying small jitter.\n")
  # Add minimal random noise (1/10000th of score range)
  score_range <- max(scores_data$DAII_3.5_Score, na.rm = TRUE) - 
                min(scores_data$DAII_3.5_Score, na.rm = TRUE)
  jitter_amount <- score_range * 0.0001
  
  scores_data$DAII_3.5_Score <- scores_data$DAII_3.5_Score + 
    runif(nrow(scores_data), -jitter_amount, jitter_amount)
  
  # Recalculate breaks
  quartile_breaks <- quantile(scores_data$DAII_3.5_Score, 
                             probs = seq(0, 1, 0.25), 
                             na.rm = TRUE)
}

scores_data$innovation_quartile <- cut(scores_data$DAII_3.5_Score,
                                      breaks = quartile_breaks,
                                      labels = c("Q1", "Q2", "Q3", "Q4"),
                                      include.lowest = TRUE)

