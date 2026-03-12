# =============================================================================
# OUTPUT: Create Visualizations
# Version: 1.0 | Date: 2026-03-11
# Description: Generates plots for anomaly distribution and AI vs Innovation
# =============================================================================

#' Create all standard visualizations
#'
#' @param df Dataframe with results
#' @param run_dir Output directory path
#' @param run_timestamp Run timestamp
#' @return NULL (saves PNG files)
#' @export
create_visualizations <- function(df, run_dir, run_timestamp) {

  message("🎨 Creating visualizations...")

  # 1. Anomaly score histogram
  create_anomaly_histogram(df, run_dir, run_timestamp)

  # 2. AI vs Innovation scatter plot
  create_ai_vs_innovation_scatter(df, run_dir, run_timestamp)

  message("   ✅ Visualizations saved")
}

#' Create anomaly score histogram
#'
#' @param df Dataframe with anomaly_score column
#' @param run_dir Output directory
#' @param run_timestamp Run timestamp
create_anomaly_histogram <- function(df, run_dir, run_timestamp) {

  if (!"anomaly_score" %in% names(df)) {
    message("   ⚠️ No anomaly scores found. Skipping histogram.")
    return()
  }

  # Clean scores for plotting
  scores_clean <- df$anomaly_score[!is.na(df$anomaly_score)]

  if (length(scores_clean) == 0) {
    message("   ⚠️ No valid anomaly scores for histogram")
    return()
  }

  grDevices::png(file.path(run_dir, paste0(run_timestamp, "_anomaly_histogram.png")), 
                 width = 800, height = 600)

  graphics::hist(scores_clean, breaks = 30, 
                 main = "Anomaly Score Distribution",
                 xlab = "Anomaly Score", col = "steelblue", border = "white")

  # Add threshold line
  if (length(scores_clean) >= 10) {
    threshold <- stats::quantile(scores_clean, 0.9, na.rm = TRUE)
    graphics::abline(v = threshold, col = "red", lwd = 2, lty = 2)
    graphics::legend("topright", legend = "Top 10% Threshold", 
                     col = "red", lwd = 2, lty = 2)
  }

  grDevices::dev.off()
}

#' Create AI vs Innovation scatter plot
#'
#' @param df Dataframe with ai_score and innovation_score columns
#' @param run_dir Output directory
#' @param run_timestamp Run timestamp
create_ai_vs_innovation_scatter <- function(df, run_dir, run_timestamp) {

  required_cols <- c("innovation_score", "ai_score", "is_anomaly", "in_portfolio")
  if (!all(required_cols %in% names(df))) {
    message("   ⚠️ Missing required columns for scatter plot")
    return()
  }

  # Filter out NAs
  plot_data <- df %>%
    dplyr::filter(!is.na(innovation_score) & !is.na(ai_score))

  if (nrow(plot_data) == 0) {
    message("   ⚠️ No valid data for scatter plot")
    return()
  }

  grDevices::png(file.path(run_dir, paste0(run_timestamp, "_ai_vs_innovation.png")), 
                 width = 800, height = 600)

  # Create color vector
  plot_colors <- dplyr::case_when(
    plot_data$in_portfolio & plot_data$is_anomaly ~ "darkred",
    plot_data$in_portfolio & !plot_data$is_anomaly ~ "steelblue",
    !plot_data$in_portfolio & plot_data$is_anomaly ~ "orange",
    TRUE ~ "lightgreen"
  )

  graphics::plot(plot_data$innovation_score, plot_data$ai_score, 
                 col = plot_colors,
                 pch = 19, cex = 1.2,
                 xlab = "Innovation Score", ylab = "AI Score",
                 main = "AI Score vs Innovation Score")

  # Add regression line
  if (nrow(plot_data) > 5) {
    graphics::abline(stats::lm(ai_score ~ innovation_score, data = plot_data), 
                     col = "darkgreen", lwd = 2)
  }

  graphics::legend("topleft", 
                   legend = c("Portfolio - Normal", "Portfolio - Anomaly", 
                              "Discovery - Normal", "Discovery - Anomaly"),
                   col = c("steelblue", "darkred", "lightgreen", "orange"), 
                   pch = 19)

  grDevices::dev.off()
}