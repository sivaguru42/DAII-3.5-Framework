# =============================================================================
# MODULE 6: Anomaly Detection (Isolation Forest)
# Version: 1.0 | Date: 2026-03-11
# Description: Detects anomalous companies based on AI/innovation metrics
#              using Isolation Forest algorithm.
# =============================================================================

#' Detect anomalies using Isolation Forest
#'
#' @param df Dataframe with features for anomaly detection
#' @param threshold_percentile Percentile threshold for anomaly flag (default: 0.9)
#' @param seed Random seed for reproducibility (default: 42)
#' @return Dataframe with added anomaly_score and is_anomaly columns
#' @export
detect_anomalies <- function(df, threshold_percentile = 0.9, seed = 42) {

  message("🔍 [Module 6] Running Isolation Forest for anomaly detection...")

  # Define features for anomaly detection
  feature_cols <- c("rd_intensity", "patent_activity", "revenue_growth", 
                    "volatility", "market_cap", "innovation_score", "ai_score")
  
  # Check which features exist
  available_features <- intersect(feature_cols, names(df))
  
  if (length(available_features) < 3) {
    message("   ⚠️ Insufficient features for anomaly detection. Need at least 3.")
    message("   Available: ", paste(available_features, collapse = ", "))
    return(df)
  }

  # Prepare features
  anomaly_features <- df %>%
    dplyr::select(dplyr::all_of(available_features)) %>%
    dplyr::mutate(dplyr::across(dplyr::everything(), as.numeric))

  # Handle NA/Inf values
  anomaly_features <- anomaly_features %>%
    dplyr::mutate(dplyr::across(dplyr::everything(), 
                                 ~ ifelse(is.infinite(.) | is.na(.), 0, .)))

  # Standardize features
  feature_means <- colMeans(anomaly_features, na.rm = TRUE)
  feature_sds <- apply(anomaly_features, 2, sd, na.rm = TRUE)
  
  anomaly_features_scaled <- as.data.frame(scale(anomaly_features))
  anomaly_features_scaled[is.na(anomaly_features_scaled)] <- 0

  set.seed(seed)

  # Check if isotree is available
  if (!requireNamespace("isotree", quietly = TRUE)) {
    message("   ⚠️ isotree package not available. Using statistical method instead.")
    return(detect_anomalies_statistical(df, anomaly_features_scaled))
  }

  tryCatch({
    # Train Isolation Forest
    iso_model <- isotree::isolation.forest(
      data = anomaly_features_scaled,
      ntrees = 100,
      sample_size = min(256, nrow(anomaly_features_scaled)),
      ndim = ncol(anomaly_features_scaled),
      seed = seed
    )

    # Get anomaly scores
    df$anomaly_score <- predict(iso_model, anomaly_features_scaled)

    message("   ✅ Isolation Forest completed successfully")

  }, error = function(e) {
    message("   ⚠️ Isolation Forest error: ", e$message)
    message("   Falling back to statistical method.")
    df <- detect_anomalies_statistical(df, anomaly_features_scaled)
  })

  # Normalize scores to 0-1 range if they're not already
  if (exists("iso_model") && !is.null(df$anomaly_score)) {
    df <- normalize_anomaly_scores(df, threshold_percentile)
  }

  return(df)
}

#' Statistical anomaly detection (fallback method)
#'
#' @param df Original dataframe
#' @param scaled_features Standardized feature matrix
#' @return Dataframe with anomaly scores
detect_anomalies_statistical <- function(df, scaled_features) {

  message("   Using statistical anomaly detection (z-score method)")

  # Calculate z-scores for each feature
  z_scores <- abs(scaled_features)
  
  # Average z-scores across features
  df$anomaly_score <- rowMeans(z_scores, na.rm = TRUE)

  return(normalize_anomaly_scores(df))
}

#' Normalize anomaly scores and flag anomalies
#'
#' @param df Dataframe with anomaly_score column
#' @param threshold_percentile Percentile threshold
#' @return Dataframe with normalized scores and flags
normalize_anomaly_scores <- function(df, threshold_percentile = 0.9) {

  # Normalize to 0-1 range
  min_score <- min(df$anomaly_score, na.rm = TRUE)
  max_score <- max(df$anomaly_score, na.rm = TRUE)
  
  if (max_score > min_score) {
    df$anomaly_score <- (df$anomaly_score - min_score) / (max_score - min_score)
  }

  # Identify anomalies (top X% by score)
  threshold <- stats::quantile(df$anomaly_score, threshold_percentile, na.rm = TRUE)
  df$is_anomaly <- df$anomaly_score >= threshold

  message(sprintf("   Anomalies detected: %d companies (top %.0f%% with score > %.3f)", 
                  sum(df$is_anomaly, na.rm = TRUE), 
                  threshold_percentile * 100,
                  threshold))

  return(df)
}

#' Get top anomalies
#'
#' @param df Dataframe with anomaly scores
#' @param n Number of top anomalies to return (default: 10)
#' @return Dataframe with top anomalies
get_top_anomalies <- function(df, n = 10) {

  if (!"anomaly_score" %in% names(df)) {
    stop("Dataframe must contain 'anomaly_score' column")
  }

  top_anomalies <- df %>%
    dplyr::filter(!is.na(anomaly_score)) %>%
    dplyr::arrange(dplyr::desc(anomaly_score)) %>%
    dplyr::slice_head(n = n) %>%
    dplyr::select(ticker, ai_score, innovation_score, anomaly_score, 
                  dplyr::any_of(c("strategic_profile", "rd_intensity", 
                                  "patent_activity", "industry")))

  return(top_anomalies)
}

#' Plot anomaly score distribution
#'
#' @param df Dataframe with anomaly_score column
#' @param threshold_percentile Percentile threshold (default: 0.9)
#' @return ggplot object
plot_anomaly_distribution <- function(df, threshold_percentile = 0.9) {

  if (!"anomaly_score" %in% names(df)) {
    stop("Dataframe must contain 'anomaly_score' column")
  }

  threshold <- stats::quantile(df$anomaly_score, threshold_percentile, na.rm = TRUE)

  p <- ggplot2::ggplot(df, ggplot2::aes(x = anomaly_score)) +
    ggplot2::geom_histogram(bins = 30, fill = "steelblue", color = "white", alpha = 0.7) +
    ggplot2::geom_vline(xintercept = threshold, color = "red", 
                        linetype = "dashed", size = 1) +
    ggplot2::labs(
      title = "Anomaly Score Distribution",
      subtitle = paste("Top", threshold_percentile * 100, "% threshold shown in red"),
      x = "Anomaly Score",
      y = "Count"
    ) +
    ggplot2::theme_minimal()

  return(p)
}