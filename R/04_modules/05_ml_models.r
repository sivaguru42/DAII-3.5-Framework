# =============================================================================
# MODULE 5: ML Models - Random Forest for AI Leader Prediction
# Version: 1.0 | Date: 2026-03-11
# Description: Trains Random Forest model to predict AI Leaders (top quartile)
#              and calculates feature importance.
# =============================================================================

#' Train Random Forest model to predict AI Leaders
#'
#' @param df Dataframe with features and ai_quartile column
#' @param seed Random seed for reproducibility (default: 42)
#' @return List containing model, feature importance, and predictions
#' @export
train_ai_leader_model <- function(df, seed = 42) {

  message("🌲 [Module 5] Training Random Forest model to predict AI Leaders...")

  # Prepare features
  feature_cols <- c("rd_intensity", "patent_activity", "revenue_growth", 
                    "volatility", "market_cap", "total_return")
  
  # Check which features exist
  available_features <- intersect(feature_cols, names(df))
  missing_features <- setdiff(feature_cols, available_features)
  
  if (length(missing_features) > 0) {
    message("   ⚠️ Missing features: ", paste(missing_features, collapse = ", "))
    message("   Continuing with available features only.")
  }

  # Create feature matrix
  ml_features <- df %>%
    dplyr::select(dplyr::all_of(available_features)) %>%
    dplyr::mutate(dplyr::across(dplyr::everything(), as.numeric))

  # Target variable: AI Leader (top quartile)
  target <- factor(ifelse(df$ai_quartile == 4, "Leader", "Other"))

  # Check if we have enough data
  n_leaders <- sum(target == "Leader")
  n_others <- sum(target == "Other")

  message(sprintf("   Training data: %d leaders, %d others", n_leaders, n_others))

  if (nrow(ml_features) < 20 || n_leaders < 5) {
    message("   ⚠️ Insufficient data for Random Forest. Returning NULL.")
    return(NULL)
  }

  # Train Random Forest
  set.seed(seed)
  
  tryCatch({
    rf_model <- randomForest::randomForest(
      x = ml_features,
      y = target,
      ntree = 100,
      importance = TRUE,
      proximity = FALSE,
      na.action = na.omit
    )

    # Feature importance
    feature_importance <- as.data.frame(randomForest::importance(rf_model))
    feature_importance$feature <- rownames(feature_importance)
    feature_importance <- feature_importance %>%
      dplyr::arrange(dplyr::desc(MeanDecreaseGini))

    # Predictions
    predictions <- predict(rf_model, ml_features)

    message("\n🔍 Top 5 Predictive Features:")
    print(utils::head(feature_importance[, c("feature", "MeanDecreaseGini")], 5))

    # Confusion matrix
    cm <- table(Predicted = predictions, Actual = target)
    message("\n📊 Confusion Matrix:")
    print(cm)

    # Calculate accuracy
    accuracy <- sum(diag(cm)) / sum(cm)
    message(sprintf("\n✅ Model Accuracy: %.1f%%", accuracy * 100))

    return(list(
      model = rf_model,
      feature_importance = feature_importance,
      predictions = predictions,
      accuracy = accuracy,
      features_used = available_features
    ))

  }, error = function(e) {
    message("   ❌ Random Forest failed: ", e$message)
    return(NULL)
  })
}

#' Get feature importance plot
#'
#' @param feature_importance Dataframe from train_ai_leader_model()
#' @param top_n Number of top features to plot (default: 10)
#' @return ggplot object
plot_feature_importance <- function(feature_importance, top_n = 10) {

  plot_data <- feature_importance %>%
    dplyr::slice_max(MeanDecreaseGini, n = top_n) %>%
    dplyr::mutate(feature = forcats::fct_reorder(feature, MeanDecreaseGini))

  p <- ggplot2::ggplot(plot_data, 
                       ggplot2::aes(x = MeanDecreaseGini, y = feature)) +
    ggplot2::geom_col(fill = "steelblue") +
    ggplot2::labs(
      title = "Feature Importance for AI Leader Prediction",
      x = "Mean Decrease Gini",
      y = NULL
    ) +
    ggplot2::theme_minimal()

  return(p)
}