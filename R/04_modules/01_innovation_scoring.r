# =============================================================================
# MODULE 1-3: Innovation Scoring (WITH IMPUTATION FLAGGING)
# Version: 2.2 | Date: 2026-03-30
# Description: Calculates quartiles for R&D, patents, growth, and volatility,
#              then computes innovation scores and labels.
#              Includes imputation for missing values with flagging.
# =============================================================================

#' Calculate innovation scores for all companies
#'
#' @param df Dataframe with columns: rd_intensity, patent_activity, 
#'           revenue_growth, volatility, total_patents
#' @return Dataframe with added columns: rd_quartile, patent_quartile, 
#'         growth_quartile, volatility_quartile, innovation_score,
#'         innovation_quartile, innovation_label, and imputation flags
#' @export
calculate_innovation_scores <- function(df) {
  
  message("   [Module 1-3] Calculating innovation scores...")
  
  # Ensure we're working with company-level data
  df <- df %>%
    dplyr::distinct(ticker, .keep_all = TRUE)
  
  # Initialize imputation flags
  df <- df %>%
    mutate(
      rd_imputed = FALSE,
      growth_imputed = FALSE,
      volatility_imputed = FALSE,
      innovation_imputed = FALSE
    )
  
  # ==========================================================================
  # IMPUTATION: Fill missing rd_intensity using patent data
  # ==========================================================================
  missing_before <- sum(is.na(df$rd_intensity))
  
  if(missing_before > 0) {
    message(sprintf("   ℹ️ Missing rd_intensity: %d companies", missing_before))
    
    # Get median rd_intensity by industry for imputation
    industry_medians <- df %>%
      filter(!is.na(rd_intensity), rd_intensity > 0) %>%
      group_by(industry) %>%
      summarise(median_rd = median(rd_intensity, na.rm = TRUE), .groups = "drop")
    
    df <- df %>%
      left_join(industry_medians, by = "industry") %>%
      mutate(
        # Impute based on patent activity first
        rd_imputed = is.na(rd_intensity),
        rd_intensity = case_when(
          is.na(rd_intensity) & !is.na(patent_activity) & patent_activity > 0 ~
            pmin(0.15, log(patent_activity + 1) / 1000),  # Patent-based estimate
          is.na(rd_intensity) & !is.na(total_patents) & total_patents > 0 ~
            pmin(0.15, log(total_patents + 1) / 1000),     # Total patents estimate
          is.na(rd_intensity) & !is.na(median_rd) ~ median_rd,  # Industry median
          is.na(rd_intensity) ~ 0.01,                         # Default 1%
          TRUE ~ rd_intensity
        )
      ) %>%
      select(-median_rd)
    
    missing_after <- sum(is.na(df$rd_intensity))
    imputed_count <- missing_before - missing_after
    message(sprintf("   ✅ Imputed rd_intensity for %d companies", imputed_count))
  }
  
  # ==========================================================================
  # IMPUTATION: Fill missing revenue_growth
  # ==========================================================================
  growth_missing <- sum(is.na(df$revenue_growth))
  if(growth_missing > 0) {
    median_growth <- median(df$revenue_growth, na.rm = TRUE)
    df <- df %>%
      mutate(
        growth_imputed = is.na(revenue_growth),
        revenue_growth = ifelse(is.na(revenue_growth), median_growth, revenue_growth)
      )
    message(sprintf("   ℹ️ Imputed revenue_growth for %d companies (median: %.3f)", 
                    growth_missing, median_growth))
  }
  
  # ==========================================================================
  # IMPUTATION: Fill missing volatility
  # ==========================================================================
  vol_missing <- sum(is.na(df$volatility))
  if(vol_missing > 0) {
    median_vol <- median(df$volatility, na.rm = TRUE)
    df <- df %>%
      mutate(
        volatility_imputed = is.na(volatility),
        volatility = ifelse(is.na(volatility), median_vol, volatility)
      )
    message(sprintf("   ℹ️ Imputed volatility for %d companies (median: %.3f)", 
                    vol_missing, median_vol))
  }
  
  # ==========================================================================
  # CALCULATE QUARTILES
  # ==========================================================================
  df <- df %>%
    dplyr::mutate(
      # R&D Intensity Quartile (higher is better)
      rd_quartile = dplyr::ntile(rd_intensity, 4),
      
      # Patent Activity Quartile (higher is better)
      patent_quartile = dplyr::ntile(patent_activity, 4),
      
      # Revenue Growth Quartile (higher is better)
      growth_quartile = dplyr::ntile(revenue_growth, 4),
      
      # Volatility Quartile (lower is better - invert)
      volatility_quartile = 5 - dplyr::ntile(volatility, 4)
    )
  
  # Handle potential NA in quartiles (companies with all same values)
  df <- df %>%
    mutate(
      rd_quartile = ifelse(is.na(rd_quartile), 2, rd_quartile),
      patent_quartile = ifelse(is.na(patent_quartile), 2, patent_quartile),
      growth_quartile = ifelse(is.na(growth_quartile), 2, growth_quartile),
      volatility_quartile = ifelse(is.na(volatility_quartile), 2, volatility_quartile)
    )
  
  # ==========================================================================
  # CALCULATE INNOVATION SCORE
  # ==========================================================================
  df <- df %>%
    dplyr::mutate(
      # Innovation Score (weighted average of quartiles, normalized 0-1)
      innovation_score = (
        rd_quartile * 0.30 +
          patent_quartile * 0.30 +
          growth_quartile * 0.20 +
          volatility_quartile * 0.20
      ) / 4,
      
      # Innovation Quartile (final categorization)
      innovation_quartile = dplyr::ntile(innovation_score, 4),
      
      innovation_label = dplyr::case_when(
        innovation_quartile == 4 ~ "Leader",
        innovation_quartile == 3 ~ "Strong",
        innovation_quartile == 2 ~ "Developing",
        TRUE ~ "Emerging"
      ),
      
      # Flag if any component was imputed (affects innovation score)
      innovation_imputed = rd_imputed | growth_imputed | volatility_imputed
    )
  
  # Final check for any remaining NA
  final_missing <- sum(is.na(df$innovation_score))
  if(final_missing > 0) {
    df$innovation_score[is.na(df$innovation_score)] <- 0.5
    df$innovation_quartile[is.na(df$innovation_quartile)] <- 2
    df$innovation_label[is.na(df$innovation_label)] <- "Emerging"
    df$innovation_imputed[is.na(df$innovation_imputed)] <- TRUE
    message(sprintf("   ⚠️ Filled %d remaining NA innovation scores with default", final_missing))
  }
  
  # ==========================================================================
  # SUMMARY REPORT
  # ==========================================================================
  message(sprintf("   [Module 1-3] Innovation scores calculated for %d companies", 
                  nrow(df)))
  message("   Distribution:")
  print(table(df$innovation_label))
  
  # Imputation summary
  cat("\n   📊 IMPUTATION SUMMARY:\n")
  cat(sprintf("      rd_intensity imputed: %d (%.1f%%)\n", 
              sum(df$rd_imputed), mean(df$rd_imputed) * 100))
  cat(sprintf("      revenue_growth imputed: %d (%.1f%%)\n", 
              sum(df$growth_imputed), mean(df$growth_imputed) * 100))
  cat(sprintf("      volatility imputed: %d (%.1f%%)\n", 
              sum(df$volatility_imputed), mean(df$volatility_imputed) * 100))
  cat(sprintf("      innovation_score imputed: %d (%.1f%%)\n", 
              sum(df$innovation_imputed), mean(df$innovation_imputed) * 100))
  
  return(df)
}