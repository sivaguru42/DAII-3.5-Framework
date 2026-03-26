# ============================================================================
# MODULE 4: AI Exposure Cube
# Version: 2.0 | Date: 2026-03-17
# Description: Creates strategic profiles and exposure categories based on
#              AI and innovation scores. FIXED version - no '...' error.
# ============================================================================

#' Create AI Exposure Cube
#'
#' Generates strategic profiles and exposure categories for all companies
#' based on their AI and innovation quartiles.
#'
#' @param df Dataframe with ai_quartile and innovation_quartile columns
#' @return Dataframe with added strategic_profile, ai_exposure, innovation_exposure
#' @export
create_ai_cube <- function(df) {
  
  message("\n🧊 Creating AI cube...")
  
  # Input validation
  required_cols <- c("ai_quartile", "innovation_quartile")
  missing_cols <- setdiff(required_cols, names(df))
  
  if(length(missing_cols) > 0) {
    stop("❌ Missing required columns: ", paste(missing_cols, collapse = ", "))
  }
  
  # Create strategic profiles
  df <- df %>%
    dplyr::mutate(
      # Strategic profiles based on quartile combinations
      strategic_profile = dplyr::case_when(
        ai_quartile == 4 & innovation_quartile == 4 ~ "AI Pioneer",
        ai_quartile == 4 & innovation_quartile <= 2 ~ "AI Focused",
        ai_quartile <= 2 & innovation_quartile == 4 ~ "Innovation Leader",
        ai_quartile >= 3 & innovation_quartile >= 3 ~ "Balanced Performer",
        TRUE ~ "Underperformer"
      ),
      
      # Exposure categories
      ai_exposure = dplyr::case_when(
        ai_quartile == 4 ~ "High",
        ai_quartile == 3 ~ "Medium",
        TRUE ~ "Low"
      ),
      
      innovation_exposure = dplyr::case_when(
        innovation_quartile == 4 ~ "High",
        innovation_quartile == 3 ~ "Medium",
        TRUE ~ "Low"
      )
    )
  
  # Fill any missing values
  df$strategic_profile[is.na(df$strategic_profile)] <- "Underperformer"
  df$ai_exposure[is.na(df$ai_exposure)] <- "Low"
  df$innovation_exposure[is.na(df$innovation_exposure)] <- "Low"
  
  # Summary
  cat("\n📊 Strategic Profile Distribution:\n")
  print(table(df$strategic_profile))
  
  cat("\n📊 AI Exposure Distribution:\n")
  print(table(df$ai_exposure))
  
  return(df)
}

#' Fill missing strategic profiles
#'
#' Ensures all companies have a strategic profile by filling NAs
#' based on quartile logic.
#'
#' @param df Dataframe with strategic_profile column (may have NAs)
#' @return Dataframe with all strategic profiles filled
#' @export
fill_strategic_profiles <- function(df) {
  
  na_count <- sum(is.na(df$strategic_profile))
  
  if (na_count == 0) {
    message("   ✅ No missing strategic profiles found")
    return(df)
  }
  
  message(sprintf("   ⚠️ Filling %d missing strategic profiles...", na_count))
  
  df <- df %>%
    dplyr::mutate(
      strategic_profile = ifelse(
        is.na(strategic_profile),
        dplyr::case_when(
          ai_quartile == 4 & innovation_quartile == 4 ~ "AI Pioneer",
          ai_quartile == 4 ~ "AI Focused",
          innovation_quartile == 4 ~ "Innovation Leader",
          ai_quartile >= 3 & innovation_quartile >= 3 ~ "Balanced",
          TRUE ~ "Underperformer"
        ),
        strategic_profile
      )
    )
  
  message(sprintf("   ✅ After fix: %d NAs remaining", 
                  sum(is.na(df$strategic_profile))))
  
  return(df)
}