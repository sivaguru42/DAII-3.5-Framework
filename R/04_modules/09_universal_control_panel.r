# ============================================================================
# UNIVERSAL CONTROL PANEL MODULE
# Version: 1.0 | Date: 2026-03-30
# Description: Design, maintain, and validate a universal control panel
#              for maximum statistical power across diverse sample sizes.
# ============================================================================

library(dplyr)
library(tidyr)
library(pwr)

#' Design Universal Control Panel for Maximum Statistical Power
#' 
#' @param daii_scored Full universe of companies with AI scores
#' @param target_power Desired statistical power (default 0.8)
#' @param min_controls_per_stratum Minimum controls per stratum (default 10)
#' @param effect_size_estimate Expected monthly alpha from AI exposure (default 0.005 = 0.5%)
#' @return List with panel, metadata, quality metrics, and selection function
design_universal_control_panel <- function(daii_scored, 
                                           target_power = 0.8,
                                           min_controls_per_stratum = 10,
                                           effect_size_estimate = 0.005) {
  
  message("\n🎯 Designing Universal Control Panel for Statistical Power\n")
  message(sprintf("   Target power: %.1f%%", target_power * 100))
  message(sprintf("   Effect size estimate: %.2f%% monthly", effect_size_estimate * 100))
  
  # Step 1: Define stratification variables
  strata_vars <- c("size_tercile", "sector", "region", "volatility_tercile")
  
  # Step 2: Prepare eligible controls (low-AI companies)
  eligible_controls <- daii_scored %>%
    filter(
      ai_label %in% c("AI Laggard", "AI Follower"),
      ai_score <= 0.3,
      !is.na(market_cap), market_cap > 0,
      !is.na(revenue_growth),
      !is.na(volatility),
      volatility > 0,
      !is.na(TRBC_Industry)
    ) %>%
    mutate(
      # Size terciles (captures market beta)
      size_tercile = ntile(market_cap, 3),
      size_label = case_when(
        size_tercile == 1 ~ "Small",
        size_tercile == 2 ~ "Mid",
        TRUE ~ "Large"
      ),
      # Volatility terciles (captures risk)
      volatility_tercile = ntile(volatility, 3),
      vol_label = case_when(
        volatility_tercile == 1 ~ "Low Vol",
        volatility_tercile == 2 ~ "Mid Vol",
        TRUE ~ "High Vol"
      ),
      # Region (captures geographic exposure)
      region = case_when(
        Country == "USA" ~ "US",
        Country %in% c("DEU", "GBR", "FRA", "NLD", "CHE") ~ "Europe",
        Country %in% c("JPN", "KOR", "CHN") ~ "Asia",
        TRUE ~ "Other"
      ),
      # Sector (captures industry effects)
      sector = case_when(
        grepl("Semiconductor|Software|Technology|Hardware|Electronics", TRBC_Industry, ignore.case = TRUE) ~ "Tech",
        grepl("Pharmaceutical|Biotech|Medical|Biotechnology", TRBC_Industry, ignore.case = TRUE) ~ "Healthcare",
        grepl("Bank|Financial|Insurance|Investment|Services", TRBC_Industry, ignore.case = TRUE) ~ "Financials",
        grepl("Retail|Consumer|Household|Food|Beverage", TRBC_Industry, ignore.case = TRUE) ~ "Consumer",
        grepl("Energy|Oil|Gas|Resources", TRBC_Industry, ignore.case = TRUE) ~ "Energy",
        grepl("Industrial|Capital|Machinery|Construction", TRBC_Industry, ignore.case = TRUE) ~ "Industrials",
        TRUE ~ "Other"
      ),
      # Quality score (lower AI is better)
      control_quality_score = 1 - (ai_score / 0.3)
    )
  
  message(sprintf("   Eligible controls: %d companies", nrow(eligible_controls)))
  
  if(nrow(eligible_controls) < 50) {
    warning("Limited eligible controls available. Power may be reduced.")
  }
  
  # Step 3: Calculate required sample size for target power
  # Using power.t.test for two-sample comparison
  power_result <- pwr.t.test(
    d = effect_size_estimate / 0.05,  # Cohen's d (effect / pooled SD)
    power = target_power,
    sig.level = 0.05,
    type = "two.sample",
    alternative = "two.sided"
  )
  
  required_n <- ceiling(power_result$n)
  message(sprintf("   Required controls for power = %.1f: %d per group", 
                  target_power, required_n))
  
  # Step 4: Calculate target panel size (over-sample by 2-3x)
  panel_size <- max(required_n * 3, 100)
  message(sprintf("   Target panel size: %d controls (%.0fx over-sampled)", 
                  panel_size, panel_size / required_n))
  
  # Step 5: Calculate samples per stratum
  stratum_counts <- eligible_controls %>%
    group_by(size_tercile, sector, region, volatility_tercile) %>%
    summarise(n_available = n(), .groups = "drop") %>%
    mutate(
      proportion = n_available / sum(n_available),
      target_n = ceiling(panel_size * proportion),
      target_n = pmax(target_n, min_controls_per_stratum),
      target_n = pmin(target_n, n_available)  # Can't exceed available
    ) %>%
    filter(target_n > 0)
  
  message(sprintf("   Strata: %d combinations", nrow(stratum_counts)))
  
  # Step 6: Select controls using stratified random sampling
  set.seed(42)
  universal_panel <- data.frame()
  selection_details <- data.frame()
  
  for(i in 1:nrow(stratum_counts)) {
    stratum <- stratum_counts[i, ]
    
    stratum_controls <- eligible_controls %>%
      filter(
        size_tercile == stratum$size_tercile,
        sector == stratum$sector,
        region == stratum$region,
        volatility_tercile == stratum$volatility_tercile
      )
    
    n_select <- stratum$target_n
    
    if(n_select > 0 && nrow(stratum_controls) >= n_select) {
      selected <- stratum_controls %>%
        slice_sample(n = n_select) %>%
        mutate(
          stratum_id = i,
          selection_weight = n_select / nrow(stratum_controls)
        )
      
      universal_panel <- bind_rows(universal_panel, selected)
      
      selection_details <- rbind(selection_details, data.frame(
        stratum_id = i,
        size = stratum$size_tercile,
        sector = stratum$sector,
        region = stratum$region,
        volatility = stratum$volatility_tercile,
        n_available = nrow(stratum_controls),
        n_selected = n_select,
        selection_rate = n_select / nrow(stratum_controls)
      ))
    }
  }
  
  message(sprintf("   Selected %d controls", nrow(universal_panel)))
  
  # Step 7: Calculate panel quality metrics
  panel_quality <- universal_panel %>%
    summarise(
      total_controls = n(),
      unique_sectors = n_distinct(sector),
      unique_regions = n_distinct(region),
      avg_ai_score = mean(ai_score, na.rm = TRUE),
      max_ai_score = max(ai_score, na.rm = TRUE),
      ai_score_sd = sd(ai_score, na.rm = TRUE),
      ai_score_025 = quantile(ai_score, 0.025, na.rm = TRUE),
      ai_score_975 = quantile(ai_score, 0.975, na.rm = TRUE),
      avg_quality = mean(control_quality_score, na.rm = TRUE),
      coverage_score = n_distinct(paste(size_tercile, sector, region, volatility_tercile)) / 
                       nrow(stratum_counts) * 100,
      .groups = "drop"
    )
  
  # Step 8: Calculate effective sample size and achievable power
  effective_n <- panel_quality$total_controls
  achievable_power <- pwr.t.test(
    n = effective_n,
    d = effect_size_estimate / 0.05,
    sig.level = 0.05,
    type = "two.sample"
  )$power
  
  message("\n   📊 Panel Quality Metrics:")
  message(sprintf("      Coverage: %.1f%% of strata", panel_quality$coverage_score))
  message(sprintf("      Avg AI Score: %.3f (max: %.3f)", 
                  panel_quality$avg_ai_score, panel_quality$max_ai_score))
  message(sprintf("      AI Score SD: %.3f", panel_quality$ai_score_sd))
  message(sprintf("      Achievable Power: %.1f%% (target: %.1f%%)", 
                  achievable_power * 100, target_power * 100))
  
  # Step 9: Create adaptive selection function
  select_matched_controls <- function(test_companies, 
                                      control_panel = universal_panel,
                                      method = "optimal",
                                      k = 1) {
    
    results <- data.frame()
    
    for(i in 1:nrow(test_companies)) {
      test <- test_companies[i, ]
      
      # Skip if missing key data
      if(is.na(test$market_cap) || is.na(test$revenue_growth)) next
      
      # Calculate distance scores
      distances <- control_panel %>%
        mutate(
          # Size distance (log scale for multiplicative effects)
          size_dist = abs(log(market_cap) - log(test$market_cap)) / 
                      (sd(log(control_panel$market_cap)) + 0.01),
          # Growth distance
          growth_dist = abs(revenue_growth - test$revenue_growth) / 
                        (sd(control_panel$revenue_growth) + 0.01),
          # Volatility distance
          vol_dist = if("volatility" %in% names(test) && !is.na(test$volatility)) {
            abs(volatility - test$volatility) / (sd(control_panel$volatility) + 0.01)
          } else { 0 },
          # Combined distance
          total_dist = size_dist + growth_dist + vol_dist,
          # Bonus for same sector/region (reduces distance)
          sector_bonus = ifelse(sector == test$sector, -0.15, 0),
          region_bonus = ifelse(region == test$region, -0.1, 0),
          final_distance = total_dist + sector_bonus + region_bonus
        ) %>%
        arrange(final_distance)
      
      # Select matches based on method
      if(method == "optimal") {
        selected <- distances %>% filter(final_distance < 1)
        if(nrow(selected) == 0) selected <- distances %>% head(k)
      } else if(method == "nearest") {
        selected <- distances %>% head(k)
      } else if(method == "ensemble") {
        selected <- distances %>% head(5)
      }
      
      # Record matches
      for(j in 1:nrow(selected)) {
        control <- selected[j, ]
        
        results <- rbind(results, data.frame(
          portfolio_ticker = test$ticker,
          portfolio_name = test$company_name,
          portfolio_ai_score = test$ai_score,
          portfolio_weight = ifelse(is.na(test$fund_weight), 0, test$fund_weight),
          portfolio_mcap = test$market_cap,
          portfolio_growth = test$revenue_growth,
          control_ticker = control$ticker,
          control_name = control$company_name,
          control_ai_score = control$ai_score,
          control_quality = control$control_quality_score,
          control_mcap = control$market_cap,
          control_growth = control$revenue_growth,
          ai_score_gap = test$ai_score - control$ai_score,
          match_distance = control$final_distance,
          match_quality = 1 / (1 + control$final_distance),
          sector_match = ifelse(control$sector == test$sector, 1, 0),
          region_match = ifelse(control$region == test$region, 1, 0),
          method = method,
          stringsAsFactors = FALSE
        ))
      }
    }
    
    return(results)
  }
  
  # Step 10: Save panel metadata
  panel_metadata <- data.frame(
    design_date = as.character(Sys.Date()),
    pipeline_version = "5.3 FINAL",
    target_power = target_power,
    effect_size_estimate = effect_size_estimate,
    panel_size = nrow(universal_panel),
    strata_count = nrow(stratum_counts),
    coverage_pct = round(panel_quality$coverage_score, 1),
    achievable_power = round(achievable_power, 3),
    eligible_controls = nrow(eligible_controls),
    stringsAsFactors = FALSE
  )
  
  # Save to disk
  saveRDS(universal_panel, "data/00_reference/universal_control_panel.rds")
  saveRDS(panel_metadata, "data/00_reference/universal_control_panel_metadata.rds")
  saveRDS(select_matched_controls, "data/00_reference/control_selection_function.rds")
  
  message("\n   💾 Saved universal control panel to data/00_reference/")
  
  return(list(
    panel = universal_panel,
    metadata = panel_metadata,
    quality = panel_quality,
    select_matches = select_matched_controls,
    strata = stratum_counts,
    selection_details = selection_details,
    power_result = list(
      target_power = target_power,
      achievable_power = achievable_power,
      required_n = required_n,
      effective_n = effective_n
    )
  ))
}

#' Calculate statistical power for a given sample
#' 
#' @param portfolio_size Number of portfolio companies
#' @param control_panel_size Size of control panel
#' @param effect_size Expected AI alpha effect size
#' @return Power estimate
calculate_power <- function(portfolio_size, 
                            control_panel_size = NULL,
                            effect_size = 0.005) {
  
  if(is.null(control_panel_size)) {
    # Load panel metadata
    panel_metadata <- readRDS("data/00_reference/universal_control_panel_metadata.rds")
    control_panel_size <- panel_metadata$panel_size
  }
  
  # Effective sample size for two-sample t-test
  # Using harmonic mean of group sizes
  effective_n <- 2 / (1/portfolio_size + 1/control_panel_size)
  
  power_result <- pwr.t.test(
    n = effective_n,
    d = effect_size / 0.05,
    sig.level = 0.05,
    type = "two.sample"
  )
  
  return(list(
    portfolio_size = portfolio_size,
    control_panel_size = control_panel_size,
    effective_n = effective_n,
    power = power_result$power,
    interpretation = case_when(
      power_result$power >= 0.8 ~ "Adequate power for hypothesis testing",
      power_result$power >= 0.6 ~ "Moderate power - consider larger sample",
      TRUE ~ "Low power - results may be unreliable"
    )
  ))
}

#' Run power simulation for validation
#' 
#' @param universal_panel The universal control panel
#' @param n_simulations Number of simulations to run
#' @param effect_sizes Vector of effect sizes to test
#' @return Dataframe with simulation results
simulate_power <- function(universal_panel, 
                           n_simulations = 500,
                           effect_sizes = c(0.001, 0.002, 0.003, 0.005, 0.007, 0.01)) {
  
  message("\n🔬 Running Power Simulation\n")
  message(sprintf("   Simulations per effect size: %d", n_simulations))
  
  results <- data.frame()
  
  for(effect in effect_sizes) {
    message(sprintf("   Simulating effect size: %.2f%% monthly", effect * 100))
    
    power_count <- 0
    
    for(i in 1:n_simulations) {
      # Simulate portfolio returns with AI alpha
      n_portfolio <- sample(10:50, 1)  # Random portfolio size
      
      portfolio_sample <- universal_panel$panel %>%
        slice_sample(n = n_portfolio, replace = TRUE) %>%
        mutate(
          ai_exposure = ai_score,
          simulated_return = rnorm(n(), mean = effect * ai_exposure, sd = 0.02)
        )
      
      # Simulate control returns (no AI alpha)
      control_sample <- universal_panel$panel %>%
        slice_sample(n = n_portfolio, replace = TRUE) %>%
        mutate(
          simulated_return = rnorm(n(), mean = 0, sd = 0.02)
        )
      
      # T-test for difference
      t_result <- t.test(portfolio_sample$simulated_return, 
                         control_sample$simulated_return)
      
      if(t_result$p.value < 0.05) power_count <- power_count + 1
    }
    
    power <- power_count / n_simulations
    results <- rbind(results, data.frame(
      effect_size = effect,
      effect_size_pct = effect * 100,
      power = power,
      power_pct = power * 100,
      n_simulations = n_simulations,
      interpretation = case_when(
        power >= 0.8 ~ "Adequate",
        power >= 0.6 ~ "Moderate",
        TRUE ~ "Low"
      )
    ))
    
    message(sprintf("      → Power: %.1f%% (%s)", power * 100, tail(results$interpretation, 1)))
  }
  
  return(results)
}