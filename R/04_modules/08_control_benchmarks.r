# ============================================================================
# CONTROL BENCHMARK ANALYSIS MODULE
# Version: 1.0 | Date: 2026-03-26
# ============================================================================

library(dplyr)
library(tidyr)

#' Construct AI-Low Benchmark
#' 
#' Creates a benchmark of companies with minimal AI exposure,
#' matched to portfolio sector weights.
#' 
#' @param daii_scored Dataframe with AI scores and company data
#' @param portfolio_weights Dataframe with portfolio holdings and weights
#' @param n_components Number of companies in benchmark (default: 100)
#' @return Dataframe with benchmark weights
construct_ai_low_benchmark <- function(daii_scored, portfolio_weights, n_components = 100) {
  
  # Identify low-AI companies
  low_ai_companies <- daii_scored %>%
    filter(ai_label %in% c("AI Laggard", "AI Follower")) %>%
    arrange(ai_score) %>%
    head(n_components)
  
  # Get portfolio sector weights
  sector_weights <- portfolio_weights %>%
    left_join(daii_scored %>% select(ticker, TRBC_Industry), by = "ticker") %>%
    group_by(TRBC_Industry) %>%
    summarise(sector_weight = sum(fund_weight, na.rm = TRUE)) %>%
    ungroup()
  
  # Match sector weights in low-AI benchmark
  benchmark_weights <- low_ai_companies %>%
    group_by(TRBC_Industry) %>%
    mutate(
      sector_rank = row_number(),
      sector_n = n()
    ) %>%
    ungroup() %>%
    left_join(sector_weights, by = "TRBC_Industry") %>%
    mutate(
      # Distribute sector weight evenly among companies in that sector
      weight = sector_weight / sector_n,
      weight = ifelse(is.na(weight), 0, weight)
    ) %>%
    select(ticker, company_name, ai_score, TRBC_Industry, weight)
  
  return(benchmark_weights)
}

#' Construct Matched-Pair Controls
#' 
#' For each portfolio company, finds a non-AI peer with similar characteristics.
#' 
#' @param portfolio_companies Dataframe with portfolio holdings
#' @param universe Dataframe with all companies and AI scores
#' @param match_vars Variables to match on
#' @return Dataframe with matched pairs
construct_matched_controls <- function(portfolio_companies, universe, 
                                        match_vars = c("TRBC_Industry", "market_cap", "revenue_growth")) {
  
  matches <- data.frame()
  
  for(i in 1:nrow(portfolio_companies)) {
    
    portfolio_data <- portfolio_companies[i, ]
    
    # Find potential controls (low AI, similar characteristics)
    controls <- universe %>%
      filter(
        ai_label %in% c("AI Laggard", "AI Follower"),
        ticker != portfolio_data$ticker
      )
    
    # Apply matching criteria
    if("TRBC_Industry" %in% match_vars) {
      controls <- controls %>% filter(TRBC_Industry == portfolio_data$TRBC_Industry)
    }
    
    if("market_cap" %in% match_vars) {
      controls <- controls %>%
        filter(
          market_cap > portfolio_data$market_cap * 0.5,
          market_cap < portfolio_data$market_cap * 2
        )
    }
    
    # Find closest match on growth
    controls <- controls %>%
      mutate(
        growth_diff = abs(revenue_growth - portfolio_data$revenue_growth)
      ) %>%
      arrange(growth_diff) %>%
      slice(1)
    
    if(nrow(controls) > 0) {
      matches <- rbind(matches, data.frame(
        portfolio_ticker = portfolio_data$ticker,
        portfolio_company = portfolio_data$company_name,
        portfolio_ai_score = portfolio_data$ai_score,
        control_ticker = controls$ticker,
        control_company = controls$company_name,
        control_ai_score = controls$ai_score,
        ai_score_gap = portfolio_data$ai_score - controls$ai_score,
        sector = portfolio_data$TRBC_Industry,
        market_cap_ratio = portfolio_data$market_cap / controls$market_cap,
        growth_diff = portfolio_data$revenue_growth - controls$revenue_growth,
        stringsAsFactors = FALSE
      ))
    }
  }
  
  return(matches)
}

#' Calculate AI Alpha using Synthetic Control
#' 
#' Uses low-AI universe to predict counterfactual returns.
#' 
#' @param portfolio_returns Portfolio return time series
#' @param factor_returns Factor return data (market, size, value, momentum)
#' @param low_ai_returns Returns of low-AI companies
#' @param returns_data Daily returns for all companies
#' @return List with alpha metrics
calculate_synthetic_alpha <- function(portfolio_returns, factor_returns, 
                                       low_ai_returns, returns_data) {
  
  # Step 1: Train model on low-AI universe
  # (Simplified - in production, use proper factor models)
  
  # Align dates
  common_dates <- intersect(portfolio_returns$date, factor_returns$date)
  
  train_data <- low_ai_returns %>%
    filter(date %in% common_dates) %>%
    left_join(factor_returns, by = "date")
  
  # Fit model
  model <- lm(returns ~ market_return + size_return + value_return, data = train_data)
  
  # Step 2: Predict counterfactual returns
  predicted_returns <- predict(model, newdata = factor_returns %>% filter(date %in% common_dates))
  
  # Step 3: Calculate AI alpha
  actual_returns <- portfolio_returns %>%
    filter(date %in% common_dates) %>%
    pull(portfolio_return)
  
  ai_alpha <- actual_returns - predicted_returns
  
  return(list(
    alpha = mean(ai_alpha, na.rm = TRUE),
    alpha_annualized = mean(ai_alpha, na.rm = TRUE) * 252,
    cumulative_alpha = cumprod(1 + ai_alpha) - 1,
    t_stat = t.test(ai_alpha)$statistic,
    p_value = t.test(ai_alpha)$p.value,
    positive_days = mean(ai_alpha > 0, na.rm = TRUE) * 100
  ))
}

#' Compare All Control Methods
#' 
#' Returns a comparison table of all control methods.
#' 
#' @param portfolio_returns Portfolio returns
#' @param benchmark_returns Traditional benchmark returns
#' @param ai_low_returns AI-Low benchmark returns
#' @param matched_pairs_returns Matched-pair control returns
#' @return Dataframe with comparison
compare_control_methods <- function(portfolio_returns, benchmark_returns,
                                     ai_low_returns, matched_pairs_returns) {
  
  # Calculate excess returns over each control
  controls <- list(
    "S&P 500" = benchmark_returns %>% filter(benchmark == "SPX") %>% pull(return),
    "NASDAQ 100" = benchmark_returns %>% filter(benchmark == "IXIC") %>% pull(return),
    "AI-Low Benchmark" = ai_low_returns$return,
    "Matched-Pair Control" = matched_pairs_returns$return
  )
  
  results <- data.frame()
  
  for(control_name in names(controls)) {
    excess <- portfolio_returns$return - controls[[control_name]]
    
    results <- rbind(results, data.frame(
      Control_Method = control_name,
      Alpha = mean(excess, na.rm = TRUE),
      Alpha_Annualized = mean(excess, na.rm = TRUE) * 252,
      Tracking_Error = sd(excess, na.rm = TRUE) * sqrt(252),
      Information_Ratio = mean(excess, na.rm = TRUE) / sd(excess, na.rm = TRUE) * sqrt(252),
      Win_Rate = mean(excess > 0, na.rm = TRUE) * 100,
      stringsAsFactors = FALSE
    ))
  }
  
  return(results)
}