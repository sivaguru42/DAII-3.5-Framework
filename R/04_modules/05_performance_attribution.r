# ============================================================================
# PERFORMANCE ATTRIBUTION MODULE
# File: R/04_modules/05_performance_attribution.r
# Version: 1.0 | Date: 2026-03-26
# ============================================================================

library(dplyr)
library(tidyr)

#' Calculate portfolio returns and attribution
#' @param holdings_data Dataframe with holdings and weights
#' @param benchmark_data Dataframe with benchmark returns
#' @param return_data Dataframe with stock returns (daily)
#' @param start_date Start date for analysis
#' @param end_date End date for analysis
#' @return List with attribution results and metrics
calculate_performance_attribution <- function(holdings_data, 
                                              benchmark_data,
                                              return_data,
                                              start_date = NULL,
                                              end_date = NULL) {
  
  # Filter date range
  if(!is.null(start_date)) {
    return_data <- return_data %>% filter(date >= as.Date(start_date))
    benchmark_data <- benchmark_data %>% filter(date >= as.Date(start_date))
  }
  if(!is.null(end_date)) {
    return_data <- return_data %>% filter(date <= as.Date(end_date))
    benchmark_data <- benchmark_data %>% filter(date <= as.Date(end_date))
  }
  
  # Ensure daily returns are available
  if(nrow(return_data) == 0) {
    warning("No return data available for specified date range")
    return(NULL)
  }
  
  # Calculate portfolio returns (weighted average)
  portfolio_returns <- return_data %>%
    inner_join(holdings_data %>% select(Ticker, fund_weight), 
               by = c("ticker" = "Ticker")) %>%
    mutate(weighted_return = daily_return * fund_weight) %>%
    group_by(date) %>%
    summarise(
      portfolio_return = sum(weighted_return, na.rm = TRUE),
      n_stocks = n(),
      .groups = "drop"
    )
  
  # Merge with benchmark
  attribution <- portfolio_returns %>%
    left_join(benchmark_data, by = "date") %>%
    mutate(
      excess_return = portfolio_return - benchmark_return,
      cumulative_portfolio = cumprod(1 + coalesce(portfolio_return, 0)),
      cumulative_benchmark = cumprod(1 + coalesce(benchmark_return, 0)),
      cumulative_excess = cumulative_portfolio / cumulative_benchmark - 1
    )
  
  # Calculate attribution metrics
  metrics <- attribution %>%
    summarise(
      total_return = prod(1 + portfolio_return, na.rm = TRUE) - 1,
      benchmark_return = prod(1 + benchmark_return, na.rm = TRUE) - 1,
      alpha = total_return - benchmark_return,
      tracking_error = sd(portfolio_return - benchmark_return, na.rm = TRUE) * sqrt(252),
      information_ratio = alpha / tracking_error,
      beta = cov(portfolio_return, benchmark_return, use = "complete.obs") / 
             var(benchmark_return, use = "complete.obs"),
      correlation = cor(portfolio_return, benchmark_return, use = "complete.obs"),
      sharpe_ratio = mean(portfolio_return, na.rm = TRUE) / sd(portfolio_return, na.rm = TRUE) * sqrt(252),
      max_drawdown = max(cummax(cumulative_portfolio) - cumulative_portfolio, na.rm = TRUE),
      win_rate = mean(portfolio_return > benchmark_return, na.rm = TRUE) * 100
    )
  
  return(list(
    attribution = attribution,
    metrics = metrics,
    summary = data.frame(
      Metric = names(metrics),
      Value = as.numeric(metrics)
    )
  ))
}

#' Download benchmark data from daily_ratios
#' @param benchmarks Vector of benchmark tickers (e.g., c(".SPX", ".IXIC"))
#' @param daily_ratios Dataframe with daily returns
#' @return Dataframe with benchmark returns
get_benchmark_data <- function(benchmarks = c(".SPX", ".IXIC"), 
                               daily_ratios = NULL) {
  
  if(is.null(daily_ratios)) {
    daily_ratios <- readRDS("data/01_raw/daily_ratios.rds")
  }
  
  benchmark_returns <- daily_ratios %>%
    filter(ticker %in% benchmarks) %>%
    select(date, ticker, daily_return) %>%
    arrange(ticker, date)
  
  return(benchmark_returns)
}

#' Create benchmark comparison table
#' @param portfolio_data Your company features data
#' @param attribution_results Results from calculate_performance_attribution
#' @return Comparison summary dataframe
create_benchmark_comparison <- function(portfolio_data, attribution_results) {
  
  if(is.null(attribution_results)) return(NULL)
  
  # Portfolio AI score distribution
  portfolio_summary <- portfolio_data %>%
    filter(in_portfolio == TRUE) %>%
    summarise(
      n_companies = n(),
      avg_ai_score = mean(ai_score, na.rm = TRUE),
      pct_ai_leaders = mean(ai_label == "AI Leader", na.rm = TRUE) * 100,
      total_exposure = sum(total_net_exposure_usd, na.rm = TRUE),
      .groups = "drop"
    )
  
  # Create comparison dataframe
  comparison <- data.frame(
    Metric = c(
      "Companies Covered",
      "Avg AI Score", 
      "% AI Leaders",
      "Total Return (%)",
      "Annualized Volatility (%)",
      "Sharpe Ratio",
      "Alpha (%)",
      "Beta",
      "Information Ratio",
      "Max Drawdown (%)",
      "Win Rate (%)"
    ),
    Portfolio = c(
      portfolio_summary$n_companies,
      round(portfolio_summary$avg_ai_score, 3),
      round(portfolio_summary$pct_ai_leaders, 1),
      round(attribution_results$metrics$total_return * 100, 2),
      round(attribution_results$metrics$tracking_error * 100, 2),
      round(attribution_results$metrics$sharpe_ratio, 2),
      round(attribution_results$metrics$alpha * 100, 2),
      round(attribution_results$metrics$beta, 2),
      round(attribution_results$metrics$information_ratio, 2),
      round(attribution_results$metrics$max_drawdown * 100, 2),
      round(attribution_results$metrics$win_rate, 1)
    )
  )
  
  return(comparison)
}