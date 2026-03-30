# ============================================================================
# BENCHMARK RETURNS MODULE
# Version: 1.0 | Date: 2026-03-30
# ============================================================================

library(dplyr)
library(tidyr)

# Benchmark definitions
BENCHMARKS <- list(
  "S&P 500" = list(ticker = ".SPX", name = "S&P 500", type = "market"),
  "NASDAQ 100" = list(ticker = ".IXIC", name = "NASDAQ 100", type = "market"),
  "S&P 400 Mid Cap" = list(ticker = ".MID", name = "S&P 400 Mid Cap", type = "market"),
  "S&P 600 Small Cap" = list(ticker = ".SPCY", name = "S&P 600 Small Cap", type = "market"),
  "Russell 2000" = list(ticker = ".RUT", name = "Russell 2000", type = "market")
)

#' Get benchmark returns from daily_ratios
#' @param benchmarks Vector of benchmark names or tickers
#' @param start_date Start date for returns
#' @param end_date End date for returns
#' @return Dataframe with benchmark returns
get_benchmark_returns <- function(benchmarks = c("S&P 500", "NASDAQ 100", "Russell 2000"),
                                  start_date = "2020-01-01",
                                  end_date = Sys.Date()) {
  
  # Load daily ratios
  daily_ratios <- readRDS("data/01_raw/daily_ratios.rds")
  
  # Map benchmark names to tickers
  benchmark_tickers <- c()
  for(b in benchmarks) {
    if(b %in% names(BENCHMARKS)) {
      benchmark_tickers <- c(benchmark_tickers, BENCHMARKS[[b]]$ticker)
    } else {
      benchmark_tickers <- c(benchmark_tickers, b)
    }
  }
  
  # Filter and format
  benchmark_returns <- daily_ratios %>%
    filter(Ticker %in% benchmark_tickers) %>%
    select(
      date = as_of_date,
      ticker = Ticker,
      daily_return = price_1d_pct_change
    ) %>%
    filter(date >= as.Date(start_date), date <= as.Date(end_date)) %>%
    arrange(ticker, date)
  
  # Add benchmark names
  name_map <- data.frame(
    ticker = sapply(BENCHMARKS, `[[`, "ticker"),
    benchmark = names(BENCHMARKS),
    stringsAsFactors = FALSE
  )
  
  benchmark_returns <- benchmark_returns %>%
    left_join(name_map, by = "ticker") %>%
    select(date, benchmark, ticker, daily_return)
  
  return(benchmark_returns)
}

#' Calculate benchmark statistics
#' @param benchmark_returns Dataframe from get_benchmark_returns
#' @return Dataframe with summary statistics
calculate_benchmark_stats <- function(benchmark_returns) {
  
  benchmark_returns %>%
    group_by(benchmark) %>%
    summarise(
      n_days = n(),
      total_return = prod(1 + daily_return, na.rm = TRUE) - 1,
      annualized_return = (1 + total_return)^(252 / n()) - 1,
      volatility = sd(daily_return, na.rm = TRUE) * sqrt(252),
      sharpe_ratio = annualized_return / volatility,
      max_drawdown = max(cummax(cumprod(1 + daily_return)) - cumprod(1 + daily_return), na.rm = TRUE),
      win_rate = mean(daily_return > 0, na.rm = TRUE) * 100,
      .groups = "drop"
    )
}

#' Calculate performance attribution vs benchmarks
#' @param portfolio_returns Daily returns of portfolio
#' @param benchmark_returns Daily returns of benchmark
#' @return List with attribution metrics
calculate_attribution <- function(portfolio_returns, benchmark_returns) {
  
  # Align dates
  combined <- portfolio_returns %>%
    left_join(benchmark_returns, by = "date", suffix = c("_portfolio", "_benchmark"))
  
  # Calculate metrics
  metrics <- combined %>%
    summarise(
      portfolio_return = prod(1 + portfolio_return, na.rm = TRUE) - 1,
      benchmark_return = prod(1 + benchmark_return, na.rm = TRUE) - 1,
      alpha = portfolio_return - benchmark_return,
      tracking_error = sd(portfolio_return - benchmark_return, na.rm = TRUE) * sqrt(252),
      information_ratio = alpha / tracking_error,
      beta = cov(portfolio_return, benchmark_return, use = "complete.obs") / 
             var(benchmark_return, use = "complete.obs"),
      correlation = cor(portfolio_return, benchmark_return, use = "complete.obs"),
      win_rate = mean(portfolio_return > benchmark_return, na.rm = TRUE) * 100,
      .groups = "drop"
    )
  
  return(list(
    metrics = metrics,
    daily = combined
  ))
}