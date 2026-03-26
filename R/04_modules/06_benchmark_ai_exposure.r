# ============================================================================
# BENCHMARK AI EXPOSURE MODULE
# File: R/04_modules/06_benchmark_ai_exposure.r
# Version: 1.0 | Date: 2026-03-26
# Description: Calculate AI exposure for benchmarks and compare to portfolio
# ============================================================================

library(dplyr)
library(tidyr)

#' Calculate AI exposure for any benchmark
#' 
#' @param benchmark_tickers Vector of tickers in the benchmark
#' @param company_data Your daii_scored dataframe with AI scores
#' @param weights Weighting method: "market_cap", "equal", or custom weight vector
#' @return Dataframe with benchmark AI exposure metrics
calculate_benchmark_ai_exposure <- function(benchmark_tickers, 
                                             company_data,
                                             weights = "market_cap",
                                             custom_weights = NULL) {
  
  # Filter to companies in benchmark
  benchmark_companies <- company_data %>%
    filter(ticker %in% benchmark_tickers)
  
  if(nrow(benchmark_companies) == 0) {
    warning("No matching companies found in benchmark")
    return(NULL)
  }
  
  # Calculate weights based on method
  if(weights == "market_cap") {
    # Market-cap weighted
    total_mcap <- sum(benchmark_companies$market_cap, na.rm = TRUE)
    if(total_mcap == 0) {
      # Fallback to equal weight if no market cap data
      benchmark_companies <- benchmark_companies %>%
        mutate(weight = 1 / n())
    } else {
      benchmark_companies <- benchmark_companies %>%
        mutate(weight = market_cap / total_mcap)
    }
  } else if(weights == "equal") {
    # Equal weighted
    benchmark_companies <- benchmark_companies %>%
      mutate(weight = 1 / n())
  } else if(weights == "custom" && !is.null(custom_weights)) {
    # Custom weights
    benchmark_companies <- benchmark_companies %>%
      left_join(custom_weights, by = "ticker") %>%
      mutate(weight = weight / sum(weight, na.rm = TRUE))
  }
  
  # Replace NA weights with 0
  benchmark_companies$weight[is.na(benchmark_companies$weight)] <- 0
  
  # Calculate aggregate metrics
  benchmark_ai <- benchmark_companies %>%
    summarise(
      # Core metrics
      n_companies = n(),
      coverage_pct = n() / length(benchmark_tickers) * 100,
      avg_ai_score = mean(ai_score, na.rm = TRUE),
      weighted_ai_score = sum(ai_score * weight, na.rm = TRUE),
      median_ai_score = median(ai_score, na.rm = TRUE),
      ai_score_std = sd(ai_score, na.rm = TRUE),
      
      # Distribution metrics (weighted)
      pct_ai_leaders = sum((ai_label == "AI Leader") * weight, na.rm = TRUE) * 100,
      pct_ai_adopters = sum((ai_label == "AI Adopter") * weight, na.rm = TRUE) * 100,
      pct_ai_followers = sum((ai_label == "AI Follower") * weight, na.rm = TRUE) * 100,
      pct_ai_laggards = sum((ai_label == "AI Laggard") * weight, na.rm = TRUE) * 100,
      
      # Concentration metrics
      top_10_weight = sum(head(sort(weight[ai_score > 0.6], decreasing = TRUE), 10), na.rm = TRUE),
      herfindahl = sum(weight^2, na.rm = TRUE),  # HHI concentration index
      effective_n = 1 / sum(weight^2, na.rm = TRUE),  # Effective number of holdings
      
      # Innovation metrics (weighted)
      avg_innovation_score = weighted.mean(innovation_score, weight, na.rm = TRUE),
      avg_patents = weighted.mean(total_patents, weight, na.rm = TRUE),
      avg_rd_intensity = weighted.mean(rd_intensity, weight, na.rm = TRUE),
      
      # Growth & Stability metrics
      avg_revenue_growth = weighted.mean(revenue_growth, weight, na.rm = TRUE),
      avg_volatility = weighted.mean(volatility, weight, na.rm = TRUE),
      
      # Unicorn metrics
      pct_unicorns = sum((unicorn_tier %in% c("Top Unicorn Candidate", "Emerging Unicorn Candidate")) * weight, 
                         na.rm = TRUE) * 100,
      
      .groups = "drop"
    )
  
  return(benchmark_ai)
}

#' Get benchmark constituents (from your existing benchmark data)
#' 
#' @param benchmark_name Name of benchmark (e.g., "S&P 500", "NASDAQ 100")
#' @param benchmark_data Your existing benchmark constituents data
#' @return Vector of tickers in the benchmark
get_benchmark_constituents <- function(benchmark_name, benchmark_data) {
  
  # This function should retrieve the tickers for each benchmark
  # Based on your Section 10.6 benchmark constituents query
  
  if(benchmark_name == "S&P 500") {
    return(benchmark_data$sp500_tickers)
  } else if(benchmark_name == "NASDAQ 100") {
    return(benchmark_data$nasdaq100_tickers)
  } else if(benchmark_name == "S&P 400 Mid Cap") {
    return(benchmark_data$sp400_tickers)
  } else if(benchmark_name == "S&P 600 Small Cap") {
    return(benchmark_data$sp600_tickers)
  } else {
    stop("Unknown benchmark name")
  }
}

#' Compare portfolio AI exposure to multiple benchmarks
#' 
#' @param portfolio_companies Your portfolio holdings with weights
#' @param benchmarks List of benchmarks with names and tickers
#' @param company_data Your daii_scored dataframe
#' @return Comparison dataframe
compare_ai_exposure <- function(portfolio_companies, benchmarks, company_data) {
  
  # Calculate portfolio AI exposure
  portfolio_ai <- portfolio_companies %>%
    summarise(
      name = "DUMAC Portfolio",
      weighted_ai_score = sum(ai_score * fund_weight, na.rm = TRUE),
      pct_ai_leaders = sum((ai_label == "AI Leader") * fund_weight, na.rm = TRUE) * 100,
      pct_ai_adopters = sum((ai_label == "AI Adopter") * fund_weight, na.rm = TRUE) * 100,
      pct_ai_followers = sum((ai_label == "AI Follower") * fund_weight, na.rm = TRUE) * 100,
      pct_ai_laggards = sum((ai_label == "AI Laggard") * fund_weight, na.rm = TRUE) * 100,
      concentration = sum(fund_weight^2, na.rm = TRUE),
      avg_revenue_growth = weighted.mean(revenue_growth, fund_weight, na.rm = TRUE),
      avg_volatility = weighted.mean(volatility, fund_weight, na.rm = TRUE),
      pct_unicorns = sum((unicorn_tier %in% c("Top Unicorn Candidate", "Emerging Unicorn Candidate")) * fund_weight, 
                         na.rm = TRUE) * 100,
      .groups = "drop"
    )
  
  # Calculate each benchmark's AI exposure
  benchmark_results <- list()
  for(bench_name in names(benchmarks)) {
    benchmark_ai <- calculate_benchmark_ai_exposure(
      benchmarks[[bench_name]], 
      company_data,
      weights = "market_cap"
    )
    
    if(!is.null(benchmark_ai)) {
      benchmark_ai$name <- bench_name
      benchmark_results[[bench_name]] <- benchmark_ai
    }
  }
  
  # Combine and compare
  comparison <- bind_rows(portfolio_ai, bind_rows(benchmark_results)) %>%
    mutate(
      # Exposure gaps relative to portfolio
      ai_score_gap = weighted_ai_score - weighted_ai_score[1],
      leader_gap = pct_ai_leaders - pct_ai_leaders[1],
      adopter_gap = pct_ai_adopters - pct_ai_adopters[1],
      laggard_gap = pct_ai_laggards - pct_ai_laggards[1],
      
      # Active share (how different is AI exposure profile)
      active_share = 0.5 * abs(pct_ai_leaders - pct_ai_leaders[1]) + 
                     0.5 * abs(pct_ai_laggards - pct_ai_laggards[1])
    )
  
  return(comparison)
}

#' Calculate AI exposure by sector within a benchmark
#' 
#' @param benchmark_tickers Vector of benchmark tickers
#' @param company_data Your daii_scored dataframe
#' @return Dataframe with sector-level AI exposure
calculate_sector_ai_exposure <- function(benchmark_tickers, company_data) {
  
  benchmark_companies <- company_data %>%
    filter(ticker %in% benchmark_tickers) %>%
    filter(!is.na(TRBC_Industry)) %>%
    group_by(TRBC_Industry) %>%
    summarise(
      n_companies = n(),
      total_mcap = sum(market_cap, na.rm = TRUE),
      weighted_ai = weighted.mean(ai_score, market_cap, na.rm = TRUE),
      pct_ai_leaders = sum((ai_label == "AI Leader") * market_cap, na.rm = TRUE) / 
                       sum(market_cap, na.rm = TRUE) * 100,
      avg_revenue_growth = weighted.mean(revenue_growth, market_cap, na.rm = TRUE),
      .groups = "drop"
    ) %>%
    arrange(desc(weighted_ai))
  
  return(benchmark_companies)
}

#' Create AI Exposure Radar Data
#' 
#' @param portfolio_ai Portfolio AI exposure metrics
#' @param benchmark_results List of benchmark AI exposure metrics
#' @return Dataframe ready for radar chart
create_ai_radar_data <- function(portfolio_ai, benchmark_results) {
  
  radar_data <- data.frame(
    Metric = c("AI Score", "% AI Leaders", "% AI Adopters", 
               "% AI Followers", "% AI Laggards",
               "Innovation Score", "Revenue Growth", "Unicorn %"),
    Portfolio = c(portfolio_ai$weighted_ai_score, 
                  portfolio_ai$pct_ai_leaders,
                  portfolio_ai$pct_ai_adopters,
                  portfolio_ai$pct_ai_followers,
                  portfolio_ai$pct_ai_laggards,
                  portfolio_ai$avg_innovation_score,
                  portfolio_ai$avg_revenue_growth * 100,
                  portfolio_ai$pct_unicorns)
  )
  
  for(bench_name in names(benchmark_results)) {
    bench <- benchmark_results[[bench_name]]
    radar_data[[bench_name]] <- c(bench$weighted_ai_score,
                                   bench$pct_ai_leaders,
                                   bench$pct_ai_adopters,
                                   bench$pct_ai_followers,
                                   bench$pct_ai_laggards,
                                   bench$avg_innovation_score,
                                   bench$avg_revenue_growth * 100,
                                   bench$pct_unicorns)
  }
  
  return(radar_data)
}