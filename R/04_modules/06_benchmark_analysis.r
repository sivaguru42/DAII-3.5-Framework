# ============================================================================
# MODULE: Benchmark Analysis
# Version: 1.0 | Date: 2026-03-24
# Description: Compares portfolio AI exposure to major indices
# ============================================================================

library(dplyr)
library(ggplot2)

# ============================================================================
# BENCHMARK CONSTITUENTS (Pre-defined or dynamically fetched)
# ============================================================================

# Option 1: Pre-defined benchmark constituents (for now)
# These will be expanded with actual Refinitiv queries later

benchmark_constituents <- list(
  "S&P 500" = c("AAPL", "MSFT", "NVDA", "GOOGL", "META", "AMZN", "TSLA", "BRK.B", "JPM", "V"), # Placeholder
  "Russell 3000" = c("AAPL", "MSFT", "NVDA", "GOOGL", "META", "AMZN", "TSLA", "JPM", "V", "WMT"), # Placeholder
  "MSCI ACWI" = c("AAPL", "MSFT", "NVDA", "GOOGL", "META", "TSM", "ASML", "NVO", "LVMH", "SAP") # Placeholder
)

#' Calculate AI exposure for benchmarks
#' @param benchmark_constituents List of tickers per benchmark
#' @param company_features Dataframe with AI scores for all companies
#' @return Dataframe with benchmark AI exposure metrics
calculate_benchmark_ai_exposure <- function(benchmark_constituents, company_features) {
  
  results <- data.frame()
  
  for(benchmark in names(benchmark_constituents)) {
    tickers <- benchmark_constituents[[benchmark]]
    
    # Get AI scores for benchmark constituents
    benchmark_data <- company_features %>%
      filter(ticker %in% tickers) %>%
      summarise(
        benchmark = benchmark,
        n_companies = n(),
        avg_ai_score = mean(ai_score, na.rm = TRUE),
        median_ai_score = median(ai_score, na.rm = TRUE),
        pct_ai_leaders = mean(ai_label == "AI Leader", na.rm = TRUE) * 100,
        pct_ai_adopters = mean(ai_label == "AI Adopter", na.rm = TRUE) * 100,
        pct_ai_followers = mean(ai_label == "AI Follower", na.rm = TRUE) * 100,
        pct_ai_laggards = mean(ai_label == "AI Laggard", na.rm = TRUE) * 100,
        .groups = "drop"
      )
    
    results <- bind_rows(results, benchmark_data)
  }
  
  return(results)
}

#' Compare portfolio to benchmarks
#' @param portfolio_data Dataframe with portfolio AI scores
#' @param benchmark_results Dataframe from calculate_benchmark_ai_exposure
#' @return Dataframe with comparison metrics
compare_to_benchmarks <- function(portfolio_data, benchmark_results) {
  
  portfolio_metrics <- portfolio_data %>%
    summarise(
      benchmark = "DUMAC Portfolio",
      n_companies = n(),
      avg_ai_score = mean(ai_score, na.rm = TRUE),
      median_ai_score = median(ai_score, na.rm = TRUE),
      pct_ai_leaders = mean(ai_label == "AI Leader", na.rm = TRUE) * 100,
      pct_ai_adopters = mean(ai_label == "AI Adopter", na.rm = TRUE) * 100,
      pct_ai_followers = mean(ai_label == "AI Follower", na.rm = TRUE) * 100,
      pct_ai_laggards = mean(ai_label == "AI Laggard", na.rm = TRUE) * 100
    )
  
  comparison <- bind_rows(portfolio_metrics, benchmark_results)
  
  # Calculate percentile rank of portfolio vs benchmarks
  all_avg <- c(portfolio_metrics$avg_ai_score, benchmark_results$avg_ai_score)
  portfolio_percentile <- ecdf(all_avg)(portfolio_metrics$avg_ai_score) * 100
  
  comparison$percentile_rank <- NA
  comparison$percentile_rank[1] <- portfolio_percentile
  
  return(comparison)
}

#' Create benchmark comparison visualization
#' @param comparison_df Dataframe from compare_to_benchmarks
#' @return ggplot object
plot_benchmark_comparison <- function(comparison_df) {
  
  p <- ggplot(comparison_df, aes(x = reorder(benchmark, avg_ai_score), y = avg_ai_score, fill = benchmark == "DUMAC Portfolio")) +
    geom_bar(stat = "identity") +
    coord_flip() +
    scale_fill_manual(values = c("TRUE" = "darkblue", "FALSE" = "steelblue"), guide = "none") +
    labs(title = "Average AI Score: Portfolio vs Benchmarks",
         x = "", y = "Average AI Score") +
    theme_minimal()
  
  return(p)
}

# ============================================================================
# MAIN EXECUTION (to be called from pipeline)
# ============================================================================

run_benchmark_analysis <- function(portfolio_data, company_features, output_dir) {
  
  message("📊 Running benchmark analysis...")
  
  # Calculate benchmark exposures
  benchmark_results <- calculate_benchmark_ai_exposure(benchmark_constituents, company_features)
  
  # Compare portfolio to benchmarks
  comparison <- compare_to_benchmarks(portfolio_data, benchmark_results)
  
  # Save results
  write.csv(comparison, file.path(output_dir, "benchmark_comparison.csv"), row.names = FALSE)
  
  # Create visualization
  p <- plot_benchmark_comparison(comparison)
  ggsave(file.path(output_dir, "benchmark_comparison.png"), p, width = 8, height = 5)
  
  # Print summary
  cat("\n📊 BENCHMARK COMPARISON:\n")
  print(comparison)
  
  message("✅ Benchmark analysis complete")
  
  return(comparison)
}