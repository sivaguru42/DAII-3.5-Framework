# ============================================================================
# CONTROL BENCHMARK ANALYSIS MODULE
# Version: 3.0 | Date: 2026-03-30
# Description: AI-Low benchmark, Matched-Pair controls, and Synthetic Control
# ============================================================================

library(dplyr)
library(tidyr)

# ============================================================================
# PART 1: AI-LOW BENCHMARK (Primary Control)
# ============================================================================

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
  
  message("   Constructing AI-Low benchmark...")
  
  # Identify low-AI companies
  low_ai_companies <- daii_scored %>%
    filter(ai_label %in% c("AI Laggard", "AI Follower")) %>%
    arrange(ai_score) %>%
    head(n_components)
  
  message(sprintf("      Found %d low-AI companies", nrow(low_ai_companies)))
  
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
      weight = sector_weight / sector_n,
      weight = ifelse(is.na(weight), 0, weight)
    ) %>%
    select(ticker, company_name, ai_score, TRBC_Industry, weight)
  
  message(sprintf("      AI-Low benchmark created with %d companies", 
                  sum(benchmark_weights$weight > 0)))
  
  return(benchmark_weights)
}

# ============================================================================
# PART 2: MATCHED-PAIR CONTROLS (Phase 4A)
# ============================================================================

#' Construct Matched-Pair Controls (Enhanced)
#' 
#' For each portfolio company, finds a non-AI peer with similar characteristics.
#' Supports multiple matching methods and k-nearest neighbors.
#' 
#' @param portfolio_companies Dataframe with portfolio holdings
#' @param universe Dataframe with all companies and AI scores
#' @param match_vars Variables to match on
#' @param k Number of controls per portfolio company (default: 1)
#' @param method Matching method: "nearest", "radius", or "stratified"
#' @param radius Radius for radius matching (if method = "radius")
#' @return List with pairs and quality metrics
construct_matched_controls <- function(portfolio_companies, 
                                       universe, 
                                       match_vars = c("TRBC_Industry", "market_cap", "revenue_growth"),
                                       k = 1,
                                       method = "nearest",
                                       radius = 0.5) {
  
  message("   Constructing matched-pair controls...")
  message(sprintf("      Method: %s, k = %d", method, k))
  
  # Standardize numeric variables for distance calculation
  universe_std <- universe %>%
    mutate(
      market_cap_z = (log(market_cap) - mean(log(market_cap), na.rm = TRUE)) / 
        sd(log(market_cap), na.rm = TRUE),
      revenue_growth_z = (revenue_growth - mean(revenue_growth, na.rm = TRUE)) / 
        sd(revenue_growth, na.rm = TRUE)
    )
  
  matches <- data.frame()
  
  for(i in 1:nrow(portfolio_companies)) {
    
    portfolio_data <- portfolio_companies[i, ]
    
    # Find eligible controls (low AI, not in portfolio)
    controls <- universe_std %>%
      filter(
        ai_label %in% c("AI Laggard", "AI Follower"),
        !ticker %in% portfolio_companies$ticker
      )
    
    if(nrow(controls) == 0) next
    
    # Apply matching criteria
    if("TRBC_Industry" %in% match_vars) {
      controls <- controls %>% filter(TRBC_Industry == portfolio_data$TRBC_Industry)
    }
    
    if(nrow(controls) == 0) next
    
    # Calculate distance scores
    controls <- controls %>%
      mutate(
        size_distance = abs(log(market_cap) - log(portfolio_data$market_cap)) / 
          (sd(log(universe$market_cap), na.rm = TRUE) + 0.01),
        growth_distance = abs(revenue_growth - portfolio_data$revenue_growth) / 
          (sd(universe$revenue_growth, na.rm = TRUE) + 0.01),
        total_distance = size_distance + growth_distance
      )
    
    # Apply matching method
    if(method == "nearest") {
      selected <- controls %>%
        arrange(total_distance) %>%
        head(k)
    } else if(method == "radius") {
      selected <- controls %>%
        filter(total_distance <= radius) %>%
        arrange(total_distance) %>%
        head(k)
    } else if(method == "stratified") {
      controls <- controls %>%
        mutate(
          size_quartile = ntile(market_cap, 4),
          growth_quartile = ntile(revenue_growth, 4)
        ) %>%
        filter(size_quartile == ntile(portfolio_data$market_cap, 4),
               growth_quartile == ntile(portfolio_data$revenue_growth, 4))
      
      if(nrow(controls) > 0) {
        selected <- controls %>%
          arrange(total_distance) %>%
          head(k)
      } else {
        selected <- data.frame()
      }
    }
    
    # Record matches
    if(nrow(selected) > 0) {
      for(j in 1:nrow(selected)) {
        control <- selected[j, ]
        
        matches <- rbind(matches, data.frame(
          portfolio_ticker = portfolio_data$ticker,
          portfolio_name = portfolio_data$company_name,
          portfolio_ai_score = portfolio_data$ai_score,
          portfolio_weight = portfolio_data$fund_weight,
          portfolio_mcap = portfolio_data$market_cap,
          portfolio_growth = portfolio_data$revenue_growth,
          control_ticker = control$ticker,
          control_name = control$company_name,
          control_ai_score = control$ai_score,
          control_mcap = control$market_cap,
          control_growth = control$revenue_growth,
          ai_score_gap = portfolio_data$ai_score - control$ai_score,
          size_ratio = portfolio_data$market_cap / control$market_cap,
          growth_diff = portfolio_data$revenue_growth - control$revenue_growth,
          distance = control$total_distance,
          sector = portfolio_data$TRBC_Industry,
          method = method,
          stringsAsFactors = FALSE
        ))
      }
    }
  }
  
  # Calculate match quality metrics
  quality_metrics <- data.frame()
  if(nrow(matches) > 0) {
    quality_metrics <- matches %>%
      summarise(
        total_pairs = n(),
        unique_portfolio_companies = n_distinct(portfolio_ticker),
        avg_ai_gap = mean(ai_score_gap, na.rm = TRUE),
        median_ai_gap = median(ai_score_gap, na.rm = TRUE),
        avg_size_ratio = mean(size_ratio, na.rm = TRUE),
        avg_growth_diff = mean(growth_diff, na.rm = TRUE),
        avg_distance = mean(distance, na.rm = TRUE),
        pct_good_matches = mean(distance < 1, na.rm = TRUE) * 100,
        .groups = "drop"
      )
    
    message(sprintf("      Created %d matched pairs for %d unique portfolio companies", 
                    nrow(matches), quality_metrics$unique_portfolio_companies[1]))
    message(sprintf("      Average AI gap: %.3f", quality_metrics$avg_ai_gap[1]))
    message(sprintf("      Good matches (distance < 1): %.1f%%", quality_metrics$pct_good_matches[1]))
  } else {
    message("      No matches found")
  }
  
  return(list(
    pairs = matches,
    quality = quality_metrics
  ))
}

#' Calculate Matched-Pair Returns
#' 
#' For each matched pair, calculate the return difference (AI alpha)
#' 
#' @param matched_pairs Dataframe from construct_matched_controls
#' @param returns_data Daily returns for all companies
#' @return Dataframe with daily alpha series
calculate_matched_pair_alpha <- function(matched_pairs, returns_data) {
  
  message("   Calculating matched-pair alpha...")
  
  if(nrow(matched_pairs) == 0) {
    message("      No matched pairs to calculate")
    return(NULL)
  }
  
  # Get unique tickers from both portfolios and controls
  all_tickers <- unique(c(matched_pairs$portfolio_ticker, matched_pairs$control_ticker))
  
  # Get returns for all tickers
  returns_wide <- returns_data %>%
    filter(ticker %in% all_tickers) %>%
    select(date, ticker, daily_return) %>%
    pivot_wider(id_cols = date, names_from = ticker, values_from = daily_return)
  
  # Calculate daily alpha for each pair (portfolio - control)
  alpha_series <- data.frame(date = returns_wide$date)
  
  for(i in 1:nrow(matched_pairs)) {
    port_ticker <- matched_pairs$portfolio_ticker[i]
    ctrl_ticker <- matched_pairs$control_ticker[i]
    
    if(port_ticker %in% names(returns_wide) && ctrl_ticker %in% names(returns_wide)) {
      pair_alpha <- returns_wide[[port_ticker]] - returns_wide[[ctrl_ticker]]
      alpha_series[[paste0(port_ticker, "_alpha")]] <- pair_alpha
    }
  }
  
  # Calculate average alpha across all pairs (weighted by portfolio weight)
  if(ncol(alpha_series) > 1) {
    weight_map <- setNames(matched_pairs$portfolio_weight, matched_pairs$portfolio_ticker)
    
    alpha_cols <- names(alpha_series)[-1]
    weighted_alpha <- rep(0, nrow(alpha_series))
    total_weight <- 0
    
    for(col in alpha_cols) {
      ticker <- gsub("_alpha", "", col)
      weight <- weight_map[ticker]
      if(!is.na(weight)) {
        weighted_alpha <- weighted_alpha + alpha_series[[col]] * weight
        total_weight <- total_weight + weight
      }
    }
    
    if(total_weight > 0) {
      weighted_alpha <- weighted_alpha / total_weight
    }
    
    result <- data.frame(
      date = alpha_series$date,
      alpha = weighted_alpha
    )
  } else {
    result <- data.frame(date = alpha_series$date, alpha = 0)
  }
  
  return(result)
}

# ============================================================================
# PART 3: SYNTHETIC CONTROL (Phase 4B)
# ============================================================================

#' Calculate AI Alpha using Synthetic Control
#' 
#' Uses machine learning (ridge regression) to create a synthetic portfolio
#' from low-AI donor companies that best matches the actual portfolio.
#' The difference between actual and synthetic returns = AI alpha.
#' 
#' @param portfolio_returns Portfolio return time series (dataframe with date and portfolio_return)
#' @param donor_returns Returns of donor companies (low-AI universe)
#' @param donor_ai_scores Optional vector of AI scores for donors
#' @param method Method for optimization: "ridge", "lasso", or "equal"
#' @param train_ratio Proportion of data to use for training (default: 0.7)
#' @return List with synthetic control results and alpha metrics
calculate_synthetic_alpha <- function(portfolio_returns, 
                                      donor_returns,
                                      donor_ai_scores = NULL,
                                      method = "ridge",
                                      train_ratio = 0.7) {
  
  message("   Calculating synthetic control alpha...")
  message(sprintf("      Method: %s", method))
  
  # Align dates
  common_dates <- intersect(portfolio_returns$date, donor_returns$date)
  
  if(length(common_dates) < 60) {
    message("      ⚠️ Insufficient common dates (need 60+, have ", length(common_dates), ")")
    return(NULL)
  }
  
  message(sprintf("      Common dates: %d", length(common_dates)))
  
  # Prepare donor matrix
  donor_wide <- donor_returns %>%
    filter(date %in% common_dates) %>%
    pivot_wider(id_cols = date, names_from = ticker, values_from = daily_return)
  
  # Prepare target returns
  target_returns <- portfolio_returns %>%
    filter(date %in% common_dates) %>%
    pull(portfolio_return)
  
  # Remove date column for modeling
  X <- as.matrix(donor_wide[, -1])
  y <- target_returns
  
  # Handle NA values (replace with 0 for missing returns)
  X[is.na(X)] <- 0
  
  # Split into training and validation sets
  n_train <- floor(nrow(X) * train_ratio)
  train_idx <- 1:n_train
  test_idx <- (n_train + 1):nrow(X)
  
  X_train <- X[train_idx, ]
  y_train <- y[train_idx]
  X_test <- X[test_idx, ]
  y_test <- y[test_idx]
  
  # Fit model based on method
  if(method %in% c("ridge", "lasso")) {
    library(glmnet)
    
    alpha_param <- ifelse(method == "ridge", 0, 1)
    cv_model <- cv.glmnet(X_train, y_train, alpha = alpha_param)
    
    # Get optimal weights
    weights <- as.vector(coef(cv_model, s = "lambda.min"))[-1]
    
    # Constrain weights (non-negative, sum to 1)
    weights[weights < 0] <- 0
    if(sum(weights) > 0) {
      weights <- weights / sum(weights)
    }
    
    # Calculate R-squared on validation set
    y_pred <- predict(cv_model, newx = X_test, s = "lambda.min")
    ss_res <- sum((y_test - y_pred)^2)
    ss_tot <- sum((y_test - mean(y_test))^2)
    r_squared <- 1 - ss_res / ss_tot
    
  } else if(method == "equal") {
    weights <- rep(1/ncol(X), ncol(X))
    r_squared <- NA
    
  } else if(method == "market_cap") {
    # Would need market cap data - fallback to equal weight
    weights <- rep(1/ncol(X), ncol(X))
    r_squared <- NA
  }
  
  # Calculate synthetic returns (in-sample and out-of-sample)
  synthetic_returns_train <- as.vector(X_train %*% weights)
  synthetic_returns_test <- as.vector(X_test %*% weights)
  synthetic_returns_all <- as.vector(X %*% weights)
  
  # Calculate AI alpha
  ai_alpha_train <- y_train - synthetic_returns_train
  ai_alpha_test <- y_test - synthetic_returns_test
  ai_alpha_all <- y - synthetic_returns_all
  
  # Calculate metrics
  alpha_annualized <- mean(ai_alpha_all, na.rm = TRUE) * 252
  tracking_error <- sd(ai_alpha_all, na.rm = TRUE) * sqrt(252)
  information_ratio <- ifelse(tracking_error > 0, alpha_annualized / tracking_error, NA)
  
  # Identify top contributing donors
  top_donors <- data.frame(
    ticker = colnames(X),
    weight = weights
  ) %>%
    filter(weight > 0.01) %>%
    arrange(desc(weight))
  
  message(sprintf("      Synthetic control created with %d active donors", nrow(top_donors)))
  message(sprintf("      R-squared (validation): %.3f", ifelse(is.na(r_squared), 0, r_squared)))
  message(sprintf("      Alpha (annualized): %.2f%%", alpha_annualized * 100))
  
  return(list(
    method = method,
    n_donors = ncol(X),
    active_donors = nrow(top_donors),
    top_donors = top_donors,
    weights = setNames(weights, colnames(X)),
    r_squared = r_squared,
    synthetic_returns = synthetic_returns_all,
    ai_alpha = ai_alpha_all,
    ai_alpha_train = ai_alpha_train,
    ai_alpha_test = ai_alpha_test,
    cumulative_alpha = cumprod(1 + ai_alpha_all) - 1,
    mean_alpha = mean(ai_alpha_all, na.rm = TRUE),
    alpha_annualized = alpha_annualized,
    tracking_error = tracking_error,
    information_ratio = information_ratio,
    t_stat = t.test(ai_alpha_all)$statistic,
    p_value = t.test(ai_alpha_all)$p.value,
    win_rate = mean(ai_alpha_all > 0, na.rm = TRUE) * 100,
    daily_alpha = data.frame(
      date = common_dates,
      alpha = ai_alpha_all,
      synthetic_return = synthetic_returns_all,
      actual_return = y
    )
  ))
}

#' Prepare Donor Pool for Synthetic Control
#' 
#' @param daii_scored Dataframe with AI scores
#' @param exclude_tickers Tickers to exclude (portfolio holdings)
#' @param ai_threshold Maximum AI score for donors (default: 0.3)
#' @return Vector of donor tickers
prepare_donor_pool <- function(daii_scored, exclude_tickers = NULL, ai_threshold = 0.3) {
  
  donor_tickers <- daii_scored %>%
    filter(
      ai_score <= ai_threshold,
      ai_label %in% c("AI Laggard", "AI Follower"),
      !is.na(market_cap),
      market_cap > 0
    )
  
  if(!is.null(exclude_tickers)) {
    donor_tickers <- donor_tickers %>%
      filter(!ticker %in% exclude_tickers)
  }
  
  donor_tickers <- donor_tickers %>%
    pull(ticker)
  
  message(sprintf("      Donor pool prepared: %d companies (AI score ≤ %.2f)", 
                  length(donor_tickers), ai_threshold))
  
  return(donor_tickers)
}

# ============================================================================
# PART 4: COMPARISON FUNCTION (All Methods)
# ============================================================================

#' Compare All Control Methods
#' 
#' Returns a comparison table of all control methods.
#' 
#' @param portfolio_returns Portfolio returns
#' @param benchmark_returns Traditional benchmark returns
#' @param ai_low_returns AI-Low benchmark returns
#' @param matched_pairs_returns Matched-pair control returns
#' @param synthetic_control_results Results from calculate_synthetic_alpha
#' @return Dataframe with comparison
compare_control_methods <- function(portfolio_returns, 
                                    benchmark_returns = NULL,
                                    ai_low_returns = NULL, 
                                    matched_pairs_returns = NULL,
                                    synthetic_control_results = NULL) {
  
  message("   Comparing control methods...")
  
  results <- data.frame()
  
  # Traditional benchmarks
  if(!is.null(benchmark_returns) && nrow(benchmark_returns) > 0) {
    if("SPX" %in% unique(benchmark_returns$benchmark)) {
      spx_returns <- benchmark_returns %>% 
        filter(benchmark == "SPX") %>% 
        pull(return)
      
      # Align lengths
      min_len <- min(length(portfolio_returns$portfolio_return), length(spx_returns))
      excess <- tail(portfolio_returns$portfolio_return, min_len) - tail(spx_returns, min_len)
      
      results <- rbind(results, data.frame(
        Control_Method = "S&P 500",
        Alpha = mean(excess, na.rm = TRUE),
        Alpha_Annualized = mean(excess, na.rm = TRUE) * 252,
        Tracking_Error = sd(excess, na.rm = TRUE) * sqrt(252),
        Information_Ratio = mean(excess, na.rm = TRUE) / sd(excess, na.rm = TRUE) * sqrt(252),
        Win_Rate = mean(excess > 0, na.rm = TRUE) * 100,
        stringsAsFactors = FALSE
      ))
    }
    
    if("IXIC" %in% unique(benchmark_returns$benchmark)) {
      ixic_returns <- benchmark_returns %>% 
        filter(benchmark == "IXIC") %>% 
        pull(return)
      
      min_len <- min(length(portfolio_returns$portfolio_return), length(ixic_returns))
      excess <- tail(portfolio_returns$portfolio_return, min_len) - tail(ixic_returns, min_len)
      
      results <- rbind(results, data.frame(
        Control_Method = "NASDAQ 100",
        Alpha = mean(excess, na.rm = TRUE),
        Alpha_Annualized = mean(excess, na.rm = TRUE) * 252,
        Tracking_Error = sd(excess, na.rm = TRUE) * sqrt(252),
        Information_Ratio = mean(excess, na.rm = TRUE) / sd(excess, na.rm = TRUE) * sqrt(252),
        Win_Rate = mean(excess > 0, na.rm = TRUE) * 100,
        stringsAsFactors = FALSE
      ))
    }
  }
  
  # AI-Low benchmark
  if(!is.null(ai_low_returns) && nrow(ai_low_returns) > 0) {
    min_len <- min(length(portfolio_returns$portfolio_return), nrow(ai_low_returns))
    excess <- tail(portfolio_returns$portfolio_return, min_len) - tail(ai_low_returns$return, min_len)
    
    results <- rbind(results, data.frame(
      Control_Method = "AI-Low Benchmark",
      Alpha = mean(excess, na.rm = TRUE),
      Alpha_Annualized = mean(excess, na.rm = TRUE) * 252,
      Tracking_Error = sd(excess, na.rm = TRUE) * sqrt(252),
      Information_Ratio = mean(excess, na.rm = TRUE) / sd(excess, na.rm = TRUE) * sqrt(252),
      Win_Rate = mean(excess > 0, na.rm = TRUE) * 100,
      stringsAsFactors = FALSE
    ))
  }
  
  # Matched-pair control
  if(!is.null(matched_pairs_returns) && nrow(matched_pairs_returns) > 0) {
    min_len <- min(length(portfolio_returns$portfolio_return), nrow(matched_pairs_returns))
    excess <- tail(portfolio_returns$portfolio_return, min_len) - tail(matched_pairs_returns$alpha, min_len)
    
    results <- rbind(results, data.frame(
      Control_Method = "Matched-Pair Control",
      Alpha = mean(excess, na.rm = TRUE),
      Alpha_Annualized = mean(excess, na.rm = TRUE) * 252,
      Tracking_Error = sd(excess, na.rm = TRUE) * sqrt(252),
      Information_Ratio = mean(excess, na.rm = TRUE) / sd(excess, na.rm = TRUE) * sqrt(252),
      Win_Rate = mean(excess > 0, na.rm = TRUE) * 100,
      stringsAsFactors = FALSE
    ))
  }
  
  # Synthetic control
  if(!is.null(synthetic_control_results)) {
    results <- rbind(results, data.frame(
      Control_Method = paste0("Synthetic Control (", synthetic_control_results$method, ")"),
      Alpha = synthetic_control_results$mean_alpha,
      Alpha_Annualized = synthetic_control_results$alpha_annualized,
      Tracking_Error = synthetic_control_results$tracking_error,
      Information_Ratio = synthetic_control_results$information_ratio,
      Win_Rate = synthetic_control_results$win_rate,
      stringsAsFactors = FALSE
    ))
  }
  
  return(results)
}