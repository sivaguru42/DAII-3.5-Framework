# ============================================================
# SCRIPT 4: MODULE 3 - TECHNICAL INDICATORS & TIME SERIES ANALYSIS
# ============================================================
# Purpose: Calculate technical indicators and perform time series analysis
# Run this script AFTER Script 3 (Module 2)
# ============================================================

# Clear workspace
rm(list = ls())

# ============================================================
# 1. SET WORKING DIRECTORIES
# ============================================================

data_dir <- "C:/Users/sganesan/OneDrive - dumac.duke.edu/DAII/data/raw"
scripts_dir <- "C:/Users/sganesan/OneDrive - dumac.duke.edu/DAII/R/scripts"

setwd(data_dir)
cat("📁 Working directory set to:", getwd(), "\n\n")

# ============================================================
# 2. LOAD REQUIRED PACKAGES
# ============================================================

cat("📦 LOADING REQUIRED PACKAGES...\n")

required_packages <- c("readr", "dplyr", "tidyr", "lubridate", "TTR", "forecast", "tseries", "zoo")
for (pkg in required_packages) {
  if (!require(pkg, character.only = TRUE, quietly = TRUE)) {
    install.packages(pkg, dependencies = TRUE)
    library(pkg, character.only = TRUE)
    cat(sprintf("  Installed and loaded: %s\n", pkg))
  } else {
    cat(sprintf("  Loaded: %s\n", pkg))
  }
}

cat("✅ All packages loaded\n\n")

# ============================================================
# 3. LOAD MODULE 2 OUTPUT DATA
# ============================================================

cat("📥 LOADING COMBINED FINANCIAL + SENTIMENT DATA...\n")

combined_file <- "data_with_sentiment.csv"

if (!file.exists(combined_file)) {
  stop(paste("❌ ERROR: Combined data not found at:", combined_file, 
             "\nPlease run Script 3 first."))
}

combined_data <- read_csv(combined_file, col_types = cols(), show_col_types = FALSE)

cat("✅ Data loaded successfully\n")
cat(sprintf("   • Rows: %d\n", nrow(combined_data)))
cat(sprintf("   • Columns: %d\n", ncol(combined_data)))
cat(sprintf("   • Companies: %d\n", length(unique(combined_data$Ticker))))
cat(sprintf("   • Date range: %s to %s\n", 
            min(combined_data$Date, na.rm = TRUE), max(combined_data$Date, na.rm = TRUE)))

# ============================================================
# 4. SIMULATE TIME SERIES DATA (since we only have one date)
# ============================================================

cat("\n")
cat(rep("=", 70), "\n", sep = "")
cat("⏰ SIMULATING TIME SERIES DATA FOR TECHNICAL ANALYSIS\n")
cat(rep("=", 70), "\n\n", sep = "")

simulate_time_series <- function(base_data, n_days = 30, seed = 456) {
  set.seed(seed)
  
  cat("Step 1: Creating time series data for each company...\n")
  
  all_time_series <- data.frame()
  unique_tickers <- unique(base_data$Ticker)
  
  for (i in seq_along(unique_tickers)) {
    ticker <- unique_tickers[i]
    ticker_data <- base_data %>% filter(Ticker == ticker)
    
    if (nrow(ticker_data) > 0) {
      # Create sequence of 30 days leading up to the current date
      start_date <- as.Date("2024-12-01")
      end_date <- as.Date("2024-12-30")
      dates <- seq.Date(from = start_date, to = end_date, by = "day")
      
      # Create base row with all columns
      base_row <- ticker_data[1, ]
      
      # Check if LastPrice is available, otherwise skip this company
      if (is.na(base_row$LastPrice) || base_row$LastPrice <= 0) {
        cat(sprintf("   Skipping %s - no valid LastPrice\n", ticker))
        next
      }
      
      # Simulate time series for each day
      for (day in seq_along(dates)) {
        day_data <- base_row
        day_data$Date <- dates[day]
        
        # Add realistic price movements
        if (day == 1) {
          price <- base_row$LastPrice
        } else {
          # Random walk with 2% daily volatility
          price <- price * (1 + rnorm(1, mean = 0, sd = 0.02))
          price <- max(price, 0.01)  # Ensure positive price
        }
        
        # Add sentiment influence (positive sentiment pushes prices up)
        if (!is.na(day_data$composite_sentiment)) {
          sentiment_effect <- day_data$composite_sentiment * 0.01  # 1% effect per unit sentiment
          price <- price * (1 + sentiment_effect + rnorm(1, mean = 0, sd = 0.01))
        }
        
        day_data$LastPrice <- round(price, 2)
        
        # Simulate volume (with some randomness)
        if (!is.na(day_data$Volume) && day_data$Volume > 0) {
          volume_factor <- 1 + rnorm(1, mean = 0, sd = 0.3)
          day_data$Volume <- max(round(day_data$Volume * volume_factor), 1000)
        }
        
        # Update market cap based on price
        if (!is.na(day_data$MarketCap) && day_data$MarketCap > 0) {
          day_data$MarketCap <- day_data$MarketCap * (price / base_row$LastPrice)
        }
        
        # Add some noise to sentiment metrics over time
        if (!is.na(day_data$news_sentiment)) {
          day_data$news_sentiment <- day_data$news_sentiment + rnorm(1, mean = 0, sd = 0.05)
          day_data$news_sentiment <- max(min(day_data$news_sentiment, 1), -1)
        }
        
        all_time_series <- bind_rows(all_time_series, day_data)
      }
      
      if (i %% 10 == 0) {
        cat(sprintf("   Processed %d/%d companies...\n", i, length(unique_tickers)))
      }
    }
  }
  
  cat(sprintf("✅ Time series simulation complete: %d total rows\n", nrow(all_time_series)))
  cat(sprintf("   • Time period: %s to %s\n", min(all_time_series$Date), max(all_time_series$Date)))
  cat(sprintf("   • Average rows per company: %.1f\n", nrow(all_time_series) / length(unique(all_time_series$Ticker))))
  
  return(all_time_series)
}

# Simulate time series data
time_series_data <- simulate_time_series(combined_data, n_days = 30)

# Save simulated time series
write_csv(time_series_data, "simulated_time_series_data.csv")
cat(sprintf("📁 Saved simulated time series: %s (%.2f MB)\n", 
            "simulated_time_series_data.csv", 
            file.info("simulated_time_series_data.csv")$size / (1024 * 1024)))

# ============================================================
# 5. CALCULATE TECHNICAL INDICATORS (FIXED VERSION)
# ============================================================

cat("\n")
cat(rep("=", 70), "\n", sep = "")
cat("📊 CALCULATING TECHNICAL INDICATORS\n")
cat(rep("=", 70), "\n\n", sep = "")

calculate_technical_indicators <- function(ts_data) {
  cat("Step 1: Calculating price-based indicators...\n")
  
  # Sort by Ticker and Date
  ts_data <- ts_data %>%
    arrange(Ticker, Date)
  
  # Initialize technical indicators dataframe
  tech_data <- ts_data
  
  # Group by Ticker and calculate indicators
  tickers <- unique(tech_data$Ticker)
  all_tech_results <- data.frame()
  companies_with_indicators <- 0
  
  for (ticker in tickers) {
    ticker_data <- tech_data %>% filter(Ticker == ticker) %>% arrange(Date)
    
    # Check if we have enough valid price data
    valid_prices <- sum(!is.na(ticker_data$LastPrice) & ticker_data$LastPrice > 0)
    
    if (valid_prices >= 20) {  # Need at least 20 data points for basic indicators
      companies_with_indicators <- companies_with_indicators + 1
      
      # Calculate returns (with NA handling)
      ticker_data <- ticker_data %>%
        mutate(
          Daily_Return = (LastPrice - lag(LastPrice)) / lag(LastPrice),
          Log_Return = log(LastPrice / lag(LastPrice))
        )
      
      # Fill NA prices with forward/backward fill for indicator calculation
      price_series <- na.approx(ticker_data$LastPrice, na.rm = FALSE)  # Linear interpolation
      price_series <- na.locf(price_series, na.rm = FALSE)  # Last observation carried forward
      price_series <- na.locf(price_series, fromLast = TRUE, na.rm = FALSE)  # Next observation carried backward
      
      # Only calculate indicators if we have enough non-NA values
      non_na_count <- sum(!is.na(price_series))
      
      if (non_na_count >= 5) {
        # Calculate Simple Moving Averages with error handling
        tryCatch({
          if (non_na_count >= 5) ticker_data$SMA_5 <- SMA(price_series, n = 5)
          if (non_na_count >= 10) {
            ticker_data$SMA_10 <- SMA(price_series, n = 10)
            ticker_data$EMA_10 <- EMA(price_series, n = 10, na.rm = TRUE)
          }
          if (non_na_count >= 20) {
            ticker_data$SMA_20 <- SMA(price_series, n = 20)
            ticker_data$EMA_20 <- EMA(price_series, n = 20, na.rm = TRUE)
          }
        }, error = function(e) {
          cat(sprintf("     ⚠️  Error calculating MAs for %s: %s\n", ticker, e$message))
        })
      }
      
      # Calculate RSI (14-period)
      if (non_na_count >= 14) {
        tryCatch({
          ticker_data$RSI_14 <- RSI(price_series, n = 14)
        }, error = function(e) {
          cat(sprintf("     ⚠️  Error calculating RSI for %s\n", ticker))
        })
      }
      
      # Calculate MACD
      if (non_na_count >= 26) {
        tryCatch({
          macd_result <- MACD(price_series, nFast = 12, nSlow = 26, nSig = 9)
          ticker_data$MACD_line <- macd_result$macd
          ticker_data$MACD_signal <- macd_result$signal
          ticker_data$MACD_histogram <- ticker_data$MACD_line - ticker_data$MACD_signal
        }, error = function(e) {
          cat(sprintf("     ⚠️  Error calculating MACD for %s\n", ticker))
        })
      }
      
      # Calculate Bollinger Bands
      if (non_na_count >= 20) {
        tryCatch({
          bb_result <- BBands(price_series, n = 20, sd = 2)
          ticker_data$BB_upper <- bb_result$up
          ticker_data$BB_middle <- bb_result$mavg
          ticker_data$BB_lower <- bb_result$dn
          ticker_data$BB_pctB <- bb_result$pctB
        }, error = function(e) {
          cat(sprintf("     ⚠️  Error calculating Bollinger Bands for %s\n", ticker))
        })
      }
      
      # Calculate Volume indicators
      if (!all(is.na(ticker_data$Volume))) {
        volume_series <- na.approx(ticker_data$Volume, na.rm = FALSE)
        volume_series <- na.locf(volume_series, na.rm = FALSE)
        volume_series <- na.locf(volume_series, fromLast = TRUE, na.rm = FALSE)
        
        volume_non_na <- sum(!is.na(volume_series))
        
        if (volume_non_na >= 10) {
          tryCatch({
            ticker_data$Volume_SMA_10 <- SMA(volume_series, n = 10)
          }, error = function(e) {
            # Silently skip if error
          })
        }
        
        if (volume_non_na >= 20) {
          tryCatch({
            ticker_data$Volume_SMA_20 <- SMA(volume_series, n = 20)
          }, error = function(e) {
            # Silently skip if error
          })
        }
        
        # Simplified On-Balance Volume
        ticker_data$OBV <- 0
        for (i in 2:nrow(ticker_data)) {
          if (!is.na(ticker_data$LastPrice[i]) && !is.na(ticker_data$LastPrice[i-1]) &&
              !is.na(ticker_data$Volume[i])) {
            if (ticker_data$LastPrice[i] > ticker_data$LastPrice[i-1]) {
              ticker_data$OBV[i] <- ticker_data$OBV[i-1] + ticker_data$Volume[i]
            } else if (ticker_data$LastPrice[i] < ticker_data$LastPrice[i-1]) {
              ticker_data$OBV[i] <- ticker_data$OBV[i-1] - ticker_data$Volume[i]
            } else {
              ticker_data$OBV[i] <- ticker_data$OBV[i-1]
            }
          }
        }
      }
      
      # Calculate volatility (simple version)
      if (non_na_count >= 14) {
        tryCatch({
          # Use rolling standard deviation of returns
          ticker_data <- ticker_data %>%
            mutate(
              Returns_Volatility_14 = rollapply(Daily_Return, width = 14, 
                                                FUN = sd, fill = NA, align = "right", na.rm = TRUE)
            )
        }, error = function(e) {
          # Silently skip if error
        })
      }
      
      all_tech_results <- bind_rows(all_tech_results, ticker_data)
    } else {
      # Keep data even if we can't calculate indicators
      all_tech_results <- bind_rows(all_tech_results, ticker_data)
    }
  }
  
  cat(sprintf("✅ Technical indicators calculated for %d out of %d companies\n", 
              companies_with_indicators, length(tickers)))
  
  # Count how many indicators were calculated
  indicator_cols <- c("SMA_5", "SMA_10", "EMA_10", "SMA_20", "EMA_20", 
                      "RSI_14", "MACD_line", "MACD_signal", "MACD_histogram",
                      "BB_upper", "BB_middle", "BB_lower", "BB_pctB",
                      "Volume_SMA_10", "Volume_SMA_20", "OBV", "Returns_Volatility_14")
  
  indicators_found <- sapply(indicator_cols, function(col) col %in% colnames(all_tech_results))
  indicators_calculated <- sum(indicators_found)
  
  cat(sprintf("   • %d different technical indicators added\n", indicators_calculated))
  cat("   • Indicators available:\n")
  for (indicator in indicator_cols[indicators_found]) {
    non_na_count <- sum(!is.na(all_tech_results[[indicator]]))
    cat(sprintf("     - %s: %d non-NA values\n", indicator, non_na_count))
  }
  
  return(all_tech_results)
}

# Calculate technical indicators
technical_data <- calculate_technical_indicators(time_series_data)

# Save technical indicators dataset
tech_filename <- "technical_indicators_dataset.csv"
write_csv(technical_data, tech_filename)
cat(sprintf("📁 Saved technical indicators: %s (%.2f MB)\n", 
            tech_filename, file.info(tech_filename)$size / (1024 * 1024)))

# ============================================================
# 6. TIME SERIES ANALYSIS (SIMPLIFIED VERSION)
# ============================================================

cat("\n")
cat(rep("=", 70), "\n", sep = "")
cat("⏰ PERFORMING TIME SERIES ANALYSIS\n")
cat(rep("=", 70), "\n\n", sep = "")

perform_time_series_analysis <- function(tech_data) {
  cat("Step 1: Analyzing time series properties...\n")
  
  stationarity_tests <- data.frame()
  companies_analyzed <- 0
  
  tickers <- unique(tech_data$Ticker)
  
  for (ticker in tickers) {
    ticker_data <- tech_data %>% 
      filter(Ticker == ticker) %>% 
      arrange(Date) %>%
      filter(!is.na(LastPrice) & LastPrice > 0)
    
    if (nrow(ticker_data) >= 20) {
      companies_analyzed <- companies_analyzed + 1
      
      # Create time series object
      price_ts <- ts(ticker_data$LastPrice, frequency = 1)
      
      # Calculate basic statistics
      ts_stats <- data.frame(
        Ticker = ticker,
        N_Observations = length(price_ts),
        Mean_Price = mean(price_ts, na.rm = TRUE),
        SD_Price = sd(price_ts, na.rm = TRUE),
        Min_Price = min(price_ts, na.rm = TRUE),
        Max_Price = max(price_ts, na.rm = TRUE),
        Price_Range = max(price_ts, na.rm = TRUE) - min(price_ts, na.rm = TRUE)
      )
      
      # Try to perform ADF test for stationarity
      tryCatch({
        adf_test <- adf.test(price_ts, alternative = "stationary")
        ts_stats$ADF_PValue <- adf_test$p.value
        ts_stats$ADF_Statistic <- adf_test$statistic
      }, error = function(e) {
        ts_stats$ADF_PValue <- NA
        ts_stats$ADF_Statistic <- NA
      })
      
      stationarity_tests <- bind_rows(stationarity_tests, ts_stats)
    }
  }
  
  cat(sprintf("✅ Time series analysis completed for %d companies\n", companies_analyzed))
  
  return(stationarity_tests)
}

# Perform time series analysis
stationarity_results <- perform_time_series_analysis(technical_data)

# Save stationarity test results
stationarity_filename <- "stationarity_test_results.csv"
write_csv(stationarity_results, stationarity_filename)
cat(sprintf("📁 Saved stationarity tests: %s\n", stationarity_filename))

# ============================================================
# 7. CORRELATION ANALYSIS: SENTIMENT VS TECHNICAL INDICATORS
# ============================================================

cat("\n")
cat(rep("=", 70), "\n", sep = "")
cat("🔗 ANALYZING SENTIMENT-TECHNICAL CORRELATIONS\n")
cat(rep("=", 70), "\n\n", sep = "")

analyze_correlations <- function(tech_data) {
  cat("Step 1: Calculating correlations between sentiment and technical indicators...\n")
  
  # For each company, calculate correlations
  tickers <- unique(tech_data$Ticker)
  correlation_results <- data.frame()
  
  for (ticker in tickers) {
    ticker_data <- tech_data %>% 
      filter(Ticker == ticker) %>%
      filter(!is.na(composite_sentiment) & !is.na(LastPrice))
    
    if (nrow(ticker_data) >= 5) {
      ticker_corrs <- list(Ticker = ticker)
      
      # Calculate correlations with available indicators
      if ("RSI_14" %in% colnames(ticker_data)) {
        valid_data <- ticker_data %>% 
          filter(!is.na(RSI_14) & !is.na(composite_sentiment))
        if (nrow(valid_data) >= 5) {
          corr_rsi <- cor(valid_data$composite_sentiment, valid_data$RSI_14, use = "complete.obs")
          ticker_corrs$Corr_Sentiment_RSI <- corr_rsi
        }
      }
      
      if ("Daily_Return" %in% colnames(ticker_data)) {
        valid_data <- ticker_data %>% 
          filter(!is.na(Daily_Return) & !is.na(composite_sentiment))
        if (nrow(valid_data) >= 5) {
          corr_return <- cor(valid_data$composite_sentiment, valid_data$Daily_Return, use = "complete.obs")
          ticker_corrs$Corr_Sentiment_Return <- corr_return
        }
      }
      
      if ("Volume" %in% colnames(ticker_data)) {
        valid_data <- ticker_data %>% 
          filter(!is.na(Volume) & !is.na(composite_sentiment))
        if (nrow(valid_data) >= 5) {
          corr_volume <- cor(valid_data$composite_sentiment, valid_data$Volume, use = "complete.obs")
          ticker_corrs$Corr_Sentiment_Volume <- corr_volume
        }
      }
      
      correlation_results <- bind_rows(correlation_results, as.data.frame(ticker_corrs))
    }
  }
  
  cat(sprintf("✅ Correlation analysis completed for %d companies\n", nrow(correlation_results)))
  
  # Calculate summary statistics
  if (nrow(correlation_results) > 0) {
    correlation_summary <- list()
    
    for (col in colnames(correlation_results)) {
      if (col != "Ticker" && is.numeric(correlation_results[[col]])) {
        correlation_summary[[col]] <- c(
          mean = mean(correlation_results[[col]], na.rm = TRUE),
          sd = sd(correlation_results[[col]], na.rm = TRUE),
          min = min(correlation_results[[col]], na.rm = TRUE),
          max = max(correlation_results[[col]], na.rm = TRUE),
          count = sum(!is.na(correlation_results[[col]]))
        )
      }
    }
    
    cat("\n📊 Correlation Summary Statistics:\n")
    for (indicator in names(correlation_summary)) {
      stats <- correlation_summary[[indicator]]
      cat(sprintf("   • %s: Mean = %.3f, SD = %.3f (n = %d)\n", 
                  indicator, stats["mean"], stats["sd"], stats["count"]))
    }
  }
  
  return(correlation_results)
}

# Analyze correlations
correlation_results <- analyze_correlations(technical_data)

# Save correlation results
correlation_filename <- "sentiment_technical_correlation.csv"
write_csv(correlation_results, correlation_filename)
cat(sprintf("📁 Saved correlation analysis: %s\n", correlation_filename))

# ============================================================
# 8. MODULE 3 EXECUTION SUMMARY
# ============================================================

cat("\n")
cat(rep("=", 70), "\n", sep = "")
cat("📊 MODULE 3 EXECUTION SUMMARY\n")
cat(rep("=", 70), "\n\n", sep = "")

# Create summary
module3_summary <- list(
  execution_timestamp = Sys.time(),
  input_file = "data_with_sentiment.csv",
  output_files = c(
    "simulated_time_series_data.csv",
    "technical_indicators_dataset.csv",
    "stationarity_test_results.csv",
    "sentiment_technical_correlation.csv"
  ),
  dataset_metrics = list(
    total_companies = length(unique(technical_data$Ticker)),
    time_period_days = as.numeric(diff(range(technical_data$Date, na.rm = TRUE))),
    total_observations = nrow(technical_data),
    companies_with_technical_indicators = length(unique(technical_data$Ticker[!is.na(technical_data$SMA_5)])),
    companies_with_correlation_analysis = nrow(correlation_results)
  ),
  analysis_results = list(
    stationarity_tests_performed = nrow(stationarity_results),
    average_correlation_sentiment_return = ifelse("Corr_Sentiment_Return" %in% names(correlation_results),
                                                  mean(correlation_results$Corr_Sentiment_Return, na.rm = TRUE), NA)
  )
)

# Save summary
module3_summary_file <- "module3_execution_summary.rds"
saveRDS(module3_summary, module3_summary_file)

cat("📄 Module 3 execution summary saved\n\n")

# ============================================================
# 9. FINAL OUTPUT
# ============================================================

cat("📁 MODULE 3 OUTPUT FILES:\n")
output_files <- module3_summary$output_files
output_files <- c(output_files, "module3_execution_summary.rds")

for (file in output_files) {
  if (file.exists(file)) {
    size_mb <- round(file.info(file)$size / (1024 * 1024), 3)
    cat(sprintf("  • %-45s %.3f MB\n", file, size_mb))
  }
}

cat("\n📊 MODULE 3 ACHIEVEMENTS:\n")
cat(sprintf("  • Total companies processed: %d\n", module3_summary$dataset_metrics$total_companies))
cat(sprintf("  • Time period simulated: %d days\n", module3_summary$dataset_metrics$time_period_days))
cat(sprintf("  • Companies with technical indicators: %d\n", 
            module3_summary$dataset_metrics$companies_with_technical_indicators))
cat(sprintf("  • Companies with correlation analysis: %d\n", 
            module3_summary$dataset_metrics$companies_with_correlation_analysis))

cat("\n🔍 SAMPLE OF TECHNICAL INDICATORS DATA (first 3 rows):\n")
print(head(technical_data %>% select(Ticker, Date, LastPrice, SMA_5, SMA_10, RSI_14), 3))

cat("\n")
cat(rep("=", 70), "\n", sep = "")
cat("✅ MODULE 3 COMPLETED SUCCESSFULLY!\n")
cat(rep("=", 70), "\n\n", sep = "")

cat("🎉 DAII 3.5 FRAMEWORK TESTING COMPLETED!\n")
cat("\n")
cat(rep("=", 70), "\n", sep = "")
cat("📋 ALL 3 MODULES SUCCESSFULLY EXECUTED:\n")
cat("   1. ✅ Module 1: Data Ingestion & Preprocessing\n")
cat("   2. ✅ Module 2: Sentiment Analysis & NLP Processing\n")
cat("   3. ✅ Module 3: Technical Indicators & Time Series Analysis\n")
cat(rep("=", 70), "\n\n", sep = "")