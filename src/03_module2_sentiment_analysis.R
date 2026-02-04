# ============================================================
# SCRIPT 3: MODULE 2 - SENTIMENT ANALYSIS & NLP PROCESSING
# ============================================================
# Purpose: Generate synthetic sentiment data and analyze
# Run this script AFTER Script 2
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

required_packages <- c("readr", "dplyr", "tidyr", "lubridate", "stringr")
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
# 3. LOAD MODULE 1 PROCESSED DATA
# ============================================================

cat("📥 LOADING MODULE 1 PROCESSED DATA...\n")

processed_file <- "module1_processed_data.csv"

if (!file.exists(processed_file)) {
  stop(paste("❌ ERROR: Processed data not found at:", processed_file, 
             "\nPlease run Script 2 first."))
}

processed_data <- read_csv(processed_file, col_types = cols(), show_col_types = FALSE)

cat("✅ Processed data loaded successfully\n")
cat(sprintf("   • Rows: %d\n", nrow(processed_data)))
cat(sprintf("   • Columns: %d\n", ncol(processed_data)))
cat(sprintf("   • Companies: %d\n", length(unique(processed_data$Ticker))))

# Show column names to understand structure
cat("\n📋 COLUMNS AVAILABLE FOR SENTIMENT ANALYSIS:\n")
print(names(processed_data))

# ============================================================
# 4. GENERATE SYNTHETIC SENTIMENT DATA
# ============================================================

cat("\n")
cat(rep("=", 70), "\n", sep = "")
cat("🧠 GENERATING SYNTHETIC SENTIMENT DATA\n")
cat(rep("=", 70), "\n\n", sep = "")

generate_sentiment_data <- function(financial_data, seed = 123) {
  set.seed(seed)
  
  cat("Step 1: Creating sentiment observations for each date-ticker pair...\n")
  
  # Create base sentiment dataset
  sentiment_base <- financial_data %>%
    select(Ticker, Date) %>%
    distinct()
  
  cat(sprintf("   • Creating %d sentiment observations\n", nrow(sentiment_base)))
  
  # Generate synthetic sentiment metrics
  sentiment_data <- sentiment_base %>%
    mutate(
      # News sentiment (-1 to 1 scale)
      news_sentiment = rnorm(n(), mean = 0.2, sd = 0.3),
      
      # Social media sentiment
      social_media_sentiment = rnorm(n(), mean = 0.1, sd = 0.4),
      
      # Earnings call sentiment (higher during earnings months)
      # Since we only have one date (2024-12-31), we'll use month 12
      earnings_call_sentiment = case_when(
        month(Date) %in% c(1, 4, 7, 10) ~ rnorm(n(), mean = 0.3, sd = 0.2),
        TRUE ~ rnorm(n(), mean = 0, sd = 0.1)
      ),
      
      # Sentiment volatility
      sentiment_volatility = abs(rnorm(n(), mean = 0.15, sd = 0.05)),
      
      # Sentiment momentum
      sentiment_momentum = rnorm(n(), mean = 0, sd = 0.1),
      
      # Volume metrics
      news_volume = sample(10:1000, n(), replace = TRUE),
      social_mentions = sample(50:5000, n(), replace = TRUE),
      
      # Binary indicators
      positive_news = as.integer(news_sentiment > 0.2),
      negative_news = as.integer(news_sentiment < -0.2),
      neutral_news = as.integer(news_sentiment >= -0.2 & news_sentiment <= 0.2),
      
      # Composite sentiment score (weighted average)
      composite_sentiment = 0.4*news_sentiment + 0.3*social_media_sentiment + 0.3*earnings_call_sentiment,
      
      # Sentiment categories
      sentiment_category = case_when(
        composite_sentiment > 0.3 ~ "Very Positive",
        composite_sentiment > 0.1 ~ "Positive",
        composite_sentiment > -0.1 ~ "Neutral",
        composite_sentiment > -0.3 ~ "Negative",
        TRUE ~ "Very Negative"
      )
    )
  
  cat("Step 2: Adding industry-based sentiment variations...\n")
  
  # Get industry information from financial data
  if ("IndustryGroup" %in% colnames(financial_data)) {
    industry_info <- financial_data %>%
      select(Ticker, IndustryGroup) %>%
      distinct()
    
    sentiment_data <- sentiment_data %>%
      left_join(industry_info, by = "Ticker")
    
    # Add industry-based sentiment adjustments
    sentiment_data <- sentiment_data %>%
      mutate(
        industry_adjustment = case_when(
          grepl("Technology|Software|Semiconductors", IndustryGroup, ignore.case = TRUE) ~ 0.1,
          grepl("Health Care|Pharmaceuticals|Biotechnology", IndustryGroup, ignore.case = TRUE) ~ 0.05,
          grepl("Financial|Banks|Insurance", IndustryGroup, ignore.case = TRUE) ~ -0.05,
          grepl("Energy|Oil|Gas", IndustryGroup, ignore.case = TRUE) ~ -0.1,
          grepl("Consumer|Retail", IndustryGroup, ignore.case = TRUE) ~ 0,
          TRUE ~ 0
        ),
        adjusted_sentiment = composite_sentiment + industry_adjustment
      )
  }
  
  cat("✅ Synthetic sentiment data generated\n")
  cat(sprintf("   • Added %d sentiment metrics\n", ncol(sentiment_data) - 3))  # Excluding Ticker, Date, IndustryGroup
  
  return(sentiment_data)
}

# Generate sentiment data
sentiment_data <- generate_sentiment_data(processed_data)

# ============================================================
# 5. MERGE SENTIMENT WITH FINANCIAL DATA
# ============================================================

cat("\n🔗 MERGING SENTIMENT WITH FINANCIAL DATA...\n")

combined_data <- processed_data %>%
  left_join(sentiment_data, by = c("Ticker", "Date"))

# Save combined dataset
combined_filename <- "data_with_sentiment.csv"
write_csv(combined_data, combined_filename)

cat(sprintf("✅ Combined dataset created: %d rows × %d columns\n", 
            nrow(combined_data), ncol(combined_data)))
cat(sprintf("   • Saved: %s\n", combined_filename))
cat(sprintf("   • File size: %.3f MB\n", 
            file.info(combined_filename)$size / (1024 * 1024)))

# ============================================================
# 6. SENTIMENT ANALYSIS REPORT
# ============================================================

cat("\n📊 GENERATING SENTIMENT ANALYSIS REPORT...\n")

# Company-level sentiment summary
company_sentiment <- combined_data %>%
  group_by(Ticker) %>%
  summarise(
    avg_sentiment = mean(composite_sentiment, na.rm = TRUE),
    sentiment_volatility = sd(composite_sentiment, na.rm = TRUE),
    positive_news_count = sum(positive_news, na.rm = TRUE),
    negative_news_count = sum(negative_news, na.rm = TRUE),
    neutral_news_count = sum(neutral_news, na.rm = TRUE),
    total_news = n(),
    .groups = "drop"
  ) %>%
  mutate(
    sentiment_category = case_when(
      avg_sentiment > 0.3 ~ "Very Positive",
      avg_sentiment > 0.1 ~ "Positive",
      avg_sentiment > -0.1 ~ "Neutral",
      avg_sentiment > -0.3 ~ "Negative",
      TRUE ~ "Very Negative"
    ),
    positivity_ratio = positive_news_count / total_news
  )

# Add industry information if available
if ("IndustryGroup" %in% colnames(combined_data)) {
  industry_info <- combined_data %>%
    select(Ticker, IndustryGroup) %>%
    distinct()
  
  company_sentiment <- company_sentiment %>%
    left_join(industry_info, by = "Ticker")
}

# Save sentiment report
sentiment_report_file <- "sentiment_analysis_report.csv"
write_csv(company_sentiment, sentiment_report_file)

cat(sprintf("✅ Sentiment analysis report created for %d companies\n", 
            nrow(company_sentiment)))
cat(sprintf("   • Saved: %s\n", sentiment_report_file))

# ============================================================
# 7. VISUALIZATION USING BASE R GRAPHICS
# ============================================================

cat("\n🎨 CREATING SENTIMENT VISUALIZATIONS (Base R)...\n")

# Visualization 1: Sentiment Distribution (Bar Plot)
png("sentiment_distribution.png", width = 800, height = 600, res = 100)

# Create table of sentiment categories
sentiment_counts <- table(company_sentiment$sentiment_category)

# Order categories properly
category_order <- c("Very Positive", "Positive", "Neutral", "Negative", "Very Negative")
sentiment_counts <- sentiment_counts[category_order[category_order %in% names(sentiment_counts)]]

# Create bar plot with colors
colors <- c("darkgreen", "lightgreen", "lightblue", "orange", "red")
colors <- colors[1:length(sentiment_counts)]

par(mar = c(7, 5, 4, 2))  # Adjust margins for longer x-axis labels

barplot(sentiment_counts,
        main = "Distribution of Company Sentiment Categories",
        xlab = "",
        ylab = "Number of Companies",
        col = colors,
        ylim = c(0, max(sentiment_counts) * 1.2),
        cex.names = 1.1,
        cex.axis = 1.1,
        cex.main = 1.3)

title(xlab = "Sentiment Category", line = 5, cex.lab = 1.2)

# Add value labels on top of bars
text(x = barplot(sentiment_counts, plot = FALSE), 
     y = sentiment_counts + 1, 
     labels = sentiment_counts, 
     pos = 3, 
     cex = 1.1)

# Add subtitle
mtext(paste("Based on", nrow(company_sentiment), "companies"), 
      side = 3, line = 0.5, cex = 1.0)

dev.off()
cat("   • sentiment_distribution.png created\n")

# Visualization 2: Sentiment vs Market Cap Scatter Plot
if ("MarketCap" %in% colnames(combined_data)) {
  # Get average market cap and sentiment per company
  marketcap_sentiment <- combined_data %>%
    group_by(Ticker) %>%
    summarise(
      avg_marketcap = mean(MarketCap, na.rm = TRUE) / 1e9,  # Convert to billions
      avg_sentiment = mean(composite_sentiment, na.rm = TRUE),
      .groups = "drop"
    )
  
  png("sentiment_vs_marketcap.png", width = 1000, height = 700, res = 100)
  
  # Set up the plot
  par(mar = c(5, 5, 4, 5))
  
  # Create scatter plot with colored points
  plot(marketcap_sentiment$avg_sentiment, 
       marketcap_sentiment$avg_marketcap,
       xlab = "Average Sentiment Score",
       ylab = "Average Market Cap (Billions)",
       main = "Sentiment vs. Market Capitalization",
       pch = 19,
       cex = 1.2,
       col = "steelblue",
       cex.lab = 1.2,
       cex.axis = 1.1,
       cex.main = 1.4,
       xlim = range(marketcap_sentiment$avg_sentiment, na.rm = TRUE) * 1.1,
       ylim = range(marketcap_sentiment$avg_marketcap, na.rm = TRUE) * 1.1)
  
  # Add grid
  grid(col = "gray", lty = "dotted")
  
  # Add regression line
  if (nrow(marketcap_sentiment) > 1) {
    lm_fit <- lm(avg_marketcap ~ avg_sentiment, data = marketcap_sentiment)
    abline(lm_fit, col = "red", lwd = 2, lty = "dashed")
    
    # Add R-squared value
    r2 <- summary(lm_fit)$r.squared
    legend("topright", 
           legend = paste("R² =", round(r2, 3)),
           bty = "n",
           cex = 1.1)
  }
  
  # Add company count
  mtext(paste("Based on", nrow(marketcap_sentiment), "companies"), 
        side = 3, line = 0.5, cex = 1.0)
  
  dev.off()
  cat("   • sentiment_vs_marketcap.png created\n")
}

# Visualization 3: Sentiment Score Distribution (Histogram)
png("sentiment_histogram.png", width = 800, height = 600, res = 100)

hist(company_sentiment$avg_sentiment,
     breaks = 20,
     main = "Distribution of Average Sentiment Scores",
     xlab = "Average Sentiment Score",
     ylab = "Number of Companies",
     col = "lightblue",
     border = "darkblue",
     cex.lab = 1.2,
     cex.axis = 1.1,
     cex.main = 1.3)

# Add mean line
abline(v = mean(company_sentiment$avg_sentiment, na.rm = TRUE), 
       col = "red", lwd = 2, lty = "dashed")

# Add mean value text
mean_val <- mean(company_sentiment$avg_sentiment, na.rm = TRUE)
text(x = mean_val, 
     y = par("usr")[4] * 0.9, 
     labels = paste("Mean =", round(mean_val, 3)), 
     pos = ifelse(mean_val > 0, 2, 4), 
     col = "red", 
     cex = 1.1)

dev.off()
cat("   • sentiment_histogram.png created\n")

cat("✅ All visualizations created using Base R graphics\n")

# ============================================================
# 8. MODULE 2 EXECUTION SUMMARY
# ============================================================

cat("\n")
cat(rep("=", 70), "\n", sep = "")
cat("📊 MODULE 2 EXECUTION SUMMARY\n")
cat(rep("=", 70), "\n\n", sep = "")

# Create summary
module2_summary <- list(
  execution_timestamp = Sys.time(),
  input_file = "module1_processed_data.csv",
  output_files = c(
    "data_with_sentiment.csv",
    "sentiment_analysis_report.csv",
    "sentiment_distribution.png",
    "sentiment_histogram.png",
    if ("MarketCap" %in% colnames(combined_data)) "sentiment_vs_marketcap.png" else NULL
  ),
  dataset_metrics = list(
    rows_with_sentiment = nrow(combined_data),
    columns_with_sentiment = ncol(combined_data),
    companies_analyzed = length(unique(company_sentiment$Ticker)),
    sentiment_categories = table(company_sentiment$sentiment_category)
  ),
  sentiment_statistics = list(
    avg_composite_sentiment = mean(combined_data$composite_sentiment, na.rm = TRUE),
    sentiment_volatility = sd(combined_data$composite_sentiment, na.rm = TRUE),
    positive_news_total = sum(combined_data$positive_news, na.rm = TRUE),
    negative_news_total = sum(combined_data$negative_news, na.rm = TRUE)
  ),
  visualization_method = "Base R graphics (no ggplot2 dependency)"
)

# Remove NULL values from output files
module2_summary$output_files <- module2_summary$output_files[!sapply(module2_summary$output_files, is.null)]

# Save summary
module2_summary_file <- "module2_execution_summary.rds"
saveRDS(module2_summary, module2_summary_file)

cat("📄 Module 2 execution summary saved\n\n")

# ============================================================
# 9. FINAL OUTPUT
# ============================================================

cat("📁 MODULE 2 OUTPUT FILES:\n")
output_files <- module2_summary$output_files
output_files <- c(output_files, "module2_execution_summary.rds")

for (file in output_files) {
  if (file.exists(file)) {
    size_kb <- ifelse(grepl("\\.png$", file), 
                      round(file.info(file)$size / 1024, 2),
                      round(file.info(file)$size / 1024, 2))
    cat(sprintf("  • %-40s %.2f KB\n", file, size_kb))
  }
}

cat("\n📊 SENTIMENT ANALYSIS INSIGHTS:\n")
cat(sprintf("  • Companies analyzed: %d\n", nrow(company_sentiment)))
cat(sprintf("  • Average sentiment score: %.3f\n", mean(company_sentiment$avg_sentiment, na.rm = TRUE)))
cat(sprintf("  • Most common sentiment category: %s\n", 
            names(which.max(table(company_sentiment$sentiment_category)))))

cat("\n🔍 SAMPLE OF SENTIMENT DATA (first 5 rows):\n")
print(head(company_sentiment, 5))

cat("\n🔍 SAMPLE OF COMBINED DATA (first 3 rows):\n")
print(head(combined_data, 3))

cat("\n")
cat(rep("=", 70), "\n", sep = "")
cat("✅ MODULE 2 COMPLETED SUCCESSFULLY!\n")
cat(rep("=", 70), "\n\n", sep = "")

cat("🚀 READY FOR MODULE 3: TECHNICAL INDICATORS & TIME SERIES ANALYSIS\n")