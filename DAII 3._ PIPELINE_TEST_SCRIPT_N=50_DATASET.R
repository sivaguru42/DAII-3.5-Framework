# ============================================================================
# DAII 3.5 PIPELINE TEST SCRIPT - N=50 DATASET
# ============================================================================
# Purpose: Test the complete pipeline with your existing data
# Input: chunk_1.csv through chunk_5.csv files in working directory
# ============================================================================

cat("🧪 STARTING DAII 3.5 PRODUCTION PIPELINE TEST\n")
cat(paste(rep("=", 80), collapse = ""), "\n")

# Step 1: Reassemble the N=50 dataset from chunks
cat("\n1️⃣ REASSEMBLING N=50 DATASET\n")
cat(paste(rep("-", 50), collapse = ""), "\n")

# Check for chunk files
chunk_files <- c("chunk_1.csv", "chunk_2.csv", "chunk_3.csv", "chunk_4.csv", "chunk_5.csv")
missing_chunks <- chunk_files[!file.exists(chunk_files)]

if (length(missing_chunks) > 0) {
  stop(sprintf("❌ Missing chunk files: %s", paste(missing_chunks, collapse = ", ")))
}

# Load and combine chunks
chunks <- lapply(chunk_files, function(f) {
  read.csv(f, stringsAsFactors = FALSE, check.names = FALSE)
})

full_data <- do.call(rbind, chunks)
cat(sprintf("✅ Reassembled: %d rows, %d columns\n", nrow(full_data), ncol(full_data)))

# Step 2: Run the field mapping system
cat("\n2️⃣ TESTING FIELD MAPPING SYSTEM\n")
cat(paste(rep("-", 50), collapse = ""), "\n")

# Create a test configuration that matches N=50 data
test_config <- list(
  daii_fields = list(
    ticker = list(expected = "Ticker", alternatives = c("Symbol")),
    rd_expense = list(expected = "R.D.Exp", alternatives = c("RD_Expense")),
    market_cap = list(expected = "Mkt.Cap", alternatives = c("MarketCap")),
    analyst_rating = list(expected = "BEst.Analyst.Rtg", alternatives = c("AnalystRating")),
    patent_count = list(expected = "Patents...Trademarks...Copy.Rgt", 
                        alternatives = c("Patent_Count")),
    news_sentiment = list(expected = "News.Sent", alternatives = c("News_Sentiment")),
    revenue_growth = list(expected = "Rev...1.Yr.Gr", alternatives = c("Revenue_Growth")),
    industry = list(expected = "GICS.Ind.Grp.Name", alternatives = c("Industry")),
    fund_name = list(expected = "fund_name", alternatives = c("Fund_Name")),
    fund_weight = list(expected = "fund_weight", alternatives = c("Weight"))
  )
)

# Save test config
writeLines(yaml::as.yaml(test_config), "test_config.yaml")

# Run field mapping
source("DAII_3_5_Production_Pipeline.R")  # Load the pipeline functions

std_data <- prepare_daii_data(
  raw_data = full_data,
  config_path = "test_config.yaml",
  user_col_map = list()  # No overrides needed for N-50
)

cat("\n📊 FIELD MAPPING RESULTS:\n")
mapping_report <- attr(std_data, "field_mapping")
for (field in names(mapping_report)) {
  cat(sprintf("   %-20s → %s\n", field, mapping_report[[field]]))
}

# Step 3: Test validation with standardized data
cat("\n3️⃣ TESTING DATA VALIDATION\n")
cat(paste(rep("-", 50), collapse = ""), "\n")

validation <- validate_standardized_data(std_data)
print(validation$validation_report$missing_data$summary)

# Step 4: Test company extraction
cat("\n4️⃣ TESTING COMPANY EXTRACTION\n")
cat(paste(rep("-", 50), collapse = ""), "\n")

companies <- extract_standardized_companies(validation)
cat(sprintf("Extracted %d unique companies\n", nrow(companies$companies)))

# Step 5: Test imputation
cat("\n5️⃣ TESTING IMPUTATION ENGINE\n")
cat(paste(rep("-", 50), collapse = ""), "\n")

imputed <- impute_missing_data_std(companies$companies, industry_col = "industry")
cat(sprintf("Imputed data: %d rows, %d columns\n", 
            nrow(imputed$imputed_data), ncol(imputed$imputed_data)))
cat(sprintf("Imputation flags: %d records had imputations\n", 
            sum(imputed$imputation_flags != "")))

# Step 6: Test scoring engine
cat("\n6️⃣ TESTING SCORING ENGINE\n")
cat(paste(rep("-", 50), collapse = ""), "\n")

scores <- calculate_component_scores_std(imputed$imputed_data)

cat("\n📈 SCORING SUMMARY:\n")
cat(sprintf("   DAII 3.5 Score range: %.1f to %.1f\n",
            min(scores$scores_data$daii_35_score, na.rm = TRUE),
            max(scores$scores_data$daii_35_score, na.rm = TRUE)))
cat(sprintf("   Mean DAII 3.5 Score: %.1f\n",
            mean(scores$scores_data$daii_35_score, na.rm = TRUE)))

# Step 7: Test Module 4 adaptation
cat("\n7️⃣ TESTING MODULE 4 ADAPTER\n")
cat(paste(rep("-", 50), collapse = ""), "\n")

scores_for_module4 <- adapt_for_module4(scores$scores_data)
cat(sprintf("Adapted columns: %s\n", 
            paste(colnames(scores_for_module4)[grep("_Score$", colnames(scores_for_module4))], 
                  collapse = ", ")))

# Step 8: Test Module 5 adaptation
cat("\n8️⃣ TESTING MODULE 5 ADAPTER\n")
cat(paste(rep("-", 50), collapse = ""), "\n")

module5_data <- adapt_for_module5(full_data, scores_for_module4)
cat(sprintf("Ticker column for merging: %s\n", module5_data$ticker_col))

# Step 9: Quick Portfolio Analysis Simulation
cat("\n9️⃣ SIMULATING PORTFOLIO ANALYSIS\n")
cat(paste(rep("-", 50), collapse = ""), "\n")

# Simple portfolio metrics without loading external modules
if ("fund_weight" %in% names(std_data) && "fund_name" %in% names(std_data)) {
  # Merge scores with standardized data for portfolio analysis
  portfolio_merged <- merge(
    std_data[, c("ticker", "fund_name", "fund_weight")],
    scores_for_module4[, c("ticker", "daii_35_score", "daii_quartile")],
    by = "ticker"
  )
  
  # Calculate weighted portfolio DAII
  portfolio_daii <- weighted.mean(portfolio_merged$daii_35_score, 
                                  portfolio_merged$fund_weight, 
                                  na.rm = TRUE)
  
  # Fund-level analysis
  fund_analysis <- portfolio_merged %>%
    group_by(fund_name) %>%
    summarise(
      n_holdings = n(),
      total_weight = sum(fund_weight, na.rm = TRUE),
      weighted_daii = weighted.mean(daii_35_score, fund_weight, na.rm = TRUE),
      .groups = 'drop'
    ) %>%
    arrange(desc(weighted_daii))
  
  cat(sprintf("Overall Portfolio DAII (weighted): %.1f\n", portfolio_daii))
  cat(sprintf("Number of funds: %d\n", nrow(fund_analysis)))
  
  cat("\n🏆 TOP 5 FUNDS BY INNOVATION SCORE:\n")
  print(head(fund_analysis, 5))
}

# Step 10: Save test results
cat("\n🔟 SAVING TEST RESULTS\n")
cat(paste(rep("-", 50), collapse = ""), "\n")

# Save intermediate results for debugging
write.csv(std_data, "test_standardized_data.csv", row.names = FALSE)
write.csv(scores$scores_data, "test_scored_data.csv", row.names = FALSE)

# Create a summary report
test_summary <- list(
  test_timestamp = Sys.time(),
  dataset_info = list(
    original_rows = nrow(full_data),
    original_cols = ncol(full_data),
    unique_companies = nrow(companies$companies),
    standardized_cols = ncol(std_data)
  ),
  field_mapping = mapping_report,
  scoring_results = list(
    daii_min = min(scores$scores_data$daii_35_score, na.rm = TRUE),
    daii_max = max(scores$scores_data$daii_35_score, na.rm = TRUE),
    daii_mean = mean(scores$scores_data$daii_35_score, na.rm = TRUE),
    quartile_distribution = table(scores$scores_data$daii_quartile)
  ),
  imputation_stats = list(
    records_imputed = sum(imputed$imputation_flags != ""),
    imputation_flags_sample = head(imputed$imputation_flags[imputed$imputation_flags != ""], 5)
  )
)

saveRDS(test_summary, "test_summary_report.rds")

cat("\n✅ TEST COMPLETE - RESULTS SUMMARY:\n")
cat(paste(rep("=", 60), collapse = ""), "\n")
cat(sprintf("1. Field Mapping: %d of %d fields mapped successfully\n",
            length(mapping_report), length(test_config$daii_fields)))
cat(sprintf("2. Data Processing: %d unique companies extracted\n", 
            test_summary$dataset_info$unique_companies))
cat(sprintf("3. DAII Scoring: Range %.1f to %.1f (Mean: %.1f)\n",
            test_summary$scoring_results$daii_min,
            test_summary$scoring_results$daii_max,
            test_summary$scoring_results$daii_mean))
cat(sprintf("4. Imputation: %d records required imputation\n",
            test_summary$imputation_stats$records_imputed))

cat("\n📁 TEST OUTPUT FILES:\n")
cat("   - test_standardized_data.csv: Field-mapped standardized data\n")
cat("   - test_scored_data.csv: Complete scoring results\n")
cat("   - test_summary_report.rds: Detailed test summary\n")
cat("   - test_config.yaml: Configuration used for mapping\n")

cat("\n" + paste(rep("=", 80), collapse = ""), "\n")
cat("🎯 DAII 3.5 PIPELINE TEST: READY FOR PRODUCTION DATA\n")
cat(paste(rep("=", 80), collapse = ""), "\n")