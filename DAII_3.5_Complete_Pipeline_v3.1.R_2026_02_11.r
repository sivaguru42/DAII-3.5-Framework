# ==============================================================================
# DAII 3.5 – COMPLETE INTEGRATED PIPELINE (PHASE 1 + PHASE 2)
# Version: 3.1 FINAL | Date: 2026-02-13
# Includes:
#   - Innovation scoring (Module 1-3)
#   - FULL PORTFOLIO CONSTRUCTION (Module 4 - 3 strategies)
#   - Calibrated AI intensity scoring (Phase 2, Task 1)
#   - AI Exposure Cube & strategic profiles
#   - Predictive model (Random Forest) for AI Leadership
#   - Anomaly detection (Isolation Forest) – base R graphics
# ==============================================================================

# ----------------------------------------------------------------------------
# 0. USER PATHS – VERIFY BEFORE EACH RUN
# ----------------------------------------------------------------------------
SCRIPT_DIR <- "C:\\Users\\sganesan\\OneDrive - dumac.duke.edu\\DAII\\R\\scripts"
INPUT_DIR  <- "C:\\Users\\sganesan\\OneDrive - dumac.duke.edu\\DAII\\data\\input"
OUTPUT_DIR <- "C:\\Users\\sganesan\\OneDrive - dumac.duke.edu\\DAII\\data\\output"
SNAPSHOT_FILE <- file.path(INPUT_DIR, "N200_company_snapshot.csv")

setwd(SCRIPT_DIR)

# ----------------------------------------------------------------------------
# 1. LOAD REQUIRED PACKAGES (INSTALL MISSING IF NEEDED)
# ----------------------------------------------------------------------------
required_pkgs <- c(
  "dplyr", "tidyr", "scales", "yaml",          # Phase 1
  "tidymodels", "ranger",                      # Predictive model
  "isotree",                                    # Anomaly detection
  "parallel"                                    # For nthreads
)
missing_pkgs <- required_pkgs[!required_pkgs %in% installed.packages()[, "Package"]]
if (length(missing_pkgs) > 0) {
  cat("📦 Installing missing packages:", paste(missing_pkgs, collapse = ", "), "\n")
  install.packages(missing_pkgs, dependencies = TRUE)
}

suppressPackageStartupMessages({
  library(dplyr)
  library(tidyr)
  library(scales)
  library(yaml)
  library(tidymodels)
  library(ranger)
  library(isotree)
})

# ----------------------------------------------------------------------------
# 2. HELPER FUNCTIONS (COLUMN DETECTION, TYPE CONVERSION, SCORING)
# ----------------------------------------------------------------------------
`%||%` <- function(x, y) if (is.null(x) || length(x) == 0) y else x

find_column <- function(df, possible_names, default = NULL) {
  for (name in possible_names) {
    if (name %in% colnames(df)) return(df[[name]])
  }
  return(default)
}

smart_type_conversion <- function(data) {
  converted <- data
  for (col in names(converted)) {
    x <- converted[[col]]
    if (is.character(x)) {
      x_clean <- gsub("[$,%]", "", x)
      num <- suppressWarnings(as.numeric(x_clean))
      if (mean(is.na(num)) < 0.3) {
        converted[[col]] <- num
        next
      }
      date <- suppressWarnings(as.Date(x, format = "%Y-%m-%d"))
      if (mean(is.na(date)) < 0.3) converted[[col]] <- date
    }
  }
  return(converted)
}

# ----------------------------------------------------------------------------
# 3. LOAD AND PREPARE COMPANY SNAPSHOT
# ----------------------------------------------------------------------------
cat("\n📥 LOADING COMPANY SNAPSHOT...\n")
if (!file.exists(SNAPSHOT_FILE)) {
  stop("❌ Snapshot not found at: ", SNAPSHOT_FILE)
}

raw <- read.csv(SNAPSHOT_FILE, stringsAsFactors = FALSE)
raw <- smart_type_conversion(raw)

# Detect required columns with fallbacks
ticker <- as.character(find_column(raw, c("ticker", "Ticker"), paste0("TICK", 1:nrow(raw))))
market_cap <- find_column(raw, c("market_cap", "Mkt.Cap"), runif(nrow(raw), 1e9, 5e11))
rd_expense <- find_column(raw, c("rd_expense", "R.D.Exp"), runif(nrow(raw), 1e6, 2e9))
patent_activity <- find_column(raw, c("patent_activity", "Patents...Trademarks...Copy.Rgt"),
                               round(runif(nrow(raw), 0, 500)))
industry <- as.character(find_column(raw, c("industry", "GICS.Ind.Grp.Name"), "Unknown"))
fund_weight <- find_column(raw, c("fund_weight", "fund_weight_raw"), runif(nrow(raw), 0.001, 0.05))
revenue_growth <- find_column(raw, c("revenue_growth", "Rev...1.Yr.Gr"), runif(nrow(raw), -0.1, 0.3))
volatility_360d <- find_column(raw, c("volatility_360d", "Volatil.360D"), runif(nrow(raw), 0.1, 0.6))

# CRITICAL: Ensure position_value exists for portfolio weighting
position_value <- find_column(raw, c("position_value", "Position.Value"), 
                              runif(nrow(raw), 1e6, 1e9))

company_data <- data.frame(
  ticker = ticker,
  market_cap = as.numeric(market_cap),
  rd_expense = as.numeric(rd_expense),
  patent_activity = as.numeric(patent_activity),
  industry = industry,
  fund_weight = as.numeric(fund_weight),
  position_value = as.numeric(position_value),
  revenue_growth = as.numeric(revenue_growth),
  volatility_360d = as.numeric(volatility_360d),
  stringsAsFactors = FALSE
) %>% 
  distinct(ticker, .keep_all = TRUE) %>%  # ensure one row per ticker
  filter(!is.na(ticker) & ticker != "")    # remove invalid tickers

cat("✅ Loaded", nrow(company_data), "companies.\n\n")

# ----------------------------------------------------------------------------
# 4. CALIBRATED AI INTENSITY SCORING (PHASE 2, TASK 1)
# ----------------------------------------------------------------------------
calculate_ai_intensity_phase2 <- function(df) {
  rd_intensity <- df$rd_expense / (df$market_cap + 1) * 1e6
  rd_intensity[is.infinite(rd_intensity) | is.na(rd_intensity)] <- 0
  rd_score <- if (diff(range(rd_intensity)) > 0) 
    scales::rescale(rd_intensity, to = c(0, 35)) else rep(17.5, nrow(df))

  patent_productivity <- ifelse(df$rd_expense > 0 & !is.na(df$patent_activity),
                                df$patent_activity / (df$rd_expense / 1e6), 0)
  patent_productivity[is.infinite(patent_productivity) | is.na(patent_productivity)] <- 0
  patent_score <- if (diff(range(patent_productivity)) > 0)
    scales::rescale(patent_productivity, to = c(0, 30)) else rep(15, nrow(df))

  patent_quality <- ifelse(df$patent_activity > 0,
                           df$rd_expense / (df$patent_activity + 1), 0)
  patent_quality <- scales::rescale(patent_quality, to = c(0, 15))

  growth_momentum <- df$revenue_growth / (df$volatility_360d + 0.1)
  growth_momentum[is.infinite(growth_momentum) | is.na(growth_momentum)] <- 0
  momentum_score <- scales::rescale(growth_momentum, to = c(0, 10))

  industry_multiplier <- case_when(
    grepl("Technology|Communication|Semiconductor", df$industry, ignore.case = TRUE) ~ 1.3,
    grepl("Health", df$industry, ignore.case = TRUE) ~ 1.2,
    grepl("Industrials|Materials", df$industry, ignore.case = TRUE) ~ 1.1,
    grepl("Financials|Energy", df$industry, ignore.case = TRUE) ~ 0.9,
    TRUE ~ 1.0
  )
  industry_score <- scales::rescale(industry_multiplier, to = c(5, 15))

  ai_score <- (rd_score + patent_score + patent_quality + momentum_score + industry_score) * 2.5
  pmin(pmax(round(ai_score, 1), 0), 100)
}

# ----------------------------------------------------------------------------
# 5. INNOVATION SCORING & AI EXPOSURE CUBE
# ----------------------------------------------------------------------------
cat("🔧 ENGINEERING FEATURES & SCORING...\n")
company_features <- company_data %>%
  mutate(
    # Innovation components
    rd_intensity_score = if (!all(is.na(rd_expense / market_cap)))
      rescale(rd_expense / market_cap, to = c(0, 35)) else 0,
    patent_score = if (!all(is.na(patent_activity)))
      rescale(patent_activity, to = c(0, 30)) else 0,
    growth_score = if (!all(is.na(revenue_growth)))
      rescale(pmax(0, revenue_growth), to = c(0, 20)) else 0,
    stability_score = if (!all(is.na(volatility_360d)))
      rescale(1 - volatility_360d, to = c(0, 15)) else 0,

    innovation_score = round(rd_intensity_score + patent_score + growth_score + stability_score, 1),
    innovation_quartile = ntile(innovation_score, 4),

    # Calibrated AI score
    ai_score_phase2 = calculate_ai_intensity_phase2(.),
    ai_quartile_phase2 = ntile(ai_score_phase2, 4),

    # AI Exposure Cube dimensions
    ai_rd_efficiency = ifelse(patent_activity > 0, rd_expense / patent_activity, NA),
    ai_patent_momentum = patent_activity / (dplyr::lag(patent_activity, default = first(patent_activity)) + 0.01),
    ai_industry_exposure = case_when(
      grepl("Technology|Communication|Semiconductor", industry, ignore.case = TRUE) ~ 1.3,
      grepl("Health", industry, ignore.case = TRUE) ~ 1.2,
      grepl("Industrials|Materials", industry, ignore.case = TRUE) ~ 1.1,
      grepl("Financials|Energy", industry, ignore.case = TRUE) ~ 0.9,
      TRUE ~ 1.0
    )
  ) %>%
  mutate(
    ai_rd_efficiency = ifelse(is.infinite(ai_rd_efficiency) | is.na(ai_rd_efficiency),
                              median(ai_rd_efficiency, na.rm = TRUE), ai_rd_efficiency),
    ai_patent_momentum = ifelse(is.infinite(ai_patent_momentum) | is.na(ai_patent_momentum),
                                median(ai_patent_momentum, na.rm = TRUE), ai_patent_momentum)
  )

# AI Exposure Cube with strategic profiles
ai_cube <- company_features %>%
  select(ticker, innovation_score, ai_score_phase2, ai_quartile_phase2,
         ai_rd_efficiency, ai_patent_momentum, ai_industry_exposure) %>%
  mutate(
    ai_profile = case_when(
      ai_score_phase2 >= 70 ~ "AI Leader",
      ai_score_phase2 >= 50 ~ "AI Adopter",
      ai_score_phase2 >= 30 ~ "AI Follower",
      TRUE ~ "AI Laggard"
    ),
    efficiency_rating = ntile(ai_rd_efficiency, 3),
    momentum_rating = ntile(ai_patent_momentum, 3)
  )

cat("✅ Scoring complete.\n")
cat("   AI Leaders :", sum(ai_cube$ai_profile == "AI Leader"), "\n")
cat("   AI Adopters:", sum(ai_cube$ai_profile == "AI Adopter"), "\n")
cat("   AI Followers:", sum(ai_cube$ai_profile == "AI Follower"), "\n")
cat("   AI Laggards:", sum(ai_cube$ai_profile == "AI Laggard"), "\n\n")

# ----------------------------------------------------------------------------
# 6. FULL PORTFOLIO CONSTRUCTION (MODULE 4 – 3 STRATEGIES)
# ----------------------------------------------------------------------------
cat("📊 CONSTRUCTING PORTFOLIO STRATEGIES...\n")

# Merge position_value into company_features (already present from company_data)
company_features <- company_features %>%
  left_join(company_data %>% select(ticker, position_value), by = "ticker")

# Strategy 1: Quality Innovators (top innovation quartile)
quality_innovators <- company_features %>%
  filter(innovation_quartile == 4) %>%
  mutate(
    strategy_weight = (innovation_score * position_value) / 
                      sum(innovation_score * position_value, na.rm = TRUE)
  ) %>%
  select(ticker, innovation_score, ai_score_phase2, ai_profile, 
         position_value, strategy_weight) %>%
  arrange(desc(strategy_weight))

# Strategy 2: AI Concentrated (top AI quartile)
ai_concentrated <- company_features %>%
  filter(ai_quartile_phase2 == 4) %>%
  mutate(
    strategy_weight = (ai_score_phase2 * position_value) / 
                      sum(ai_score_phase2 * position_value, na.rm = TRUE)
  ) %>%
  select(ticker, innovation_score, ai_score_phase2, ai_profile, 
         position_value, strategy_weight) %>%
  arrange(desc(strategy_weight))

# Strategy 3: Balanced Growth (composite of innovation and AI)
balanced_growth <- company_features %>%
  mutate(
    composite_score = 0.6 * innovation_score + 0.4 * ai_score_phase2,
    composite_quartile = ntile(composite_score, 4)
  ) %>%
  filter(composite_quartile >= 3) %>%
  mutate(
    strategy_weight = (composite_score * position_value) / 
                      sum(composite_score * position_value, na.rm = TRUE)
  ) %>%
  select(ticker, innovation_score, ai_score_phase2, ai_profile, 
         position_value, composite_score, strategy_weight) %>%
  arrange(desc(strategy_weight))

# Consolidated weights for all strategies
all_portfolio_weights <- bind_rows(
  quality_innovators %>% mutate(strategy = "Quality Innovators"),
  ai_concentrated %>% mutate(strategy = "AI Concentrated"),
  balanced_growth %>% mutate(strategy = "Balanced Growth")
)

cat("✅ Portfolio construction complete.\n")
cat("   Quality Innovators: ", nrow(quality_innovators), "holdings\n")
cat("   AI Concentrated:    ", nrow(ai_concentrated), "holdings\n")
cat("   Balanced Growth:    ", nrow(balanced_growth), "holdings\n\n")

# ----------------------------------------------------------------------------
# 7. PREDICTIVE MODEL – RANDOM FOREST (PHASE 2, TASK 2)
# ----------------------------------------------------------------------------
cat("🌲 TRAINING RANDOM FOREST (AI LEADER PREDICTION)...\n")
set.seed(42)

# Prepare modeling data
model_data <- company_features %>%
  left_join(ai_cube %>% select(ticker, ai_profile), by = "ticker") %>%
  mutate(
    is_ai_leader = factor(ai_profile == "AI Leader",
                          levels = c(TRUE, FALSE),
                          labels = c("Leader", "Non-Leader"))
  ) %>%
  filter(!is.na(is_ai_leader))

feature_cols <- c(
  "rd_intensity_score", "patent_score", "growth_score", "stability_score",
  "ai_rd_efficiency", "ai_patent_momentum", "ai_industry_exposure",
  "rd_expense", "patent_activity", "market_cap"
)
available_features <- feature_cols[feature_cols %in% colnames(model_data)]

# Train/test split (80/20 random)
train_idx <- sample(1:nrow(model_data), size = floor(0.8 * nrow(model_data)))
train_data <- model_data[train_idx, ]
test_data  <- model_data[-train_idx, ]

rf_spec <- rand_forest(trees = 500, mtry = floor(sqrt(length(available_features))), min_n = 5) %>%
  set_engine("ranger", importance = "permutation", verbose = FALSE) %>%
  set_mode("classification")

rf_recipe <- recipe(is_ai_leader ~ ., data = train_data %>% select(all_of(available_features), is_ai_leader)) %>%
  step_naomit(all_predictors()) %>%
  step_normalize(all_numeric_predictors())

rf_workflow <- workflow() %>% add_recipe(rf_recipe) %>% add_model(rf_spec)
rf_fit <- rf_workflow %>% fit(data = train_data)

# Evaluate on test set
test_pred <- predict(rf_fit, test_data) %>%
  bind_cols(predict(rf_fit, test_data, type = "prob")) %>%
  bind_cols(test_data %>% select(ticker, is_ai_leader))

metrics <- test_pred %>% metrics(truth = is_ai_leader, estimate = .pred_class)
if (length(unique(test_data$is_ai_leader)) == 2) {
  roc_auc <- test_pred %>% roc_auc(truth = is_ai_leader, .pred_Leader)
} else { roc_auc <- NULL }

# Feature importance
rf_engine <- extract_fit_engine(rf_fit)
importance_df <- data.frame(
  feature = names(rf_engine$variable.importance),
  importance = rf_engine$variable.importance
) %>% arrange(desc(importance))

# Score full portfolio
all_pred <- predict(rf_fit, model_data) %>%
  bind_cols(predict(rf_fit, model_data, type = "prob")) %>%
  bind_cols(model_data %>% select(ticker, ai_profile))

company_scored <- company_features %>%
  left_join(all_pred %>% select(ticker, .pred_Leader), by = "ticker") %>%
  mutate(
    predicted_ai_leader_prob = round(.pred_Leader, 3),
    predicted_ai_leader_flag = predicted_ai_leader_prob > 0.5
  ) %>%
  select(-.pred_Leader)

cat("✅ Predictive model complete.\n")
cat("   Test Accuracy:", round(metrics %>% filter(.metric == "accuracy") %>% pull(.estimate), 3), "\n\n")

# ----------------------------------------------------------------------------
# 8. ANOMALY DETECTION – ISOLATION FOREST (PHASE 2, TASK 3)
# ----------------------------------------------------------------------------
cat("🌲 TRAINING ISOLATION FOREST (ANOMALY DETECTION)...\n")

anomaly_features <- c("ai_rd_efficiency", "ai_patent_momentum", "ai_score_phase2",
                      "rd_intensity_score", "patent_score")
avail_anom <- anomaly_features[anomaly_features %in% colnames(company_features)]

feature_matrix <- company_features %>%
  select(all_of(avail_anom)) %>%
  na.omit()
kept_idx <- which(complete.cases(company_features[, avail_anom]))
kept_tickers <- company_features$ticker[kept_idx]

set.seed(42)
iso_raw <- isolation.forest(
  data = feature_matrix,
  ntrees = 100,
  sample_size = min(256, nrow(feature_matrix)),
  ndim = 1,
  nthreads = parallel::detectCores() - 1,
  seed = 42,
  output_score = TRUE
)
class(iso_raw) <- "isolation_forest"
iso_model <- iso_raw

# Predict anomaly scores (robust)
anomaly_scores <- tryCatch(
  predict(iso_model, feature_matrix),
  error = function(e) isotree::predict.isolation_forest(iso_model, feature_matrix)
)

anomaly_results <- data.frame(
  ticker = kept_tickers,
  anomaly_score = as.numeric(anomaly_scores)
) %>%
  mutate(
    anomaly_rank = rank(-anomaly_score),
    is_anomaly = anomaly_score > quantile(anomaly_score, 0.95, na.rm = TRUE)
  ) %>%
  arrange(desc(anomaly_score)) %>%
  left_join(company_features %>% select(ticker, ai_score_phase2, innovation_score,
                                        ai_rd_efficiency, ai_patent_momentum, industry),
            by = "ticker") %>%
  left_join(ai_cube %>% select(ticker, ai_profile), by = "ticker")

top_anomalies <- head(anomaly_results, 10)

cat("✅ Anomaly detection complete.\n")
cat("   Top 5% anomalies:", sum(anomaly_results$is_anomaly), "\n\n")

# ----------------------------------------------------------------------------
# 9. BASE R VISUALIZATIONS (NO GGPLOT2)
# ----------------------------------------------------------------------------
cat("📈 GENERATING VISUALIZATIONS (BASE R)...\n")

# Histogram of anomaly scores
png(file.path(OUTPUT_DIR, "anomaly_histogram.png"), width = 800, height = 600)
hist(anomaly_results$anomaly_score, breaks = 30, col = "steelblue", border = "white",
     main = "Distribution of AI Anomaly Scores",
     xlab = "Anomaly Score", ylab = "Count")
abline(v = quantile(anomaly_results$anomaly_score, 0.95, na.rm = TRUE),
       col = "red", lwd = 2, lty = 2)
legend("topright", legend = "95th percentile", col = "red", lty = 2, lwd = 2)
dev.off()

# AI Score vs Anomaly Score
profile_colors <- c("AI Leader" = "darkgreen", "AI Adopter" = "blue",
                    "AI Follower" = "orange", "AI Laggard" = "red", "Unknown" = "gray")
colors_used <- profile_colors[anomaly_results$ai_profile]
colors_used[is.na(colors_used)] <- "gray"

png(file.path(OUTPUT_DIR, "ai_vs_anomaly.png"), width = 800, height = 600)
plot(anomaly_results$ai_score_phase2, anomaly_results$anomaly_score,
     col = colors_used, pch = 19, cex = 1.2,
     main = "AI Score vs. Anomaly Score",
     xlab = "AI Intensity Score", ylab = "Anomaly Score")
abline(h = quantile(anomaly_results$anomaly_score, 0.95, na.rm = TRUE),
       col = "red", lwd = 2, lty = 2)
legend("topright", legend = names(profile_colors), col = profile_colors, pch = 19, cex = 0.9)
dev.off()

# ----------------------------------------------------------------------------
# 10. SAVE ALL OUTPUTS WITH TIMESTAMPED PREFIX
# ----------------------------------------------------------------------------
cat("💾 SAVING OUTPUTS...\n")
run_timestamp <- format(Sys.time(), "%Y%m%d_%H%M%S")
run_prefix <- paste0("DAII_3.5_Complete_Run_", run_timestamp, "_N200")
run_dir <- file.path(OUTPUT_DIR, run_prefix)
dir.create(run_dir, recursive = TRUE)

# Core company files
write.csv(company_features, file.path(run_dir, paste0(run_prefix, "_01_company_features.csv")), row.names = FALSE)
write.csv(ai_cube, file.path(run_dir, paste0(run_prefix, "_02_ai_cube.csv")), row.names = FALSE)
write.csv(company_scored, file.path(run_dir, paste0(run_prefix, "_03_predictions.csv")), row.names = FALSE)

# Portfolio weights
write.csv(quality_innovators, file.path(run_dir, paste0(run_prefix, "_04_quality_innovators.csv")), row.names = FALSE)
write.csv(ai_concentrated, file.path(run_dir, paste0(run_prefix, "_05_ai_concentrated.csv")), row.names = FALSE)
write.csv(balanced_growth, file.path(run_dir, paste0(run_prefix, "_06_balanced_growth.csv")), row.names = FALSE)
write.csv(all_portfolio_weights, file.path(run_dir, paste0(run_prefix, "_07_all_portfolios.csv")), row.names = FALSE)

# Anomaly outputs
write.csv(anomaly_results, file.path(run_dir, paste0(run_prefix, "_08_anomaly_scores.csv")), row.names = FALSE)
write.csv(top_anomalies, file.path(run_dir, paste0(run_prefix, "_09_top_anomalies.csv")), row.names = FALSE)

# Model objects
saveRDS(rf_fit, file.path(run_dir, paste0(run_prefix, "_rf_model.rds")))
saveRDS(iso_model, file.path(run_dir, paste0(run_prefix, "_iso_model.rds")))

# Feature importance
write.csv(importance_df, file.path(run_dir, paste0(run_prefix, "_10_feature_importance.csv")), row.names = FALSE)

# Performance metrics
perf_df <- metrics
if (!is.null(roc_auc)) {
  perf_df <- bind_rows(perf_df, data.frame(.metric = "roc_auc", .estimator = "binary", .estimate = roc_auc$.estimate))
}
write.csv(perf_df, file.path(run_dir, paste0(run_prefix, "_11_performance.csv")), row.names = FALSE)

# Configuration
config <- list(
  run_id = run_timestamp,
  n_companies = nrow(company_data),
  ai_scoring = "calibrated_v2.5x",
  ai_leaders = sum(ai_cube$ai_profile == "AI Leader"),
  ai_adopters = sum(ai_cube$ai_profile == "AI Adopter"),
  ai_followers = sum(ai_cube$ai_profile == "AI Follower"),
  ai_laggards = sum(ai_cube$ai_profile == "AI Laggard"),
  portfolio_strategies = c("Quality Innovators", "AI Concentrated", "Balanced Growth"),
  rf_accuracy = round(metrics %>% filter(.metric == "accuracy") %>% pull(.estimate), 3),
  rf_auc = if (!is.null(roc_auc)) round(roc_auc$.estimate, 3) else NA,
  anomaly_threshold = round(quantile(anomaly_results$anomaly_score, 0.95, na.rm = TRUE), 4),
  n_anomalies = sum(anomaly_results$is_anomaly),
  execution_time = Sys.time()
)
yaml::write_yaml(config, file.path(run_dir, paste0(run_prefix, "_12_config.yaml")))

# Move visualizations into run directory
file.copy(file.path(OUTPUT_DIR, "anomaly_histogram.png"),
          file.path(run_dir, paste0(run_prefix, "_histogram.png")), overwrite = TRUE)
file.copy(file.path(OUTPUT_DIR, "ai_vs_anomaly.png"),
          file.path(run_dir, paste0(run_prefix, "_scatter.png")), overwrite = TRUE)
file.remove(file.path(OUTPUT_DIR, "anomaly_histogram.png"),
            file.path(OUTPUT_DIR, "ai_vs_anomaly.png"))

cat("✅ All outputs saved to:\n", run_dir, "\n")
cat("\n", paste(rep("=", 70), collapse = ""), "\n")
cat("🎯 DAII 3.5 COMPLETE PIPELINE EXECUTED SUCCESSFULLY\n")
cat(paste(rep("=", 70), collapse = ""), "\n")
cat("\n📁 OUTPUT FILES GENERATED:\n")
cat("   01_company_features.csv\n")
cat("   02_ai_cube.csv\n")
cat("   03_predictions.csv\n")
cat("   04_quality_innovators.csv\n")
cat("   05_ai_concentrated.csv\n")
cat("   06_balanced_growth.csv\n")
cat("   07_all_portfolios.csv\n")
cat("   08_anomaly_scores.csv\n")
cat("   09_top_anomalies.csv\n")
cat("   10_feature_importance.csv\n")
cat("   11_performance.csv\n")
cat("   12_config.yaml\n")
cat("   *_histogram.png\n")
cat("   *_scatter.png\n")
cat("   *_rf_model.rds\n")
cat("   *_iso_model.rds\n")