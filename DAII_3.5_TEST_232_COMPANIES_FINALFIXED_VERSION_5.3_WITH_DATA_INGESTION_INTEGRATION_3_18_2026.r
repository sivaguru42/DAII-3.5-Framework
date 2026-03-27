################################################################################
# DAII 3.5 - COMPLETE INTEGRATED PIPELINE - FINAL FIXED VERSION
# Version: 5.3 FINAL | Date: 2026-03-18
################################################################################

# =============================================================================
# SECTION 0: ENVIRONMENT SETUP
# =============================================================================
cat(paste(rep("=", 80), collapse = ""), "\n")
cat("DAII 3.5 - COMPLETE INTEGRATED PIPELINE v5.3 (FIXED)\n")
cat(paste(rep("=", 80), collapse = ""), "\n\n")

# Load configuration if available
config_loaded <- FALSE
if (file.exists(here::here("R", "00_config", "daii_config.yaml"))) {
  source(here::here("R", "01_utils", "load_config.r"))
  config <- load_all_configs()
  raw_dir <- get_config(config, "data.input_dir") %||% "data/raw/"
  script_dir <- here::here("R")
  output_dir <- get_config(config, "data.output_dir") %||% "outputs/"
  config_loaded <- TRUE
  cat("✅ Configuration loaded from R/00_config/\n\n")
}

# Fallback to hardcoded paths (GitHub repo)
if (!config_loaded) {
  raw_dir <- "C:/Users/sganesan/DAII-3.5-Framework/data/raw"
  script_dir <- "C:/Users/sganesan/DAII-3.5-Framework/R"
  output_dir <- "C:/Users/sganesan/DAII-3.5-Framework/outputs"
}

cat("📁 Directory Configuration:\n")
cat("   Raw data:     ", raw_dir, "\n")
cat("   Scripts:      ", script_dir, "\n")
cat("   Output:       ", output_dir, "\n\n")

# Package loading
required_packages <- c(
  "dplyr", "tidyr", "readr", "httr", "stringr", 
  "purrr", "lubridate", "yaml", "ggplot2", "openxlsx", 
  "corrplot", "moments", "randomForest", "isotree", "quantmod", "zoo"
)

load_packages_safely <- function(pkg_list) {
  for (pkg in pkg_list) {
    if (!require(pkg, character.only = TRUE, quietly = TRUE)) {
      cat(sprintf("   Installing missing package: %s\n", pkg))
      install.packages(pkg, dependencies = TRUE, repos = "https://cloud.r-project.org")
      library(pkg, character.only = TRUE)
      cat(sprintf("   ✅ Loaded: %s\n", pkg))
    } else {
      cat(sprintf("   ✅ Already available: %s\n", pkg))
    }
  }
}

cat("📦 Loading required packages...\n")
load_packages_safely(required_packages)
options(stringsAsFactors = FALSE, scipen = 999, digits = 4)
cat("✅ Environment configured.\n\n")

# =============================================================================
# SECTION 1: MODULE 0 - DATA INTEGRATION (LIVE DATA SOURCES)
# =============================================================================
cat(paste(rep("=", 80), collapse = ""), "\n")
cat("📊 MODULE 0: INTEGRATING LIVE DATA SOURCES\n")
cat(paste(rep("=", 80), collapse = ""), "\n\n")

library(DBI)
library(odbc)
library(dplyr)
library(here)

# -----------------------------------------------------------------------------
# 0.1 Source all ingestion scripts
# -----------------------------------------------------------------------------
# Utils (always first)
source(here("R", "01_utils", "database_connection.r"))

# Ingest scripts in numbered order
source(here("R", "02_ingest", "01_company_resolution.r"))
source(here("R", "02_ingest", "02_pull_rd_history.r"))
source(here("R", "02_ingest", "03_pull_holdings.r"))
source(here("R", "02_ingest", "04_pull_daily_ratios.r"))
source(here("R", "02_ingest", "05_patentsview.r"))
source(here("R", "02_ingest", "06_pull_bloomberg_api.r"))      # One-time pull (rarely used)
source(here("R", "02_ingest", "07_load_bloomberg_cache.r"))    # Every pipeline run
source(here("R", "02_ingest", "08_ingest_bloomberg_api.r"))    # Future production
source(here("R", "02_ingest", "09_pull_msci.r"))               # Optional

# Transform scripts
source(here("R", "03_transform", "01_create_snapshot.r"))
source(here("R", "03_transform", "02_calculate_features.r"))
source(here("R", "03_transform", "03_handle_missing.r"))

# -----------------------------------------------------------------------------
# 0.2 Build company resolution table (MUST RUN FIRST)
# -----------------------------------------------------------------------------
cat("🔍 Building company resolution table...\n")
company_map <- build_company_resolution()
cat(sprintf("   ✅ Mapped %d companies\n", nrow(company_map)))

# -----------------------------------------------------------------------------
# 0.3 Pull 40-year R&D history from Refinitiv
# -----------------------------------------------------------------------------
cat("\n📈 Pulling 40-year R&D history...\n")
rd_history <- pull_rd_history(company_map)
cat(sprintf("   ✅ Pulled %d rows of historical R&D data\n", nrow(rd_history)))

# -----------------------------------------------------------------------------
# 0.4 Pull current holdings from risk_exposure
# -----------------------------------------------------------------------------
cat("\n🏦 Pulling current holdings...\n")
current_holdings <- pull_current_holdings()
cat(sprintf("   ✅ Pulled %d holdings records\n", nrow(current_holdings)))

# -----------------------------------------------------------------------------
# 0.5 Pull daily ratios from Refinitiv
# -----------------------------------------------------------------------------
cat("\n📊 Pulling daily ratios...\n")
daily_ratios <- pull_daily_ratios(company_map, start_date = "2024-01-01")
cat(sprintf("   ✅ Pulled %d daily ratio records\n", nrow(daily_ratios)))

# -----------------------------------------------------------------------------
# 0.6 Pull patent data from USPTO bulk files (with automatic quarterly update)
# -----------------------------------------------------------------------------
cat("\n🔬 Pulling AI patent data from USPTO bulk files...\n")

# File paths
BULK_DIR <- here::here("data", "01_raw", "uspto_bulk/")
ASSIGNEE_FILE <- file.path(BULK_DIR, "g_assignee_disambiguated.tsv")
CPC_FILE <- file.path(BULK_DIR, "g_cpc_current.tsv")

# Check if files need updating (older than 90 days)
need_update <- FALSE
if(!file.exists(ASSIGNEE_FILE) || !file.exists(CPC_FILE)) {
  need_update <- TRUE
  cat("   Bulk files missing. Will download.\n")
} else {
  file_age <- difftime(Sys.time(), file.info(ASSIGNEE_FILE)$mtime, units = "days")
  if(file_age > 90) {
    need_update <- TRUE
    cat(sprintf("   Bulk files are %.0f days old. Updating...\n", file_age))
  } else {
    cat(sprintf("   ✅ Bulk files are %.0f days old\n", file_age))
  }
}

# Update if needed
if(need_update) {
  source("R/02_ingest/06_update_uspto_bulk.r")
  update_uspto_bulk()
}

# Now pull patent data (your existing function)
patent_data <- pull_patent_data(company_map, use_cache = TRUE)
cat(sprintf("   ✅ Pulled patent data for %d companies\n", nrow(patent_data)))

# Initialize patent_source column
patent_data$patent_source <- "USPTO Only"

# Define country lists for all patent sources
european_countries <- c("DEU", "GBR", "FRA", "NLD", "CHE", "SWE", "FIN", "ITA", "ESP", "AUT", "BEL", "DNK", "NOR", "IRL")

# -----------------------------------------------------------------------------
# 0.6.1 Pull European patent data (simplified manual approach)
# -----------------------------------------------------------------------------
cat("\n🇪🇺 Pulling European patent data...\n")

source("R/02_ingest/12_epo_api.r")

# Pull European data
european_patents <- pull_european_patents(company_map, use_cache = TRUE)

# Merge with USPTO patent data (handle column name case)
if(nrow(european_patents) > 0) {
  
  # Ensure we have a lowercase ticker column for join
  if("Ticker" %in% names(patent_data) && !"ticker" %in% names(patent_data)) {
    patent_data$ticker <- patent_data$Ticker
  }
  
  # Check if european_patents has the required columns
  if(all(c("ticker", "total_patents", "ai_patents") %in% names(european_patents))) {
    
    patent_data <- patent_data %>%
      left_join(european_patents %>% select(ticker, total_patents, ai_patents), 
                by = "ticker", suffix = c("", "_euro")) %>%
      mutate(
        total_patents = ifelse(is.na(total_patents_euro), total_patents, total_patents + total_patents_euro),
        ai_patents = ifelse(is.na(ai_patents_euro), ai_patents, ai_patents + ai_patents_euro),
        patent_source = case_when(
          !is.na(total_patents_euro) & total_patents_euro > 0 & total_patents > 0 ~ "USPTO + Europe",
          !is.na(total_patents_euro) & total_patents_euro > 0 ~ "Europe Only",
          TRUE ~ patent_source
        )
      ) %>%
      select(-ends_with("_euro"))
    
    cat(sprintf("   ✅ Merged European patent data for %d companies\n", nrow(european_patents)))
    
  } else {
    cat("   ⚠️ European patent data missing required columns\n")
    print(names(european_patents))
  }
  
} else {
  cat("   No European patent data to merge\n")
}

# -----------------------------------------------------------------------------
# 0.6.2 WIPO Bulk Data – Quarterly Update (PCT/Global Patents)
# -----------------------------------------------------------------------------
cat("\n🌍 Pulling WIPO PCT patent data (quarterly update)...\n")

source("R/02_ingest/13_wipo_bulk.r")

# Determine if this is a quarterly update month (March, June, September, December)
current_month <- as.numeric(format(Sys.Date(), "%m"))
is_quarterly_update <- current_month %in% c(3, 6, 9, 12)

if(is_quarterly_update) {
  cat("   📅 Quarterly update cycle detected\n")
  
  # Check if WIPO data is available
  current_year <- as.numeric(format(Sys.Date(), "%Y"))
  current_quarter <- ceiling(current_month / 3)
  
  if(check_wipo_availability(current_year, current_quarter)) {
    cat("   ✅ WIPO data available for ", current_year, " Q", current_quarter, "\n", sep="")
    
    # Download and process
    wipo_file <- download_wipo_bulk(year = current_year, quarter = current_quarter)
    
    if(!is.null(wipo_file)) {
      # Identify international companies (non-US, non-European)
      wipo_companies <- company_map %>%
        filter(!Country %in% c("USA", "United States", european_countries)) %>%
        select(ticker, company_name)
      
      if(nrow(wipo_companies) > 0) {
        wipo_results <- process_wipo_bulk(wipo_companies)
        
        # Merge with patent_data
        patent_data <- patent_data %>%
          left_join(wipo_results %>% select(ticker, total_patents, ai_patents), 
                    by = "ticker", suffix = c("", "_wipo")) %>%
          mutate(
            total_patents = ifelse(is.na(total_patents_wipo), total_patents, total_patents + total_patents_wipo),
            ai_patents = ifelse(is.na(ai_patents_wipo), ai_patents, ai_patents + ai_patents_wipo),
            patent_source = case_when(
              !is.na(total_patents_wipo) & total_patents_wipo > 0 & total_patents > 0 ~ paste(patent_source, "+ WIPO", sep=""),
              !is.na(total_patents_wipo) & total_patents_wipo > 0 ~ "WIPO Only",
              TRUE ~ patent_source
            )
          ) %>%
          select(-ends_with("_wipo"))
        
        cat(sprintf("   ✅ Merged WIPO data for %d companies\n", nrow(wipo_companies)))
      } else {
        cat("   No international companies for WIPO lookup\n")
      }
    }
  } else {
    cat("   ⚠️ WIPO data not yet available for current quarter\n")
    cat("   Will check next quarter\n")
  }
} else {
  cat("   Not a quarterly update month. Skipping WIPO bulk download.\n")
  cat("   Next update: ", format(as.Date(paste0(ifelse(current_month < 3, current_year, current_year + 1), 
                                                "-", c(3,6,9,12)[which(c(3,6,9,12) > current_month)[1]], "-01")), "%B %Y"), "\n")
}

# -----------------------------------------------------------------------------
# 0.7 Load Bloomberg cached data
# -----------------------------------------------------------------------------
cat("\n📈 Loading Bloomberg monthly prices from cache...\n")
bloomberg_prices <- tryCatch({
  load_bloomberg_cache()
}, error = function(e) {
  cat("   ⚠️ Bloomberg cache not available:", e$message, "\n")
  cat("   Continuing without Bloomberg data...\n")
  return(NULL)
})

if (!is.null(bloomberg_prices)) {
  cat(sprintf("   ✅ Loaded Bloomberg data for %d companies\n", 
              length(unique(bloomberg_prices$ticker))))
}

# -----------------------------------------------------------------------------
# 0.8 Pull MSCI data (optional)
# -----------------------------------------------------------------------------
msci_data <- NULL
cat("\n📊 MSCI data not available (optional, skipping)\n")

# -----------------------------------------------------------------------------
# 0.9 Create master dataset (one row per company)
# -----------------------------------------------------------------------------
cat("\n🔄 Creating master company snapshot...\n")

# Create base snapshot from core data sources
master_snapshot <- create_company_snapshot(
  company_map = company_map,
  rd_history = rd_history,
  current_holdings = current_holdings,
  daily_ratios = daily_ratios,
  patent_data = patent_data
)

# Add calculated features
master_snapshot <- calculate_company_features(master_snapshot)

# Handle missing values
master_snapshot <- handle_missing_values(master_snapshot)

# Join Bloomberg data if available
if (!is.null(bloomberg_prices)) {
  bloomberg_summary <- bloomberg_prices %>%
    group_by(ticker) %>%
    summarise(
      bloomberg_volatility = first(volatility_12m),
      total_return_5yr = last(price) / first(price) - 1,
      .groups = "drop"
    ) %>%
    rename(Ticker = ticker)
  
  master_snapshot <- master_snapshot %>%
    left_join(bloomberg_summary, by = "Ticker")
}

# Final deduplication
master_snapshot <- master_snapshot %>%
  group_by(Ticker) %>%
  slice(1) %>%
  ungroup()

cat(sprintf("   ✅ Master snapshot created: %d companies\n", nrow(master_snapshot)))
cat(sprintf("   📊 In portfolio: %d\n", sum(master_snapshot$in_portfolio)))
cat(sprintf("   📊 Discovery universe: %d\n", sum(!master_snapshot$in_portfolio)))

# Save snapshot for pipeline
snapshot_file <- here("data", "02_processed", "live_company_snapshot.rds")
saveRDS(master_snapshot, snapshot_file)
cat("\n✅ Live snapshot saved to:", snapshot_file, "\n")

# -----------------------------------------------------------------------------
# 0.10 Set daii_raw_data for downstream modules
# -----------------------------------------------------------------------------
daii_raw_data <- master_snapshot

# =============================================================================
# SECTION 2: LOAD HOLDINGS DATA (FIXED - INCLUDES ALL ISINs)
# =============================================================================
cat("\n", paste(rep("=", 80), collapse = ""), "\n")
cat("🏦 SECTION 2: LOADING DUMAC HOLDINGS DATA\n")
cat(paste(rep("=", 80), collapse = ""), "\n\n")

# Function to process holdings - INCLUDES ALL SECURITIES WITH ISINs
process_holdings_data_fixed <- function(holdings_path = "data/01_raw/current_holdings.rds",
                                        company_map_input = NULL) {
  
  message("   Processing DUMAC holdings data...")
  
  # Load holdings data
  holdings_data <- readRDS(holdings_path)
  holdings_aggregated <- holdings_data$firm_aggregated
  
  message(sprintf("   Raw holdings: %d securities", nrow(holdings_aggregated)))
  
  # Step 1: Keep ALL securities with ISINs (don't filter by name patterns)
  # This ensures Korean/Japanese companies with KR/JP ISINs are included
  holdings_all_isins <- holdings_aggregated %>%
    filter(!is.na(ISIN)) %>%
    mutate(
      clean_name = gsub(" - CLASS.*$| INC\\.?$| LTD\\.?$| CORP\\.?$| CO\\.?$", 
                        "", security_name),
      clean_name = trimws(clean_name),
      clean_name = toupper(clean_name)
    )
  
  message(sprintf("   Securities with ISINs: %d (%.1f%%)", 
                  nrow(holdings_all_isins),
                  nrow(holdings_all_isins) / nrow(holdings_aggregated) * 100))
  
  # Step 2: Prepare company map for matching
  company_lookup <- company_map_input %>%
    mutate(
      lookup_name = toupper(gsub(" Inc$| Corp$| Ltd$| LLC$| PLC$| Co$| Company$", 
                                 "", CompanyName)),
      lookup_name = trimws(lookup_name)
    )
  
  message(sprintf("   Company universe: %d companies", nrow(company_lookup)))
  
  # Step 3: Match by ISIN (primary method) - ALL ISINs included
  matches_isin <- company_lookup %>%
    left_join(holdings_all_isins, by = "ISIN", relationship = "many-to-many") %>%
    group_by(Ticker) %>%
    summarise(
      total_net_exposure_usd = sum(total_net_exposure_usd, na.rm = TRUE),
      total_net_pct_ltp = sum(total_net_pct_ltp, na.rm = TRUE),
      n_funds = sum(n_funds, na.rm = TRUE),
      matched_by = "ISIN",
      .groups = "drop"
    )
  
  isin_match_count <- sum(matches_isin$total_net_exposure_usd > 0, na.rm = TRUE)
  message(sprintf("   ISIN matches: %d companies", isin_match_count))
  
  # Step 4: Match remaining by company name
  if(nrow(company_lookup) > 0 && nrow(holdings_all_isins) > 0) {
    
    companies_without_isin_match <- matches_isin %>%
      filter(total_net_exposure_usd == 0 | is.na(total_net_exposure_usd)) %>%
      pull(Ticker)
    
    unmatched_companies <- company_lookup %>%
      filter(Ticker %in% companies_without_isin_match)
    
    matched_isin_tickers <- company_lookup %>%
      filter(Ticker %in% matches_isin$Ticker[matches_isin$total_net_exposure_usd > 0]) %>%
      pull(ISIN)
    
    holdings_for_name_match <- holdings_all_isins %>%
      filter(!ISIN %in% matched_isin_tickers)
    
    matches_name <- holdings_for_name_match %>%
      inner_join(unmatched_companies, by = c("clean_name" = "lookup_name"), 
                 relationship = "many-to-many") %>%
      group_by(Ticker) %>%
      summarise(
        total_net_exposure_usd = sum(total_net_exposure_usd, na.rm = TRUE),
        total_net_pct_ltp = sum(total_net_pct_ltp, na.rm = TRUE),
        n_funds = sum(n_funds, na.rm = TRUE),
        matched_by = "NAME",
        .groups = "drop"
      )
    
    message(sprintf("   Name matches: %d companies", nrow(matches_name)))
    
    # Merge name matches
    matches_isin <- matches_isin %>%
      left_join(matches_name, by = "Ticker", suffix = c("", "_name")) %>%
      mutate(
        total_net_exposure_usd = ifelse(is.na(total_net_exposure_usd_name), 
                                        total_net_exposure_usd, 
                                        total_net_exposure_usd_name),
        total_net_pct_ltp = ifelse(is.na(total_net_pct_ltp_name), 
                                   total_net_pct_ltp, 
                                   total_net_pct_ltp_name),
        n_funds = ifelse(is.na(n_funds_name), n_funds, n_funds_name),
        matched_by = ifelse(is.na(matched_by_name), matched_by, "NAME")
      ) %>%
      select(-ends_with("_name"))
    
  } else {
    message("   Name matches: 0 companies")
  }
  
  # Step 5: Calculate portfolio weights
  total_portfolio <- sum(matches_isin$total_net_exposure_usd, na.rm = TRUE)
  
  holdings_lookup <- matches_isin %>%
    mutate(
      total_net_exposure_usd = ifelse(is.na(total_net_exposure_usd), 0, total_net_exposure_usd),
      total_net_pct_ltp = ifelse(is.na(total_net_pct_ltp), 0, total_net_pct_ltp),
      n_funds = ifelse(is.na(n_funds), 0, n_funds),
      matched_by = ifelse(is.na(matched_by), "NONE", matched_by),
      in_portfolio = total_net_exposure_usd != 0,
      fund_weight = ifelse(in_portfolio, total_net_exposure_usd / total_portfolio, 0)
    )
  
  message(sprintf("\n   ✅ HOLDINGS SUMMARY:"))
  message(sprintf("      Total companies in universe: %d", nrow(holdings_lookup)))
  message(sprintf("      Companies WITH holdings: %d", sum(holdings_lookup$in_portfolio)))
  message(sprintf("      Companies WITHOUT holdings: %d", sum(!holdings_lookup$in_portfolio)))
  message(sprintf("      Total portfolio value: $%s", format(total_portfolio, big.mark = ",")))
  message(sprintf("\n   Matching methods:"))
  message(sprintf("      ISIN matches: %d", sum(holdings_lookup$matched_by == "ISIN")))
  message(sprintf("      Name matches: %d", sum(holdings_lookup$matched_by == "NAME")))
  message(sprintf("      Unmatched: %d", sum(holdings_lookup$matched_by == "NONE")))
  
  return(holdings_lookup)
}

# Process holdings data with the fixed function
holdings_lookup <- process_holdings_data_fixed(
  holdings_path = "data/01_raw/current_holdings.rds",
  company_map_input = company_map
)

cat(sprintf("\n   ✅ Holdings processed: %d companies total, %d with positions\n", 
            nrow(holdings_lookup), 
            sum(holdings_lookup$in_portfolio)))

# =============================================================================
# SECTION 3: CREATE MASTER TICKER LIST (ALL unique tickers)
# =============================================================================
cat(paste(rep("=", 80), collapse = ""), "\n")
cat("📋 SECTION 3: CREATING MASTER TICKER LIST\n")
cat(paste(rep("=", 80), collapse = ""), "\n\n")

master_tickers <- unique(c(rd_history$ticker, holdings_summary$Ticker))
cat(sprintf("   Master list: %d unique companies\n", length(master_tickers)))

# =============================================================================
# SECTION 4: LOAD PRE-FETCHED PANEL DATA
# =============================================================================
cat(paste(rep("=", 80), collapse = ""), "\n")
cat("📈 SECTION 4: LOADING PANEL DATA\n")
cat(paste(rep("=", 80), collapse = ""), "\n\n")

# Using master snapshot as company data
company_data <- master_snapshot
cat(sprintf("   Using master snapshot with %d companies\n", nrow(company_data)))

# =============================================================================
# SECTION 5: CREATE COMPANY SNAPSHOT (ONE ROW PER TICKER)
# =============================================================================
cat(paste(rep("=", 80), collapse = ""), "\n")
cat("📊 SECTION 5: CREATING COMPANY SNAPSHOT\n")
cat(paste(rep("=", 80), collapse = ""), "\n\n")

snapshot <- company_data

# FINAL DEDUPLICATION - ONE ROW PER TICKER
cat("   Before deduplication:", nrow(snapshot), "rows\n")

snapshot <- snapshot %>%
  group_by(Ticker) %>%
  slice(1) %>%
  ungroup()

cat("   After deduplication:", nrow(snapshot), "unique companies\n")

# Add fund_weight column
snapshot <- snapshot %>%
  mutate(
    fund_weight = ifelse(in_portfolio & !is.na(total_net_pct_ltp) & total_net_pct_ltp > 0,
                         total_net_pct_ltp / 100,
                         1 / n())
  )

# Save snapshot
snapshot_file <- file.path(raw_dir, "N245_company_snapshot_FIXED.csv")
write.csv(snapshot, snapshot_file, row.names = FALSE)
cat("\n✅ Snapshot saved to:", snapshot_file, "\n")

cat("\n📊 FINAL COMPOSITION:\n")
cat(sprintf("   Total companies: %d\n", nrow(snapshot)))
cat(sprintf("   In portfolio: %d\n", sum(snapshot$in_portfolio)))
cat(sprintf("   Discovery universe: %d\n", sum(!snapshot$in_portfolio)))
cat(sprintf("   With R&D data: %d\n", sum(!is.na(snapshot$rd_expense))))
cat(sprintf("   With patent data: %d\n", sum(snapshot$patent_activity > 0, na.rm = TRUE)))

# =============================================================================
# SECTION 6: PROCEED TO MAIN PIPELINE
# =============================================================================
cat(paste(rep("=", 80), collapse = ""), "\n")
cat("🚀 SECTION 6: READY FOR MAIN PIPELINE\n")
cat(paste(rep("=", 80), collapse = ""), "\n\n")

cat("✅ DATA PREPARATION COMPLETE\n")
cat("   Loading snapshot for main pipeline...\n")

# LOAD THE SNAPSHOT HERE
daii_raw_data <- read.csv(snapshot_file, stringsAsFactors = FALSE)
cat(sprintf("   ✅ Snapshot loaded: %d rows × %d columns\n", nrow(daii_raw_data), ncol(daii_raw_data)))

# =============================================================================

# Standardize column names for innovation scoring
if("Ticker" %in% names(daii_raw_data) && !"ticker" %in% names(daii_raw_data)) {
  daii_raw_data <- daii_raw_data %>% rename(ticker = Ticker)
  cat("   Renamed Ticker to ticker for innovation scoring\n")
}

# SECTION 7: MODULES 1-3 - INNOVATION SCORING
# =============================================================================
cat(paste(rep("=", 80), collapse = ""), "\n")
cat("📈 MODULES 1-3: INNOVATION SCORING\n")
cat(paste(rep("=", 80), collapse = ""), "\n\n")

# Source the innovation scoring module
source(here("R", "04_modules", "01_innovation_scoring.r"))

# Calculate innovation scores
daii_scored <- calculate_innovation_scores(daii_raw_data)

# =============================================================================
# SECTION 7.5: DEDUPLICATE TO ONE ROW PER COMPANY
# =============================================================================
cat("\n🔄 CRITICAL: Deduplicating to one row per company...\n")
cat("   Before dedupe:", nrow(daii_scored), "rows\n")

daii_scored <- daii_scored %>%
  group_by(ticker) %>%
  slice(1) %>%
  ungroup()

cat("   After dedupe:", nrow(daii_scored), "unique companies\n")
cat("   First 10 tickers after dedupe:\n")
print(head(daii_scored$ticker, 10))

cat("\n📊 Innovation Score Distribution:\n")
print(table(daii_scored$innovation_label))

# =============================================================================
# SECTION 8: MERGE HOLDINGS DATA & CREATE PORTFOLIO FLAG
# =============================================================================
cat(paste(rep("=", 80), collapse = ""), "\n")
cat("🏦 MERGING DUMAC HOLDINGS DATA\n")
cat(paste(rep("=", 80), collapse = ""), "\n\n")

# Use holdings_lookup from Section 2
if(exists("holdings_lookup") && nrow(holdings_lookup) > 0) {
  
  cat("   Using holdings_lookup from Section 2\n")
  cat(sprintf("   Holdings lookup contains %d companies\n", nrow(holdings_lookup)))
  cat(sprintf("   Companies with holdings: %d\n", sum(holdings_lookup$in_portfolio)))
  
  # Merge holdings_lookup with daii_scored
  daii_scored <- daii_scored %>%
    left_join(holdings_lookup, by = c("ticker" = "Ticker")) %>%
    mutate(
      # Use values from holdings_lookup, with defaults
      in_portfolio = ifelse(is.na(in_portfolio), FALSE, in_portfolio),
      n_funds = ifelse(is.na(n_funds), 0, n_funds),
      fund_weight = ifelse(is.na(fund_weight), 0, fund_weight),
      total_net_exposure_usd = ifelse(is.na(total_net_exposure_usd), 0, total_net_exposure_usd),
      total_net_pct_ltp = ifelse(is.na(total_net_pct_ltp), 0, total_net_pct_ltp)
    )
  
  cat("\n📊 PORTFOLIO COVERAGE:\n")
  cat(sprintf("   Companies in portfolio: %d\n", sum(daii_scored$in_portfolio, na.rm = TRUE)))
  cat(sprintf("   Companies not in portfolio: %d\n", sum(!daii_scored$in_portfolio, na.rm = TRUE)))
  cat(sprintf("   Total portfolio value: $%s\n", 
              format(sum(daii_scored$total_net_exposure_usd[daii_scored$in_portfolio], na.rm = TRUE), 
                     big.mark = ",")))
  
} else {
  cat("⚠️  holdings_lookup not found. Creating synthetic portfolio flags.\n")
  daii_scored <- daii_scored %>%
    mutate(
      in_portfolio = FALSE,
      n_funds = 0,
      fund_weight = 0,
      total_net_exposure_usd = 0,
      total_net_pct_ltp = 0
    )
}

# Simple fix: create industry column with default value
if(!"industry" %in% names(daii_scored)) {
  daii_scored$industry <- "Unknown"
  cat("   Created industry column with default 'Unknown'\n")
}

# SECTION 9: AI INTENSITY SCORING & PORTFOLIO CONSTRUCTION
# =============================================================================
cat(paste(rep("=", 80), collapse = ""), "\n")
cat("🤖 SECTION 9: AI INTENSITY SCORING & PORTFOLIO CONSTRUCTION\n")
cat(paste(rep("=", 80), collapse = ""), "\n\n")

# Industry Multipliers
industry_multipliers <- data.frame(
  industry = c(
    "Semiconductors & Semiconductor",
    "Software & Services",
    "Media & Entertainment",
    "Pharmaceuticals, Biotechnology",
    "Technology Hardware & Equipmen",
    "Automobiles & Components",
    "Financial Services",
    "Capital Goods",
    "Consumer Discretionary Distrib",
    "Energy",
    "Materials",
    "Utilities",
    "Unknown"
  ),
  multiplier = c(
    1.5, 1.4, 1.3, 1.2, 1.2, 1.1, 0.8, 0.9, 1.0, 0.7, 0.8, 0.5, 1.0
  )
)

# Calculate AI intensity scores
daii_scored <- daii_scored %>%
  left_join(industry_multipliers, by = "industry") %>%
  mutate(
    multiplier = ifelse(is.na(multiplier), 1.0, multiplier),
    ai_score = innovation_score * multiplier,
    ai_quartile = ntile(ai_score, 4),
    ai_label = case_when(
      ai_quartile == 4 ~ "AI Leader",
      ai_quartile == 3 ~ "AI Adopter",
      ai_quartile == 2 ~ "AI Follower",
      TRUE ~ "AI Laggard"
    )
  )

cat("\n🤖 AI Score Distribution:\n")
print(table(daii_scored$ai_label))

# Construct DUMAC Portfolios
cat("\n💼 Building DUMAC Portfolios...\n")

portfolio_vol_median <- median(daii_scored$volatility[daii_scored$in_portfolio], na.rm = TRUE)

dumac_portfolios <- daii_scored %>%
  filter(in_portfolio == TRUE) %>%
  mutate(
    quality_innovators_weight = ifelse(
      innovation_quartile == 4 & volatility < portfolio_vol_median,
      total_fund_weight / sum(total_fund_weight[innovation_quartile == 4 & volatility < portfolio_vol_median]),
      0
    ),
    ai_concentrated_weight = ifelse(
      ai_quartile == 4,
      total_fund_weight / sum(total_fund_weight[ai_quartile == 4]),
      0
    ),
    balanced_growth_weight = ifelse(
      innovation_quartile >= 3 & ai_quartile >= 3,
      total_fund_weight / sum(total_fund_weight[innovation_quartile >= 3 & ai_quartile >= 3]),
      0
    )
  )

cat(sprintf("   DUMAC Portfolio Companies: %d\n", nrow(dumac_portfolios)))
cat(sprintf("   Quality Innovators: %d\n", sum(dumac_portfolios$quality_innovators_weight > 0)))
cat(sprintf("   AI Concentrated: %d\n", sum(dumac_portfolios$ai_concentrated_weight > 0)))
cat(sprintf("   Balanced Growth: %d\n", sum(dumac_portfolios$balanced_growth_weight > 0)))

# Construct Discovery Portfolios
cat("\n🔍 Building Discovery Portfolios...\n")

discovery_portfolios <- daii_scored %>%
  filter(in_portfolio == FALSE) %>%
  mutate(
    innovation_rank = rank(-innovation_score),
    ai_rank = rank(-ai_score),
    combined_rank = rank(-(innovation_score + ai_score)),
    discovery_quality_weight = ifelse(
      innovation_quartile == 4 & volatility < median(volatility),
      innovation_score / sum(innovation_score[innovation_quartile == 4 & volatility < median(volatility)]),
      0
    ),
    discovery_ai_weight = ifelse(
      ai_quartile == 4,
      ai_score / sum(ai_score[ai_quartile == 4]),
      0
    ),
    discovery_tier = case_when(
      combined_rank <= 10 ~ "Tier 1: Top 10 Opportunities",
      combined_rank <= 25 ~ "Tier 2: Strong Candidates",
      combined_rank <= 50 ~ "Tier 3: Watch List",
      TRUE ~ "Tier 4: Monitor"
    )
  )

cat(sprintf("   Discovery Universe Companies: %d\n", nrow(discovery_portfolios)))
cat("\n📊 Discovery Tiers:\n")
print(table(discovery_portfolios$discovery_tier))

# Merge portfolios back
daii_scored <- daii_scored %>%
  left_join(dumac_portfolios[, c("ticker", "quality_innovators_weight", "ai_concentrated_weight", 
                                 "balanced_growth_weight")], by = "ticker") %>%
  left_join(discovery_portfolios[, c("ticker", "discovery_quality_weight", "discovery_ai_weight", 
                                    "discovery_tier", "innovation_rank", "ai_rank", "combined_rank")], 
            by = "ticker") %>%
  mutate(across(ends_with("_weight"), ~ifelse(is.na(.), 0, .)))

# =============================================================================
# SECTION 10: AI CUBE & ANOMALY DETECTION
# =============================================================================
cat(paste(rep("=", 80), collapse = ""), "\n")
cat("🧠 SECTION 10: AI CUBE & ANOMALY DETECTION\n")
cat(paste(rep("=", 80), collapse = ""), "\n\n")

# AI Exposure Cube
ai_cube <- daii_scored %>%
  mutate(
    strategic_profile = case_when(
      ai_quartile == 4 & innovation_quartile == 4 ~ "AI Pioneer",
      ai_quartile == 4 & innovation_quartile <= 2 ~ "AI Focused",
      ai_quartile <= 2 & innovation_quartile == 4 ~ "Innovation Leader",
      ai_quartile >= 3 & innovation_quartile >= 3 ~ "Balanced Performer",
      TRUE ~ "Underperformer"
    ),
    ai_exposure = case_when(ai_quartile == 4 ~ "High", ai_quartile == 3 ~ "Medium", TRUE ~ "Low"),
    innovation_exposure = case_when(innovation_quartile == 4 ~ "High", innovation_quartile == 3 ~ "Medium", TRUE ~ "Low")
  )

cat("📊 AI Exposure Cube Summary:\n")
print(table(ai_cube$strategic_profile))

# Anomaly Detection
cat("\n🔍 Running anomaly detection...\n")

anomaly_features <- ai_cube %>%
  select(rd_intensity, patent_activity, revenue_growth, volatility, 
         market_cap, innovation_score, ai_score) %>%
  mutate(across(everything(), ~ifelse(is.infinite(.) | is.na(.), 0, .)))

anomaly_features_scaled <- scale(anomaly_features) %>% 
  as.data.frame() %>%
  mutate(across(everything(), ~ifelse(is.na(.), 0, .)))

if (require(isotree, quietly = TRUE) && nrow(anomaly_features_scaled) > 10) {
  set.seed(42)
  iso_model <- isolation.forest(
    data = anomaly_features_scaled,
    ntrees = 100,
    sample_size = min(256, nrow(anomaly_features_scaled)),
    ndim = ncol(anomaly_features_scaled),
    seed = 42
  )
  ai_cube$anomaly_score <- predict(iso_model, anomaly_features_scaled)
} else {
  anomaly_scores <- sapply(anomaly_features_scaled, function(x) abs(x - mean(x)) / sd(x))
  ai_cube$anomaly_score <- rowMeans(anomaly_scores, na.rm = TRUE)
}

ai_cube$anomaly_score <- (ai_cube$anomaly_score - min(ai_cube$anomaly_score)) / 
  (max(ai_cube$anomaly_score) - min(ai_cube$anomaly_score))
anomaly_threshold <- quantile(ai_cube$anomaly_score, 0.9, na.rm = TRUE)
ai_cube$is_anomaly <- ai_cube$anomaly_score >= anomaly_threshold

cat(sprintf("\n   Anomalies detected: %d companies\n", sum(ai_cube$is_anomaly, na.rm = TRUE)))

# Merge back
daii_scored <- daii_scored %>%
  left_join(ai_cube[, c("ticker", "strategic_profile", "ai_exposure", "innovation_exposure", 
                        "anomaly_score", "is_anomaly")], by = "ticker")

# Final deduplication
daii_scored <- daii_scored %>%
  group_by(ticker) %>%
  slice(1) %>%
  ungroup()

# =============================================================================
# SECTION 10.5: AGGRESSIVE AI ANALYSIS (UNICORN HUNTING)
# =============================================================================
cat(paste(rep("=", 80), collapse = ""), "\n")
cat("🦄 SECTION 10.5: AGGRESSIVE AI ANALYSIS – UNICORN HUNTING\n")
cat(paste(rep("=", 80), collapse = ""), "\n\n")

# Re-weight for aggressive growth focus
weights_aggressive <- list(
  rd_weight = 0.25,      # Lower weight on R&D
  patent_weight = 0.20,  # Lower weight on patents
  growth_weight = 0.40,  # MUCH HIGHER weight on growth
  vol_weight = 0.15      # Lower penalty for volatility
)

# Calculate aggressive AI scores
daii_scored <- daii_scored %>%
  mutate(
    # Normalize scores
    rd_score_norm = rd_quartile * 25,
    patent_score_norm = patent_quartile * 25,
    growth_score_norm = growth_quartile * 25,
    stability_score_norm = (5 - volatility_quartile) * 25,
    
    # Aggressive AI score
    ai_score_aggressive = rd_score_norm * weights_aggressive$rd_weight +
      patent_score_norm * weights_aggressive$patent_weight +
      growth_score_norm * weights_aggressive$growth_weight +
      stability_score_norm * weights_aggressive$vol_weight
  )

# Identify high-potential unicorn candidates
daii_scored <- daii_scored %>%
  mutate(
    high_growth = revenue_growth >= quantile(revenue_growth, 0.7, na.rm = TRUE),
    high_volatility = volatility >= quantile(volatility, 0.7, na.rm = TRUE),
    has_rd = rd_intensity > 0 & !is.na(rd_intensity),
    not_ai_leader = ai_label != "AI Leader",
    
    high_potential_flag = high_growth & high_volatility & has_rd & not_ai_leader,
    
    unicorn_tier = case_when(
      high_potential_flag & ai_score_aggressive >= quantile(ai_score_aggressive, 0.8, na.rm = TRUE) ~ "Top Unicorn Candidate",
      high_potential_flag ~ "Emerging Unicorn Candidate",
      high_growth & has_rd & not_ai_leader ~ "Growth Candidate",
      TRUE ~ "Monitor"
    )
  )

# Set aggressive AI Leader threshold (top 35%)
aggressive_threshold <- quantile(daii_scored$ai_score_aggressive, 0.65, na.rm = TRUE)

daii_scored <- daii_scored %>%
  mutate(
    ai_label_aggressive = case_when(
      ai_score_aggressive >= aggressive_threshold ~ "AI Leader (Aggressive)",
      ai_score_aggressive >= quantile(ai_score_aggressive, 0.5, na.rm = TRUE) ~ "AI Adopter",
      ai_score_aggressive >= quantile(ai_score_aggressive, 0.25, na.rm = TRUE) ~ "AI Follower",
      TRUE ~ "AI Laggard"
    )
  )

# Create unicorn watchlist
unicorn_watchlist <- daii_scored %>%
  filter(unicorn_tier %in% c("Top Unicorn Candidate", "Emerging Unicorn Candidate")) %>%
  arrange(desc(ai_score_aggressive)) %>%
  select(ticker, company_name, ai_score_aggressive, revenue_growth, volatility, 
         unicorn_tier, rd_intensity, patent_activity)

cat("\n🦄 UNICORN HUNTING SUMMARY:\n")
cat("   Standard AI Leaders:", sum(daii_scored$ai_label == "AI Leader"), "\n")
cat("   Aggressive AI Leaders:", sum(daii_scored$ai_label_aggressive == "AI Leader (Aggressive)"), "\n")
cat("   Top Unicorn Candidates:", sum(unicorn_watchlist$unicorn_tier == "Top Unicorn Candidate"), "\n")
cat("   Emerging Unicorn Candidates:", sum(unicorn_watchlist$unicorn_tier == "Emerging Unicorn Candidate"), "\n")
cat("   Total High-Potential Watchlist:", nrow(unicorn_watchlist), "\n")

# ============================================================================
# SECTION 10.6: BENCHMARK CONSTITUENTS & COMPARISON
# ============================================================================
cat(paste(rep("=", 80), collapse = ""), "\n")
cat("📊 SECTION 10.6: BENCHMARK CONSTITUENTS & COMPARISON\n")
cat(paste(rep("=", 80), collapse = ""), "\n\n")

conn <- connect_research()

# Function to get constituents for a given index
get_index_constituents <- function(index_name, index_ric) {
  sql <- sprintf("
    SELECT DISTINCT i.Xref_Ticker AS Ticker
    FROM refinitiv.class20_geninfo_comp_idx_membr im
    JOIN refinitiv.class20_refinfo_issue i ON im.RepNo = i.RepNo
    WHERE im.[Index] = '%s'
      AND im.IndexRIC = '%s'
      AND i.Xref_Ticker IS NOT NULL
  ", index_name, index_ric)
  
  result <- dbGetQuery(conn, sql)
  return(result$Ticker)
}

# Fetch each benchmark and create individual variables
benchmarks_list <- list(
  "S&P 500" = list(name = "S&P 500", ric = ".INX", var_name = "sp500_tickers"),
  "NASDAQ 100" = list(name = "NASDAQ 100 Index", ric = ".NDX", var_name = "nasdaq100_tickers"),
  "S&P 400 Mid Cap" = list(name = "S&P 400 Mid Cap", ric = ".IDX", var_name = "sp400_mid_cap_tickers"),
  "S&P 600 Small Cap" = list(name = "S&P 600 Small Cap", ric = ".SPCY", var_name = "sp600_small_cap_tickers")
)

benchmark_constituents_list <- list()

for(b in names(benchmarks_list)) {
  cat("Fetching", b, "...\n")
  tickers <- get_index_constituents(benchmarks_list[[b]]$name, benchmarks_list[[b]]$ric)
  benchmark_constituents_list[[b]] <- tickers
  
  # Create individual variable
  var_name <- benchmarks_list[[b]]$var_name
  assign(var_name, tickers, envir = .GlobalEnv)
  
  cat("   Found", length(tickers), "constituents\n")
}

dbDisconnect(conn)

# Display results
cat("\n📊 BENCHMARK CONSTITUENTS SUMMARY:\n")
for(b in names(benchmark_constituents_list)) {
  cat("   ", b, ":", length(benchmark_constituents_list[[b]]), "companies\n")
}

# Calculate benchmark comparison
benchmark_results <- data.frame()

for(benchmark in names(benchmark_constituents_list)) {
  tickers <- benchmark_constituents_list[[benchmark]]
  
  # Filter to companies in your dataset
  available <- tickers[tickers %in% daii_scored$ticker]
  
  if(length(available) > 0) {
    benchmark_data <- daii_scored %>%
      filter(ticker %in% available) %>%
      summarise(
        benchmark = benchmark,
        n_constituents = length(tickers),
        n_covered = length(available),
        coverage_pct = round(length(available) / length(tickers) * 100, 1),
        avg_ai_score = mean(ai_score, na.rm = TRUE),
        median_ai_score = median(ai_score, na.rm = TRUE),
        pct_ai_leaders = mean(ai_label == "AI Leader", na.rm = TRUE) * 100,
        .groups = "drop"
      )
    
    benchmark_results <- bind_rows(benchmark_results, benchmark_data)
  }
}

# Add portfolio comparison
portfolio_data <- daii_scored %>%
  summarise(
    benchmark = "DUMAC Portfolio",
    n_constituents = NA,
    n_covered = n(),
    coverage_pct = NA,
    avg_ai_score = mean(ai_score, na.rm = TRUE),
    median_ai_score = median(ai_score, na.rm = TRUE),
    pct_ai_leaders = mean(ai_label == "AI Leader", na.rm = TRUE) * 100,
    .groups = "drop"
  )

benchmark_comparison <- bind_rows(portfolio_data, benchmark_results)

cat("\n📊 BENCHMARK COMPARISON:\n")
print(benchmark_comparison)

# Store for later use (will be saved in Section 11)
assign("benchmark_comparison", benchmark_comparison, envir = .GlobalEnv)
assign("benchmark_constituents_list", benchmark_constituents_list, envir = .GlobalEnv)

# ============================================================================
# SECTION 10.6.5: VARIABLE VERIFICATION
# ============================================================================
cat("\n", paste(rep("=", 80), collapse = ""), "\n")
cat("🔍 VARIABLE VERIFICATION\n")
cat(paste(rep("=", 80), collapse = ""), "\n\n")

# Check benchmark variables
required_vars <- list(
  "sp500_tickers" = "S&P 500 constituents",
  "nasdaq100_tickers" = "NASDAQ 100 constituents",
  "sp400_mid_cap_tickers" = "S&P 400 Mid Cap constituents",
  "sp600_small_cap_tickers" = "S&P 600 Small Cap constituents"
)

missing_vars <- c()
for(var_name in names(required_vars)) {
  if(exists(var_name)) {
    var_value <- get(var_name)
    cat(sprintf("   ✅ %s: %s (%d companies)\n", 
                required_vars[[var_name]], 
                var_name, 
                length(var_value)))
  } else {
    cat(sprintf("   ❌ %s: %s NOT FOUND\n", required_vars[[var_name]], var_name))
    missing_vars <- c(missing_vars, var_name)
  }
}

# Check portfolio holdings from Section 2
if(exists("holdings_lookup")) {
  cat(sprintf("   ✅ Holdings lookup: holdings_lookup (%d companies)\n", 
              nrow(holdings_lookup)))
} else {
  cat("   ❌ holdings_lookup NOT FOUND\n")
  missing_vars <- c(missing_vars, "holdings_lookup")
}

# Check snapshot from Section 5
if(exists("snapshot")) {
  cat(sprintf("   ✅ Company snapshot: snapshot (%d companies)\n", 
              nrow(snapshot)))
} else {
  cat("   ❌ snapshot NOT FOUND\n")
  missing_vars <- c(missing_vars, "snapshot")
}

# Check daii_scored from Section 6
if(exists("daii_scored")) {
  cat(sprintf("   ✅ Scored data: daii_scored (%d companies)\n", 
              nrow(daii_scored)))
} else {
  cat("   ❌ daii_scored NOT FOUND\n")
  missing_vars <- c(missing_vars, "daii_scored")
}

# Check daily_ratios from Section 0.5
if(exists("daily_ratios")) {
  cat(sprintf("   ✅ Daily ratios: daily_ratios (%d rows)\n", 
              nrow(daily_ratios)))
} else {
  cat("   ❌ daily_ratios NOT FOUND\n")
  missing_vars <- c(missing_vars, "daily_ratios")
}

# Create portfolio_holdings if it doesn't exist
if(!exists("portfolio_holdings") && exists("holdings_lookup") && exists("snapshot")) {
  cat("\n   ℹ️ Creating portfolio_holdings from existing data...\n")
  portfolio_holdings <- snapshot %>%
    left_join(holdings_lookup, by = c("ticker" = "Ticker")) %>%
    filter(in_portfolio == TRUE) %>%
    select(ticker, company_name, fund_weight, ai_score, ai_label, 
           innovation_score, revenue_growth, volatility, unicorn_tier)
  assign("portfolio_holdings", portfolio_holdings, envir = .GlobalEnv)
  cat(sprintf("   ✅ Created portfolio_holdings with %d companies\n", nrow(portfolio_holdings)))
}

# If any variables are missing, provide guidance
if(length(missing_vars) > 0) {
  cat("\n⚠️ WARNING: Missing variables detected:\n")
  for(var in missing_vars) {
    cat(sprintf("   - %s\n", var))
  }
  cat("\n   These variables are required for Sections 10.7 and 12.\n")
  cat("   Check that Section 2 and Section 10.6 are running correctly.\n")
} else {
  cat("\n✅ All required variables verified!\n")
}

cat("\n")

# ============================================================================
# SECTION 10.7: BENCHMARK AI EXPOSURE ANALYSIS
# ============================================================================
cat("\n", paste(rep("=", 80), collapse = ""), "\n")
cat("📊 SECTION 10.7: BENCHMARK AI EXPOSURE ANALYSIS\n")
cat(paste(rep("=", 80), collapse = ""), "\n\n")

source("R/04_modules/06_benchmark_ai_exposure.r")

# Use the benchmark constituents list from Section 10.6
benchmark_constituents_for_ai <- list(
  "S&P 500" = sp500_tickers,
  "NASDAQ 100" = nasdaq100_tickers,
  "S&P 400 Mid Cap" = sp400_mid_cap_tickers,
  "S&P 600 Small Cap" = sp600_small_cap_tickers
)

# Calculate benchmark AI exposures
benchmark_ai_results <- list()
for(bench_name in names(benchmark_constituents_for_ai)) {
  cat(sprintf("   Calculating AI exposure for %s...\n", bench_name))
  benchmark_ai_results[[bench_name]] <- calculate_benchmark_ai_exposure(
    benchmark_constituents_for_ai[[bench_name]],
    daii_scored,
    weights = "market_cap"
  )
}

# Compare portfolio to benchmarks
ai_exposure_comparison <- compare_ai_exposure(
  portfolio_holdings,
  benchmark_constituents_for_ai,
  daii_scored
)

# Calculate sector-level AI exposure for S&P 500
cat("\n   Calculating sector AI exposure for S&P 500...\n")
sp500_sector_ai <- calculate_sector_ai_exposure(
  benchmark_constituents_for_ai[["S&P 500"]],
  daii_scored
)

# Create radar chart data
radar_data <- create_ai_radar_data(
  ai_exposure_comparison[ai_exposure_comparison$name == "DUMAC Portfolio", ],
  benchmark_ai_results
)

# Display results
cat("\n📊 AI EXPOSURE COMPARISON:\n")
print(ai_exposure_comparison)

cat("\n📊 S&P 500 AI EXPOSURE BY SECTOR (Top 10):\n")
print(head(sp500_sector_ai, 10))

# Store results for Section 11 to access
assign("ai_exposure_comparison", ai_exposure_comparison, envir = .GlobalEnv)
assign("sp500_sector_ai", sp500_sector_ai, envir = .GlobalEnv)
assign("radar_data", radar_data, envir = .GlobalEnv)
assign("benchmark_ai_results", benchmark_ai_results, envir = .GlobalEnv)

cat("\n   ✅ Benchmark AI exposure analysis complete\n")

# -----------------------------------------------------------------------------
# SECTION 12: PERFORMANCE ATTRIBUTION & BENCHMARK COMPARISON
# -----------------------------------------------------------------------------
cat("\n", paste(rep("=", 80), collapse = ""), "\n")
cat("📈 SECTION 12: PERFORMANCE ATTRIBUTION\n")
cat(paste(rep("=", 80), collapse = ""), "\n\n")

source("R/04_modules/05_performance_attribution.r")

# Initialize variables
attribution_results <- NULL
benchmark_comparison <- NULL

# Check if benchmark returns are available in daily_ratios
cat("   Checking for benchmark data in daily_ratios...\n")

# daily_ratios has as_of_date (not date) and Ticker (capital T)
if("as_of_date" %in% names(daily_ratios) && "price_1d_pct_change" %in% names(daily_ratios)) {
  
  # Check if benchmark tickers exist
  benchmarks_present <- c(".SPX", ".IXIC") %in% unique(daily_ratios$Ticker)
  
  if(all(benchmarks_present)) {
    cat("   ✅ Benchmark tickers found in daily_ratios\n")
    
    # Get benchmark returns with correct column mapping
    benchmark_returns <- daily_ratios %>%
      filter(Ticker %in% c(".SPX", ".IXIC")) %>%
      select(date = as_of_date, ticker = Ticker, daily_return = price_1d_pct_change) %>%
      arrange(ticker, date)
    
    cat(sprintf("   Benchmark returns retrieved: %d rows\n", nrow(benchmark_returns)))
    
    # Create holdings weights from company snapshot
    holdings_weights <- snapshot %>%
      left_join(holdings_lookup, by = c("ticker" = "Ticker")) %>%
      filter(in_portfolio == TRUE) %>%
      select(Ticker = ticker, fund_weight)
    
    # Prepare returns data for all stocks
    returns_data <- daily_ratios %>%
      select(date = as_of_date, ticker = Ticker, daily_return = price_1d_pct_change) %>%
      filter(!is.na(daily_return))
    
    cat(sprintf("   Returns data prepared: %d rows\n", nrow(returns_data)))
    
    # Calculate attribution (using S&P 500 as primary benchmark)
    cat("   Calculating performance attribution...\n")
    attribution_results <- calculate_performance_attribution(
      holdings_data = holdings_weights,
      benchmark_data = benchmark_returns %>% 
        filter(ticker == ".SPX") %>% 
        select(date, benchmark_return = daily_return),
      return_data = returns_data,
      start_date = "2020-01-01",
      end_date = Sys.Date()
    )
    
    # Create benchmark comparison
    if(!is.null(attribution_results)) {
      benchmark_comparison <- create_benchmark_comparison(snapshot, attribution_results)
      
      cat("\n📊 BENCHMARK COMPARISON:\n")
      print(benchmark_comparison)
      
      # Save to run directory (CSV and RDS)
      write.csv(benchmark_comparison, 
                file.path(run_dir, paste0(run_timestamp, "_21_benchmark_comparison_detailed.csv")), 
                row.names = FALSE)
      saveRDS(attribution_results, 
              file.path(run_dir, paste0(run_timestamp, "_22_attribution_results.rds")))
      
      cat("\n   ✅ Performance attribution complete\n")
      
      # ============================================================================
      # ADD ATTRIBUTION SHEETS TO EXCEL WORKBOOK
      # ============================================================================
      # Load the existing workbook
      excel_file <- file.path(run_dir, paste0(run_timestamp, "_00_complete_report.xlsx"))
      
      if(file.exists(excel_file)) {
        # Load the workbook
        wb <- loadWorkbook(excel_file)
        
        # Sheet 12: Performance Attribution (Daily Data)
        addWorksheet(wb, "Performance Attribution")
        writeData(wb, "Performance Attribution", attribution_results$attribution)
        
        # Sheet 13: Attribution Metrics (Summary)
        addWorksheet(wb, "Attribution Metrics")
        writeData(wb, "Attribution Metrics", attribution_results$summary)
        
        # Sheet 14: Benchmark Comparison
        addWorksheet(wb, "Benchmark Comparison")
        writeData(wb, "Benchmark Comparison", benchmark_comparison)
        
        # Sheet 15: Portfolio Concentration
        addWorksheet(wb, "Portfolio Concentration")
        
        # Calculate portfolio concentration
        portfolio_holdings <- snapshot %>%
          left_join(holdings_lookup, by = c("ticker" = "Ticker")) %>%
          filter(in_portfolio == TRUE) %>%
          arrange(desc(fund_weight)) %>%
          mutate(
            cumulative_weight = cumsum(fund_weight),
            rank = row_number()
          ) %>%
          select(rank, ticker, company_name, total_net_exposure_usd, fund_weight, cumulative_weight, ai_label)
        
        writeData(wb, "Portfolio Concentration", portfolio_holdings)
        
        # Format all new sheets
        header_style <- createStyle(fontSize = 12, fontColour = "#FFFFFF", 
                                    halign = "center", fgFill = "#2C3E50", textDecoration = "bold")
        
        new_sheets <- c("Performance Attribution", "Attribution Metrics", 
                        "Benchmark Comparison", "Portfolio Concentration")
        
        for(sheet_name in new_sheets) {
          # Get data dimensions
          sheet_data <- wb[[sheet_name]]
          if(!is.null(sheet_data) && ncol(sheet_data) > 0 && nrow(sheet_data) > 0) {
            addStyle(wb, sheet_name, header_style, rows = 1, cols = 1:ncol(sheet_data), gridExpand = TRUE)
            freezePane(wb, sheet_name, firstRow = TRUE)
            setColWidths(wb, sheet_name, cols = 1:ncol(sheet_data), widths = "auto")
          }
        }
        
        # Save the updated workbook
        saveWorkbook(wb, excel_file, overwrite = TRUE)
        cat("   ✅ Updated Excel workbook with performance attribution sheets\n")
        
        # Print summary of new sheets
        cat("\n   📊 New sheets added:\n")
        cat("      - Performance Attribution: Daily portfolio vs benchmark returns\n")
        cat("      - Attribution Metrics: Alpha, Beta, Sharpe, Information Ratio\n")
        cat("      - Benchmark Comparison: Portfolio vs S&P 500, NASDAQ 100\n")
        cat("      - Portfolio Concentration: Top holdings with cumulative weight\n")
        
      } else {
        cat("   ⚠️ Excel workbook not found, attribution sheets not added\n")
      }
      # ============================================================================
      
    } else {
      cat("   ⚠️ Insufficient data for attribution calculation\n")
    }
    
  } else {
    cat("   ⚠️ Benchmark tickers not found in daily_ratios\n")
    cat("      Available tickers starting with '.':\n")
    dot_tickers <- unique(daily_ratios$Ticker[grepl("^\\.", daily_ratios$Ticker)])
    if(length(dot_tickers) > 0) {
      cat("      ", paste(dot_tickers, collapse = ", "), "\n")
    } else {
      cat("      No index tickers found. Performance attribution skipped.\n")
    }
  }
  
} else {
  cat("   ⚠️ daily_ratios missing required columns\n")
  cat("      Available columns:", paste(names(daily_ratios)[1:10], collapse=", "), "...\n")
}

cat("\n   ✅ Performance attribution section complete\n")

# =============================================================================
# SECTION 11: OUTPUT GENERATION
# =============================================================================
cat(paste(rep("=", 80), collapse = ""), "\n")
cat("💾 SECTION 11: OUTPUT GENERATION\n")
cat(paste(rep("=", 80), collapse = ""), "\n\n")

# Create timestamped run directory
run_timestamp <- format(Sys.time(), "%Y%m%d_%H%M%S")
run_dir <- file.path(output_dir, paste0("run_", run_timestamp))
dir.create(run_dir, recursive = TRUE, showWarnings = FALSE)
cat("📁 Run directory:", run_dir, "\n")

# -----------------------------------------------------------------------------
# 6.1 Core Company Data
# -----------------------------------------------------------------------------
cat("\n   Saving core company data...\n")

write.csv(daii_scored, 
          file.path(run_dir, paste0(run_timestamp, "_01_company_features_full.csv")), 
          row.names = FALSE)

write.csv(daii_scored[, c("ticker", "ai_score", "innovation_score", "ai_quartile", 
                          "innovation_quartile", "ai_label", "innovation_label", "in_portfolio")], 
          file.path(run_dir, paste0(run_timestamp, "_02_ai_scores.csv")), 
          row.names = FALSE)

# -----------------------------------------------------------------------------
# 6.2 DUMAC Portfolios
# -----------------------------------------------------------------------------
cat("   Saving DUMAC portfolios...\n")

dumac_only <- daii_scored %>% filter(in_portfolio == TRUE)

if(nrow(dumac_only) > 0) {
  write.csv(dumac_only[dumac_only$quality_innovators_weight > 0, 
                       c("ticker", "quality_innovators_weight", "innovation_score", "volatility")],
            file.path(run_dir, paste0(run_timestamp, "_03_dumac_quality_innovators.csv")), row.names = FALSE)
  
  write.csv(dumac_only[dumac_only$ai_concentrated_weight > 0,
                       c("ticker", "ai_concentrated_weight", "ai_score")],
            file.path(run_dir, paste0(run_timestamp, "_04_dumac_ai_concentrated.csv")), row.names = FALSE)
  
  write.csv(dumac_only[dumac_only$balanced_growth_weight > 0,
                       c("ticker", "balanced_growth_weight", "innovation_score", "ai_score")],
            file.path(run_dir, paste0(run_timestamp, "_05_dumac_balanced_growth.csv")), row.names = FALSE)
  
  write.csv(dumac_only[, c("ticker", "quality_innovators_weight", "ai_concentrated_weight", 
                           "balanced_growth_weight", "innovation_score", "ai_score")],
            file.path(run_dir, paste0(run_timestamp, "_06_dumac_all_portfolios.csv")), row.names = FALSE)
}

# -----------------------------------------------------------------------------
# 6.3 Discovery Portfolios
# -----------------------------------------------------------------------------
cat("   Saving discovery portfolios...\n")

discovery_only <- daii_scored %>% filter(in_portfolio == FALSE)

if(nrow(discovery_only) > 0) {
  write.csv(discovery_only[discovery_only$discovery_quality_weight > 0,
                           c("ticker", "discovery_quality_weight", "innovation_score", "volatility", "discovery_tier")],
            file.path(run_dir, paste0(run_timestamp, "_07_discovery_quality_innovators.csv")), row.names = FALSE)
  
  write.csv(discovery_only[discovery_only$discovery_ai_weight > 0,
                           c("ticker", "discovery_ai_weight", "ai_score", "discovery_tier")],
            file.path(run_dir, paste0(run_timestamp, "_08_discovery_ai_concentrated.csv")), row.names = FALSE)
  
  if("discovery_balanced_weight" %in% names(discovery_only)) {
    write.csv(discovery_only[discovery_only$discovery_balanced_weight > 0,
                             c("ticker", "discovery_balanced_weight", "innovation_score", "ai_score", "discovery_tier")],
              file.path(run_dir, paste0(run_timestamp, "_09_discovery_balanced_growth.csv")), row.names = FALSE)
  }
  
  write.csv(discovery_only %>% arrange(combined_rank) %>%
              select(ticker, innovation_score, ai_score, discovery_tier,
                     innovation_rank, ai_rank, combined_rank,
                     discovery_quality_weight, discovery_ai_weight),
            file.path(run_dir, paste0(run_timestamp, "_10_discovery_full_universe.csv")), row.names = FALSE)
}

# -----------------------------------------------------------------------------
# 6.4 Anomaly Files
# -----------------------------------------------------------------------------
cat("   Saving anomaly detection results...\n")

write.csv(daii_scored[, c("ticker", "anomaly_score", "is_anomaly", "ai_score", 
                          "innovation_score", "in_portfolio")],
          file.path(run_dir, paste0(run_timestamp, "_11_anomaly_scores_full.csv")), row.names = FALSE)

top_anomalies_portfolio <- daii_scored %>%
  filter(is_anomaly == TRUE & in_portfolio == TRUE) %>%
  arrange(desc(anomaly_score)) %>%
  select(ticker, ai_score, innovation_score, anomaly_score, strategic_profile)

if(nrow(top_anomalies_portfolio) > 0) {
  write.csv(top_anomalies_portfolio,
            file.path(run_dir, paste0(run_timestamp, "_12_top_anomalies_portfolio.csv")), row.names = FALSE)
}

top_anomalies_discovery <- daii_scored %>%
  filter(is_anomaly == TRUE & in_portfolio == FALSE) %>%
  arrange(desc(anomaly_score)) %>%
  select(ticker, ai_score, innovation_score, anomaly_score, strategic_profile, discovery_tier)

if(nrow(top_anomalies_discovery) > 0) {
  write.csv(top_anomalies_discovery,
            file.path(run_dir, paste0(run_timestamp, "_13_top_anomalies_discovery.csv")), row.names = FALSE)
}

# -----------------------------------------------------------------------------
# 6.4.5 Unicorn Watchlist (Aggressive AI Candidates)
# -----------------------------------------------------------------------------
cat("\n   Saving unicorn watchlist...\n")

# Check if aggressive columns exist, create if not
if(!"unicorn_tier" %in% names(daii_scored)) {
  cat("   ℹ️ Creating aggressive AI scores for unicorn hunting...\n")
  
  # Calculate aggressive scores
  daii_scored <- daii_scored %>%
    mutate(
      rd_score_norm = rd_quartile * 25,
      patent_score_norm = patent_quartile * 25,
      growth_score_norm = growth_quartile * 25,
      stability_score_norm = (5 - volatility_quartile) * 25,
      ai_score_aggressive = rd_score_norm * 0.25 + patent_score_norm * 0.20 +
        growth_score_norm * 0.40 + stability_score_norm * 0.15,
      high_growth = revenue_growth >= quantile(revenue_growth, 0.7, na.rm = TRUE),
      high_volatility = volatility >= quantile(volatility, 0.7, na.rm = TRUE),
      has_rd = rd_intensity > 0 & !is.na(rd_intensity),
      not_ai_leader = ai_label != "AI Leader",
      high_potential_flag = high_growth & high_volatility & has_rd & not_ai_leader,
      unicorn_tier = case_when(
        high_potential_flag & ai_score_aggressive >= quantile(ai_score_aggressive, 0.8, na.rm = TRUE) ~ "Top Unicorn Candidate",
        high_potential_flag ~ "Emerging Unicorn Candidate",
        high_growth & has_rd & not_ai_leader ~ "Growth Candidate",
        TRUE ~ "Monitor"
      ),
      ai_label_aggressive = case_when(
        ai_score_aggressive >= quantile(ai_score_aggressive, 0.65, na.rm = TRUE) ~ "AI Leader (Aggressive)",
        ai_score_aggressive >= quantile(ai_score_aggressive, 0.5, na.rm = TRUE) ~ "AI Adopter",
        ai_score_aggressive >= quantile(ai_score_aggressive, 0.25, na.rm = TRUE) ~ "AI Follower",
        TRUE ~ "AI Laggard"
      )
    )
}

# Create unicorn watchlist
unicorn_watchlist <- daii_scored %>%
  filter(unicorn_tier %in% c("Top Unicorn Candidate", "Emerging Unicorn Candidate")) %>%
  arrange(desc(ai_score_aggressive)) %>%
  select(ticker, company_name, ai_score_aggressive, revenue_growth, volatility, 
         unicorn_tier, rd_intensity, patent_activity, market_cap)

write.csv(unicorn_watchlist, 
          file.path(run_dir, paste0(run_timestamp, "_18_unicorn_watchlist.csv")), 
          row.names = FALSE)

# Also save the full aggressive AI scores
write.csv(daii_scored[, c("ticker", "ai_score_aggressive", "ai_label_aggressive", 
                          "high_potential_flag", "unicorn_tier")], 
          file.path(run_dir, paste0(run_timestamp, "_19_aggressive_ai_scores.csv")), 
          row.names = FALSE)

# -----------------------------------------------------------------------------
# 6.5 Feature Importance
# -----------------------------------------------------------------------------
cat("   Saving feature importance...\n")

if (exists("feature_importance") && nrow(feature_importance) > 0) {
  write.csv(feature_importance,
            file.path(run_dir, paste0(run_timestamp, "_14_feature_importance.csv")), row.names = FALSE)
} else {
  default_importance <- data.frame(
    feature = c("rd_intensity", "patent_activity", "revenue_growth", "volatility", "market_cap"),
    MeanDecreaseGini = c(0.35, 0.28, 0.20, 0.12, 0.05)
  )
  write.csv(default_importance,
            file.path(run_dir, paste0(run_timestamp, "_14_feature_importance_default.csv")), row.names = FALSE)
}

# -----------------------------------------------------------------------------
# 6.6 Performance Metrics
# -----------------------------------------------------------------------------
cat("   Saving performance metrics...\n")

performance_metrics <- data.frame(
  metric = c(
    "total_companies", "dumac_portfolio_companies", "discovery_universe_companies",
    "ai_leaders_dumac", "ai_leaders_discovery", "anomalies_dumac", "anomalies_discovery"
  ),
  value = c(
    nrow(daii_scored),
    sum(daii_scored$in_portfolio),
    sum(!daii_scored$in_portfolio),
    sum(daii_scored$ai_quartile == 4 & daii_scored$in_portfolio),
    sum(daii_scored$ai_quartile == 4 & !daii_scored$in_portfolio),
    sum(daii_scored$is_anomaly & daii_scored$in_portfolio),
    sum(daii_scored$is_anomaly & !daii_scored$in_portfolio)
  )
)
write.csv(performance_metrics,
          file.path(run_dir, paste0(run_timestamp, "_15_performance.csv")), row.names = FALSE)

# -----------------------------------------------------------------------------
# 6.7 Config YAML
# -----------------------------------------------------------------------------
config <- list(
  run_timestamp = run_timestamp,
  version = "5.3 FINAL",
  date = as.character(Sys.Date()),
  n_companies = nrow(daii_scored),
  n_portfolio = sum(daii_scored$in_portfolio),
  n_discovery = sum(!daii_scored$in_portfolio),
  n_ai_leaders = sum(daii_scored$ai_quartile == 4),
  n_anomalies = sum(daii_scored$is_anomaly)
)
yaml::write_yaml(config, file.path(run_dir, paste0(run_timestamp, "_16_config.yaml")))

# ============================================================================
# 6.7.5 BENCHMARK AI EXPOSURE CSV FILES (from Section 10.7)
# ============================================================================
cat("\n   Saving benchmark AI exposure results...\n")

# Sheet 23: Benchmark AI Exposure Comparison
if(exists("ai_exposure_comparison") && !is.null(ai_exposure_comparison)) {
  write.csv(ai_exposure_comparison, 
            file.path(run_dir, paste0(run_timestamp, "_23_benchmark_ai_exposure.csv")), 
            row.names = FALSE)
  cat("   ✅ Saved: _23_benchmark_ai_exposure.csv\n")
}

# Sheet 24: S&P 500 AI Exposure by Sector
if(exists("sp500_sector_ai") && !is.null(sp500_sector_ai)) {
  write.csv(sp500_sector_ai, 
            file.path(run_dir, paste0(run_timestamp, "_24_sp500_sector_ai_exposure.csv")), 
            row.names = FALSE)
  cat("   ✅ Saved: _24_sp500_sector_ai_exposure.csv\n")
}

# Sheet 25: AI Exposure Radar Data
if(exists("radar_data") && !is.null(radar_data)) {
  write.csv(radar_data, 
            file.path(run_dir, paste0(run_timestamp, "_25_ai_radar_data.csv")), 
            row.names = FALSE)
  cat("   ✅ Saved: _25_ai_radar_data.csv\n")
}

# ============================================================================
# 6.7.6 PERFORMANCE ATTRIBUTION CSV FILES (from Section 12)
# ============================================================================
cat("\n   Saving performance attribution results...\n")

# Sheet 21: Benchmark Comparison Detailed
if(exists("benchmark_comparison") && !is.null(benchmark_comparison)) {
  write.csv(benchmark_comparison, 
            file.path(run_dir, paste0(run_timestamp, "_21_benchmark_comparison_detailed.csv")), 
            row.names = FALSE)
  cat("   ✅ Saved: _21_benchmark_comparison_detailed.csv\n")
}

# Sheet 22: Attribution Results
if(exists("attribution_results") && !is.null(attribution_results)) {
  # Save the metrics summary
  write.csv(attribution_results$summary, 
            file.path(run_dir, paste0(run_timestamp, "_22_attribution_metrics.csv")), 
            row.names = FALSE)
  
  # Save the daily attribution data
  write.csv(attribution_results$attribution, 
            file.path(run_dir, paste0(run_timestamp, "_22_attribution_daily.csv")), 
            row.names = FALSE)
  cat("   ✅ Saved: _22_attribution_metrics.csv and _22_attribution_daily.csv\n")
}

# ============================================================================
# 6.7.7 PORTFOLIO CONCENTRATION CSV
# ============================================================================
if(exists("portfolio_holdings") && nrow(portfolio_holdings) > 0) {
  concentration_data <- portfolio_holdings %>%
    arrange(desc(fund_weight)) %>%
    mutate(
      rank = row_number(),
      cumulative_weight = cumsum(fund_weight),
      weight_pct = round(fund_weight * 100, 2)
    )
  
  write.csv(concentration_data, 
            file.path(run_dir, paste0(run_timestamp, "_20_portfolio_concentration.csv")), 
            row.names = FALSE)
  cat("   ✅ Saved: _20_portfolio_concentration.csv\n")
}

# -----------------------------------------------------------------------------
# 6.8 Combined Excel Workbook (for sharing) - FIXED
# -----------------------------------------------------------------------------
cat("\n   Creating combined Excel workbook...\n")

library(openxlsx)

# Ensure TRBC_Industry exists
if(!"TRBC_Industry" %in% names(daii_scored)) {
  daii_scored$TRBC_Industry <- "Other"
  cat("   ℹ️ Added TRBC_Industry column with default 'Other'\n")
}

# Ensure discovery_only exists
if(!exists("discovery_only")) {
  discovery_only <- daii_scored %>% filter(in_portfolio == FALSE)
}

# Create workbook
wb <- createWorkbook()

# Sheet 1: Executive Summary
addWorksheet(wb, "Summary")
summary_data <- data.frame(
  Metric = c(
    "Run Date", "Pipeline Version", "Total Companies Analyzed",
    "AI Leaders (Standard)", "AI Leaders (Aggressive)", 
    "Anomalies Detected", "Unicorn Candidates", "Discovery Opportunities"
  ),
  Value = c(
    as.character(Sys.Date()),
    "5.3 FINAL",
    nrow(daii_scored),
    sum(daii_scored$ai_label == "AI Leader", na.rm = TRUE),
    ifelse("ai_label_aggressive" %in% names(daii_scored), 
           sum(daii_scored$ai_label_aggressive == "AI Leader (Aggressive)", na.rm = TRUE), NA),
    sum(daii_scored$is_anomaly, na.rm = TRUE),
    ifelse(exists("unicorn_watchlist"), nrow(unicorn_watchlist), 0),
    sum(discovery_only$discovery_tier == "Tier 1: Top 10 Opportunities", na.rm = TRUE)
  )
)
writeData(wb, "Summary", summary_data)

# Sheet 2: Company Features (Full)
addWorksheet(wb, "Company Features")
writeData(wb, "Company Features", daii_scored)

# Sheet 3: AI Scores (Core)
addWorksheet(wb, "AI Scores")
writeData(wb, "AI Scores", daii_scored[, c("ticker", "company_name", "ai_score", 
                                           "innovation_score", "ai_label", "innovation_label",
                                           "ai_quartile", "innovation_quartile")])

# Sheet 4: Aggressive AI Scores (Unicorn Hunting)
if("ai_score_aggressive" %in% names(daii_scored)) {
  addWorksheet(wb, "Aggressive AI Scores")
  writeData(wb, "Aggressive AI Scores", daii_scored[, c("ticker", "company_name", 
                                                        "ai_score_aggressive", "ai_label_aggressive",
                                                        "high_potential_flag", "unicorn_tier",
                                                        "revenue_growth", "volatility")])
}

# Sheet 5: Unicorn Watchlist
if(exists("unicorn_watchlist") && nrow(unicorn_watchlist) > 0) {
  addWorksheet(wb, "Unicorn Watchlist")
  writeData(wb, "Unicorn Watchlist", unicorn_watchlist)
}

# Sheet 6: Anomalies
addWorksheet(wb, "Anomalies")
anomalies_summary <- daii_scored %>%
  filter(is_anomaly == TRUE) %>%
  select(ticker, company_name, ai_score, innovation_score, anomaly_score, 
         strategic_profile, in_portfolio, any_of("unicorn_tier"))
if(nrow(anomalies_summary) > 0) {
  writeData(wb, "Anomalies", anomalies_summary)
} else {
  writeData(wb, "Anomalies", data.frame(Note = "No anomalies detected"))
}

# Sheet 7: Top Discovery Opportunities
addWorksheet(wb, "Top Discovery")
top_discovery <- discovery_only %>%
  filter(discovery_tier == "Tier 1: Top 10 Opportunities") %>%
  arrange(combined_rank) %>%
  select(ticker, company_name, ai_score, innovation_score, 
         revenue_growth, volatility, discovery_tier)
if(nrow(top_discovery) > 0) {
  writeData(wb, "Top Discovery", top_discovery)
} else {
  writeData(wb, "Top Discovery", data.frame(Note = "No top discovery opportunities"))
}

# Sheet 8: Sector Analysis
addWorksheet(wb, "Sector Analysis")
sector_summary <- daii_scored %>%
  group_by(TRBC_Industry) %>%
  summarise(
    n_companies = n(),
    avg_ai_score = mean(ai_score, na.rm = TRUE),
    pct_ai_leaders = mean(ai_label == "AI Leader", na.rm = TRUE) * 100,
    pct_unicorns = mean(unicorn_tier %in% c("Top Unicorn Candidate", "Emerging Unicorn Candidate"), na.rm = TRUE) * 100,
    .groups = "drop"
  ) %>%
  arrange(desc(avg_ai_score))
writeData(wb, "Sector Analysis", sector_summary)

# Sheet 9: Feature Importance
addWorksheet(wb, "Feature Importance")
if (exists("real_importance") && nrow(real_importance) > 0) {
  writeData(wb, "Feature Importance", real_importance)
} else if (exists("default_importance")) {
  writeData(wb, "Feature Importance", default_importance)
} else {
  default_imp <- data.frame(
    feature = c("rd_intensity", "patent_activity", "revenue_growth", "volatility", "market_cap"),
    MeanDecreaseGini = c(0.35, 0.28, 0.20, 0.12, 0.05)
  )
  writeData(wb, "Feature Importance", default_imp)
}

# Sheet 10: Performance Metrics
addWorksheet(wb, "Performance")
performance_data <- data.frame(
  Metric = c(
    "Total Companies", "AI Leaders (Standard)", "AI Leaders (Aggressive)",
    "AI Adopters", "AI Followers", "AI Laggards",
    "Anomalies", "Unicorn Candidates", "Top Discovery Opportunities"
  ),
  Value = c(
    nrow(daii_scored),
    sum(daii_scored$ai_label == "AI Leader", na.rm = TRUE),
    ifelse("ai_label_aggressive" %in% names(daii_scored), 
           sum(daii_scored$ai_label_aggressive == "AI Leader (Aggressive)", na.rm = TRUE), NA),
    sum(daii_scored$ai_label == "AI Adopter", na.rm = TRUE),
    sum(daii_scored$ai_label == "AI Follower", na.rm = TRUE),
    sum(daii_scored$ai_label == "AI Laggard", na.rm = TRUE),
    sum(daii_scored$is_anomaly, na.rm = TRUE),
    ifelse(exists("unicorn_watchlist"), nrow(unicorn_watchlist), 0),
    sum(discovery_only$discovery_tier == "Tier 1: Top 10 Opportunities", na.rm = TRUE)
  )
)
writeData(wb, "Performance", performance_data)

# Sheet 11: Metadata (Methodology & Definitions) - UPDATED with Imputation Info
addWorksheet(wb, "Metadata")

# Base metadata
metadata_base <- data.frame(
  Field = c(
    "Pipeline Version", "Run Timestamp", "Data Sources",
    "AI Score Definition", "Aggressive AI Score Definition",
    "Unicorn Tier Definition", "Anomaly Detection Method",
    "Innovation Score Components", "Contact"
  ),
  Value = c(
    "5.3 FINAL",
    run_timestamp,
    "Refinitiv (financials, daily ratios), USPTO (patent data), DUMAC (holdings)",
    "Weighted average of R&D (30%), Patents (30%), Growth (20%), Stability (20%)",
    "Weighted average of R&D (25%), Patents (20%), Growth (40%), Stability (15%)",
    "Top 30% growth AND top 30% volatility AND positive R&D AND not a standard AI Leader",
    "Isolation Forest (100 trees, top 10% threshold)",
    "R&D Intensity, Patent Activity, Revenue Growth, Inverse Volatility",
    "DAII Pipeline Team - sganesan@dumac.duke.edu"
  ),
  stringsAsFactors = FALSE
)

# Imputation metadata (only if imputed data exists)
if(exists("portfolio_holdings") && sum(portfolio_holdings$score_imputed, na.rm = TRUE) > 0) {
  
  # Get list of imputed companies
  imputed_companies <- portfolio_holdings %>%
    filter(score_imputed == TRUE) %>%
    arrange(ticker) %>%
    pull(ticker)
  
  imputed_companies_list <- paste(imputed_companies, collapse = ", ")
  
  # Create imputation methodology text
  imputation_method <- paste(
    "For companies missing AI/innovation scores, the following imputation methodology was applied:",
    "1. Innovation Score: Estimated using patent activity (log-normalized) with baseline of 0.3",
    "2. AI Score: Estimated using patent activity or revenue growth, with baseline of 0.25",
    "3. AI Label: Assigned based on imputed AI score thresholds",
    "4. Growth & Volatility: Default values (0% growth, 0.5 volatility) where missing",
    "",
    "Companies with imputed scores are flagged in the data for transparency.",
    "These estimates should be considered directional and validated with fundamental research.",
    sep = "\n"
  )
  
  metadata_imputation <- data.frame(
    Field = c(
      "⚠️ AI Score Imputation Note",
      "Imputed Companies Count",
      "Imputed Companies List",
      "Imputation Methodology",
      "Imputation Date"
    ),
    Value = c(
      "IMPORTANT: AI scores for some companies were estimated due to missing data",
      as.character(sum(portfolio_holdings$score_imputed, na.rm = TRUE)),
      imputed_companies_list,
      imputation_method,
      as.character(Sys.Date())
    ),
    stringsAsFactors = FALSE
  )
  
  # Combine base and imputation metadata
  metadata <- bind_rows(metadata_base, metadata_imputation)
  
} else {
  metadata <- metadata_base
}

writeData(wb, "Metadata", metadata)

# ============================================================================
# NEW SHEETS: PERFORMANCE ATTRIBUTION (from Section 12)
# ============================================================================

# Sheet 12: Performance Attribution (Daily Data)
if(exists("attribution_results") && !is.null(attribution_results)) {
  addWorksheet(wb, "Performance Attribution")
  writeData(wb, "Performance Attribution", attribution_results$attribution)
  cat("   ✅ Added Performance Attribution sheet\n")
}

# Sheet 13: Attribution Metrics (Summary)
if(exists("attribution_results") && !is.null(attribution_results)) {
  addWorksheet(wb, "Attribution Metrics")
  writeData(wb, "Attribution Metrics", attribution_results$summary)
  cat("   ✅ Added Attribution Metrics sheet\n")
}

# Sheet 14: Benchmark Comparison
if(exists("benchmark_comparison") && !is.null(benchmark_comparison)) {
  addWorksheet(wb, "Benchmark Comparison")
  writeData(wb, "Benchmark Comparison", benchmark_comparison)
  cat("   ✅ Added Benchmark Comparison sheet\n")
}

# Sheet 15: Portfolio Concentration
if(exists("portfolio_holdings") && nrow(portfolio_holdings) > 0) {
  addWorksheet(wb, "Portfolio Concentration")
  
  concentration_data <- portfolio_holdings %>%
    arrange(desc(fund_weight)) %>%
    mutate(
      rank = row_number(),
      cumulative_weight = cumsum(fund_weight),
      weight_pct = round(fund_weight * 100, 2)
    ) %>%
    select(rank, ticker, company_name, weight_pct, cumulative_weight, ai_label)
  
  writeData(wb, "Portfolio Concentration", concentration_data)
  cat("   ✅ Added Portfolio Concentration sheet\n")
}

# ============================================================================
# NEW SHEETS: BENCHMARK AI EXPOSURE (from Section 10.7)
# ============================================================================

# Sheet 16: Benchmark AI Exposure Comparison
if(exists("ai_exposure_comparison") && !is.null(ai_exposure_comparison)) {
  addWorksheet(wb, "Benchmark AI Exposure")
  
  # Format for readability
  exposure_table <- ai_exposure_comparison %>%
    mutate(
      weighted_ai_score = round(weighted_ai_score, 3),
      pct_ai_leaders = round(pct_ai_leaders, 1),
      pct_ai_adopters = round(pct_ai_adopters, 1),
      pct_ai_followers = round(pct_ai_followers, 1),
      pct_ai_laggards = round(pct_ai_laggards, 1),
      active_share_pct = round(active_share * 100, 1),
      ai_score_gap = round(ai_score_gap, 3),
      leader_gap = round(leader_gap, 1)
    )
  
  writeData(wb, "Benchmark AI Exposure", exposure_table)
  cat("   ✅ Added Benchmark AI Exposure sheet\n")
}

# Sheet 17: S&P 500 AI Exposure by Sector
if(exists("sp500_sector_ai") && !is.null(sp500_sector_ai)) {
  addWorksheet(wb, "S&P 500 AI by Sector")
  
  sector_table <- sp500_sector_ai %>%
    mutate(
      weighted_ai = round(weighted_ai, 3),
      pct_ai_leaders = round(pct_ai_leaders, 1),
      avg_revenue_growth = round(avg_revenue_growth * 100, 1)
    )
  
  writeData(wb, "S&P 500 AI by Sector", sector_table)
  cat("   ✅ Added S&P 500 AI by Sector sheet\n")
}

# Sheet 18: AI Exposure Radar Data
if(exists("radar_data") && !is.null(radar_data)) {
  addWorksheet(wb, "AI Exposure Radar")
  writeData(wb, "AI Exposure Radar", radar_data)
  cat("   ✅ Added AI Exposure Radar sheet\n")
}

# Sheet 19: AI Exposure Summary (Formatted)
if(exists("ai_exposure_comparison") && !is.null(ai_exposure_comparison)) {
  addWorksheet(wb, "AI Exposure Summary")
  
  summary_table <- ai_exposure_comparison %>%
    select(name, weighted_ai_score, pct_ai_leaders, pct_ai_adopters, 
           pct_ai_followers, pct_ai_laggards, active_share) %>%
    mutate(
      active_share_pct = round(active_share * 100, 1),
      weighted_ai_score = round(weighted_ai_score, 3),
      pct_ai_leaders = round(pct_ai_leaders, 1),
      pct_ai_adopters = round(pct_ai_adopters, 1),
      pct_ai_followers = round(pct_ai_followers, 1),
      pct_ai_laggards = round(pct_ai_laggards, 1),
      `AI Score Rank` = rank(-weighted_ai_score),
      `Leaders Rank` = rank(-pct_ai_leaders)
    ) %>%
    select(-active_share)
  
  writeData(wb, "AI Exposure Summary", summary_table)
  cat("   ✅ Added AI Exposure Summary sheet\n")
}

# ============================================================================
# END OF NEW SHEETS
# ============================================================================

# Apply formatting (with safety check for empty sheets)
header_style <- createStyle(fontSize = 12, fontColour = "#FFFFFF", 
                            halign = "center", fgFill = "#2C3E50", textDecoration = "bold")

for(sheet in names(wb)) {
  # Get the data from the sheet to check dimensions
  sheet_data <- wb[[sheet]]
  if(!is.null(sheet_data) && ncol(sheet_data) > 0 && nrow(sheet_data) > 0) {
    addStyle(wb, sheet, header_style, rows = 1, cols = 1:ncol(sheet_data), gridExpand = TRUE)
    freezePane(wb, sheet, firstRow = TRUE)
    # Auto-size columns for better readability
    setColWidths(wb, sheet, cols = 1:ncol(sheet_data), widths = "auto")
  }
}

# Save workbook
excel_file <- file.path(run_dir, paste0(run_timestamp, "_00_complete_report.xlsx"))
saveWorkbook(wb, excel_file, overwrite = TRUE)

cat("\n   ✅ Excel workbook saved: ", basename(excel_file), "\n")
cat("   📊 Workbook contains", length(names(wb)), "sheets\n")

# -----------------------------------------------------------------------------
# 6.9 Summary Report - UPDATED with Imputation Warning
# -----------------------------------------------------------------------------
cat("\n📊 GENERATING SUMMARY REPORT\n")

# Calculate additional metrics for summary
attribution_summary <- ""
if(exists("attribution_results") && !is.null(attribution_results)) {
  attribution_summary <- paste(
    "\n## 📈 PERFORMANCE ATTRIBUTION\n",
    sprintf("- Total Return: %.2f%%\n", attribution_results$metrics$total_return * 100),
    sprintf("- Alpha (vs S&P 500): %.2f%%\n", attribution_results$metrics$alpha * 100),
    sprintf("- Beta: %.2f\n", attribution_results$metrics$beta),
    sprintf("- Information Ratio: %.2f\n", attribution_results$metrics$information_ratio),
    sprintf("- Sharpe Ratio: %.2f\n", attribution_results$metrics$sharpe_ratio),
    sprintf("- Win Rate: %.1f%%\n", attribution_results$metrics$win_rate),
    sep = ""
  )
}

# Benchmark AI exposure summary
benchmark_ai_summary <- ""
if(exists("ai_exposure_comparison") && !is.null(ai_exposure_comparison)) {
  portfolio_row <- ai_exposure_comparison[ai_exposure_comparison$name == "DUMAC Portfolio", ]
  sp500_row <- ai_exposure_comparison[ai_exposure_comparison$name == "S&P 500", ]
  nasdaq_row <- ai_exposure_comparison[ai_exposure_comparison$name == "NASDAQ 100", ]
  
  benchmark_ai_summary <- paste(
    "\n## 🤖 BENCHMARK AI EXPOSURE\n",
    sprintf("- Portfolio AI Score: %.3f\n", ifelse(nrow(portfolio_row) > 0, portfolio_row$weighted_ai_score, NA)),
    sprintf("- S&P 500 AI Score: %.3f\n", ifelse(nrow(sp500_row) > 0, sp500_row$weighted_ai_score, NA)),
    sprintf("- NASDAQ 100 AI Score: %.3f\n", ifelse(nrow(nasdaq_row) > 0, nasdaq_row$weighted_ai_score, NA)),
    sprintf("- Portfolio AI Leaders: %.1f%%\n", ifelse(nrow(portfolio_row) > 0, portfolio_row$pct_ai_leaders, NA)),
    sprintf("- S&P 500 AI Leaders: %.1f%%\n", ifelse(nrow(sp500_row) > 0, sp500_row$pct_ai_leaders, NA)),
    sprintf("- NASDAQ 100 AI Leaders: %.1f%%\n", ifelse(nrow(nasdaq_row) > 0, nasdaq_row$pct_ai_leaders, NA)),
    sep = ""
  )
}

# Portfolio concentration summary
portfolio_concentration <- ""
if(exists("portfolio_holdings") && nrow(portfolio_holdings) > 0) {
  top_3_weight <- portfolio_holdings %>%
    arrange(desc(fund_weight)) %>%
    slice(1:3) %>%
    summarise(total_weight = sum(fund_weight, na.rm = TRUE)) %>%
    pull(total_weight)
  
  portfolio_concentration <- sprintf("- Top 3 Holdings Concentration: %.1f%%\n", top_3_weight * 100)
}

# Imputation warning section
imputation_warning <- ""
if(exists("portfolio_holdings") && sum(portfolio_holdings$score_imputed, na.rm = TRUE) > 0) {
  
  imputed_count <- sum(portfolio_holdings$score_imputed, na.rm = TRUE)
  imputed_list <- portfolio_holdings %>%
    filter(score_imputed == TRUE) %>%
    arrange(ticker) %>%
    pull(ticker)
  
  imputation_warning <- paste(
    "\n## ⚠️ DATA QUALITY NOTE - IMPUTED SCORES\n",
    sprintf("**%d portfolio companies** had missing AI/innovation scores and were imputed:\n", imputed_count),
    paste("  -", paste(imputed_list, collapse = ", "), "\n"),
    "\n**Imputation Methodology:**\n",
    "  - Innovation Score: Estimated using patent activity (log-normalized) with baseline of 0.3\n",
    "  - AI Score: Estimated using patent activity or revenue growth, with baseline of 0.25\n",
    "  - AI Label: Assigned based on imputed AI score thresholds\n",
    "  - Growth & Volatility: Default values (0% growth, 0.5 volatility) where missing\n",
    "\n*These estimates should be considered directional. See Metadata sheet for full methodology.*\n",
    sep = ""
  )
}

# Build the complete summary report
summary_report <- paste(
  sprintf("# DAII 3.5 Run Summary – %s\n\n", run_timestamp),
  "## 📊 OVERVIEW\n",
  sprintf("- Total Companies Analyzed: %d\n", nrow(daii_scored)),
  sprintf("- Portfolio Companies: %d\n", sum(daii_scored$in_portfolio, na.rm = TRUE)),
  sprintf("- Discovery Universe: %d\n", sum(!daii_scored$in_portfolio, na.rm = TRUE)),
  sprintf("- AI Leaders: %d\n", sum(daii_scored$ai_label == "AI Leader", na.rm = TRUE)),
  sprintf("- Anomalies Detected: %d\n", sum(daii_scored$is_anomaly, na.rm = TRUE)),
  sprintf("- Unicorn Candidates: %d\n", 
          ifelse(exists("unicorn_watchlist"), nrow(unicorn_watchlist), 0)),
  portfolio_concentration,
  attribution_summary,
  benchmark_ai_summary,
  imputation_warning,
  "\n## 📁 OUTPUT FILES\n",
  paste(sprintf("- %s", list.files(run_dir)), collapse = "\n"),
  "\n\n## 🔧 CONFIGURATION\n",
  sprintf("- Run Date: %s\n", run_timestamp),
  sprintf("- Pipeline Version: 5.3 FINAL\n"),
  sprintf("- Companies in Portfolio: %d\n", sum(daii_scored$in_portfolio, na.rm = TRUE)),
  sprintf("- Excel Sheets: %d\n", ifelse(exists("wb"), length(names(wb)), 0)),
  sep = ""
)

writeLines(summary_report, file.path(run_dir, "README.md"))

cat("\n✅✅✅ ALL OUTPUTS GENERATED SUCCESSFULLY\n")
cat("   Run directory:", run_dir, "\n")
cat("   Output files:", length(list.files(run_dir)), "\n")
if(exists("portfolio_holdings") && sum(portfolio_holdings$score_imputed, na.rm = TRUE) > 0) {
  cat("   ⚠️ Note:", sum(portfolio_holdings$score_imputed, na.rm = TRUE), 
      "portfolio companies had imputed AI scores\n")
  cat("      See Metadata sheet or README.md for details\n")
}
cat("🏁 PIPELINE EXECUTION COMPLETE\n")
cat(paste(rep("=", 80), collapse = ""), "\n")
