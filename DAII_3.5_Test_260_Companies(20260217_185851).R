################################################################################
> # DAII 3.5 - COMPLETE INTEGRATED PIPELINE
> # Version: 4.0 FINAL | Date: 2026-02-16
> # Author: Siva Ganesan
> # 
> # ARCHITECTURE:
> #   MODULE 0: Data Integration (Panel + Fundamentals → Company Snapshot)
> #   MODULE 0.5: Field Mapping & Standardization (from v3.5.9)
> #   MODULES 1-3: Innovation Scoring (from v3.5.9 with quartile fix)
> #   MODULE 4: Portfolio Construction & AI Intensity (from v3.1)
> #   PHASE 2: AI Cube, ML Models, Anomaly Detection (from v3.1)
> ################################################################################
> 
> # =============================================================================
> # SECTION 0: ENVIRONMENT SETUP & DIRECTORY CONFIGURATION
> # =============================================================================
> 
> cat(paste(rep("=", 80), collapse = ""), "\n")
================================================================================ 
> cat("DAII 3.5 - COMPLETE INTEGRATED PIPELINE v4.0\n")
DAII 3.5 - COMPLETE INTEGRATED PIPELINE v4.0
> cat(paste(rep("=", 80), collapse = ""), "\n\n")
================================================================================ 

> 
> # -----------------------------------------------------------------------------
> # 0.1 SIMPLE DIRECTORY STRUCTURE
> # -----------------------------------------------------------------------------
> raw_dir    <- "C:\\Users\\sganesan\\OneDrive - dumac.duke.edu\\DAII\\data\\raw"
> script_dir <- "C:\\Users\\sganesan\\OneDrive - dumac.duke.edu\\DAII\\R\\scripts"
> output_dir <- "C:\\Users\\sganesan\\OneDrive - dumac.duke.edu\\DAII\\data\\output"
> 
> cat("📁 Directory Configuration:\n")
📁 Directory Configuration:
> cat("   Raw data:     ", raw_dir, "\n")
   Raw data:      C:\Users\sganesan\OneDrive - dumac.duke.edu\DAII\data\raw 
> cat("   Scripts:      ", script_dir, "\n")
   Scripts:       C:\Users\sganesan\OneDrive - dumac.duke.edu\DAII\R\scripts 
> cat("   Output:       ", output_dir, "\n\n")
   Output:        C:\Users\sganesan\OneDrive - dumac.duke.edu\DAII\data\output 

> 
> # -----------------------------------------------------------------------------
> # 0.2 PACKAGE LOADING (from v3.5.9)
> # -----------------------------------------------------------------------------
> required_packages <- c(
+   "dplyr", "tidyr", "readr", "httr", "stringr", 
+   "purrr", "lubridate", "yaml", "ggplot2", "openxlsx", 
+   "corrplot", "moments", "randomForest", "isotree"
+ )
> 
> load_packages_safely <- function(pkg_list) {
+   for (pkg in pkg_list) {
+     if (!require(pkg, character.only = TRUE, quietly = TRUE)) {
+       cat(sprintf("   Installing missing package: %s\n", pkg))
+       install.packages(pkg, dependencies = TRUE, repos = "https://cloud.r-project.org")
+       library(pkg, character.only = TRUE)
+       cat(sprintf("   ✅ Loaded: %s\n", pkg))
+     } else {
+       cat(sprintf("   ✅ Already available: %s\n", pkg))
+     }
+   }
+ }
> 
> cat("📦 Loading required packages...\n")
📦 Loading required packages...
> load_packages_safely(required_packages)
   ✅ Already available: dplyr
   ✅ Already available: tidyr
   ✅ Already available: readr
   ✅ Already available: httr
   ✅ Already available: stringr
   ✅ Already available: purrr
   ✅ Already available: lubridate
   ✅ Already available: yaml
   ✅ Already available: ggplot2
   ✅ Already available: openxlsx
   ✅ Already available: corrplot
   ✅ Already available: moments
   ✅ Already available: randomForest
   ✅ Already available: isotree
> options(stringsAsFactors = FALSE, scipen = 999, digits = 4)
> cat("✅ Environment configured.\n\n")
✅ Environment configured.

> 
> # =============================================================================
> # SECTION 1: MODULE 0 - DATA INTEGRATION (FIXED FOR 245 COMPANIES)
> # =============================================================================
> cat(paste(rep("=", 80), collapse = ""), "\n")
================================================================================ 
> cat("📊 MODULE 0: DATA INTEGRATION (Building Master Dataset for 245 Companies)\n")
📊 MODULE 0: DATA INTEGRATION (Building Master Dataset for 245 Companies)
> cat(paste(rep("=", 80), collapse = ""), "\n\n")
================================================================================ 

> 
> library(dplyr)
> library(tidyr)
> library(quantmod)
> library(lubridate)
> library(zoo)
> 
> # -----------------------------------------------------------------------------
> # 1.1 LOAD FUNDAMENTALS (245 companies from cleaned file)
> # -----------------------------------------------------------------------------
> cat("   Loading fundamentals from cleaned file...\n")
   Loading fundamentals from cleaned file...
> fundamentals_raw <- read.csv(fundamentals_file, stringsAsFactors = FALSE)
> cat(sprintf("   Loaded: %d rows\n", nrow(fundamentals_raw)))
   Loaded: 260 rows
> 
> # Clean column names
> names(fundamentals_raw) <- gsub("\\.+", "_", names(fundamentals_raw))
> names(fundamentals_raw) <- gsub("__", "_", names(fundamentals_raw))
> 
> # Get master ticker list (ALL 245)
> master_tickers <- unique(fundamentals_raw$Ticker)
> master_tickers <- master_tickers[!is.na(master_tickers) & master_tickers != ""]
> cat(sprintf("   Master ticker list: %d unique companies\n", length(master_tickers)))
   Master ticker list: 232 unique companies
> 
> # Extract one row per company for fundamentals
> fundamentals <- fundamentals_raw %>%
+   group_by(Ticker) %>%
+   summarise(
+     market_cap = first(na.omit(as.numeric(gsub("[^0-9.Ee+-]", "", Mkt_Cap)))),
+     rd_expense = first(na.omit(as.numeric(gsub("[^0-9.Ee+-]", "", R_D_Exp)))),
+     patent_activity = first(na.omit(as.numeric(gsub("[^0-9.Ee+-]", "", Patents_Trademarks_Copy_Rgt)))),
+     industry = first(na.omit(GICS_Ind_Grp_Name)),
+     revenue_growth = first(na.omit(as.numeric(gsub("[^0-9.Ee+-]", "", Rev_1_Yr_Gr)))) / 100,
+     volatility_fund = first(na.omit(as.numeric(gsub("[^0-9.Ee+-]", "", Volatil_360D)))) / 100,
+     employees = first(na.omit(as.numeric(gsub("[^0-9.Ee+-]", "", Number_of_Employees)))),
+     last_price = first(na.omit(as.numeric(gsub("[^0-9.Ee+-]", "", Last_Price)))),
+     .groups = "drop"
+   ) %>%
+   mutate(
+     rd_expense = ifelse(is.na(rd_expense) | rd_expense <= 0, NA, rd_expense),
+     patent_activity = ifelse(is.na(patent_activity) | patent_activity < 0, 0, patent_activity),
+     industry = ifelse(is.na(industry) | industry == "", "Unknown", industry)
+   )
Warning messages:
1: There were 101 warnings in `summarise()`.
The first warning was:
ℹ In argument: `market_cap = first(na.omit(as.numeric(gsub("[^0-9.Ee+-]", "", Mkt_Cap))))`.
ℹ In group 53: `Ticker = "CITADL 5.9 02/10/30"`.
Caused by warning in `na.omit()`:
! NAs introduced by coercion
ℹ Run warnings()dplyr::last_dplyr_warnings() to see the 100 remaining warnings. 
2: Returning more (or less) than 1 row per `summarise()` group was deprecated in dplyr 1.1.0.
ℹ Please use `reframe()` instead.
ℹ When switching from `summarise()` to `reframe()`, remember that `reframe()` always returns an
  ungrouped data frame and adjust accordingly.
Call `lifecycle::last_lifecycle_warnings()` to see where this warning was generated. 
> 
> cat(sprintf("   Fundamentals prepared: %d companies\n", nrow(fundamentals)))
   Fundamentals prepared: 75 companies
> cat(sprintf("   With R&D data: %d\n", sum(!is.na(fundamentals$rd_expense))))
   With R&D data: 59
> cat(sprintf("   With patent data: %d\n", sum(fundamentals$patent_activity > 0)))
   With patent data: 74
> 
> # -----------------------------------------------------------------------------
> # 1.2 LOAD HOLDINGS DATA (for portfolio flags)
> # -----------------------------------------------------------------------------
> cat("\n   Loading DUMAC holdings data...\n")

   Loading DUMAC holdings data...
> holdings_raw <- read.csv(holdings_file, stringsAsFactors = FALSE)
> names(holdings_raw) <- gsub("\\.+", "_", names(holdings_raw))
> 
> holdings_summary <- holdings_raw %>%
+   group_by(Ticker) %>%
+   summarise(
+     n_funds = n_distinct(fund_id, na.rm = TRUE),
+     total_fund_weight = sum(as.numeric(fund_weight), na.rm = TRUE),
+     total_position_value = sum(as.numeric(position_value), na.rm = TRUE),
+     .groups = "drop"
+   ) %>%
+   mutate(
+     in_portfolio = TRUE,
+     total_fund_weight = ifelse(is.na(total_fund_weight) | is.infinite(total_fund_weight), 0, total_fund_weight)
+   )
> 
> cat(sprintf("   Found %d companies with portfolio data\n", nrow(holdings_summary)))
   Found 232 companies with portfolio data
> 
> # -----------------------------------------------------------------------------
> # 1.3 BUILD TIME SERIES PANEL FOR ALL 245 COMPANIES
> # -----------------------------------------------------------------------------
> cat("\n📈 Building time series panel for all 245 companies...\n")

📈 Building time series panel for all 245 companies...
> cat("   This will take several minutes. Progress shown below:\n")
   This will take several minutes. Progress shown below:
> 
> # Date range
> start_date <- "2019-01-01"
> end_date <- "2024-12-31"
> 
> # Function to fetch Yahoo Finance data
> fetch_yahoo_data <- function(ticker, start, end) {
+   # Map ticker formats
+   yahoo_ticker <- case_when(
+     grepl(" US$", ticker) ~ gsub(" US$", "", ticker),
+     grepl(" JT$", ticker) ~ gsub(" JT$", ".T", ticker),
+     grepl(" KS$", ticker) ~ gsub(" KS$", ".KS", ticker),
+     grepl(" GR$", ticker) ~ gsub(" GR$", ".DE", ticker),
+     TRUE ~ ticker
+   )
+   
+   tryCatch({
+     data <- getSymbols(yahoo_ticker, from = start, to = end, 
+                        auto.assign = FALSE, warnings = FALSE)
+     df <- data.frame(
+       Date = index(data),
+       Open = as.numeric(Op(data)),
+       High = as.numeric(Hi(data)),
+       Low = as.numeric(Lo(data)),
+       Close = as.numeric(Cl(data)),
+       Volume = as.numeric(Vo(data)),
+       Adjusted = as.numeric(Ad(data))
+     )
+     df$Ticker <- ticker
+     return(df)
+   }, error = function(e) {
+     return(NULL)
+   })
+ }
> 
> # Fetch data for all master tickers
> all_panel <- list()
> success_count <- 0
> failed_tickers <- c()
> 
> for(i in seq_along(master_tickers)) {
+   ticker <- master_tickers[i]
+   cat(sprintf("   [%d/%d] %s... ", i, length(master_tickers), ticker))
+   
+   result <- fetch_yahoo_data(ticker, start_date, end_date)
+   
+   if(!is.null(result)) {
+     all_panel[[ticker]] <- result
+     success_count <- success_count + 1
+     cat(sprintf("✅ %d rows\n", nrow(result)))
+   } else {
+     failed_tickers <- c(failed_tickers, ticker)
+     cat("❌ Failed\n")
+   }
+   
+   Sys.sleep(0.5)  # Rate limiting
+ }
   [1/232] NVDA US... ✅ 1509 rows
   [2/232] V US... ✅ 1509 rows
   [3/232] LLY US... ✅ 1509 rows
   [4/232] BIO US... ✅ 1509 rows
   [5/232] META US... ✅ 1509 rows
   [6/232] AMZN US... ✅ 1509 rows
   [7/232] MSFT US... ✅ 1509 rows
   [8/232] TSLA US... ✅ 1509 rows
   [9/232] HDB US... ✅ 1509 rows
   [10/232] AMD US... ✅ 1509 rows
   [11/232] ORCL US... ✅ 1509 rows
   [12/232] 9984 JT... ✅ 1463 rows
   [13/232] 005930 KS... ✅ 1475 rows
   [14/232] BABA US... ✅ 1509 rows
   [15/232] LMT US... ✅ 1509 rows
   [16/232] 8TRA GR... ✅ 1402 rows
   [17/232] 6501 JT... ✅ 1463 rows
   [18/232] GE US... ✅ 1509 rows
   [19/232] AMT US... ✅ 1509 rows
   [20/232] NET US... ✅ 1333 rows
   [21/232] SPOT US... ✅ 1509 rows
   [22/232] PCOR US... ✅ 909 rows
   [23/232] JBL US... ✅ 1509 rows
   [24/232] J US... ✅ 1509 rows
   [25/232] INF US... ❌ Failed
   [26/232] XOM US... ✅ 1509 rows
   [27/232] CVX US... ✅ 1509 rows
   [28/232] LIN US... ✅ 1509 rows
   [29/232] APD US... ✅ 1509 rows
   [30/232] NEE US... ✅ 1509 rows
   [31/232] DUK US... ✅ 1509 rows
   [32/232] CAT US... ✅ 1509 rows
   [33/232] DE US... ✅ 1509 rows
   [34/232] KO US... ✅ 1509 rows
   [35/232] PG US... ✅ 1509 rows
   [36/232] IBM US... ✅ 1509 rows
   [37/232] SAP GR... ✅ 1526 rows
   [38/232] CRM US... ✅ 1509 rows
   [39/232] ADBE US... ✅ 1509 rows
   [40/232] INTC US... ✅ 1509 rows
   [41/232] SMCI US... ✅ 1509 rows
   [42/232] SNPS US... ✅ 1509 rows
   [43/232] ANET US... ✅ 1509 rows
   [44/232] ASML US... ✅ 1509 rows
   [45/232] MU US... ✅ 1509 rows
   [46/232] BOTZ US... ✅ 1509 rows
   [47/232] AIQ US... ✅ 1509 rows
   [48/232] ROBT US... ✅ 1509 rows
   [49/232] QTUM US... ✅ 1509 rows
   [50/232] AIEQ US... ✅ 1509 rows
   [51/232] XLK US... ✅ 1509 rows
   [52/232] IGV US... ✅ 1509 rows
   [53/232] SOXX US... ✅ 1509 rows
   [54/232] VGT US... ✅ 1509 rows
   [55/232] FDIS US... ✅ 1509 rows
   [56/232] XLE US... ✅ 1509 rows
   [57/232] XLI US... ✅ 1509 rows
   [58/232] XLP US... ✅ 1509 rows
   [59/232] SPX... ❌ Failed
   [60/232] IWM US... ✅ 1509 rows
   [61/232] AAPL US... ✅ 1509 rows
   [62/232] GOOGL US... ✅ 1509 rows
   [63/232] BRK/B US... ❌ Failed
   [64/232] JPM US... ✅ 1509 rows
   [65/232] JNJ US... ✅ 1509 rows
   [66/232] UNH US... ✅ 1509 rows
   [67/232] HD US... ✅ 1509 rows
   [68/232] BAC US... ✅ 1509 rows
   [69/232] MA US... ✅ 1509 rows
   [70/232] PFE US... ✅ 1509 rows
   [71/232] NFLX US... ✅ 1509 rows
   [72/232] CSCO US... ✅ 1509 rows
   [73/232] VZ US... ✅ 1509 rows
   [74/232] T US... ✅ 1509 rows
   [75/232] WMT US... ✅ 1509 rows
   [76/232] MCD US... ✅ 1509 rows
   [77/232] DIS US... ✅ 1509 rows
   [78/232] ABBV US... ✅ 1509 rows
   [79/232] ABT US... ✅ 1509 rows
   [80/232] MRK US... ✅ 1509 rows
   [81/232] TMO US... ✅ 1509 rows
   [82/232] UPS US... ✅ 1509 rows
   [83/232] BA US... ✅ 1509 rows
   [84/232] RTX US... ✅ 1509 rows
   [85/232] MMM US... ✅ 1509 rows
   [86/232] GS US... ✅ 1509 rows
   [87/232] MS US... ✅ 1509 rows
   [88/232] AXP US... ✅ 1509 rows
   [89/232] QCOM US... ✅ 1509 rows
   [90/232] TXN US... ✅ 1509 rows
   [91/232] AMAT US... ✅ 1509 rows
   [92/232] LRCX US... ✅ 1509 rows
   [93/232] AVGO US... ✅ 1509 rows
   [94/232] TSM US... ✅ 1509 rows
   [95/232] TCEHY US... ✅ 1509 rows
   [96/232] MPNGF US... ✅ 1509 rows
   [97/232] JD US... ✅ 1509 rows
   [98/232] PDD US... ✅ 1509 rows
   [99/232] BIDU US... ✅ 1509 rows
   [100/232] NTES US... ✅ 1509 rows
   [101/232] RELIANCE IN... ❌ Failed
   [102/232] INFY US... ✅ 1509 rows
   [103/232] APH US... ✅ 1509 rows
   [104/232] IBN US... ✅ 1509 rows
   [105/232] 000660 KS... ✅ 1475 rows
   [106/232] 051910 KS... ✅ 1475 rows
   [107/232] 005380 KS... ✅ 1475 rows
   [108/232] 207940 KS... ✅ 1475 rows
   [109/232] 035420 KS... ✅ 1475 rows
   [110/232] 035720 KS... ✅ 1475 rows
   [111/232] SE US... ✅ 1509 rows
   [112/232] GRAB US... ✅ 1026 rows
   [113/232] LOGM TL 1L USD... ❌ Failed
   [114/232] MELI US... ✅ 1509 rows
   [115/232] STNE US... ✅ 1509 rows
   [116/232] PAGS US... ✅ 1509 rows
   [117/232] NU US... ✅ 768 rows
   [118/232] KSPI US... ✅ 239 rows
   [119/232] YNDX19... ❌ Failed
   [120/232] VERCST 13 12/15/30... ❌ Failed
   [121/232] BYDDF US... ✅ 1509 rows
   [122/232] XIACF US... ✅ 955 rows
   [123/232] NIO US... ✅ 1509 rows
   [124/232] LI US... ✅ 1112 rows
   [125/232] XPEV US... ✅ 1092 rows
   [126/232] BILI US... ✅ 1509 rows
   [127/232] KUASF US... ✅ 974 rows
   [128/232] WB US... ✅ 1509 rows
   [129/232] TCOM US... ✅ 1509 rows
   [130/232] CTREV 3 7/8 06/30/28... ❌ Failed
   [131/232] TME US... ✅ 1509 rows
   [132/232] IQ US... ✅ 1509 rows
   [133/232] PAHCF US... ❌ Failed
   [134/232] ANT US... ❌ Failed
   [135/232] WCPAY US... ❌ Failed
   [136/232] NVO US... ✅ 1509 rows
   [137/232] MC US... ✅ 1509 rows
   [138/232] RMSG US... ✅ 816 rows
   [139/232] OR US... ✅ 1509 rows
   [140/232] SAP US... ✅ 1509 rows
   [141/232] SIEGY US... ✅ 1509 rows
   [142/232] ALIZF US... ✅ 1509 rows
   [143/232] SNY US... ✅ 1509 rows
   [144/232] NVS US... ✅ 1509 rows
   [145/232] RHHBY US... ✅ 1509 rows
   [146/232] NSRGY US... ✅ 1509 rows
   [147/232] UL US... ✅ 1509 rows
   [148/232] AZN US... ✅ 1509 rows
   [149/232] HSBC US... ✅ 1509 rows
   [150/232] BP US... ✅ 1509 rows
   [151/232] SHEL US... ✅ 1509 rows
   [152/232] TTE US... ✅ 1509 rows
   [153/232] EADSY US... ✅ 1509 rows
   [154/232] SONY US... ✅ 1509 rows
   [155/232] TM US... ✅ 1509 rows
   [156/232] HMC US... ✅ 1509 rows
   [157/232] CAJPY US... ✅ 1509 rows
   [158/232] HTHIY US... ✅ 1509 rows
   [159/232] MSBHF US... ✅ 1509 rows
   [160/232] 7974 JP... ❌ Failed
   [161/232] SFTBF US... ✅ 1509 rows
   [162/232] MBFJF US... ✅ 1509 rows
   [163/232] SMFG US... ✅ 1509 rows
   [164/232] MFG US... ✅ 1509 rows
   [165/232] TAK US... ✅ 1509 rows
   [166/232] TKPHF US... ✅ 1509 rows
   [167/232] RKUNY US... ✅ 1509 rows
   [168/232] RCRTF US... ❌ Failed
   [169/232] KYCCF US... ✅ 1509 rows
   [170/232] SMCAY US... ✅ 1509 rows
   [171/232] FANUY US... ✅ 1509 rows
   [172/232] MRAAY US... ✅ 1509 rows
   [173/232] TTDKY US... ✅ 1509 rows
   [174/232] NJDCY US... ✅ 1509 rows
   [175/232] SNYFX US... ❌ Failed
   [176/232] MITEY US... ✅ 1509 rows
   [177/232] MITSY US... ✅ 1509 rows
   [178/232] ITOCY US... ✅ 1509 rows
   [179/232] MARUY US... ✅ 1509 rows
   [180/232] SUMCF US... ✅ 1509 rows
   [181/232] SVNDY US... ✅ 1509 rows
   [182/232] FRCOY US... ✅ 1509 rows
   [183/232] KAOCF US... ✅ 1509 rows
   [184/232] SSDOY US... ✅ 1509 rows
   [185/232] BX US... ✅ 1509 rows
   [186/232] KKR US... ✅ 1509 rows
   [187/232] APO US... ✅ 1509 rows
   [188/232] ARES US... ✅ 1509 rows
   [189/232] BAM US... ✅ 522 rows
   [190/232] BLK US... ✅ 1509 rows
   [191/232] STT US... ✅ 1509 rows
   [192/232] NTRS US... ✅ 1509 rows
   [193/232] MSCI US... ✅ 1509 rows
   [194/232] SPGI US... ✅ 1509 rows
   [195/232] MCO US... ✅ 1509 rows
   [196/232] ICE US... ✅ 1509 rows
   [197/232] CME US... ✅ 1509 rows
   [198/232] NDAQ US... ✅ 1509 rows
   [199/232] CBOE US... ✅ 1509 rows
   [200/232] TW US... ✅ 1445 rows
   [201/232] MKTX US... ✅ 1509 rows
   [202/232] VIRT US... ✅ 1509 rows
   [203/232] JANEST TL B 1L USD... ❌ Failed
   [204/232] CITADL 5.9 02/10/30... ❌ Failed
   [205/232] PYPL US... ✅ 1509 rows
   [206/232] SQM US... ✅ 1509 rows
   [207/232] 0170016D US... ❌ Failed
   [208/232] COIN US... ✅ 935 rows
   [209/232] HOOD US... ✅ 861 rows
   [210/232] SOFI US... ✅ 1004 rows
   [211/232] AFRM US... ✅ 997 rows
   [212/232] MQ US... ✅ 896 rows
   [213/232] BILL US... ✅ 1270 rows
   [214/232] TOST US... ✅ 823 rows
   [215/232] SNOW US... ✅ 1079 rows
   [216/232] DDOG US... ✅ 1329 rows
   [217/232] TWLO US... ✅ 1509 rows
   [218/232] FSLY US... ✅ 1415 rows
   [219/232] CRWD US... ✅ 1398 rows
   [220/232] PANW US... ✅ 1509 rows
   [221/232] ZS US... ✅ 1509 rows
   [222/232] OKTA US... ✅ 1509 rows
   [223/232] SAIL US... ❌ Failed
   [224/232] DOCU US... ✅ 1509 rows
   [225/232] ZM US... ✅ 1435 rows
   [226/232] WORK US... ❌ Failed
   [227/232] ASAN US... ✅ 1069 rows
   [228/232] TEAM US... ✅ 1509 rows
   [229/232] NOW US... ✅ 1509 rows
   [230/232] WDAY US... ✅ 1509 rows
   [231/232] HUBS US... ✅ 1509 rows
   [232/232] SHOP US... ✅ 1509 rows
There were 17 warnings (use warnings() to see them)
> 
> cat(sprintf("\n   Successfully fetched: %d/%d companies\n", 
+             success_count, length(master_tickers)))

   Successfully fetched: 213/232 companies
> 
> # -----------------------------------------------------------------------------
> # 1.4 COMBINE PANEL DATA
> # -----------------------------------------------------------------------------
> if(length(all_panel) > 0) {
+   cat("\n   Combining panel data...\n")
+   
+   panel_combined <- bind_rows(all_panel) %>%
+     arrange(Ticker, Date)
+   
+   # Calculate returns
+   panel_combined <- panel_combined %>%
+     group_by(Ticker) %>%
+     mutate(
+       daily_return = (Close / lag(Close) - 1),
+       log_return = c(NA, diff(log(Close)))
+     ) %>%
+     ungroup()
+   
+   cat(sprintf("   Panel created: %d rows × %d columns\n", 
+               nrow(panel_combined), ncol(panel_combined)))
+   cat(sprintf("   Unique tickers in panel: %d\n", 
+               length(unique(panel_combined$Ticker))))
+   
+   # -----------------------------------------------------------------------------
+   # 1.5 MERGE EVERYTHING TOGETHER
+   # -----------------------------------------------------------------------------
+   cat("\n   Merging fundamentals and holdings...\n")
+   
+   # Start with panel, add fundamentals
+   final_panel <- panel_combined %>%
+     left_join(fundamentals, by = "Ticker") %>%
+     left_join(holdings_summary, by = "Ticker") %>%
+     mutate(
+       in_portfolio = ifelse(is.na(in_portfolio), FALSE, in_portfolio),
+       n_funds = ifelse(is.na(n_funds), 0, n_funds),
+       total_fund_weight = ifelse(is.na(total_fund_weight), 0, total_fund_weight),
+       
+       # Calculate rolling volatility
+       volatility_30d = rollapply(log_return, 30, sd, fill = NA, align = "right") * sqrt(252),
+       
+       # R&D intensity
+       rd_intensity = rd_expense / market_cap
+     )
+   
+   # -----------------------------------------------------------------------------
+   # 1.6 CREATE COMPANY SNAPSHOT (ONE ROW PER COMPANY)
+   # -----------------------------------------------------------------------------
+   cat("\n   Creating company snapshot...\n")
+   
+   company_snapshot <- final_panel %>%
+     group_by(Ticker) %>%
+     summarise(
+       market_cap = last(na.omit(market_cap)),
+       volatility = sd(log_return, na.rm = TRUE) * sqrt(252),
+       total_return = (last(Close) / first(Close)) - 1,
+       rd_expense = first(na.omit(rd_expense)),
+       patent_activity = first(na.omit(patent_activity)),
+       rd_intensity = first(na.omit(rd_intensity)),
+       revenue_growth = first(na.omit(revenue_growth)),
+       industry = first(na.omit(industry)),
+       in_portfolio = first(in_portfolio),
+       n_funds = first(n_funds),
+       total_fund_weight = first(total_fund_weight),
+       n_obs = n(),
+       first_date = min(Date),
+       last_date = max(Date),
+       .groups = "drop"
+     ) %>%
+     mutate(
+       fund_weight = ifelse(in_portfolio, total_fund_weight, 1/n())
+     )
+   
+   cat(sprintf("   Snapshot created: %d unique companies\n", nrow(company_snapshot)))
+   cat(sprintf("   In portfolio: %d\n", sum(company_snapshot$in_portfolio)))
+   cat(sprintf("   Discovery universe: %d\n", sum(!company_snapshot$in_portfolio)))
+   
+   # -----------------------------------------------------------------------------
+   # 1.7 SAVE OUTPUTS
+   # -----------------------------------------------------------------------------
+   cat("\n   Saving datasets...\n")
+   
+   # Save full panel
+   panel_output <- file.path(raw_dir, "N245_Complete_Panel.csv")
+   write.csv(final_panel, panel_output, row.names = FALSE)
+   cat(sprintf("   ✅ Panel saved: %s\n", panel_output))
+   
+   # Save snapshot
+   snapshot_output <- file.path(raw_dir, "N245_company_snapshot.csv")
+   write.csv(company_snapshot, snapshot_output, row.names = FALSE)
+   cat(sprintf("   ✅ Snapshot saved: %s\n", snapshot_output))
+   
+   # Save failed tickers
+   if(length(failed_tickers) > 0) {
+     failed_output <- file.path(raw_dir, "failed_tickers.csv")
+     write.csv(data.frame(ticker = failed_tickers), failed_output, row.names = FALSE)
+     cat(sprintf("   ⚠️ Failed tickers saved: %s\n", failed_output))
+   }
+   
+   cat("\n📊 FINAL COVERAGE:\n")
+   cat("==================\n")
+   cat(sprintf("Total companies: %d\n", nrow(company_snapshot)))
+   cat(sprintf("In DUMAC portfolio: %d\n", sum(company_snapshot$in_portfolio)))
+   cat(sprintf("Discovery universe: %d\n", sum(!company_snapshot$in_portfolio)))
+   cat(sprintf("With R&D data: %d\n", sum(!is.na(company_snapshot$rd_expense))))
+   cat(sprintf("With patent data: %d\n", sum(company_snapshot$patent_activity > 0)))
+   
+ } else {
+   cat("\n❌ No panel data fetched. Check Yahoo Finance connection.\n")
+   stop("Cannot proceed without panel data")
+ }

   Combining panel data...
   Panel created: 308974 rows × 10 columns
   Unique tickers in panel: 213

   Merging fundamentals and holdings...

   Creating company snapshot...
   Snapshot created: 59 unique companies
   In portfolio: 59
   Discovery universe: 0

   Saving datasets...
   ✅ Panel saved: C:\Users\sganesan\OneDrive - dumac.duke.edu\DAII\data\raw/N245_Complete_Panel.csv
   ✅ Snapshot saved: C:\Users\sganesan\OneDrive - dumac.duke.edu\DAII\data\raw/N245_company_snapshot.csv
   ⚠️ Failed tickers saved: C:\Users\sganesan\OneDrive - dumac.duke.edu\DAII\data\raw/failed_tickers.csv

📊 FINAL COVERAGE:
==================
Total companies: 59
In DUMAC portfolio: 59
Discovery universe: 0
With R&D data: 59
With patent data: 58
Warning message:
Returning more (or less) than 1 row per `summarise()` group was deprecated in dplyr 1.1.0.
ℹ Please use `reframe()` instead.
ℹ When switching from `summarise()` to `reframe()`, remember that `reframe()` always returns an
  ungrouped data frame and adjust accordingly.
Call `lifecycle::last_lifecycle_warnings()` to see where this warning was generated. 
> 
> cat("\n✅ MODULE 0 COMPLETE - Ready for pipeline\n")

✅ MODULE 0 COMPLETE - Ready for pipeline
> 
> # -----------------------------------------------------------------------------
> # 1.2 LOAD CLEANED FUNDAMENTALS FILE
> # -----------------------------------------------------------------------------
> fundamentals_url <- "https://raw.githubusercontent.com/sivaguru42/DAII-3.5-Framework/refs/heads/main/N200_FINAL_StrategicDUMACPortfolioDistribution_BBergUploadRawData_cleaned.csv"
> cat("\n   Loading cleaned fundamentals file...\n")

   Loading cleaned fundamentals file...
> fundamentals_raw <- read.csv(fundamentals_url, stringsAsFactors = FALSE)
> cat(sprintf("   ✅ Fundamentals loaded: %d rows\n", nrow(fundamentals_raw)))
   ✅ Fundamentals loaded: 245 rows
> 
> # Clean column names
> names(fundamentals_raw) <- gsub("\\.+", "_", names(fundamentals_raw))
> names(fundamentals_raw) <- gsub("__", "_", names(fundamentals_raw))
> 
> # Extract key fundamental columns
> fundamentals <- fundamentals_raw %>%
+   select(
+     ticker = Ticker,
+     rd_expense = R_D_Exp,
+     patent_activity = Patents_Trademarks_Copy_Rgt,
+     industry = GICS_Ind_Grp_Name,
+     revenue_growth = Rev_1_Yr_Gr,
+     volatility_fund = Volatil_360D,
+     employees = Number_of_Employees,
+     gm = GM
+   ) %>%
+   mutate(
+     # Convert to numeric, handling character issues
+     rd_expense = as.numeric(gsub("[^0-9.-]", "", rd_expense)),
+     patent_activity = as.numeric(gsub("[^0-9.-]", "", patent_activity)),
+     revenue_growth = as.numeric(gsub("[^0-9.-]", "", revenue_growth)),
+     volatility_fund = as.numeric(gsub("[^0-9.-]", "", volatility_fund)) / 100, # Convert to decimal
+     industry = ifelse(is.na(industry) | industry == "", "Unknown", industry)
+   )
> 
> cat(sprintf("   ✅ Fundamentals prepared: %d companies with data\n", 
+             sum(!is.na(fundamentals$rd_expense))))
   ✅ Fundamentals prepared: 194 companies with data
> 
> # -----------------------------------------------------------------------------
> # 1.3 JOIN TO CREATE COMPLETE SNAPSHOT (LEFT JOIN to keep ALL panel tickers)
> # -----------------------------------------------------------------------------
> company_snapshot <- panel_summary %>%
+   left_join(fundamentals, by = "ticker") %>%
+   mutate(
+     # Intelligent defaults for missing fundamental data
+     rd_expense = ifelse(is.na(rd_expense) | is.infinite(rd_expense) | rd_expense <= 0,
+                         market_cap * 0.03,  # Default: 3% of market cap
+                         rd_expense),
+     patent_activity = ifelse(is.na(patent_activity) | is.infinite(patent_activity) | patent_activity < 0,
+                              0,
+                              patent_activity),
+     revenue_growth = ifelse(is.na(revenue_growth) | is.infinite(revenue_growth),
+                             0.05,  # Default: 5% growth
+                             revenue_growth / 100),  # Convert percentage to decimal
+     industry = ifelse(is.na(industry), "Unknown", industry),
+     # Use panel volatility as primary, fall back to fundamental volatility
+     volatility = ifelse(!is.na(volatility) & is.finite(volatility) & volatility > 0,
+                         volatility,
+                         ifelse(!is.na(volatility_fund) & is.finite(volatility_fund),
+                                volatility_fund,
+                                0.30)),  # Final default: 30% volatility
+     # Calculate R&D intensity
+     rd_intensity = rd_expense / market_cap,
+     # Fund weight placeholder (equal weight for now)
+     fund_weight = 1 / n()
+   ) %>%
+   select(ticker, market_cap, volatility, total_return, rd_expense, patent_activity,
+          rd_intensity, revenue_growth, industry, fund_weight, n_obs, first_date, last_date)
> 
> cat("\n📊 SNAPSHOT COMPOSITION:\n")

📊 SNAPSHOT COMPOSITION:
> cat(sprintf("   Total companies: %d\n", nrow(company_snapshot)))
   Total companies: 73
> cat(sprintf("   Companies with REAL R&D data: %d\n", 
+             sum(!is.na(fundamentals$rd_expense) & fundamentals$rd_expense > 0)))
   Companies with REAL R&D data: 168
> cat(sprintf("   Companies with DEFAULT R&D: %d\n", 
+             sum(company_snapshot$rd_intensity == 0.03)))
   Companies with DEFAULT R&D: 10
> cat(sprintf("   Companies with REAL patent data: %d\n", 
+             sum(company_snapshot$patent_activity > 0)))
   Companies with REAL patent data: 32
> cat(sprintf("   Companies with industry data: %d\n", 
+             sum(company_snapshot$industry != "Unknown")))
   Companies with industry data: 67
> 
> # -----------------------------------------------------------------------------
> # 1.4 SAVE SNAPSHOT TO raw DIRECTORY
> # -----------------------------------------------------------------------------
> snapshot_file <- file.path(raw_dir, "N245_company_snapshot.csv")
> write.csv(company_snapshot, snapshot_file, row.names = FALSE)
> cat("\n✅ Snapshot saved to:", snapshot_file, "\n\n")

✅ Snapshot saved to: C:\Users\sganesan\OneDrive - dumac.duke.edu\DAII\data\raw/N245_company_snapshot.csv 

> 
> # =============================================================================
> # SECTION 2: MODULE 0.5 - FIELD MAPPING & STANDARDIZATION (from v3.5.9)
> # =============================================================================
> cat(paste(rep("=", 80), collapse = ""), "\n")
================================================================================ 
> cat("🗺️  MODULE 0.5: FIELD MAPPING & STANDARDIZATION\n")
🗺️  MODULE 0.5: FIELD MAPPING & STANDARDIZATION
> cat(paste(rep("=", 80), collapse = ""), "\n\n")
================================================================================ 

> 
> # Load the snapshot we just created
> daii_raw_data <- read.csv(snapshot_file, stringsAsFactors = FALSE)
> cat(sprintf("   ✅ Snapshot loaded: %d rows × %d columns\n", nrow(daii_raw_data), ncol(daii_raw_data)))
   ✅ Snapshot loaded: 73 rows × 13 columns
> 
> # Standardized column definitions (from v3.5.9)
> standardized_cols <- list(
+   ticker = c("ticker", "Ticker", "Symbol"),
+   rd_expense = c("rd_expense", "R.D.Exp", "RD_Exp", "R&D"),
+   market_cap = c("market_cap", "Mkt.Cap", "MarketCap"),
+   patent_activity = c("patent_activity", "Patents", "Patent_Activity"),
+   revenue_growth = c("revenue_growth", "Rev.Growth", "Revenue_Growth"),
+   industry = c("industry", "GICS.Ind.Grp.Name", "Sector"),
+   volatility = c("volatility", "Volatility", "Volatil.360D"),
+   total_return = c("total_return", "Total.Return"),
+   rd_intensity = c("rd_intensity", "RD.Intensity"),
+   fund_weight = c("fund_weight", "Fund.Weight")
+ )
> 
> # Apply standardization
> standardize_columns <- function(data, col_definitions) {
+   cat("   Standardizing column names...\n")
+   standardized_data <- data
+   
+   for (std_name in names(col_definitions)) {
+     variations <- col_definitions[[std_name]]
+     for (variation in variations) {
+       if (variation %in% colnames(standardized_data) && variation != std_name) {
+         colnames(standardized_data)[colnames(standardized_data) == variation] <- std_name
+         cat(sprintf("      %-20s → %s\n", variation, std_name))
+         break
+       }
+     }
+   }
+   return(standardized_data)
+ }
> 
> daii_standardized <- standardize_columns(daii_raw_data, standardized_cols)
   Standardizing column names...
> 
> # Verify critical columns
> critical_cols <- c("ticker", "rd_expense", "market_cap", "patent_activity", 
+                    "revenue_growth", "industry", "volatility")
> missing_cols <- setdiff(critical_cols, colnames(daii_standardized))
> 
> if (length(missing_cols) > 0) {
+   cat("\n⚠️  WARNING: Missing critical columns:", paste(missing_cols, collapse = ", "), "\n")
+ } else {
+   cat("\n✅ All critical columns present.\n\n")
+ }

✅ All critical columns present.

> 
> # =============================================================================
> # SECTION 3: MODULES 1-3 - INNOVATION SCORING (from v3.5.9 with quartile fix)
> # =============================================================================
> cat(paste(rep("=", 80), collapse = ""), "\n")
================================================================================ 
> cat("📈 MODULES 1-3: INNOVATION SCORING (with QUARTILE FIX - company-level only)\n")
📈 MODULES 1-3: INNOVATION SCORING (with QUARTILE FIX - company-level only)
> cat(paste(rep("=", 80), collapse = ""), "\n\n")
================================================================================ 

> 
> # CRITICAL FIX: Ensure we're working with company-level data (one row per ticker)
> cat("   CRITICAL: Calculating quartiles on company-level data only\n")
   CRITICAL: Calculating quartiles on company-level data only
> cat(sprintf("   Company count: %d\n", nrow(daii_standardized)))
   Company count: 73
> 
> # Calculate quartiles for each innovation metric (using company-level data only)
> daii_scored <- daii_standardized %>%
+   mutate(
+     # R&D Intensity Quartile (higher is better)
+     rd_quartile = ntile(rd_intensity, 4),
+     
+     # Patent Activity Quartile (higher is better)
+     patent_quartile = ntile(patent_activity, 4),
+     
+     # Revenue Growth Quartile (higher is better)
+     growth_quartile = ntile(revenue_growth, 4),
+     
+     # Volatility Quartile (lower is better - invert)
+     volatility_quartile = 5 - ntile(volatility, 4),
+     
+     # Innovation Score (weighted average of quartiles)
+     innovation_score = (
+       rd_quartile * 0.30 +
+         patent_quartile * 0.30 +
+         growth_quartile * 0.20 +
+         volatility_quartile * 0.20
+     ) / 4,  # Normalize to 0-1 scale
+     
+     # Innovation Quartile (final categorization)
+     innovation_quartile = ntile(innovation_score, 4),
+     innovation_label = case_when(
+       innovation_quartile == 4 ~ "Leader",
+       innovation_quartile == 3 ~ "Strong",
+       innovation_quartile == 2 ~ "Developing",
+       TRUE ~ "Emerging"
+     )
+   )
> # =============================================================================
> # SECTION 3.5: DEDUPLICATE TO ONE ROW PER COMPANY
> # =============================================================================
> cat("\n🔄 CRITICAL: Deduplicating to one row per company...\n")

🔄 CRITICAL: Deduplicating to one row per company...
> cat("   Before dedupe:", nrow(daii_scored), "rows\n")
   Before dedupe: 73 rows
> 
> # Get unique tickers with their most complete data
> daii_scored <- daii_scored %>%
+   group_by(ticker) %>%
+   summarise(
+     # Take first non-NA values for each field
+     market_cap = first(na.omit(market_cap)),
+     volatility = first(na.omit(volatility)),
+     total_return = first(na.omit(total_return)),
+     rd_expense = first(na.omit(rd_expense)),
+     patent_activity = first(na.omit(patent_activity)),
+     rd_intensity = first(na.omit(rd_intensity)),
+     revenue_growth = first(na.omit(revenue_growth)),
+     industry = first(na.omit(industry)),
+     innovation_score = first(na.omit(innovation_score)),
+     innovation_quartile = first(na.omit(innovation_quartile)),
+     innovation_label = first(na.omit(innovation_label)),
+     n_obs = max(n_obs, na.rm = TRUE),
+     first_date = min(first_date, na.rm = TRUE),
+     last_date = max(last_date, na.rm = TRUE),
+     .groups = "drop"
+   )
> 
> cat("   After dedupe:", nrow(daii_scored), "unique companies\n")
   After dedupe: 50 unique companies
> cat("   First 10 tickers after dedupe:\n")
   First 10 tickers after dedupe:
> print(head(daii_scored$ticker, 10))
 [1] "005930 KS" "6501 JT"   "8TRA GR"   "9984 JT"   "ADBE US"   "AIEQ US"   "AIQ US"    "AMD US"   
 [9] "AMT US"    "AMZN US"  
> 
> cat(sprintf("   Before dedupe: %d rows\n", nrow(daii_scored)))
   Before dedupe: 50 rows
> cat(sprintf("   After dedupe: %d unique companies\n", nrow(daii_scored_unique)))
   After dedupe: 50 unique companies
> 
> # Replace with deduped version
> daii_scored <- daii_scored_unique
> 
> cat("\n📊 Innovation Score Distribution:\n")

📊 Innovation Score Distribution:
> print(table(daii_scored$innovation_label))

Developing   Emerging     Leader     Strong 
        12         15         10         13 
> # =============================================================================
> # SECTION 3.6: MERGE HOLDINGS DATA & CREATE PORTFOLIO FLAG
> # =============================================================================
> cat(paste(rep("=", 80), collapse = ""), "\n")
================================================================================ 
> cat("🏦 MERGING DUMAC HOLDINGS DATA\n")
🏦 MERGING DUMAC HOLDINGS DATA
> cat(paste(rep("=", 80), collapse = ""), "\n\n")
================================================================================ 

> 
> # Load holdings data from Integrated_Data file
> holdings_file <- file.path(raw_dir, "N200_FINAL_StrategicDUMACPortfolioDistribution_BBergUploadRawData_Integrated_Data.csv")
> 
> if(file.exists(holdings_file)) {
+   cat("   Loading DUMAC holdings data...\n")
+   holdings_raw <- read.csv(holdings_file, stringsAsFactors = FALSE)
+   
+   # Clean column names
+   names(holdings_raw) <- gsub("\\.+", "_", names(holdings_raw))
+   
+   # Aggregate holdings to company level - ONE ROW PER TICKER
+   holdings_summary <- holdings_raw %>%
+     group_by(Ticker) %>%
+     summarise(
+       n_funds = n_distinct(fund_id, na.rm = TRUE),
+       total_fund_weight = sum(as.numeric(fund_weight), na.rm = TRUE),
+       total_position_value = sum(as.numeric(position_value), na.rm = TRUE),
+       avg_dumac_allocation = mean(as.numeric(dumac_allocation), na.rm = TRUE),
+       latest_holdings_date = max(as.character(as_of_date), na.rm = TRUE),
+       fund_names = paste(unique(fund_name[!is.na(fund_name)]), collapse = "; "),
+       .groups = "drop"
+     ) %>%
+     mutate(
+       in_portfolio = TRUE,
+       total_fund_weight = ifelse(is.na(total_fund_weight) | is.infinite(total_fund_weight), 
+                                  0, total_fund_weight)
+     )
+   
+   cat(sprintf("   Found %d unique companies with holdings data\n", nrow(holdings_summary)))
+   
+   # Get list of tickers in holdings
+   portfolio_tickers <- unique(holdings_summary$Ticker)
+   cat(sprintf("   Portfolio tickers: %d\n", length(portfolio_tickers)))
+   
+   # Merge holdings data with daii_scored (LEFT JOIN to keep ALL companies)
+   daii_scored <- daii_scored %>%
+     left_join(holdings_summary, by = c("ticker" = "Ticker")) %>%
+     mutate(
+       # Set defaults for companies not in portfolio
+       in_portfolio = ifelse(ticker %in% portfolio_tickers, TRUE, FALSE),
+       n_funds = ifelse(is.na(n_funds), 0, n_funds),
+       total_fund_weight = ifelse(is.na(total_fund_weight), 0, total_fund_weight),
+       total_position_value = ifelse(is.na(total_position_value), 0, total_position_value)
+     )
+   
+ } else {
+   cat("⚠️  Holdings file not found. Creating synthetic portfolio flags.\n")
+   # If no holdings data, assume all companies are in portfolio with equal weight
+   daii_scored <- daii_scored %>%
+     mutate(
+       in_portfolio = TRUE,
+       n_funds = 1,
+       total_fund_weight = 1 / n()
+     )
+ }
   Loading DUMAC holdings data...
   Found 232 unique companies with holdings data
   Portfolio tickers: 232
Warning message:
There were 91 warnings in `summarise()`.
The first warning was:
ℹ In argument: `latest_holdings_date = max(as.character(as_of_date), na.rm = TRUE)`.
ℹ In group 2: `Ticker = "005380 KS"`.
Caused by warning in `max()`:
! no non-missing arguments, returning NA
ℹ Run warnings()dplyr::last_dplyr_warnings() to see the 90 remaining warnings. 
> 
> cat("\n📊 PORTFOLIO COVERAGE:\n")

📊 PORTFOLIO COVERAGE:
> cat(sprintf("   Companies in DUMAC portfolio: %d\n", sum(daii_scored$in_portfolio)))
   Companies in DUMAC portfolio: 50
> cat(sprintf("   Companies not in portfolio: %d\n", sum(!daii_scored$in_portfolio)))
   Companies not in portfolio: 0
> cat(sprintf("   Total companies: %d\n", nrow(daii_scored)))
   Total companies: 50
> 
> # Show sample of portfolio companies
> cat("\n📋 Sample of portfolio companies:\n")

📋 Sample of portfolio companies:
> portfolio_sample <- daii_scored %>%
+   filter(in_portfolio == TRUE) %>%
+   select(ticker, total_fund_weight, n_funds) %>%
+   head(10)
> print(portfolio_sample)
# A tibble: 10 × 3
   ticker    total_fund_weight n_funds
   <chr>                 <dbl>   <int>
 1 005930 KS         0.0671          3
 2 6501 JT           0.0306          8
 3 8TRA GR           0.0000824       1
 4 9984 JT           0.0264          8
 5 ADBE US           0.00534         2
 6 AIEQ US           0               0
 7 AIQ US            0               0
 8 AMD US            0.00311         2
 9 AMT US           -0.00996         5
10 AMZN US           0.149           4
> 
> # Show sample of non-portfolio companies
> if(sum(!daii_scored$in_portfolio) > 0) {
+   cat("\n📋 Sample of non-portfolio companies (discovery universe):\n")
+   discovery_sample <- daii_scored %>%
+     filter(in_portfolio == FALSE) %>%
+     select(ticker, innovation_score) %>%
+     head(10)
+   print(discovery_sample)
+ } else {
+   cat("\n📋 No non-portfolio companies in current dataset.\n")
+   cat("   This is because we're working with the 73-company sample.\n")
+   cat("   With the full 245 companies, we expect ~57 non-portfolio.\n")
+ }

📋 No non-portfolio companies in current dataset.
   This is because we're working with the 73-company sample.
   With the full 245 companies, we expect ~57 non-portfolio.
> # =============================================================================
> # SECTION 4: MODULE 4 - AI INTENSITY SCORING & PORTFOLIO CONSTRUCTION
> # =============================================================================
> cat(paste(rep("=", 80), collapse = ""), "\n")
================================================================================ 
> cat("🤖 MODULE 4: AI INTENSITY SCORING & PORTFOLIO CONSTRUCTION\n")
🤖 MODULE 4: AI INTENSITY SCORING & PORTFOLIO CONSTRUCTION
> cat(paste(rep("=", 80), collapse = ""), "\n\n")
================================================================================ 

> 
> # -----------------------------------------------------------------------------
> # 4.1 Industry Multipliers (from v3.1)
> # -----------------------------------------------------------------------------
> industry_multipliers <- data.frame(
+   industry = c(
+     "Semiconductors & Semiconductor",
+     "Software & Services",
+     "Media & Entertainment",
+     "Pharmaceuticals, Biotechnology",
+     "Technology Hardware & Equipmen",
+     "Automobiles & Components",
+     "Financial Services",
+     "Capital Goods",
+     "Consumer Discretionary Distrib",
+     "Energy",
+     "Materials",
+     "Utilities",
+     "Unknown"
+   ),
+   multiplier = c(
+     1.5,   # Semiconductors - highest AI intensity
+     1.4,   # Software
+     1.3,   # Media/Tech
+     1.2,   # Biotech
+     1.2,   # Hardware
+     1.1,   # Auto
+     0.8,   # Financial
+     0.9,   # Capital Goods
+     1.0,   # Consumer
+     0.7,   # Energy
+     0.8,   # Materials
+     0.5,   # Utilities
+     1.0    # Unknown (neutral)
+   )
+ )
> 
> # -----------------------------------------------------------------------------
> # 4.2 Calculate AI intensity scores (WORKS FOR ALL COMPANIES)
> # -----------------------------------------------------------------------------
> daii_scored <- daii_scored %>%
+   left_join(industry_multipliers, by = "industry") %>%
+   mutate(
+     multiplier = ifelse(is.na(multiplier), 1.0, multiplier),
+     ai_score = innovation_score * multiplier,
+     ai_quartile = ntile(ai_score, 4),
+     ai_label = case_when(
+       ai_quartile == 4 ~ "AI Leader",
+       ai_quartile == 3 ~ "AI Adopter",
+       ai_quartile == 2 ~ "AI Follower",
+       TRUE ~ "AI Laggard"
+     )
+   )
> 
> cat("\n🤖 AI Score Distribution (All 260 Companies):\n")

🤖 AI Score Distribution (All 260 Companies):
> print(table(daii_scored$ai_label))

 AI Adopter AI Follower  AI Laggard   AI Leader 
         12          13          13          12 
> 
> # -----------------------------------------------------------------------------
> # 4.3 Construct DUMAC Portfolios (ONLY companies in_portfolio = TRUE)
> # -----------------------------------------------------------------------------
> cat("\n💼 Building DUMAC Portfolios (companies with holdings data)...\n")

💼 Building DUMAC Portfolios (companies with holdings data)...
> 
> # Calculate medians using ONLY portfolio companies for relevant comparisons
> portfolio_vol_median <- median(daii_scored$volatility[daii_scored$in_portfolio], na.rm = TRUE)
> portfolio_mcap_sum <- sum(daii_scored$market_cap[daii_scored$in_portfolio], na.rm = TRUE)
> 
> dumac_portfolios <- daii_scored %>%
+   filter(in_portfolio == TRUE) %>%  # ONLY companies DUMAC actually holds
+   mutate(
+     # Strategy 1: Quality Innovators (top quartile innovation, below-median volatility)
+     quality_innovators_weight = ifelse(
+       innovation_quartile == 4 & volatility < portfolio_vol_median,
+       total_fund_weight / sum(total_fund_weight[innovation_quartile == 4 & volatility < portfolio_vol_median]),
+       0
+     ),
+     
+     # Strategy 2: AI Concentrated (top quartile AI score)
+     ai_concentrated_weight = ifelse(
+       ai_quartile == 4,
+       total_fund_weight / sum(total_fund_weight[ai_quartile == 4]),
+       0
+     ),
+     
+     # Strategy 3: Balanced Growth (innovation >= 3 AND AI >= 3)
+     balanced_growth_weight = ifelse(
+       innovation_quartile >= 3 & ai_quartile >= 3,
+       total_fund_weight / sum(total_fund_weight[innovation_quartile >= 3 & ai_quartile >= 3]),
+       0
+     )
+   )
> 
> cat(sprintf("   DUMAC Portfolio Companies: %d\n", nrow(dumac_portfolios)))
   DUMAC Portfolio Companies: 50
> cat(sprintf("   Quality Innovators: %d companies\n", 
+             sum(dumac_portfolios$quality_innovators_weight > 0)))
   Quality Innovators: 6 companies
> cat(sprintf("   AI Concentrated: %d companies\n", 
+             sum(dumac_portfolios$ai_concentrated_weight > 0)))
   AI Concentrated: 9 companies
> cat(sprintf("   Balanced Growth: %d companies\n", 
+             sum(dumac_portfolios$balanced_growth_weight > 0)))
   Balanced Growth: 14 companies
> 
> # =============================================================================
> # 4.4 Construct Discovery Portfolios (companies NOT in portfolio)
> # =============================================================================
> cat("\n🔍 Building Discovery Portfolios (non-portfolio companies)...\n")

🔍 Building Discovery Portfolios (non-portfolio companies)...
> 
> discovery_portfolios <- daii_scored %>%
+   filter(in_portfolio == FALSE) %>%  # Companies DUMAC doesn't yet hold
+   mutate(
+     # Rank-based scores for discovery (ADD THESE LINES)
+     innovation_rank = rank(-innovation_score),
+     ai_rank = rank(-ai_score),
+     combined_rank = rank(-(innovation_score + ai_score)),
+     
+     # Discovery weights (for hypothetical investment exploration)
+     discovery_quality_weight = ifelse(
+       innovation_quartile == 4 & volatility < median(volatility),
+       innovation_score / sum(innovation_score[innovation_quartile == 4 & volatility < median(volatility)]),
+       0
+     ),
+     
+     discovery_ai_weight = ifelse(
+       ai_quartile == 4,
+       ai_score / sum(ai_score[ai_quartile == 4]),
+       0
+     ),
+     
+     discovery_balanced_weight = ifelse(
+       innovation_quartile >= 3 & ai_quartile >= 3,
+       (innovation_score + ai_score) / sum(innovation_score[innovation_quartile >= 3 & ai_quartile >= 3] + 
+                                             ai_score[innovation_quartile >= 3 & ai_quartile >= 3]),
+       0
+     ),
+     
+     # Tier classification for discovery candidates
+     discovery_tier = case_when(
+       combined_rank <= 10 ~ "Tier 1: Top 10 Opportunities",
+       combined_rank <= 25 ~ "Tier 2: Strong Candidates",
+       combined_rank <= 50 ~ "Tier 3: Watch List",
+       TRUE ~ "Tier 4: Monitor"
+     )
+   )
> 
> cat(sprintf("   Discovery Universe Companies: %d\n", nrow(discovery_portfolios)))
   Discovery Universe Companies: 0
> cat("\n📊 Discovery Tiers:\n")

📊 Discovery Tiers:
> print(table(discovery_portfolios$discovery_tier))
< table of extent 0 >
> 
> # -----------------------------------------------------------------------------
> # 4.5 Portfolio Comparison
> # -----------------------------------------------------------------------------
> cat("\n📊 Portfolio Comparison: DUMAC vs Discovery Universe\n")

📊 Portfolio Comparison: DUMAC vs Discovery Universe
> 
> comparison_metrics <- data.frame(
+   metric = c(
+     "Number of Companies",
+     "Avg AI Score",
+     "Avg Innovation Score",
+     "AI Leaders (Q4)",
+     "Innovation Leaders (Q4)",
+     "Anomalies Detected"
+   ),
+   dumac = c(
+     nrow(dumac_portfolios),
+     mean(dumac_portfolios$ai_score, na.rm = TRUE),
+     mean(dumac_portfolios$innovation_score, na.rm = TRUE),
+     sum(dumac_portfolios$ai_quartile == 4),
+     sum(dumac_portfolios$innovation_quartile == 4),
+     sum(dumac_portfolios$is_anomaly, na.rm = TRUE)
+   ),
+   discovery = c(
+     nrow(discovery_portfolios),
+     mean(discovery_portfolios$ai_score, na.rm = TRUE),
+     mean(discovery_portfolios$innovation_score, na.rm = TRUE),
+     sum(discovery_portfolios$ai_quartile == 4),
+     sum(discovery_portfolios$innovation_quartile == 4),
+     sum(discovery_portfolios$is_anomaly, na.rm = TRUE)
+   )
+ )
Warning messages:
1: Unknown or uninitialised column: `is_anomaly`. 
2: Unknown or uninitialised column: `is_anomaly`. 
> 
> print(comparison_metrics)
                   metric   dumac discovery
1     Number of Companies 50.0000         0
2            Avg AI Score  0.6782       NaN
3    Avg Innovation Score  0.6120       NaN
4         AI Leaders (Q4) 12.0000         0
5 Innovation Leaders (Q4) 10.0000         0
6      Anomalies Detected  0.0000         0
> 
> # -----------------------------------------------------------------------------
> # 4.6 Merge portfolios back to main dataset
> # -----------------------------------------------------------------------------
> cat("\n🔗 Merging portfolio data back to main dataset...\n")

🔗 Merging portfolio data back to main dataset...
> 
> # First, merge DUMAC portfolios (created in 4.3)
> daii_scored <- daii_scored %>%
+   left_join(
+     dumac_portfolios[, c("ticker", "quality_innovators_weight", "ai_concentrated_weight", 
+                          "balanced_growth_weight")],
+     by = "ticker"
+   )
> 
> # Then, merge Discovery portfolios (created in 4.4)
> daii_scored <- daii_scored %>%
+   left_join(
+     discovery_portfolios[, c("ticker", "discovery_quality_weight", "discovery_ai_weight", 
+                              "discovery_balanced_weight", "discovery_tier",
+                              "innovation_rank", "ai_rank", "combined_rank")],
+     by = "ticker"
+   ) %>%
+   mutate(
+     # Fill NAs for DUMAC portfolio weights
+     quality_innovators_weight = ifelse(is.na(quality_innovators_weight), 0, quality_innovators_weight),
+     ai_concentrated_weight = ifelse(is.na(ai_concentrated_weight), 0, ai_concentrated_weight),
+     balanced_growth_weight = ifelse(is.na(balanced_growth_weight), 0, balanced_growth_weight),
+     
+     # Fill NAs for Discovery portfolio weights
+     discovery_quality_weight = ifelse(is.na(discovery_quality_weight), 0, discovery_quality_weight),
+     discovery_ai_weight = ifelse(is.na(discovery_ai_weight), 0, discovery_ai_weight),
+     discovery_balanced_weight = ifelse(is.na(discovery_balanced_weight), 0, discovery_balanced_weight),
+     discovery_tier = ifelse(is.na(discovery_tier), "Not in Discovery Universe", discovery_tier),
+     
+     # Fill NAs for rank columns (for companies in DUMAC portfolio only)
+     innovation_rank = ifelse(is.na(innovation_rank), NA, innovation_rank),
+     ai_rank = ifelse(is.na(ai_rank), NA, ai_rank),
+     combined_rank = ifelse(is.na(combined_rank), NA, combined_rank)
+   )
> 
> cat("✅ Portfolio data merged successfully\n")
✅ Portfolio data merged successfully
> # =============================================================================
> # SECTION 5: PHASE 2 - AI CUBE, ML MODELS, ANOMALY DETECTION (from v3.1)
> # =============================================================================
> cat(paste(rep("=", 80), collapse = ""), "\n")
================================================================================ 
> cat("🧠 PHASE 2: AI CUBE, RANDOM FOREST & ANOMALY DETECTION\n")
🧠 PHASE 2: AI CUBE, RANDOM FOREST & ANOMALY DETECTION
> cat(paste(rep("=", 80), collapse = ""), "\n\n")
================================================================================ 

> 
> # -----------------------------------------------------------------------------
> # 5.1 AI EXPOSURE CUBE
> # -----------------------------------------------------------------------------
> ai_cube <- portfolio_weights %>%
+   mutate(
+     # Strategic profiles based on AI and innovation
+     strategic_profile = case_when(
+       ai_quartile == 4 & innovation_quartile == 4 ~ "AI Pioneer",
+       ai_quartile == 4 & innovation_quartile <= 2 ~ "AI Focused (Low Innovation)",
+       ai_quartile <= 2 & innovation_quartile == 4 ~ "Innovation Leader (Low AI)",
+       ai_quartile >= 3 & innovation_quartile >= 3 ~ "Balanced Performer",
+       TRUE ~ "Underperformer"
+     ),
+     
+     # Exposure categories
+     ai_exposure = case_when(
+       ai_quartile == 4 ~ "High",
+       ai_quartile == 3 ~ "Medium",
+       TRUE ~ "Low"
+     ),
+     
+     innovation_exposure = case_when(
+       innovation_quartile == 4 ~ "High",
+       innovation_quartile == 3 ~ "Medium",
+       TRUE ~ "Low"
+     )
+   )
> 
> cat("📊 AI Exposure Cube Summary:\n")
📊 AI Exposure Cube Summary:
> print(table(ai_cube$strategic_profile, ai_cube$ai_exposure))
                             
                              High Low Medium
  AI Focused (Low Innovation)    4   0      0
  AI Pioneer                     8   0      0
  Balanced Performer             6   0     14
  Innovation Leader (Low AI)     0   2      0
  Underperformer                 0  35      4
> 
> # -----------------------------------------------------------------------------
> # 5.2 RANDOM FOREST FOR AI LEADER PREDICTION
> # -----------------------------------------------------------------------------
> cat("\n🌲 Training Random Forest model to predict AI Leaders...\n")

🌲 Training Random Forest model to predict AI Leaders...
> 
> # Prepare features for ML
> ml_features <- ai_cube %>%
+   select(
+     rd_intensity, patent_activity, revenue_growth, volatility,
+     market_cap, total_return
+   ) %>%
+   mutate(across(everything(), as.numeric))
> 
> # Target variable: AI Leader (top quartile)
> target <- factor(ifelse(ai_cube$ai_quartile == 4, "Leader", "Other"))
> 
> # Train Random Forest
> if (nrow(ml_features) > 10 && sum(target == "Leader") > 3) {
+   set.seed(42)
+   rf_model <- randomForest(
+     x = ml_features,
+     y = target,
+     ntree = 100,
+     importance = TRUE,
+     proximity = FALSE
+   )
+   
+   # Feature importance
+   feature_importance <- as.data.frame(importance(rf_model))
+   feature_importance$feature <- rownames(feature_importance)
+   feature_importance <- feature_importance[order(-feature_importance$MeanDecreaseGini), ]
+   
+   cat("\n🔍 Top 5 Predictive Features:\n")
+   print(head(feature_importance[, c("feature", "MeanDecreaseGini")], 5))
+   
+   # Predictions
+   ai_cube$predicted_ai_leader <- predict(rf_model, ml_features)
+   
+ } else {
+   cat("⚠️  Insufficient data for Random Forest. Skipping ML.\n")
+   feature_importance <- data.frame(
+     feature = c("rd_intensity", "patent_activity", "revenue_growth", "volatility", "market_cap"),
+     MeanDecreaseGini = c(0.35, 0.28, 0.20, 0.12, 0.05)
+   )
+ }

🔍 Top 5 Predictive Features:
                        feature MeanDecreaseGini
rd_intensity       rd_intensity            5.726
revenue_growth   revenue_growth            5.509
volatility           volatility            4.335
patent_activity patent_activity            3.706
market_cap           market_cap            3.494
> 
> # =============================================================================
> # SECTION 5.3: ANOMALY DETECTION (Isolation Forest) - CORRECTED
> # =============================================================================
> cat("\n🔍 Running Isolation Forest for anomaly detection...\n")

🔍 Running Isolation Forest for anomaly detection...
> 
> # Prepare numerical features for anomaly detection
> anomaly_features <- ai_cube %>%
+   select(rd_intensity, patent_activity, revenue_growth, volatility, 
+          market_cap, innovation_score, ai_score) %>%
+   mutate(across(everything(), as.numeric))
> 
> # Check for any NA/Inf values and handle them
> anomaly_features <- anomaly_features %>%
+   mutate(across(everything(), ~ifelse(is.infinite(.) | is.na(.), 0, .)))
> 
> # Standardize features
> anomaly_features_scaled <- scale(anomaly_features) %>% 
+   as.data.frame() %>%
+   mutate(across(everything(), ~ifelse(is.na(.), 0, .)))  # Handle any NAs from scaling
> 
> # Run Isolation Forest
> set.seed(42)
> 
> # Check if isotree is available
> if (require(isotree, quietly = TRUE) && nrow(anomaly_features_scaled) > 10) {
+   tryCatch({
+     # Train Isolation Forest
+     iso_model <- isolation.forest(
+       data = anomaly_features_scaled,
+       ntrees = 100,
+       sample_size = min(256, nrow(anomaly_features_scaled)),
+       ndim = ncol(anomaly_features_scaled),
+       seed = 42
+     )
+     
+     # Get anomaly scores
+     ai_cube$anomaly_score <- predict(iso_model, anomaly_features_scaled)
+     cat("   ✅ Isolation Forest completed successfully\n")
+   }, error = function(e) {
+     cat("   ⚠️ Isolation Forest error:", e$message, "\n")
+     cat("   Using statistical anomaly detection instead\n")
+     
+     # Fallback: statistical anomaly detection
+     anomaly_scores <- sapply(anomaly_features_scaled, function(x) {
+       abs(x - mean(x)) / sd(x)  # z-score
+     })
+     ai_cube$anomaly_score <- rowMeans(anomaly_scores, na.rm = TRUE)
+   })
+ } else {
+   cat("   ⚠️ isotree package not available or insufficient data\n")
+   cat("   Using statistical anomaly detection\n")
+   
+   # Statistical anomaly detection (z-score method)
+   anomaly_scores <- sapply(anomaly_features_scaled, function(x) {
+     abs(x - mean(x)) / sd(x)  # z-score
+   })
+   ai_cube$anomaly_score <- rowMeans(anomaly_scores, na.rm = TRUE)
+ }
   ✅ Isolation Forest completed successfully
> 
> # Normalize anomaly scores to 0-1 range for consistency
> ai_cube$anomaly_score <- (ai_cube$anomaly_score - min(ai_cube$anomaly_score)) / 
+   (max(ai_cube$anomaly_score) - min(ai_cube$anomaly_score))
> 
> # Identify anomalies (top 10% by score)
> anomaly_threshold <- quantile(ai_cube$anomaly_score, 0.9, na.rm = TRUE)
> ai_cube$is_anomaly <- ai_cube$anomaly_score >= anomaly_threshold
> 
> cat(sprintf("\n   Anomalies detected: %d companies (top 10%% with score > %.3f)\n", 
+             sum(ai_cube$is_anomaly, na.rm = TRUE), anomaly_threshold))

   Anomalies detected: 8 companies (top 10% with score > 0.497)
> 
> # Show anomaly score distribution
> cat("\n📊 Anomaly Score Summary:\n")

📊 Anomaly Score Summary:
> print(summary(ai_cube$anomaly_score))
   Min. 1st Qu.  Median    Mean 3rd Qu.    Max. 
  0.000   0.112   0.218   0.251   0.332   1.000 
> 
> # Get top anomalies
> top_anomalies <- ai_cube %>%
+   filter(is_anomaly) %>%
+   arrange(desc(anomaly_score)) %>%
+   select(ticker, ai_score, innovation_score, anomaly_score, strategic_profile, 
+          rd_intensity, patent_activity)
> 
> cat("\n⚠️  TOP ANOMALIES (Potential hidden AI leaders):\n")

⚠️  TOP ANOMALIES (Potential hidden AI leaders):
> if (nrow(top_anomalies) > 0) {
+   print(head(top_anomalies, 10))
+ } else {
+   cat("   No anomalies detected above threshold\n")
+ }
   ticker ai_score innovation_score anomaly_score  strategic_profile  rd_intensity patent_activity
1 9984 JT    0.775            0.775        1.0000 Balanced Performer 0.00001101414          549725
2 NVDA US    1.200            0.800        0.7186         AI Pioneer 0.00000028646             171
3 NVDA US    1.200            0.800        0.7186         AI Pioneer 0.00000028646             171
4   KO US    0.850            0.850        0.5750 Balanced Performer 0.03000000000           13301
5 SMCI US    0.660            0.550        0.5489     Underperformer 0.00000005958               0
6 ORCL US    0.700            0.500        0.5273     Underperformer 0.00000008286               0
7   PG US    0.775            0.775        0.4994 Balanced Performer 0.00000056460           21225
8   PG US    0.775            0.775        0.4994 Balanced Performer 0.00000056460           21225
> # ===== UPDATE THIS MERGE STEP =====
> cat("\n🔗 Merging anomaly scores and strategic profiles back to main dataset...\n")

🔗 Merging anomaly scores and strategic profiles back to main dataset...
> 
> # Merge anomaly scores AND strategic_profile from ai_cube to daii_scored
> daii_scored <- daii_scored %>%
+   left_join(ai_cube[, c("ticker", "anomaly_score", "is_anomaly", "strategic_profile")], 
+             by = "ticker")
> 
> cat("✅ Anomaly scores and strategic profiles merged successfully\n")
✅ Anomaly scores and strategic profiles merged successfully
> cat("   strategic_profile exists:", "strategic_profile" %in% names(daii_scored), "\n")
   strategic_profile exists: TRUE 
> # =================================
> # =============================================================================
> # SECTION 6: OUTPUT GENERATION (UPDATED FOR 260 COMPANIES)
> # =============================================================================
> cat(paste(rep("=", 80), collapse = ""), "\n")
================================================================================ 
> cat("💾 OUTPUT GENERATION\n")
💾 OUTPUT GENERATION
> cat(paste(rep("=", 80), collapse = ""), "\n\n")
================================================================================ 

> 
> # Create timestamped run directory
> run_timestamp <- format(Sys.time(), "%Y%m%d_%H%M%S")
> run_dir <- file.path(output_dir, paste0("run_", run_timestamp))
> dir.create(run_dir, recursive = TRUE, showWarnings = FALSE)
> cat("📁 Run directory:", run_dir, "\n")
📁 Run directory: C:\Users\sganesan\OneDrive - dumac.duke.edu\DAII\data\output/run_20260217_185851 
> 
> # -----------------------------------------------------------------------------
> # 6.1 Core Company Data (All 260 companies)
> # -----------------------------------------------------------------------------
> cat("\n   Saving core company data...\n")

   Saving core company data...
> 
> # Full company features (ALL 260)
> write.csv(daii_scored, 
+           file.path(run_dir, paste0(run_timestamp, "_01_company_features_full.csv")), 
+           row.names = FALSE)
> 
> # AI Cube (ALL 260)
> write.csv(daii_scored[, c("ticker", "ai_score", "innovation_score", "ai_quartile", 
+                           "innovation_quartile", "ai_label", "innovation_label", "in_portfolio")], 
+           file.path(run_dir, paste0(run_timestamp, "_02_ai_scores.csv")), 
+           row.names = FALSE)
> 
> # -----------------------------------------------------------------------------
> # 6.2 DUMAC Portfolios (Only in_portfolio = TRUE)
> # -----------------------------------------------------------------------------
> cat("   Saving DUMAC portfolios...\n")
   Saving DUMAC portfolios...
> 
> # Filter to portfolio companies only
> dumac_only <- daii_scored %>% filter(in_portfolio == TRUE)
> 
> # Quality Innovators (DUMAC)
> write.csv(dumac_only[dumac_only$quality_innovators_weight > 0, 
+                      c("ticker", "quality_innovators_weight", "innovation_score", "volatility", "total_fund_weight")],
+           file.path(run_dir, paste0(run_timestamp, "_03_dumac_quality_innovators.csv")), 
+           row.names = FALSE)
> 
> # AI Concentrated (DUMAC)
> write.csv(dumac_only[dumac_only$ai_concentrated_weight > 0,
+                      c("ticker", "ai_concentrated_weight", "ai_score", "total_fund_weight")],
+           file.path(run_dir, paste0(run_timestamp, "_04_dumac_ai_concentrated.csv")), 
+           row.names = FALSE)
> 
> # Balanced Growth (DUMAC)
> write.csv(dumac_only[dumac_only$balanced_growth_weight > 0,
+                      c("ticker", "balanced_growth_weight", "innovation_score", "ai_score", "total_fund_weight")],
+           file.path(run_dir, paste0(run_timestamp, "_05_dumac_balanced_growth.csv")), 
+           row.names = FALSE)
> 
> # All DUMAC portfolios combined
> write.csv(dumac_only[, c("ticker", "quality_innovators_weight", "ai_concentrated_weight", 
+                          "balanced_growth_weight", "innovation_score", "ai_score", "total_fund_weight")],
+           file.path(run_dir, paste0(run_timestamp, "_06_dumac_all_portfolios.csv")), 
+           row.names = FALSE)
> 
> # -----------------------------------------------------------------------------
> # 6.3 Discovery Portfolios (in_portfolio = FALSE)
> # -----------------------------------------------------------------------------
> cat("   Saving discovery portfolios...\n")
   Saving discovery portfolios...
> 
> # Filter to non-portfolio companies only
> discovery_only <- daii_scored %>% filter(in_portfolio == FALSE)
> 
> # Discovery Quality Innovators
> write.csv(discovery_only[discovery_only$discovery_quality_weight > 0,
+                          c("ticker", "discovery_quality_weight", "innovation_score", "volatility", "discovery_tier")],
+           file.path(run_dir, paste0(run_timestamp, "_07_discovery_quality_innovators.csv")), 
+           row.names = FALSE)
> 
> # Discovery AI Concentrated
> write.csv(discovery_only[discovery_only$discovery_ai_weight > 0,
+                          c("ticker", "discovery_ai_weight", "ai_score", "discovery_tier")],
+           file.path(run_dir, paste0(run_timestamp, "_08_discovery_ai_concentrated.csv")), 
+           row.names = FALSE)
> 
> # Discovery Balanced Growth
> write.csv(discovery_only[discovery_only$discovery_balanced_weight > 0,
+                          c("ticker", "discovery_balanced_weight", "innovation_score", "ai_score", "discovery_tier")],
+           file.path(run_dir, paste0(run_timestamp, "_09_discovery_balanced_growth.csv")), 
+           row.names = FALSE)
> 
> # Full Discovery Universe (all non-portfolio companies ranked)
> write.csv(discovery_only[, c("ticker", "innovation_score", "ai_score", "discovery_tier",
+                              "innovation_rank", "ai_rank", "combined_rank",
+                              "discovery_quality_weight", "discovery_ai_weight", "discovery_balanced_weight")] %>%
+             arrange(combined_rank),
+           file.path(run_dir, paste0(run_timestamp, "_10_discovery_full_universe.csv")), 
+           row.names = FALSE)
> 
> # -----------------------------------------------------------------------------
> # 6.4 Anomaly Files (Split by portfolio status)
> # -----------------------------------------------------------------------------
> cat("   Saving anomaly detection results...\n")
   Saving anomaly detection results...
> 
> # Full anomaly scores (ALL 260)
> write.csv(daii_scored[, c("ticker", "anomaly_score", "is_anomaly", "ai_score", 
+                           "innovation_score", "in_portfolio")],
+           file.path(run_dir, paste0(run_timestamp, "_11_anomaly_scores_full.csv")), 
+           row.names = FALSE)
> 
> # Top anomalies IN portfolio
> top_anomalies_portfolio <- daii_scored %>%
+   filter(is_anomaly == TRUE & in_portfolio == TRUE) %>%
+   arrange(desc(anomaly_score)) %>%
+   select(ticker, ai_score, innovation_score, anomaly_score, strategic_profile, 
+          rd_intensity, patent_activity, total_fund_weight)
> 
> write.csv(top_anomalies_portfolio,
+           file.path(run_dir, paste0(run_timestamp, "_12_top_anomalies_portfolio.csv")), 
+           row.names = FALSE)
> 
> # Top anomalies NOT in portfolio (Discovery opportunities)
> top_anomalies_discovery <- daii_scored %>%
+   filter(is_anomaly == TRUE & in_portfolio == FALSE) %>%
+   arrange(desc(anomaly_score)) %>%
+   select(ticker, ai_score, innovation_score, anomaly_score, strategic_profile, 
+          rd_intensity, patent_activity, discovery_tier)
> 
> write.csv(top_anomalies_discovery,
+           file.path(run_dir, paste0(run_timestamp, "_13_top_anomalies_discovery.csv")), 
+           row.names = FALSE)
> 
> # -----------------------------------------------------------------------------
> # 6.5 ML & Feature Importance
> # -----------------------------------------------------------------------------
> cat("   Saving ML results...\n")
   Saving ML results...
> 
> # Check if Random Forest model exists and ran successfully
> if (exists("rf_model") && !is.null(rf_model)) {
+   
+   # Check if predictions exist in daii_scored
+   if(!"predicted_ai_leader" %in% names(daii_scored)) {
+     cat("   ⚠️ predictions not found in daii_scored, creating from model...\n")
+     
+     # Recreate predictions if needed
+     ml_features <- daii_scored %>%
+       select(rd_intensity, patent_activity, revenue_growth, volatility, 
+              market_cap, total_return) %>%
+       mutate(across(everything(), as.numeric))
+     
+     daii_scored$predicted_ai_leader <- predict(rf_model, ml_features)
+   }
+   
+   # Create target variable (if it doesn't exist)
+   if(!exists("target") || length(target) == 0) {
+     target <- factor(ifelse(daii_scored$ai_quartile == 4, "Leader", "Other"))
+   }
+   
+   # Ensure lengths match
+   if(length(daii_scored$ticker) == length(target) && 
+      length(daii_scored$ticker) == length(daii_scored$predicted_ai_leader)) {
+     
+     write.csv(data.frame(
+       ticker = daii_scored$ticker,
+       actual = target,
+       predicted = daii_scored$predicted_ai_leader,
+       in_portfolio = daii_scored$in_portfolio
+     ), file.path(run_dir, paste0(run_timestamp, "_14_predictions.csv")), row.names = FALSE)
+     
+     cat("   ✅ Predictions saved successfully\n")
+   } else {
+     cat("   ⚠️ Length mismatch: tickers=", length(daii_scored$ticker), 
+         ", target=", length(target), 
+         ", predictions=", length(daii_scored$predicted_ai_leader), "\n")
+   }
+   
+ } else {
+   cat("   ⚠️ No Random Forest model found. Skipping ML predictions.\n")
+   
+   # Create a simple placeholder file
+   write.csv(data.frame(
+     ticker = daii_scored$ticker,
+     note = "Random Forest model did not run - insufficient data or model error",
+     in_portfolio = daii_scored$in_portfolio
+   ), file.path(run_dir, paste0(run_timestamp, "_14_predictions_note.csv")), row.names = FALSE)
+ }
   ⚠️ predictions not found in daii_scored, creating from model...
   ✅ Predictions saved successfully
> 
> # Feature importance (may also need checking)
> if (exists("feature_importance") && nrow(feature_importance) > 0) {
+   write.csv(feature_importance,
+             file.path(run_dir, paste0(run_timestamp, "_15_feature_importance.csv")), 
+             row.names = FALSE)
+   cat("   ✅ Feature importance saved\n")
+ } else {
+   cat("   ⚠️ No feature importance data. Creating default.\n")
+   
+   # Create default feature importance if missing
+   default_importance <- data.frame(
+     feature = c("rd_intensity", "patent_activity", "revenue_growth", "volatility", "market_cap"),
+     MeanDecreaseGini = c(0.35, 0.28, 0.20, 0.12, 0.05)
+   )
+   write.csv(default_importance,
+             file.path(run_dir, paste0(run_timestamp, "_15_feature_importance_default.csv")), 
+             row.names = FALSE)
+ }
   ✅ Feature importance saved
> 
> # -----------------------------------------------------------------------------
> # 6.6 Portfolio Comparison & Performance Metrics
> # -----------------------------------------------------------------------------
> cat("   Saving comparison metrics...\n")
   Saving comparison metrics...
> 
> # Portfolio comparison (already created in Module 4C)
> write.csv(comparison_metrics,
+           file.path(run_dir, paste0(run_timestamp, "_16_portfolio_comparison.csv")), 
+           row.names = FALSE)
> 
> # Performance metrics (split by portfolio status)
> performance_metrics <- data.frame(
+   metric = c(
+     "total_companies",
+     "dumac_portfolio_companies",
+     "discovery_universe_companies",
+     "ai_leaders_dumac",
+     "ai_leaders_discovery",
+     "anomalies_dumac",
+     "anomalies_discovery",
+     "mean_ai_score_dumac",
+     "mean_ai_score_discovery",
+     "mean_innovation_score_dumac",
+     "mean_innovation_score_discovery"
+   ),
+   value = c(
+     nrow(daii_scored),
+     sum(daii_scored$in_portfolio),
+     sum(!daii_scored$in_portfolio),
+     sum(daii_scored$ai_quartile == 4 & daii_scored$in_portfolio),
+     sum(daii_scored$ai_quartile == 4 & !daii_scored$in_portfolio),
+     sum(daii_scored$is_anomaly & daii_scored$in_portfolio),
+     sum(daii_scored$is_anomaly & !daii_scored$in_portfolio),
+     mean(daii_scored$ai_score[daii_scored$in_portfolio], na.rm = TRUE),
+     mean(daii_scored$ai_score[!daii_scored$in_portfolio], na.rm = TRUE),
+     mean(daii_scored$innovation_score[daii_scored$in_portfolio], na.rm = TRUE),
+     mean(daii_scored$innovation_score[!daii_scored$in_portfolio], na.rm = TRUE)
+   )
+ )
> write.csv(performance_metrics,
+           file.path(run_dir, paste0(run_timestamp, "_17_performance.csv")), 
+           row.names = FALSE)
> 
> # -----------------------------------------------------------------------------
> # 6.7 Configuration YAML
> # -----------------------------------------------------------------------------
> config <- list(
+   run_timestamp = run_timestamp,
+   version = "4.0 FINAL (260 Companies)",
+   date = as.character(Sys.Date()),
+   n_companies = nrow(daii_scored),
+   n_dumac_portfolio = sum(daii_scored$in_portfolio),
+   n_discovery_universe = sum(!daii_scored$in_portfolio),
+   n_ai_leaders = sum(daii_scored$ai_quartile == 4),
+   n_anomalies = sum(daii_scored$is_anomaly),
+   files_used = list(
+     fundamentals = "N200_Fundamentals_Cleaned.csv",
+     holdings = "N200_FINAL_StrategicDUMACPortfolioDistribution_BBergUploadRawData_Integrated_Data.csv",
+     snapshot = "N200_company_snapshot_260.csv"
+   ),
+   directories = list(
+     raw = raw_dir,
+     output = run_dir
+   )
+ )
> yaml::write_yaml(config, file.path(run_dir, paste0(run_timestamp, "_18_config.yaml")))
> 
> # -----------------------------------------------------------------------------
> # 6.8 Visualizations
> # -----------------------------------------------------------------------------
> cat("   Generating visualizations...\n")
   Generating visualizations...
> 
> # Anomaly score distribution (colored by portfolio status)
> png(file.path(run_dir, paste0(run_timestamp, "_19_anomaly_histogram.png")), width = 800, height = 600)
> hist(daii_scored$anomaly_score, breaks = 30, 
+      main = "Anomaly Score Distribution by Portfolio Status",
+      xlab = "Anomaly Score", col = "lightgray", border = "white")
> abline(v = quantile(daii_scored$anomaly_score, 0.9), col = "red", lwd = 2, lty = 2)
> legend("topright", legend = c("Top 10% Threshold"), col = "red", lwd = 2, lty = 2)
> dev.off()
null device 
          1 
> 
> # AI vs Innovation scatter (colored by portfolio status)
> png(file.path(run_dir, paste0(run_timestamp, "_20_ai_vs_innovation.png")), width = 800, height = 600)
> plot(daii_scored$innovation_score, daii_scored$ai_score, 
+      col = ifelse(daii_scored$in_portfolio, 
+                   ifelse(daii_scored$is_anomaly, "darkred", "steelblue"),
+                   ifelse(daii_scored$is_anomaly, "orange", "lightgreen")),
+      pch = 19, cex = 1.2,
+      xlab = "Innovation Score", ylab = "AI Score",
+      main = "AI Score vs Innovation Score")
> abline(lm(ai_score ~ innovation_score, data = daii_scored), col = "darkgreen", lwd = 2)
> legend("topleft", 
+        legend = c("Portfolio - Normal", "Portfolio - Anomaly", 
+                   "Discovery - Normal", "Discovery - Anomaly"),
+        col = c("steelblue", "darkred", "lightgreen", "orange"), 
+        pch = 19)
> dev.off()
null device 
          1 
> 
> # -----------------------------------------------------------------------------
> # 6.9 Summary Report
> # -----------------------------------------------------------------------------
> cat("\n📊 GENERATING SUMMARY REPORT\n")

📊 GENERATING SUMMARY REPORT
> cat("==========================\n")
==========================
> 
> summary_report <- paste(
+   sprintf("# DAII 3.5 Run Summary – %s\n\n", run_timestamp),
+   "## 📊 OVERVIEW\n",
+   sprintf("- Total Companies Analyzed: %d\n", nrow(daii_scored)),
+   sprintf("- DUMAC Portfolio Companies: %d\n", sum(daii_scored$in_portfolio)),
+   sprintf("- Discovery Universe Companies: %d\n", sum(!daii_scored$in_portfolio)),
+   sprintf("- AI Leaders (Q4): %d\n", sum(daii_scored$ai_quartile == 4)),
+   sprintf("- Anomalies Detected: %d\n", sum(daii_scored$is_anomaly)),
+   "\n## 🎯 TOP DISCOVERY OPPORTUNITIES\n",
+   paste(capture.output(print(head(discovery_only %>% 
+                                     filter(discovery_tier == "Tier 1: Top 10 Opportunities") %>%
+                                     select(ticker, ai_score, innovation_score, discovery_tier), 10))), 
+         collapse = "\n"),
+   "\n## 📁 OUTPUT FILES GENERATED\n",
+   paste(sprintf("- %s", list.files(run_dir)), collapse = "\n"),
+   sep = ""
+ )
> 
> writeLines(summary_report, file.path(run_dir, "README.md"))
> cat("✅ Summary report saved to run directory\n")
✅ Summary report saved to run directory
> 
> cat("\n✅✅✅ ALL OUTPUTS GENERATED SUCCESSFULLY\n")

✅✅✅ ALL OUTPUTS GENERATED SUCCESSFULLY
> cat("   Run directory:", run_dir, "\n")
   Run directory: C:\Users\sganesan\OneDrive - dumac.duke.edu\DAII\data\output/run_20260217_185851 
> cat("   Output files:", length(list.files(run_dir)), "\n")
   Output files: 21 
> # =============================================================================
> # SECTION 7: PATENTSVIEW API TARGET IDENTIFICATION
> # =============================================================================
> cat(paste(rep("=", 80), collapse = ""), "\n")
================================================================================ 
> cat("🎯 PATENTSVIEW API TARGET IDENTIFICATION\n")
🎯 PATENTSVIEW API TARGET IDENTIFICATION
> cat(paste(rep("=", 80), collapse = ""), "\n\n")
================================================================================ 

> 
> # Identify companies for PatentsView API calls
> patentsview_targets <- ai_cube %>%
+   filter(
+     # High AI score but missing/low patent data
+     ai_quartile >= 3 & (patent_activity == 0 | is.na(patent_activity)) &
+       # Not already flagged as anomaly (unless they're interesting cases)
+       !is_anomaly
+   ) %>%
+   arrange(desc(ai_score)) %>%
+   select(ticker, ai_score, innovation_score, patent_activity, industry, strategic_profile)
> 
> cat("🎯 TARGET COMPANIES FOR PATENTSVIEW API:\n")
🎯 TARGET COMPANIES FOR PATENTSVIEW API:
> cat(sprintf("   %d companies identified for API queries\n", nrow(patentsview_targets)))
   12 companies identified for API queries
> cat("\n   These companies have HIGH AI scores but NO patent data in Bloomberg.\n")

   These companies have HIGH AI scores but NO patent data in Bloomberg.
> cat("   Query PatentsView to validate and enrich.\n\n")
   Query PatentsView to validate and enrich.

> 
> if (nrow(patentsview_targets) > 0) {
+   print(head(patentsview_targets, 20))
+   
+   # Save targets list
+   write.csv(patentsview_targets,
+             file.path(run_dir, paste0(run_timestamp, "_13_patentsview_targets.csv")),
+             row.names = FALSE)
+   cat("\n✅ Targets saved to run directory\n")
+ }
      ticker ai_score innovation_score patent_activity                       industry  strategic_profile
1    MSFT US   1.0850            0.775               0            Software & Services         AI Pioneer
2    MSFT US   1.0150            0.725               0            Software & Services Balanced Performer
3      MU US   0.9750            0.650               0 Semiconductors & Semiconductor Balanced Performer
4  005930 KS   0.9300            0.775               0 Technology Hardware & Equipmen         AI Pioneer
5    PCOR US   0.8750            0.625               0            Software & Services Balanced Performer
6  005930 KS   0.8400            0.700               0 Technology Hardware & Equipmen Balanced Performer
7    SPOT US   0.8125            0.625               0          Media & Entertainment Balanced Performer
8     SAP GR   0.7700            0.550               0            Software & Services     Underperformer
9     HDB US   0.7000            0.700               0                          Banks Balanced Performer
10    HDB US   0.7000            0.700               0                          Banks Balanced Performer
11   TSLA US   0.6875            0.625               0       Automobiles & Components Balanced Performer
12   TSLA US   0.6875            0.625               0       Automobiles & Components Balanced Performer

✅ Targets saved to run directory
> 
> cat("\n")

> cat(paste(rep("=", 80), collapse = ""), "\n")
================================================================================ 
> cat("🏁 PIPELINE EXECUTION COMPLETE\n")
🏁 PIPELINE EXECUTION COMPLETE
> cat(paste(rep("=", 80), collapse = ""), "\n")
================================================================================ 
