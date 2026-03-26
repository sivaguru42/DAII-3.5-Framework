library(dplyr)
source(here::here("R", "01_utils", "database_connection.r"))

pull_daily_ratios <- function(company_map, start_date = "2024-01-01") {
  
  message("Pulling daily ratios...")
  
  conn <- connect_research()
  on.exit(dbDisconnect(conn), add = TRUE)
  
  repno_list <- paste0("'", paste(company_map$RepNo, collapse = "', '"), "'")
  
  sql <- sprintf("
    SELECT
        [RepNo],
        [as_of_date],
        [Market_capitalization]                                                AS market_cap,
        [P_÷_E_excluding_extraordinary_items_most_recent_fiscal_yearr]        AS pe_ratio_fy,
        [P_÷_E_excluding_extraordinary_items_TTM]                             AS pe_ratio_ttm,
        [Price_to_Book_most_recent_fiscal_yearr]                              AS pb_ratio,
        [Current_EV_÷_EBITDA]                                                 AS ev_ebitda,
        [Current_EV_÷_Revenue]                                                AS ev_revenue,
        [Beta_]                                                               AS beta_5y,
        [3_Year_Weekly_Beta]                                                  AS beta_3y,
        [Price_closing_or_last_bid]                                           AS close_price,
        [Volume_avg._trading_volume_for_the_last_ten_days]                    AS avg_volume_10d,
        [Dividend_Yield_indicated_annual_dividend_divided_by_closing_price]    AS div_yield,
        [Return_on_average_equity_trailing_12_month]                          AS roe_ttm,
        [Net_Profit_Margin_%%_trailing_12_month]                              AS net_margin_ttm,
        [Revenue_trailing_12_month]                                           AS revenue_ttm,
        [Free_Cash_Flow_trailing_12_month]                                    AS fcf_ttm,
        [EPS_excluding_extraordinary_items_trailing_12_month]                 AS eps_ttm,
        [Total_debt_most_recent_quarter]                                      AS total_debt,
        [Cash_and_Equiv._most_recent_quarter]                                 AS cash_equiv,
        [Price_52_week_price_percent_change]                                  AS price_52w_chg,
        [Price_YTD_price_percent_change]                                      AS price_ytd_chg,
        [Price_1_Day_%%_Change]                                               AS price_1d_pct_change,
        [Price_5_Day_%%_Change]                                               AS price_5d_pct_change,
        [Sharpe_Ratio_3_Year_Weekly]                                          AS sharpe_3y,
        [Sharpe_Ratio_5_Year_Monthly]                                         AS sharpe_5y,
        [Earnings_per_Share_Excluding_Extraordinary_Items_Avg._Diluted_Shares_Outstanding_5_Year_Interim_Trend_Volatility_%%] AS eps_trend_vol_5y,
        [Revenue_Primary_5_Year_Interim_Trend_Volatility_%%]                  AS revenue_trend_vol_5y,
        [Free_Cash_Flow_Levered_5_Year_Interim_Trend_Volatility_%%]           AS fcf_trend_vol_5y
    FROM [refinitiv].[daily_ratios_and_values]
    WHERE [RepNo] IN (%s)
      AND [as_of_date] >= '%s'
    ORDER BY [RepNo], [as_of_date] DESC
  ", repno_list, start_date)
  
  message("  Querying refinitiv.daily_ratios_and_values...")
  
  ratios <- tryCatch({
    dbGetQuery(conn, sql)
  }, error = function(e) {
    stop("Query failed: ", e$message)
  })
  
  message(sprintf("  Pulled %d daily ratio records", nrow(ratios)))
  
  # Join with ticker
  ratios <- left_join(ratios, company_map[, c("RepNo", "Ticker")], by = "RepNo")
  
  # Calculate rolling volatility
  if (nrow(ratios) > 0 && "price_1d_pct_change" %in% names(ratios)) {
    
    safe_sd <- function(x, min_obs = 15) {
      valid <- x[!is.na(x)]
      if (length(valid) < min_obs) return(NA_real_)
      sd(valid)
    }
    
    ratios <- ratios %>%
      group_by(Ticker) %>%
      arrange(as_of_date) %>%
      mutate(
        volatility_30d  = zoo::rollapply(price_1d_pct_change / 100, width = 22,
                                         FUN = safe_sd, fill = NA, align = "right") * sqrt(252),
        volatility_90d  = zoo::rollapply(price_1d_pct_change / 100, width = 63,
                                         FUN = safe_sd, fill = NA, align = "right") * sqrt(252),
        volatility_260d = zoo::rollapply(price_1d_pct_change / 100, width = 252,
                                         FUN = function(x) safe_sd(x, min_obs = 180),
                                         fill = NA, align = "right") * sqrt(252)
      ) %>%
      ungroup()
    
    message("  Calculated rolling volatility (30d/90d/260d annualized)")
  }
  
  save_path <- here::here("data", "01_raw", "daily_ratios.rds")
  saveRDS(ratios, save_path)
  message("  Saved to: ", save_path)
  
  return(ratios)
}

