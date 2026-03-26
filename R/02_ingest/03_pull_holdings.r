library(dplyr)
source(here::here("R", "01_utils", "database_connection.r"))

pull_current_holdings <- function(isins = NULL) {

  message("Pulling current holdings...")

  conn <- connect_research()
  on.exit(dbDisconnect(conn), add = TRUE)

  latest_date <- dbGetQuery(conn, "
    SELECT TOP (1) date_val
    FROM risk_exposure.ltp_dashboard_meta_table
    WHERE field_name = 'latest_date'
      AND multiplicity_index = 0
  ")$date_val[1]

  message(sprintf("  Latest holdings date: %s", latest_date))

  base_select <- sprintf("
    SELECT
        e.Name__Modified                    AS security_name,
        e.ISIN,
        e.Instr_o_BB__Code                  AS bb_ticker,
        e.Fund_o_Fund_Legal_Name            AS fund_name,
        e.Fund_o_Client_Code                AS fund_code,
        e.Portfolio_o_CL__Asset__Class      AS asset_class,
        e.Portfolio_o_CL__SubAsset__Class   AS sub_asset_class,
        e.[Direct_vs._Non___direct]         AS direct_flag,
        e.Risk_o_Long_Exposure              AS long_exposure_usd,
        e.Risk_o_Short_Exposure             AS short_exposure_usd,
        e.Risk_o_Net_Exposure               AS net_exposure_usd,
        e.Net__l___pct_LTP__l_              AS net_pct_ltp,
        e.Long__l___pct_LTP__l_             AS long_pct_ltp,
        e.Positions_o_Base_o_Total_NAV      AS nav_usd,
        e.COUNTRY__OF__RISK__Modified       AS country,
        e.Region1                           AS region1,
        e.Region2                           AS region2,
        e.GICS__Extension__Level1           AS gics_sector,
        e.Instr_o_DM_Type                   AS instrument_type,
        e.Instr_o_DM_SubType               AS instrument_subtype,
        e.Report_o_End_Date                 AS as_of_date
    FROM risk_exposure.ltp_exposure_ts e
    WHERE e.Report_o_End_Date = '%s'", latest_date)

  if (!is.null(isins)) {
    isin_string <- paste0("'", isins, "'", collapse = ", ")
    sql <- paste0(base_select,
                  sprintf("
      AND e.ISIN IN (%s)", isin_string),
                  "
    ORDER BY ABS(e.Net__l___pct_LTP__l_) DESC")
  } else {
    sql <- paste0(base_select,
                  "
    ORDER BY ABS(e.Net__l___pct_LTP__l_) DESC")
  }

  message("  Executing holdings query...")
  holdings <- dbGetQuery(conn, sql)

  message(sprintf("  Pulled %d holdings records", nrow(holdings)))
  message(sprintf("  Distinct securities: %d", length(unique(holdings$ISIN))))
  message(sprintf("  Distinct funds: %d", length(unique(holdings$fund_code))))

  firm_exposure <- holdings %>%
    group_by(ISIN, security_name) %>%
    summarise(
      total_net_exposure_usd = sum(net_exposure_usd, na.rm = TRUE),
      total_net_pct_ltp      = sum(net_pct_ltp, na.rm = TRUE),
      n_funds                = n_distinct(fund_code),
      .groups = "drop"
    ) %>%
    arrange(desc(total_net_pct_ltp))

  save_path <- here::here("data", "01_raw", "current_holdings.rds")
  saveRDS(list(raw = holdings, firm_aggregated = firm_exposure), save_path)
  message("  Saved to: ", save_path)

  return(holdings)
}
