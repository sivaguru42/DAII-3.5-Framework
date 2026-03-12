# /R/ingest/ingest_bloomberg.R
# NOTE: Requires Bloomberg API credentials from engineering

library(Rblpapi)

# Connection function
connect_bloomberg <- function() {
  tryCatch({
    blpConnect()
    message("✅ Connected to Bloomberg")
    return(TRUE)
  }, error = function(e) {
    message("❌ Bloomberg connection failed:", e$message)
    return(FALSE)
  })
}

# Pull OHLCV history
pull_ohlcv_history <- function(tickers, start_date = "2019-01-01") {
  securities <- paste0(tickers, " US Equity")
  
  data <- bdh(
    securities = securities,
    fields = c("PX_OPEN", "PX_HIGH", "PX_LOW", "PX_LAST", "VOLUME"),
    start.date = as.Date(start_date),
    end.date = Sys.Date(),
    include.non.trading.days = FALSE,
    options = c("fillDays" = "NIL_VALUE", "days" = "weekdays")
  )
  
  return(data)
}

# Pull current estimates
pull_estimates <- function(tickers) {
  securities <- paste0(tickers, " US Equity")
  
  data <- bdp(
    securities = securities,
    fields = c(
      "BEST_ANALYST_RATING",
      "TARGET_PRICE",
      "TARGET_PRICE_HIGH",
      "TARGET_PRICE_LOW",
      "NUMBER_OF_ESTIMATES",
      "RECOMMENDATION_CONSENSUS"
    )
  )
  
  return(data)
}