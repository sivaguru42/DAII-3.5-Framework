
library(readr)
library(dplyr)

cat("Reassembling N200 CSV...\n")

chunks <- list.files(pattern = "N200_chunk_\\d\\d\\.csv$")
chunks <- chunks[order(as.numeric(gsub(".*chunk_(\\d+).*", "\\\\1", chunks)))]

if(length(chunks) == 0) {
  cat("No chunk files found.\n")
  quit()
}

cat("Found", length(chunks), "chunks.\n")

full_data <- list()
for(chunk in chunks) {
  cat("Reading:", chunk, "\n")
  full_data[[chunk]] <- read_csv(chunk, col_types = cols())
}

combined <- bind_rows(full_data)
write_csv(combined, "N200_REASSEMBLED.csv")

cat("✅ Done! Saved as N200_REASSEMBLED.csv\n")
cat("Rows:", nrow(combined), "\n")
cat("Columns:", ncol(combined), "\n")

