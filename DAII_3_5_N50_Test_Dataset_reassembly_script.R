# ============================================================================
# DAII CHUNK REASSEMBLY SCRIPT
# ============================================================================
# Use this script to reassemble the chunks back into the original dataset

reassemble_daii_chunks <- function(manifest_file) {
  cat("🔗 Reassembling DAII dataset from chunks...\n")
  cat(paste(rep("=", 60), collapse = ""), "\n")
  
  # Read manifest
  manifest <- read.csv(manifest_file, stringsAsFactors = FALSE)
  chunk_files <- manifest$file_path
  
  # Sort files by chunk number
  chunk_numbers <- as.numeric(gsub(".*chunk_(\\d+)_of.*", "\\1", basename(chunk_files)))
  chunk_files <- chunk_files[order(chunk_numbers)]
  
  # Load and combine chunks
  combined_data <- NULL
  for (i in seq_along(chunk_files)) {
    cat(sprintf("Loading chunk %d/%d: %s\n", i, length(chunk_files), basename(chunk_files[i])))
    chunk_data <- read.csv(chunk_files[i], stringsAsFactors = FALSE)
    
    if (is.null(combined_data)) {
      combined_data <- chunk_data
    } else {
      combined_data <- rbind(combined_data, chunk_data)
    }
  }
  
  cat(sprintf("\n✅ Successfully reassembled: %d rows, %d columns\n", 
              nrow(combined_data), ncol(combined_data)))
  return(combined_data)
}

# Usage:
# manifest_file <- "DAII_3_5_N50_Test_Dataset_chunk_manifest.csv"
# full_data <- reassemble_daii_chunks(manifest_file)
# write.csv(full_data, "DAII_3_5_Reassembled_Dataset.csv", row.names = FALSE)

