# reorganize.R - Run this in R
setwd("C:/Users/sganesan/DAII-3.5-Framework")

# Create directories
dirs <- c("R/00_config", "R/01_utils", "R/02_ingest", "R/03_transform", 
          "R/04_modules", "R/05_output", "R/99_archive",
          "data/00_reference", "data/01_raw", "data/02_processed", "data/03_output",
          "docs/architecture", "docs/user-guide", "docs/api",
          "tests", "logs", "dash")
for(d in dirs) dir.create(d, recursive = TRUE, showWarnings = FALSE)

# Move YAML files
yaml_files <- list.files(pattern = "\\.yaml$")
file.rename(yaml_files, file.path("R/00_config", yaml_files))

# Move test files
test_files <- list.files(pattern = "^test.*\\.R$")
file.rename(test_files, file.path("tests", test_files))

# Archive old scripts
archive_files <- c("DAII_3.5_WORKING_FIXED.R", "fix_quartile_calculation.R", 
                   "temp_daii_pipeline.R")
for(f in archive_files) {
  if(file.exists(f)) file.rename(f, file.path("R/99_archive", f))
}

cat("✅ Reorganization complete!\n")