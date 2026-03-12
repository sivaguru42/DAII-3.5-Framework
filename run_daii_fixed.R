
# DAII 3.5 Wrapper - Fixes issues without modifying original script
cat(paste0("\n", paste(rep("=", 60), collapse = ""), "\n"))
cat("DAII 3.5 Pipeline Wrapper\n")
cat(paste0(paste(rep("=", 60), collapse = ""), "\n\n"))

# Set the correct GitHub URLs before sourcing main script
assign("github_config", list(
  n50_url = "https://raw.githubusercontent.com/sivaguru42/DAII-3.5-Framework/main/data/raw/DAII_3_5_N50_Test_Dataset.csv",
  n200_url = "https://raw.githubusercontent.com/sivaguru42/DAII-3.5-Framework/main/data/raw/N200_FINAL_StrategicDUMACPortfolioDistribution_BBergUploadRawData_Integrated_Data.csv",
  config_urls = list(
    scoring = "https://raw.githubusercontent.com/sivaguru42/DAII-3.5-Framework/main/daii_scoring_config.yaml",
    imputation = "https://raw.githubusercontent.com/sivaguru42/DAII-3.5-Framework/main/daii_imputation_config.yaml",
    field_mapping = "https://raw.githubusercontent.com/sivaguru42/DAII-3.5-Framework/main/daii_field_mapping.yaml"
  ),
  verification = "lenient"
), envir = .GlobalEnv)

# Set output directory to your OneDrive
output_dir <- "C:/Users/sganesan/OneDrive - dumac.duke.edu/DAII/data/output"
if (!dir.exists(output_dir)) dir.create(output_dir, recursive = TRUE)

# Modify the main script environment to use local output
cat("Output directory:", output_dir, "\n\n")

# Now source the main script
cat("Sourcing main script...\n")
source("src/consolidated/DAII_3.5_Phase1_Complete_Codebase.R")

