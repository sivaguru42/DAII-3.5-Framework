# ============================================================================
# DAII 3.5 - MAIN EXECUTION SCRIPT
# ============================================================================
#
# PURPOSE: Complete end-to-end execution of DAII 3.5 analysis
#
# USAGE:
# 1. Save this entire file as "daii_3.5_main.R"
# 2. Place input data file in working directory
# 3. Run: source("daii_3.5_main.R")
#
# CUSTOMIZATION:
# 1. Modify config.yaml for custom parameters
# 2. Adjust weights in configuration for different emphasis
# 3. Customize output formats in configuration
#
# ============================================================================

# MAIN EXECUTION FUNCTION
run_daii_3.5_analysis <- function(config_file = "daii_config.yaml",
                                 data_file = NULL,
                                 output_dir = NULL,
                                 quick_mode = FALSE) {
  #' Run Complete DAII 3.5 Analysis
  #' 
  #' @param config_file Path to configuration file
  #' @param data_file Optional override for data file path
  #' @param output_dir Optional override for output directory
  #' @param quick_mode If TRUE, skip some visualizations and detailed outputs
  #' @return Complete analysis results
  
  cat("\n" + paste(rep("🚀", 30), collapse = ""))
  cat("\n           DAII 3.5 ANALYSIS FRAMEWORK")
  cat("\n" + paste(rep("🚀", 30), collapse = ""))
  cat("\n\n")
  
  # Start timer
  start_time <- Sys.time()
  
  # Load or create configuration
  if(file.exists(config_file)) {
    config <- load_daii_configuration(config_file)
  } else {
    config <- create_default_configuration()
    save_configuration(config, config_file)
  }
  
  # Override configuration if provided
  if(!is.null(data_file)) {
    config$data_file <- data_file
  }
  
  if(!is.null(output_dir)) {
    config$output_directory <- output_dir
  }
  
  if(quick_mode) {
    config$output$create_dashboard <- FALSE
    config$output$create_presentation <- FALSE
    config$validation$sensitivity_analysis <- FALSE
  }
  
  # Check for input data
  if(!file.exists(config$data_file)) {
    stop(sprintf("Input data file not found: %s\nPlease place your data file in the working directory.", 
                 config$data_file))
  }
  
  # Run analysis
  tryCatch({
    results <- create_reproducible_analysis(config_file)
    
    # Calculate execution time
    end_time <- Sys.time()
    duration <- difftime(end_time, start_time, units = "mins")
    
    # Success message
    cat("\n" + paste(rep("✅", 30), collapse = ""))
    cat(sprintf("\n           ANALYSIS COMPLETE!"))
    cat("\n" + paste(rep("✅", 30), collapse = ""))
    
    cat(sprintf("\n\n📊 SUMMARY:\n"))
    cat(sprintf("   • Execution time: %.1f minutes\n", duration))
    cat(sprintf("   • Companies analyzed: %d\n", 
                nrow(results$results$calculate_daii$daii_data)))
    cat(sprintf("   • Portfolio DAII score: %.1f\n",
                results$results$integrate_portfolio$portfolio_metrics$overall$portfolio_daii))
    cat(sprintf("   • Output directory: %s\n", config$output_directory))
    cat(sprintf("   • Reproducibility package: %s\n",
                results$reproducibility$package$package_path))
    
    # Open output directory if interactive
    if(interactive()) {
      output_path <- normalizePath(config$output_directory)
      cat(sprintf("\n📂 Opening output directory: %s\n", output_path))
      
      if(Sys.info()["sysname"] == "Windows") {
        shell.exec(output_path)
      } else if(Sys.info()["sysname"] == "Darwin") {
        system(paste("open", output_path))
      } else {
        system(paste("xdg-open", output_path))
      }
    }
    
    return(results)
    
  }, error = function(e) {
    cat("\n" + paste(rep("❌", 30), collapse = ""))
    cat("\n           ANALYSIS FAILED!")
    cat("\n" + paste(rep("❌", 30), collapse = ""))
    cat(sprintf("\n\nError: %s\n", e$message))
    cat("\nTroubleshooting steps:\n")
    cat("1. Check input data file exists and is accessible\n")
    cat("2. Verify data format matches expected structure\n")
    cat("3. Check for missing required packages\n")
    cat("4. Review configuration file for errors\n")
    
    stop("Analysis failed. See error message above.")
  })
}

# MINIMAL WORKING EXAMPLE
run_minimal_example <- function() {
  #' Run Minimal Working Example
  
  cat("\n🔧 RUNNING MINIMAL WORKING EXAMPLE\n")
  
  # Create sample data if none exists
  if(!file.exists("sample_data.csv")) {
    create_sample_data()
  }
  
  # Create minimal configuration
  config <- list(
    data_file = "sample_data.csv",
    output_directory = "DAII_Minimal_Example",
    weights = list(
      r_d = 0.30,
      analyst = 0.20,
      patent = 0.25,
      news = 0.10,
      growth = 0.15
    )
  )
  
  save_configuration(config, "minimal_config.yaml")
  
  # Run analysis
  results <- run_daii_3.5_analysis(
    config_file = "minimal_config.yaml",
    quick_mode = TRUE
  )
  
  return(results)
}

create_sample_data <- function(n_companies = 50, n_holdings = 200) {
  #' Create Sample Data for Testing
  
  cat("Creating sample data...\n")
  
  # Generate company data
  companies <- data.frame(
    Ticker = paste0("TICK", sprintf("%03d", 1:n_companies)),
    Company.Name = paste("Company", 1:n_companies),
    GICS.Ind.Grp.Name = sample(c("Technology", "Healthcare", "Financials", 
                                 "Consumer", "Industrials"), 
                               n_companies, replace = TRUE),
    R.D.Exp = round(runif(n_companies, 0, 1000), 2),
    Mkt.Cap = round(runif(n_companies, 1000, 100000), 2),
    BEst.Analyst.Rtg = round(runif(n_companies, 1, 5), 1),
    Patents...Trademarks...Copy.Rgt = round(runif(n_companies, 0, 1000)),
    News.Sent = round(runif(n_companies, 0, 100), 1),
    Rev...1.Yr.Gr = round(runif(n_companies, -20, 50), 1),
    stringsAsFactors = FALSE
  )
  
  # Generate holdings data
  holdings <- data.frame(
    fund_name = rep(paste0("FUND", LETTERS[1:5]), each = n_holdings/5),
    Ticker = sample(companies$Ticker, n_holdings, replace = TRUE),
    fund_weight = runif(n_holdings, 0.001, 0.05),
    stringsAsFactors = FALSE
  )
  
  # Normalize weights by fund
  holdings <- holdings %>%
    group_by(fund_name) %>%
    mutate(fund_weight = fund_weight / sum(fund_weight)) %>%
    ungroup()
  
  # Merge company data
  holdings <- merge(holdings, companies, by = "Ticker", all.x = TRUE)
  
  # Add some missing values
  holdings$R.D.Exp[sample(1:nrow(holdings), 10)] <- NA
  holdings$BEst.Analyst.Rtg[sample(1:nrow(holdings), 15)] <- NA
  
  # Save data
  write.csv(holdings, "sample_data.csv", row.names = FALSE)
  
  cat(sprintf("Created sample data: %d companies, %d holdings\n", 
              n_companies, n_holdings))
  cat("Saved to: sample_data.csv\n")
}

# EXPORT ALL FUNCTIONS
export_daii_functions <- function() {
  #' Export All DAII Functions to Global Environment
  
  # List all functions from this file
  all_functions <- c(
    # Core functions
    "initialize_daii_environment",
    "load_and_validate_data",
    "extract_unique_companies",
    "impute_missing_values",
    "calculate_component_scores",
    "normalize_to_100",
    "calculate_daii_scores",
    "analyze_weight_sensitivity",
    "integrate_with_portfolio",
    "clean_portfolio_weights",
    "calculate_portfolio_metrics",
    "analyze_fund_innovation",
    "analyze_industry_exposure",
    "analyze_innovation_concentration",
    "create_portfolio_summary",
    
    # Validation functions
    "create_validation_framework",
    "perform_statistical_validation",
    "perform_business_validation",
    "perform_process_validation",
    "perform_sensitivity_validation",
    "create_validation_template",
    "generate_validation_summary",
    
    # Visualization functions
    "create_daii_visualizations",
    "create_distribution_visualizations",
    "create_relationship_visualizations",
    "create_composition_visualizations",
    "create_portfolio_visualizations",
    "create_validation_visualizations",
    "create_visualization_summary",
    "create_html_gallery",
    
    # Output functions
    "generate_daii_outputs",
    "generate_company_score_outputs",
    "generate_portfolio_outputs",
    "generate_validation_outputs",
    "generate_executive_summaries",
    "create_executive_summary_text",
    "create_key_findings_table",
    "create_recommendations_table",
    "create_executive_dashboard",
    "export_raw_data",
    "create_output_summary",
    "create_master_index",
    
    # Reproducibility functions
    "create_reproducible_analysis",
    "setup_reproducibility",
    "load_daii_configuration",
    "create_default_configuration",
    "save_configuration",
    "validate_configuration",
    "capture_system_info",
    "create_analysis_pipeline",
    "execute_analysis_pipeline",
    "create_reproducibility_package",
    "save_function_definitions",
    "calculate_data_checksums",
    "create_reproducibility_report",
    "create_reproducibility_readme",
    "validate_reproducibility",
    
    # Main execution
    "run_daii_3.5_analysis",
    "run_minimal_example",
    "create_sample_data",
    "export_daii_functions"
  )
  
  cat(sprintf("Exported %d DAII 3.5 functions to global environment\n", 
              length(all_functions)))
  
  return(all_functions)
}

# ============================================================================
# EXECUTION OPTIONS
# ============================================================================

# Option 1: Run complete analysis (uncomment to use)
# results <- run_daii_3.5_analysis()

# Option 2: Run minimal example (uncomment to use)
# results <- run_minimal_example()

# Option 3: Export all functions for manual use (uncomment to use)
# export_daii_functions()

# ============================================================================
# HELPER FUNCTIONS FOR INTERACTIVE USE
# ============================================================================

daii_help <- function() {
  #' Display DAII 3.5 Help Information
  
  cat("\n" + paste(rep("📚", 30), collapse = ""))
  cat("\n           DAII 3.5 HELP MENU")
  cat("\n" + paste(rep("📚", 30), collapse = ""))
  
  cat("\n\nMAIN FUNCTIONS:\n")
  cat("  run_daii_3.5_analysis()     - Run complete analysis\n")
  cat("  run_minimal_example()       - Run minimal working example\n")
  cat("  export_daii_functions()     - Export all functions\n")
  
  cat("\nCONFIGURATION:\n")
  cat("  Create 'daii_config.yaml' file or use default\n")
  cat("  Modify weights, thresholds, and output settings\n")
  
  cat("\nDATA REQUIREMENTS:\n")
  cat("  Required columns: Ticker, R.D.Exp, Mkt.Cap, BEst.Analyst.Rtg,\n")
  cat("  Patents...Trademarks...Copy.Rgt, News.Sent, Rev...1.Yr.Gr\n")
  cat("  Optional: fund_name, fund_weight, GICS.Ind.Grp.Name\n")
  
  cat("\nQUICK START:\n")
  cat("  1. Place data file in working directory\n")
  cat("  2. Run: results <- run_daii_3.5_analysis()\n")
  cat("  3. View outputs in generated directory\n")
  
  cat("\nFOR MORE INFORMATION:\n")
  cat("  Review the complete documentation in the code\n")
  cat("  Check configuration file for customization options\n")
  cat("  Use run_minimal_example() to test the framework\n")
  
  cat("\n" + paste(rep("📚", 30), collapse = ""))
  cat("\n")
}

# Display help on source
cat("\nDAII 3.5 Framework loaded. Type 'daii_help()' for usage information.\n")