# ============================================================================
# MODULE 9: REPRODUCIBILITY - Configuration & Customization
# ============================================================================
#
# PURPOSE: Ensure reproducibility and enable customization
#
# REPRODUCIBILITY FEATURES:
# 1. Version Control: Track all code and configuration versions
# 2. Dependency Management: Capture package versions and system info
# 3. Seed Setting: Ensure random processes are reproducible
# 4. Configuration Files: Externalize all parameters
#
# CUSTOMIZATION FEATURES:
# 1. Weight Configuration: Adjust component weights
# 2. Threshold Customization: Modify quartile and outlier thresholds
# 3. Industry Classification: Custom industry mappings
# 4. Output Formatting: Customize reports and visualizations
#
# ============================================================================

create_reproducible_analysis <- function(config_file = "daii_config.yaml") {
  #' Create Reproducible DAII 3.5 Analysis
  #' 
  #' @param config_file Path to configuration file
  #' @return Complete analysis with reproducibility features
  
  cat("\n🔬 CREATING REPRODUCIBLE DAII 3.5 ANALYSIS\n")
  cat(paste(rep("=", 60), collapse = ""), "\n")
  
  # 1. SET UP REPRODUCIBILITY
  reproducibility_setup <- setup_reproducibility(config_file)
  
  # 2. LOAD CONFIGURATION
  config <- load_daii_configuration(config_file)
  
  # 3. SET SEED FOR RANDOM PROCESSES
  set.seed(config$random_seed)
  cat(sprintf("   Random seed set: %d\n", config$random_seed))
  
  # 4. CAPTURE SYSTEM INFORMATION
  system_info <- capture_system_info()
  
  # 5. CREATE ANALYSIS PIPELINE
  analysis_pipeline <- create_analysis_pipeline(config)
  
  # 6. RUN ANALYSIS
  cat("\n🏃 RUNNING ANALYSIS PIPELINE\n")
  
  results <- execute_analysis_pipeline(
    analysis_pipeline,
    config$data_file,
    config$output_directory
  )
  
  # 7. CREATE REPRODUCIBILITY PACKAGE
  reproducibility_package <- create_reproducibility_package(
    config,
    system_info,
    analysis_pipeline,
    results,
    config$output_directory
  )
  
  # 8. VALIDATE REPRODUCIBILITY
  reproducibility_check <- validate_reproducibility(results, config)
  
  cat(sprintf("\n✅ Reproducible analysis complete\n"))
  cat(sprintf("   Output directory: %s\n", config$output_directory))
  cat(sprintf("   Reproducibility package: %s\n", 
              reproducibility_package$package_path))
  
  return(list(
    results = results,
    config = config,
    reproducibility = list(
      setup = reproducibility_setup,
      system_info = system_info,
      package = reproducibility_package,
      check = reproducibility_check
    )
  ))
}

setup_reproducibility <- function(config_file) {
  #' Set Up Reproducibility Framework
  
  setup_info <- list(
    timestamp = Sys.time(),
    r_version = R.version.string,
    platform = R.version$platform,
    config_file = config_file,
    working_directory = getwd()
  )
  
  # Create reproducibility directory
  repro_dir <- "00_Reproducibility"
  if(!dir.exists(repro_dir)) {
    dir.create(repro_dir, showWarnings = FALSE)
  }
  
  # Save session information
  sink(file.path(repro_dir, "session_info.txt"))
  print(sessionInfo())
  sink()
  
  # Save git information if available
  if(file.exists(".git")) {
    tryCatch({
      git_info <- system("git log --oneline -1", intern = TRUE)
      setup_info$git_commit <- git_info
      
      writeLines(git_info, file.path(repro_dir, "git_commit.txt"))
    }, error = function(e) {
      cat("Git information not available\n")
    })
  }
  
  return(setup_info)
}

load_daii_configuration <- function(config_file) {
  #' Load DAII Configuration from YAML File
  
  if(!file.exists(config_file)) {
    cat(sprintf("Configuration file not found: %s\n", config_file))
    cat("Creating default configuration...\n")
    config <- create_default_configuration()
    save_configuration(config, config_file)
  } else {
    if(require(yaml)) {
      config <- yaml::read_yaml(config_file)
    } else {
      stop("YAML package required for configuration loading")
    }
  }
  
  # Validate configuration
  config <- validate_configuration(config)
  
  cat("\n📋 LOADED CONFIGURATION:\n")
  cat(sprintf("   • Data file: %s\n", config$data_file))
  cat(sprintf("   • Output directory: %s\n", config$output_directory))
  cat(sprintf("   • Component weights: R&D=%.0f%%, Analyst=%.0f%%, Patent=%.0f%%, News=%.0f%%, Growth=%.0f%%\n",
              config$weights$r_d * 100, config$weights$analyst * 100,
              config$weights$patent * 100, config$weights$news * 100,
              config$weights$growth * 100))
  
  return(config)
}

create_default_configuration <- function() {
  #' Create Default DAII Configuration
  
  config <- list(
    version = "3.5",
    data_file = "input_data.csv",
    output_directory = "DAII_Output",
    
    # Component weights
    weights = list(
      r_d = 0.30,
      analyst = 0.20,
      patent = 0.25,
      news = 0.10,
      growth = 0.15
    ),
    
    # Data processing
    imputation = list(
      method = "median",
      industry_imputation = TRUE,
      max_missing_percent = 50
    ),
    
    # Normalization
    normalization = list(
      method = "min_max",
      lower_bound = 0.01,
      upper_bound = 0.99
    ),
    
    # Scoring
    scoring = list(
      log_transform = c("r_d_intensity", "patent_count"),
      epsilon = 1e-10
    ),
    
    # Portfolio integration
    portfolio = list(
      weight_column = "fund_weight",
      fund_column = "fund_name",
      calculate_hhi = TRUE,
      calculate_gini = TRUE
    ),
    
    # Validation
    validation = list(
      statistical_tests = TRUE,
      business_validation = TRUE,
      sensitivity_analysis = TRUE
    ),
    
    # Output
    output = list(
      formats = c("csv", "excel", "html"),
      create_dashboard = TRUE,
      create_presentation = TRUE
    ),
    
    # Reproducibility
    reproducibility = list(
      random_seed = 2024,
      save_raw_data = TRUE,
      save_configuration = TRUE
    )
  )
  
  return(config)
}

save_configuration <- function(config, config_file) {
  #' Save Configuration to YAML File
  
  if(require(yaml)) {
    yaml::write_yaml(config, config_file)
    cat(sprintf("Configuration saved: %s\n", config_file))
  } else {
    warning("YAML package not available. Configuration not saved.")
  }
}

validate_configuration <- function(config) {
  #' Validate Configuration Parameters
  
  # Check required fields
  required_fields <- c("data_file", "output_directory", "weights")
  missing_fields <- setdiff(required_fields, names(config))
  
  if(length(missing_fields) > 0) {
    stop(sprintf("Missing required configuration fields: %s",
                 paste(missing_fields, collapse = ", ")))
  }
  
  # Validate weights sum to 1
  weights_sum <- sum(unlist(config$weights))
  if(abs(weights_sum - 1) > 0.001) {
    warning(sprintf("Weights sum to %.3f (should be 1). Normalizing...", weights_sum))
    
    # Normalize weights
    for(w in names(config$weights)) {
      config$weights[[w]] <- config$weights[[w]] / weights_sum
    }
  }
  
  # Set defaults for missing optional fields
  defaults <- create_default_configuration()
  
  for(section in names(defaults)) {
    if(!section %in% names(config)) {
      config[[section]] <- defaults[[section]]
    } else {
      for(param in names(defaults[[section]])) {
        if(!param %in% names(config[[section]])) {
          config[[section]][[param]] <- defaults[[section]][[param]]
        }
      }
    }
  }
  
  return(config)
}

capture_system_info <- function() {
  #' Capture System Information for Reproducibility
  
  system_info <- list(
    r_version = R.version.string,
    platform = R.version$platform,
    operating_system = Sys.info()["sysname"],
    machine = Sys.info()["machine"],
    user = Sys.info()["user"],
    timezone = Sys.timezone(),
    locale = Sys.getlocale(),
    working_directory = getwd(),
    memory_limit = ifelse(.Platform$OS.type == "windows",
                         memory.limit(),
                         NA)
  )
  
  # Capture package versions
  packages <- c("dplyr", "tidyr", "ggplot2", "yaml", "openxlsx", "corrplot")
  package_versions <- sapply(packages, function(pkg) {
    if(requireNamespace(pkg, quietly = TRUE)) {
      as.character(packageVersion(pkg))
    } else {
      "Not installed"
    }
  })
  
  system_info$package_versions <- as.list(package_versions)
  
  # Save to file
  repro_dir <- "00_Reproducibility"
  if(!dir.exists(repro_dir)) {
    dir.create(repro_dir, showWarnings = FALSE)
  }
  
  writeLines(
    jsonlite::toJSON(system_info, pretty = TRUE),
    file.path(repro_dir, "system_info.json")
  )
  
  return(system_info)
}

create_analysis_pipeline <- function(config) {
  #' Create Analysis Pipeline from Configuration
  
  pipeline <- list(
    steps = list(),
    dependencies = list(),
    parameters = config
  )
  
  # Define pipeline steps
  pipeline$steps <- list(
    list(
      id = "initialize",
      function_name = "initialize_daii_environment",
      parameters = list(working_dir = NULL)
    ),
    list(
      id = "load_data",
      function_name = "load_and_validate_data",
      parameters = list(
        data_path = config$data_file,
        min_companies = 100,
        max_missing_pct = config$imputation$max_missing_percent / 100
      )
    ),
    list(
      id = "extract_companies",
      function_name = "extract_unique_companies",
      parameters = list(
        exclude_cols = c("fund_name", "fund_weight", "Position.Size")
      )
    ),
    list(
      id = "impute_missing",
      function_name = "impute_missing_values",
      parameters = list(
        imputation_methods = list(
          "BEst.Analyst.Rtg" = "mean",
          "default" = config$imputation$method
        ),
        industry_col = "GICS.Ind.Grp.Name"
      )
    ),
    list(
      id = "calculate_scores",
      function_name = "calculate_component_scores",
      parameters = list(
        weights_config = config$weights
      )
    ),
    list(
      id = "calculate_daii",
      function_name = "calculate_daii_scores",
      parameters = list(
        weights_config = list(
          default = c(R_D = config$weights$r_d,
                     Analyst = config$weights$analyst,
                     Patent = config$weights$patent,
                     News = config$weights$news,
                     Growth = config$weights$growth)
        )
      )
    ),
    list(
      id = "integrate_portfolio",
      function_name = "integrate_with_portfolio",
      parameters = list(
        weight_col = config$portfolio$weight_column,
        fund_col = config$portfolio$fund_column
      )
    ),
    list(
      id = "validate_results",
      function_name = "create_validation_framework",
      parameters = list()
    ),
    list(
      id = "create_visualizations",
      function_name = "create_daii_visualizations",
      parameters = list(
        output_dir = "05_Visualizations"
      )
    ),
    list(
      id = "generate_outputs",
      function_name = "generate_daii_outputs",
      parameters = list(
        output_dir = config$output_directory
      )
    )
  )
  
  # Define dependencies
  pipeline$dependencies <- list(
    load_data = c(),
    extract_companies = "load_data",
    impute_missing = "extract_companies",
    calculate_scores = "impute_missing",
    calculate_daii = "calculate_scores",
    integrate_portfolio = "calculate_daii",
    validate_results = "calculate_daii",
    create_visualizations = c("calculate_daii", "integrate_portfolio", "validate_results"),
    generate_outputs = c("calculate_daii", "integrate_portfolio", "validate_results")
  )
  
  # Save pipeline definition
  pipeline_file <- file.path("00_Reproducibility", "analysis_pipeline.json")
  writeLines(
    jsonlite::toJSON(pipeline, pretty = TRUE),
    pipeline_file
  )
  
  cat(sprintf("Analysis pipeline created: %s\n", pipeline_file))
  cat(sprintf("Number of steps: %d\n", length(pipeline$steps)))
  
  return(pipeline)
}

execute_analysis_pipeline <- function(pipeline, data_file, output_dir) {
  #' Execute Analysis Pipeline
  
  results <- list()
  execution_log <- list()
  
  # Initialize environment
  env_result <- do.call(pipeline$steps[[1]]$function_name,
                       pipeline$steps[[1]]$parameters)
  
  results[[pipeline$steps[[1]]$id]] <- env_result
  execution_log[[pipeline$steps[[1]]$id]] <- list(
    start_time = Sys.time(),
    status = "completed"
  )
  
  # Execute remaining steps
  for(i in 2:length(pipeline$steps)) {
    step <- pipeline$steps[[i]]
    
    cat(sprintf("\n▶️ Executing step %d/%d: %s\n", 
                i, length(pipeline$steps), step$id))
    
    # Check dependencies
    if(step$id %in% names(pipeline$dependencies)) {
      deps <- pipeline$dependencies[[step$id]]
      missing_deps <- setdiff(deps, names(results))
      
      if(length(missing_deps) > 0) {
        warning(sprintf("Missing dependencies for %s: %s",
                        step$id, paste(missing_deps, collapse = ", ")))
      }
    }
    
    # Prepare parameters
    params <- step$parameters
    
    # Add data from previous steps
    if(step$id == "load_data") {
      params$data_path <- data_file
    } else if(step$id == "extract_companies") {
      params$data <- results$load_data$data
      params$ticker_col <- results$load_data$ticker_col
    } else if(step$id == "impute_missing") {
      params$company_data <- results$extract_companies$companies
      params$ticker_col <- results$load_data$ticker_col
    } else if(step$id == "calculate_scores") {
      params$imputed_data <- results$impute_missing$imputed_data
      params$ticker_col <- results$load_data$ticker_col
    } else if(step$id == "calculate_daii") {
      params$scores_data <- results$calculate_scores$scores_data
      params$ticker_col <- results$load_data$ticker_col
    } else if(step$id == "integrate_portfolio") {
      params$holdings_data <- results$load_data$data
      params$daii_scores <- results$calculate_daii$daii_data
      params$ticker_col <- results$load_data$ticker_col
    } else if(step$id == "validate_results") {
      params$daii_results <- results$calculate_daii
      params$holdings_data <- results$load_data$data
      params$imputation_log <- results$impute_missing$imputation_log
    } else if(step$id == "create_visualizations") {
      params$daii_data <- results$calculate_daii$daii_data
      params$portfolio_results <- results$integrate_portfolio
      params$validation_report <- results$validate_results$validation_report
    } else if(step$id == "generate_outputs") {
      params$daii_results <- results$calculate_daii
      params$portfolio_results <- results$integrate_portfolio
      params$validation_results <- results$validate_results
    }
    
    # Execute step
    start_time <- Sys.time()
    
    tryCatch({
      result <- do.call(step$function_name, params)
      results[[step$id]] <- result
      
      execution_log[[step$id]] <- list(
        start_time = start_time,
        end_time = Sys.time(),
        duration = difftime(Sys.time(), start_time, units = "secs"),
        status = "completed",
        error = NA
      )
      
      cat(sprintf("   ✅ Completed in %.1f seconds\n", 
                  as.numeric(execution_log[[step$id]]$duration)))
      
    }, error = function(e) {
      execution_log[[step$id]] <- list(
        start_time = start_time,
        end_time = Sys.time(),
        duration = difftime(Sys.time(), start_time, units = "secs"),
        status = "failed",
        error = e$message
      )
      
      cat(sprintf("   ❌ Failed: %s\n", e$message))
    })
  }
  
  # Save execution log
  execution_summary <- data.frame(
    Step = names(execution_log),
    Status = sapply(execution_log, function(x) x$status),
    Duration_Seconds = sapply(execution_log, function(x) as.numeric(x$duration)),
    Error_Message = sapply(execution_log, function(x) ifelse(is.na(x$error), "", x$error)),
    stringsAsFactors = FALSE
  )
  
  write.csv(execution_summary,
            file.path(output_dir, "execution_log.csv"),
            row.names = FALSE)
  
  return(results)
}

create_reproducibility_package <- function(config,
                                          system_info,
                                          analysis_pipeline,
                                          results,
                                          output_dir) {
  #' Create Reproducibility Package
  
  repro_dir <- file.path(output_dir, "00_Reproducibility")
  if(!dir.exists(repro_dir)) {
    dir.create(repro_dir, recursive = TRUE, showWarnings = FALSE)
  }
  
  package_files <- list()
  
  # 1. Save configuration
  config_file <- file.path(repro_dir, "configuration.yaml")
  yaml::write_yaml(config, config_file)
  package_files$configuration <- config_file
  
  # 2. Save system information
  system_file <- file.path(repro_dir, "system_information.json")
  writeLines(jsonlite::toJSON(system_info, pretty = TRUE), system_file)
  package_files$system_info <- system_file
  
  # 3. Save pipeline definition
  pipeline_file <- file.path(repro_dir, "analysis_pipeline.json")
  writeLines(jsonlite::toJSON(analysis_pipeline, pretty = TRUE), pipeline_file)
  package_files$pipeline <- pipeline_file
  
  # 4. Save session info
  session_file <- file.path(repro_dir, "detailed_session_info.txt")
  sink(session_file)
  print(sessionInfo())
  sink()
  package_files$session_info <- session_file
  
  # 5. Save function definitions
  functions_file <- file.path(repro_dir, "function_definitions.R")
  save_function_definitions(functions_file)
  package_files$functions <- functions_file
  
  # 6. Save data checksums
  checksums_file <- file.path(repro_dir, "data_checksums.txt")
  calculate_data_checksums(checksums_file, config$data_file)
  package_files$checksums <- checksums_file
  
  # 7. Create reproducibility report
  report_file <- file.path(repro_dir, "reproducibility_report.html")
  create_reproducibility_report(report_file, config, system_info, package_files)
  package_files$report <- report_file
  
  # 8. Create README
  readme_file <- file.path(repro_dir, "README.md")
  create_reproducibility_readme(readme_file, package_files)
  package_files$readme <- readme_file
  
  # 9. Package everything into a zip file
  package_path <- file.path(output_dir, 
                           paste0("daii_3.5_reproducibility_",
                                 format(Sys.time(), "%Y%m%d_%H%M%S"),
                                 ".zip"))
  
  zip_files <- unlist(package_files)
  zip::zip(package_path, files = zip_files, root = output_dir)
  
  cat(sprintf("Reproducibility package created: %s\n", package_path))
  
  return(list(
    package_files = package_files,
    package_path = package_path
  ))
}

save_function_definitions <- function(output_file) {
  #' Save Function Definitions for Reproducibility
  
  # Get all functions in the global environment that start with specific patterns
  function_patterns <- c("^initialize_", "^load_", "^extract_", "^impute_",
                        "^calculate_", "^integrate_", "^create_", "^generate_",
                        "^analyze_", "^validate_", "^normalize_")
  
  all_functions <- ls(envir = .GlobalEnv)
  daii_functions <- character()
  
  for(pattern in function_patterns) {
    daii_functions <- c(daii_functions, 
                       grep(pattern, all_functions, value = TRUE))
  }
  
  # Remove duplicates
  daii_functions <- unique(daii_functions)
  
  # Write function definitions to file
  sink(output_file)
  
  cat("# DAII 3.5 Function Definitions\n")
  cat(paste("# Generated:", Sys.time(), "\n\n"))
  
  for(func_name in daii_functions) {
    func <- get(func_name, envir = .GlobalEnv)
    
    if(is.function(func)) {
      cat(paste(rep("#", 80), collapse = ""), "\n")
      cat(sprintf("# Function: %s\n", func_name))
      cat(paste(rep("#", 80), collapse = ""), "\n\n")
      
      # Get function code
      func_code <- capture.output(print(func))
      
      # Remove attributes
      func_code <- func_code[!grepl("^<bytecode|^<environment", func_code)]
      
      # Write function code
      cat(paste(func_code, collapse = "\n"))
      cat("\n\n")
    }
  }
  
  sink()
  
  cat(sprintf("Saved %d function definitions to %s\n", 
              length(daii_functions), output_file))
}

calculate_data_checksums <- function(output_file, data_file) {
  #' Calculate Data Checksums for Verification
  
  if(!file.exists(data_file)) {
    warning(sprintf("Data file not found: %s", data_file))
    return(NULL)
  }
  
  checksums <- list(
    file_name = basename(data_file),
    file_size = file.size(data_file),
    last_modified = file.info(data_file)$mtime,
    md5 = tools::md5sum(data_file),
    sha256 = digest::digest(data_file, algo = "sha256", file = TRUE)
  )
  
  # Write checksums to file
  sink(output_file)
  cat("DATA CHECKSUMS FOR REPRODUCIBILITY\n")
  cat("==================================\n\n")
  
  cat(sprintf("File: %s\n", checksums$file_name))
  cat(sprintf("Size: %s bytes\n", format(checksums$file_size, big.mark = ",")))
  cat(sprintf("Modified: %s\n", checksums$last_modified))
  cat(sprintf("MD5: %s\n", checksums$md5))
  cat(sprintf("SHA256: %s\n", checksums$sha256))
  
  sink()
  
  return(checksums)
}

create_reproducibility_report <- function(output_file, config, system_info, package_files) {
  #' Create Reproducibility Report
  
  report_html <- '
  <!DOCTYPE html>
  <html lang="en">
  <head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>DAII 3.5 Reproducibility Report</title>
    <style>
      body {
        font-family: Arial, sans-serif;
        margin: 40px;
        background-color: #f5f5f5;
      }
      .header {
        background: linear-gradient(135deg, #1F4E79 0%, #2E8B57 100%);
        color: white;
        padding: 30px;
        border-radius: 10px;
        margin-bottom: 30px;
      }
      .section {
        background-color: white;
        padding: 20px;
        border-radius: 8px;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        margin-bottom: 20px;
      }
      .section-title {
        color: #1F4E79;
        border-bottom: 2px solid #2E8B57;
        padding-bottom: 10px;
        margin-bottom: 20px;
      }
      .info-grid {
        display: grid;
        grid-template-columns: repeat(auto-fit, minmax(300px, 1fr));
        gap: 20px;
        margin-bottom: 20px;
      }
      .info-card {
        background-color: #f9f9f9;
        padding: 15px;
        border-radius: 5px;
        border-left: 4px solid #2E8B57;
      }
      .info-card h3 {
        margin-top: 0;
        color: #1F4E79;
      }
      .file-list {
        list-style-type: none;
        padding: 0;
      }
      .file-list li {
        padding: 8px;
        border-bottom: 1px solid #eee;
      }
      .file-list li:hover {
        background-color: #f9f9f9;
      }
      .verification-badge {
        display: inline-block;
        padding: 5px 10px;
        background-color: #2E8B57;
        color: white;
        border-radius: 3px;
        font-size: 12px;
        margin-right: 10px;
      }
      .timestamp {
        color: #666;
        font-size: 12px;
        margin-top: 30px;
        text-align: center;
      }
    </style>
  </head>
  <body>
    <div class="header">
      <h1>🔬 DAII 3.5 Reproducibility Report</h1>
      <p>Complete documentation for reproducing the analysis</p>
      <p><strong>Generated:</strong> '
  
  report_html <- paste0(report_html, format(Sys.time(), "%Y-%m-%d %H:%M:%S"), '</p>
    </div>')
  
  # System Information
  report_html <- paste0(report_html, '
    <div class="section">
      <h2 class="section-title">💻 System Information</h2>
      <div class="info-grid">
        <div class="info-card">
          <h3>R Environment</h3>
          <p><strong>R Version:</strong> ', system_info$r_version, '</p>
          <p><strong>Platform:</strong> ', system_info$platform, '</p>
          <p><strong>OS:</strong> ', system_info$operating_system, '</p>
        </div>
        
        <div class="info-card">
          <h3>Analysis Settings</h3>
          <p><strong>Working Directory:</strong> ', system_info$working_directory, '</p>
          <p><strong>Timezone:</strong> ', system_info$timezone, '</p>
          <p><strong>Locale:</strong> ', system_info$locale, '</p>
        </div>
        
        <div class="info-card">
          <h3>Key Packages</h3>')
  
  # Add package versions
  for(pkg in names(system_info$package_versions)) {
    report_html <- paste0(report_html, '
          <p><strong>', pkg, ':</strong> ', system_info$package_versions[[pkg]], '</p>')
  }
  
  report_html <- paste0(report_html, '
        </div>
      </div>
    </div>')
  
  # Configuration Summary
  report_html <- paste0(report_html, '
    <div class="section">
      <h2 class="section-title">⚙️ Configuration Summary</h2>
      <div class="info-grid">
        <div class="info-card">
          <h3>Data Input</h3>
          <p><strong>Data File:</strong> ', config$data_file, '</p>
          <p><strong>Output Directory:</strong> ', config$output_directory, '</p>
          <p><strong>DAII Version:</strong> ', config$version, '</p>
        </div>
        
        <div class="info-card">
          <h3>Component Weights</h3>
          <p><strong>R&D Intensity:</strong> ', config$weights$r_d * 100, '%</p>
          <p><strong>Analyst Sentiment:</strong> ', config$weights$analyst * 100, '%</p>
          <p><strong>Patent Activity:</strong> ', config$weights$patent * 100, '%</p>
          <p><strong>News Sentiment:</strong> ', config$weights$news * 100, '%</p>
          <p><strong>Growth Momentum:</strong> ', config$weights$growth * 100, '%</p>
        </div>
        
        <div class="info-card">
          <h3>Processing Settings</h3>
          <p><strong>Imputation Method:</strong> ', config$imputation$method, '</p>
          <p><strong>Normalization:</strong> ', config$normalization$method, '</p>
          <p><strong>Random Seed:</strong> ', config$reproducibility$random_seed, '</p>
        </div>
      </div>
    </div>')
  
  # Reproducibility Package Contents
  report_html <- paste0(report_html, '
    <div class="section">
      <h2 class="section-title">📦 Reproducibility Package Contents</h2>
      <ul class="file-list">')
  
  for(file_type in names(package_files)) {
    if(file_type != "report") {  # Don't list this report
      file_path <- package_files[[file_type]]
      file_name <- basename(file_path)
      
      report_html <- paste0(report_html, '
        <li>
          <span class="verification-badge">', toupper(file_type), '</span>
          <strong>', file_name, '</strong>
          <span style="color: #666; font-size: 12px; margin-left: 10px;">
            (', round(file.size(file_path) / 1024, 1), ' KB)
          </span>
        </li>')
    }
  }
  
  report_html <- paste0(report_html, '
      </ul>
    </div>')
  
  # Reproducibility Instructions
  report_html <- paste0(report_html, '
    <div class="section">
      <h2 class="section-title">📋 How to Reproduce This Analysis</h2>
      
      <h3>Step 1: Prepare Environment</h3>
      <div class="info-card">
        <p>1. Install R version ', R.version$major, '.', R.version$minor, '</p>
        <p>2. Install required packages from session_info.txt</p>
        <p>3. Set working directory to: ', getwd(), '</p>
      </div>
      
      <h3>Step 2: Restore Data</h3>
      <div class="info-card">
        <p>1. Place the input data file in the working directory</p>
        <p>2. Verify data integrity using checksums in data_checksums.txt</p>
        <p>3. Ensure the file name matches: ', basename(config$data_file), '</p>
      </div>
      
      <h3>Step 3: Restore Configuration</h3>
      <div class="info-card">
        <p>1. Load the configuration from configuration.yaml</p>
        <p>2. Verify all parameters match the original analysis</p>
        <p>3. Set the random seed to: ', config$reproducibility$random_seed, '</p>
      </div>
      
      <h3>Step 4: Execute Analysis</h3>
      <div class="info-card">
        <p>1. Load function definitions from function_definitions.R</p>
        <p>2. Follow the analysis pipeline in analysis_pipeline.json</p>
        <p>3. Execute each step in the defined order</p>
      </div>
      
      <h3>Step 5: Verify Results</h3>
      <div class="info-card">
        <p>1. Compare output files with original results</p>
        <p>2. Validate key metrics match within acceptable tolerances</p>
        <p>3. Check visualization outputs for consistency</p>
      </div>
    </div>')
  
  # Verification Checklist
  report_html <- paste0(report_html, '
    <div class="section">
      <h2 class="section-title">✅ Verification Checklist</h2>
      
      <div class="info-card">
        <h3>Essential Checks</h3>
        <p>☐ R version matches: ', system_info$r_version, '</p>
        <p>☐ Package versions are compatible</p>
        <p>☐ Input data checksums match</p>
        <p>☐ Configuration parameters are identical</p>
        <p>☐ Random seed is properly set</p>
      </div>
      
      <div class="info-card">
        <h3>Output Verification</h3>
        <p>☐ Portfolio DAII score matches within ±0.1</p>
        <p>☐ Top 10 innovators list is identical</p>
        <p>☐ Quartile distribution matches within ±1%</p>
        <p>☐ Validation status is consistent</p>
        <p>☐ All output files are generated</p>
      </div>
    </div>')
  
  # Close HTML
  report_html <- paste0(report_html, '
    <div class="timestamp">
      <p>DAII 3.5 Reproducibility Framework v3.5</p>
      <p>For support or questions, contact the Innovation Analytics Team</p>
    </div>
  </body>
  </html>')
  
  # Write report
  writeLines(report_html, output_file)
  
  return(output_file)
}

create_reproducibility_readme <- function(output_file, package_files) {
  #' Create README for Reproducibility Package
  
  readme_content <- paste(
    "# DAII 3.5 Reproducibility Package",
    "",
    "This package contains all necessary components to reproduce the DAII 3.5 analysis.",
    "",
    "## Package Contents",
    "",
    sep = "\n"
  )
  
  # List files
  for(file_type in names(package_files)) {
    file_path <- package_files[[file_type]]
    file_name <- basename(file_path)
    
    readme_content <- paste(readme_content,
                           sprintf("- **%s**: %s", file_type, file_name),
                           sep = "\n")
  }
  
  # Add instructions
  readme_content <- paste(readme_content,
                         "",
                         "## How to Use This Package",
                         "",
                         "### 1. Quick Start",
                         "```r",
                         "# Load the reproducibility package",
                         "source('function_definitions.R')",
                         "",
                         "# Load configuration",
                         "config <- yaml::read_yaml('configuration.yaml')",
                         "",
                         "# Set random seed",
                         sprintf("set.seed(%d)", 2024),  # Using default seed
                         "",
                         "# Run the analysis",
                         "results <- create_reproducible_analysis('configuration.yaml')",
                         "```",
                         "",
                         "### 2. Detailed Reproduction",
                         "",
                         "1. **Environment Setup**:",
                         "   - Install R version matching the system information",
                         "   - Install required packages with specified versions",
                         "   - Set the working directory",
                         "",
                         "2. **Data Preparation**:",
                         "   - Place the input data file in the working directory",
                         "   - Verify data integrity using checksums",
                         "",
                         "3. **Configuration**:",
                         "   - Review and load the configuration file",
                         "   - Verify all parameters match the original analysis",
                         "",
                         "4. **Execution**:",
                         "   - Load all function definitions",
                         "   - Execute the analysis pipeline step by step",
                         "   - Monitor progress and check for errors",
                         "",
                         "5. **Verification**:",
                         "   - Compare outputs with original results",
                         "   - Validate key metrics and visualizations",
                         "   - Generate verification report",
                         "",
                         "## Expected Outputs",
                         "",
                         "A successful reproduction should generate:",
                         "- Company-level DAII scores and rankings",
                         "- Portfolio innovation analysis",
                         "- Validation reports and quality checks",
                         "- Executive summaries and dashboards",
                         "- Complete visualization package",
                         "",
                         "## Troubleshooting",
                         "",
                         "### Common Issues",
                         "",
                         "1. **Package Version Conflicts**:",
                         "   - Check session_info.txt for exact versions",
                         "   - Use renv or packrat for version management",
                         "",
                         "2. **Missing Data Files**:",
                         "   - Verify the input data file exists and is accessible",
                         "   - Check file permissions and paths",
                         "",
                         "3. **Configuration Errors**:",
                         "   - Validate YAML syntax in configuration files",
                         "   - Check for missing required parameters",
                         "",
                         "4. **Memory Issues**:",
                         "   - Large datasets may require increased memory",
                         "   - Consider processing in batches if needed",
                         "",
                         "## Support",
                         "",
                         "For assistance with reproduction:",
                         "- Review the reproducibility report",
                         "- Check the execution log for errors",
                         "- Contact the Innovation Analytics Team",
                         "",
                         "---",
                         sprintf("Generated: %s", format(Sys.time(), "%Y-%m-%d %H:%M:%S")),
                         "DAII 3.5 Framework v3.5",
                         sep = "\n")
  
  # Write README
  writeLines(readme_content, output_file)
  
  return(output_file)
}

validate_reproducibility <- function(results, config) {
  #' Validate Reproducibility of Results
  
  validation_results <- list(
    checks = list(),
    status = "pending",
    issues = character()
  )
  
  # Check 1: Results structure
  expected_results <- c("load_data", "extract_companies", "impute_missing",
                       "calculate_scores", "calculate_daii", "integrate_portfolio",
                       "validate_results", "create_visualizations", "generate_outputs")
  
  missing_results <- setdiff(expected_results, names(results))
  
  if(length(missing_results) > 0) {
    validation_results$checks$structure <- list(
      passed = FALSE,
      message = sprintf("Missing results: %s", paste(missing_results, collapse = ", "))
    )
    validation_results$issues <- c(validation_results$issues,
                                   "Missing analysis results")
  } else {
    validation_results$checks$structure <- list(
      passed = TRUE,
      message = "All expected results present"
    )
  }
  
  # Check 2: Data validation
  if("load_data" %in% names(results)) {
    data_validation <- results$load_data$validation_report
    
    validation_results$checks$data_quality <- list(
      passed = TRUE,
      message = sprintf("Loaded %d companies with %d holdings",
                       data_validation$structure$unique_companies,
                       data_validation$structure$total_rows)
    )
  }
  
  # Check 3: Score validation
  if("calculate_daii" %in% names(results)) {
    daii_scores <- results$calculate_daii$daii_data$DAII_3.5_Score
    
    score_stats <- list(
      min = min(daii_scores, na.rm = TRUE),
      max = max(daii_scores, na.rm = TRUE),
      mean = mean(daii_scores, na.rm = TRUE),
      sd = sd(daii_scores, na.rm = TRUE)
    )
    
    # Check score range (should be 0-100)
    if(score_stats$min >= 0 && score_stats$max <= 100) {
      validation_results$checks$score_range <- list(
        passed = TRUE,
        message = sprintf("Scores in valid range: %.1f to %.1f",
                         score_stats$min, score_stats$max)
      )
    } else {
      validation_results$checks$score_range <- list(
        passed = FALSE,
        message = sprintf("Scores out of range: %.1f to %.1f",
                         score_stats$min, score_stats$max)
      )
      validation_results$issues <- c(validation_results$issues,
                                     "Score range validation failed")
    }
  }
  
  # Check 4: Portfolio validation
  if("integrate_portfolio" %in% names(results)) {
    portfolio_metrics <- results$integrate_portfolio$portfolio_metrics$overall
    
    validation_results$checks$portfolio <- list(
      passed = TRUE,
      message = sprintf("Portfolio DAII: %.1f, Holdings: %d",
                       portfolio_metrics$portfolio_daii,
                       portfolio_metrics$total_holdings)
    )
  }
  
  # Check 5: Output validation
  if("generate_outputs" %in% names(results)) {
    output_dir <- results$generate_outputs$output_directory
    
    if(dir.exists(output_dir)) {
      output_files <- list.files(output_dir, recursive = TRUE)
      
      validation_results$checks$output_files <- list(
        passed = TRUE,
        message = sprintf("Generated %d output files", length(output_files))
      )
    } else {
      validation_results$checks$output_files <- list(
        passed = FALSE,
        message = "Output directory not found"
      )
      validation_results$issues <- c(validation_results$issues,
                                     "Output generation failed")
    }
  }
  
  # Overall status
  passed_checks <- sum(sapply(validation_results$checks, function(x) x$passed))
  total_checks <- length(validation_results$checks)
  
  if(length(validation_results$issues) == 0) {
    validation_results$status <- "passed"
    validation_results$summary <- sprintf("All %d checks passed", total_checks)
  } else {
    validation_results$status <- "failed"
    validation_results$summary <- sprintf("%d/%d checks passed, %d issues",
                                         passed_checks, total_checks,
                                         length(validation_results$issues))
  }
  
  # Save validation results
  validation_file <- file.path(config$output_directory,
                              "00_Reproducibility",
                              "reproducibility_validation.json")
  
  writeLines(jsonlite::toJSON(validation_results, pretty = TRUE),
             validation_file)
  
  cat(sprintf("\n🔍 REPRODUCIBILITY VALIDATION: %s\n", validation_results$status))
  cat(sprintf("   %s\n", validation_results$summary))
  
  if(length(validation_results$issues) > 0) {
    cat("\n⚠️ Issues found:\n")
    for(issue in validation_results$issues) {
      cat(sprintf("   • %s\n", issue))
    }
  }
  
  return(validation_results)
}