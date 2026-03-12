# =============================================================================
# UTILITY: Load All DAII Configurations
# Version: 1.0 | Date: 2026-03-11
# Description: Loads and merges all configuration YAML files from R/00_config/
# =============================================================================

#' Load all DAII configuration files
#'
#' @param config_dir Character. Path to config directory (default: R/00_config/)
#' @param env Character. Which environment ("development" or "production").
#' @return List containing all merged configuration parameters.
#' @export
load_all_configs <- function(config_dir = NULL, env = "development") {

  if (is.null(config_dir)) {
    config_dir <- here::here("R", "00_config")
  }

  message("📂 Loading all configurations from: ", config_dir)

  # Define config files in order of precedence (later files override earlier)
  config_files <- c(
    file.path(config_dir, "daii_environment.yaml"),
    file.path(config_dir, "daii_database_config.yaml"),
    file.path(config_dir, "daii_field_mapping.yaml"),
    file.path(config_dir, "daii_scoring_config.yaml"),
    file.path(config_dir, "daii_imputation_config.yaml"),
    file.path(config_dir, "daii_validation_config.yaml"),
    file.path(config_dir, "daii_portfolio_config.yaml"),
    file.path(config_dir, "daii_module4_bridge_config.yaml"),
    file.path(config_dir, "daii_viz_config.yaml"),
    file.path(config_dir, "daii_run_config.yaml"),
    file.path(config_dir, "daii_config.yaml")  # Master config (highest precedence)
  )

  # Add environment-specific config if it exists
  if (env == "production") {
    prod_file <- file.path(config_dir, "production_field_mapping.yaml")
    if (file.exists(prod_file)) {
      config_files <- c(config_files, prod_file)
    }
  }

  # Load and merge all configs
  config <- list()
  
  for (config_file in config_files) {
    if (file.exists(config_file)) {
      message("   Loading: ", basename(config_file))
      new_config <- yaml::read_yaml(config_file)
      
      # Deep merge (recursive)
      config <- merge_configs(config, new_config)
    } else {
      message("   ⚠️  Not found: ", basename(config_file))
    }
  }

  # Add environment marker
  config$environment <- env
  
  message("✅ All configurations loaded successfully")
  return(config)
}

#' Deep merge two lists (recursive)
#'
#' @param base Base list
#' @param override Override list
#' @return Merged list
merge_configs <- function(base, override) {
  if (!is.list(base) || !is.list(override)) return(override)
  
  for (name in names(override)) {
    if (name %in% names(base) && is.list(base[[name]]) && is.list(override[[name]])) {
      base[[name]] <- merge_configs(base[[name]], override[[name]])
    } else {
      base[[name]] <- override[[name]]
    }
  }
  return(base)
}

#' Get specific configuration section
#'
#' @param config Full config list from load_all_configs()
#' @param section Dot-separated path (e.g., "scoring.weights")
#' @return Requested section or NULL
get_config <- function(config, section) {
  parts <- strsplit(section, "\\.")[[1]]
  result <- config
  for (part in parts) {
    if (!is.list(result) || !part %in% names(result)) {
      return(NULL)
    }
    result <- result[[part]]
  }
  return(result)
}