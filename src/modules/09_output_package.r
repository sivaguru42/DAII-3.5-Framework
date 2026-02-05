# ============================================================================
# MODULE 8: OUTPUT PACKAGE - Complete Deliverables Generation
# ============================================================================
#
# PURPOSE: Generate final output files and deliverables
#
# OUTPUT CATEGORIES:
# 1. Raw Data Files: Processed data in multiple formats
# 2. Analysis Results: Scores, rankings, quartiles
# 3. Portfolio Reports: Fund-level and portfolio-level analysis
# 4. Validation Reports: Quality assurance documentation
# 5. Executive Summaries: High-level insights and recommendations
# 6. Visualization Packages: Complete set of charts and graphs
#
# FILE FORMATS:
# 1. CSV: For data interchange and analysis
# 2. Excel: For business user consumption
# 3. PDF: For formal reporting and distribution
# 4. HTML: For interactive exploration
# 5. RData: For reproducibility and further analysis
#
# ============================================================================

generate_daii_outputs <- function(daii_results,
                                  portfolio_results,
                                  validation_results,
                                  output_dir = "04_Results") {
  #' Generate Complete DAII 3.5 Output Package
  #' 
  #' @param daii_results DAII scoring results
  #' @param portfolio_results Portfolio integration results
  #' @param validation_results Validation results
  #' @param output_dir Output directory
  #' @return List of generated output files
  
  cat("\n📦 GENERATING COMPLETE DAII 3.5 OUTPUT PACKAGE\n")
  cat(paste(rep("=", 60), collapse = ""), "\n")
  
  # Create output directory structure
  timestamp <- format(Sys.time(), "%Y%m%d_%H%M%S")
  run_dir <- file.path(output_dir, paste0("DAII_3.5_Run_", timestamp))
  
  dirs_to_create <- c(
    "01_Company_Scores",
    "02_Portfolio_Analysis",
    "03_Validation_Reports",
    "04_Executive_Summaries",
    "05_Raw_Data",
    "06_Visualizations"
  )
  
  for(dir_name in dirs_to_create) {
    dir_path <- file.path(run_dir, dir_name)
    if(!dir.exists(dir_path)) {
      dir.create(dir_path, recursive = TRUE, showWarnings = FALSE)
    }
  }
  
  output_files <- list()
  
  # 1. COMPANY-LEVEL SCORES
  cat("\n1️⃣ GENERATING COMPANY-LEVEL SCORE FILES\n")
  company_outputs <- generate_company_score_outputs(
    daii_results$daii_data,
    file.path(run_dir, "01_Company_Scores")
  )
  output_files$company_scores <- company_outputs
  
  # 2. PORTFOLIO ANALYSIS REPORTS
  cat("\n2️⃣ GENERATING PORTFOLIO ANALYSIS REPORTS\n")
  portfolio_outputs <- generate_portfolio_outputs(
    portfolio_results,
    file.path(run_dir, "02_Portfolio_Analysis")
  )
  output_files$portfolio_reports <- portfolio_outputs
  
  # 3. VALIDATION REPORTS
  cat("\n3️⃣ GENERATING VALIDATION REPORTS\n")
  validation_outputs <- generate_validation_outputs(
    validation_results,
    file.path(run_dir, "03_Validation_Reports")
  )
  output_files$validation_reports <- validation_outputs
  
  # 4. EXECUTIVE SUMMARIES
  cat("\n4️⃣ GENERATING EXECUTIVE SUMMARIES\n")
  executive_outputs <- generate_executive_summaries(
    daii_results,
    portfolio_results,
    validation_results,
    file.path(run_dir, "04_Executive_Summaries")
  )
  output_files$executive_summaries <- executive_outputs
  
  # 5. RAW DATA EXPORTS
  cat("\n5️⃣ EXPORTING RAW DATA FILES\n")
  raw_data_outputs <- export_raw_data(
    daii_results,
    portfolio_results,
    file.path(run_dir, "05_Raw_Data")
  )
  output_files$raw_data <- raw_data_outputs
  
  # 6. CREATE OUTPUT SUMMARY
  cat("\n6️⃣ CREATING OUTPUT SUMMARY\n")
  output_summary <- create_output_summary(
    output_files,
    run_dir
  )
  
  # 7. CREATE MASTER INDEX FILE
  create_master_index(run_dir, output_files, timestamp)
  
  cat(sprintf("\n✅ Output package generated: %s\n", run_dir))
  cat(sprintf("   Total files created: %d\n", length(unlist(output_files))))
  
  return(list(
    output_directory = run_dir,
    output_files = output_files,
    output_summary = output_summary,
    timestamp = timestamp
  ))
}

generate_company_score_outputs <- function(daii_data, output_dir) {
  #' Generate Company-Level Score Outputs
  #' 
  #' @param daii_data Company-level DAII data
  #' @param output_dir Output directory
  #' @return List of generated files
  
  files <- list()
  
  # 1. Complete Company Scores (CSV)
  file_name <- "daii_3.5_company_scores_complete.csv"
  file_path <- file.path(output_dir, file_name)
  
  # Select and order columns
  score_cols <- c(
    "Ticker", "Company.Name", "GICS.Ind.Grp.Name",
    "DAII_3.5_Score", "DAII_Quartile",
    "R_D_Score", "Analyst_Score", "Patent_Score", "News_Score", "Growth_Score",
    "R.D.Exp_num", "Mkt.Cap_num", "BEst.Analyst.Rtg_num",
    "Patents...Trademarks...Copy.Rgt_num", "News.Sent_num", "Rev...1.Yr.Gr_num"
  )
  
  score_cols <- intersect(score_cols, names(daii_data))
  output_data <- daii_data[, score_cols]
  
  # Sort by DAII score
  output_data <- output_data[order(-output_data$DAII_3.5_Score), ]
  
  write.csv(output_data, file_path, row.names = FALSE)
  files$complete_scores_csv <- file_path
  
  # 2. Top 100 Innovators (CSV)
  file_name <- "daii_3.5_top_100_innovators.csv"
  file_path <- file.path(output_dir, file_name)
  
  top_100 <- output_data %>%
    head(100) %>%
    mutate(Rank = row_number())
  
  write.csv(top_100, file_path, row.names = FALSE)
  files$top_100_csv <- file_path
  
  # 3. Quartile Breakdown (CSV)
  file_name <- "daii_3.5_quartile_breakdown.csv"
  file_path <- file.path(output_dir, file_name)
  
  quartile_summary <- daii_data %>%
    group_by(DAII_Quartile) %>%
    summarise(
      Count = n(),
      Percent = round(100 * n() / nrow(daii_data), 1),
      Avg_DAII = round(mean(DAII_3.5_Score, na.rm = TRUE), 1),
      Min_DAII = round(min(DAII_3.5_Score, na.rm = TRUE), 1),
      Max_DAII = round(max(DAII_3.5_Score, na.rm = TRUE), 1),
      Avg_R_D = round(mean(R_D_Score, na.rm = TRUE), 1),
      Avg_Analyst = round(mean(Analyst_Score, na.rm = TRUE), 1),
      Avg_Patent = round(mean(Patent_Score, na.rm = TRUE), 1),
      Avg_News = round(mean(News_Score, na.rm = TRUE), 1),
      Avg_Growth = round(mean(Growth_Score, na.rm = TRUE), 1),
      .groups = 'drop'
    )
  
  write.csv(quartile_summary, file_path, row.names = FALSE)
  files$quartile_breakdown_csv <- file_path
  
  # 4. Industry Rankings (CSV)
  if("GICS.Ind.Grp.Name" %in% names(daii_data)) {
    file_name <- "daii_3.5_industry_rankings.csv"
    file_path <- file.path(output_dir, file_name)
    
    industry_rankings <- daii_data %>%
      group_by(GICS.Ind.Grp.Name) %>%
      summarise(
        Companies = n(),
        Avg_DAII = round(mean(DAII_3.5_Score, na.rm = TRUE), 1),
        Median_DAII = round(median(DAII_3.5_Score, na.rm = TRUE), 1),
        Min_DAII = round(min(DAII_3.5_Score, na.rm = TRUE), 1),
        Max_DAII = round(max(DAII_3.5_Score, na.rm = TRUE), 1),
        Std_Dev = round(sd(DAII_3.5_Score, na.rm = TRUE), 1),
        Q1_Count = sum(DAII_Quartile == "Q1 (High)", na.rm = TRUE),
        Q1_Percent = round(100 * Q1_Count / n(), 1),
        .groups = 'drop'
      ) %>%
      arrange(desc(Avg_DAII)) %>%
      mutate(Industry_Rank = row_number())
    
    write.csv(industry_rankings, file_path, row.names = FALSE)
    files$industry_rankings_csv <- file_path
  }
  
  # 5. Excel Workbook with Multiple Sheets
  file_name <- "daii_3.5_company_scores.xlsx"
  file_path <- file.path(output_dir, file_name)
  
  wb <- createWorkbook()
  
  # Sheet 1: Complete Scores
  addWorksheet(wb, "All_Companies")
  writeData(wb, "All_Companies", output_data)
  
  # Sheet 2: Top 100
  addWorksheet(wb, "Top_100_Innovators")
  writeData(wb, "Top_100_Innovators", top_100)
  
  # Sheet 3: Quartile Summary
  addWorksheet(wb, "Quartile_Analysis")
  writeData(wb, "Quartile_Analysis", quartile_summary)
  
  # Sheet 4: Industry Analysis
  if(exists("industry_rankings")) {
    addWorksheet(wb, "Industry_Rankings")
    writeData(wb, "Industry_Rankings", industry_rankings)
  }
  
  # Apply formatting
  # Add conditional formatting for quartiles
  quartile_colors <- c("Q1 (High)" = "green", "Q2" = "lightgreen", 
                      "Q3" = "yellow", "Q4 (Low)" = "red")
  
  # Save workbook
  saveWorkbook(wb, file_path, overwrite = TRUE)
  files$excel_workbook <- file_path
  
  return(files)
}

generate_portfolio_outputs <- function(portfolio_results, output_dir) {
  #' Generate Portfolio Analysis Outputs
  #' 
  #' @param portfolio_results Portfolio integration results
  #' @param output_dir Output directory
  #' @return List of generated files
  
  files <- list()
  
  # Extract data
  integrated_data <- portfolio_results$integrated_data
  portfolio_metrics <- portfolio_results$portfolio_metrics
  fund_analysis <- portfolio_results$fund_analysis
  industry_analysis <- portfolio_results$industry_analysis
  portfolio_summary <- portfolio_results$portfolio_summary
  
  # 1. Portfolio Holdings with DAII Scores (CSV)
  file_name <- "portfolio_holdings_with_daii_scores.csv"
  file_path <- file.path(output_dir, file_name)
  
  # Select and order columns
  holding_cols <- c(
    "fund_name", "Ticker", "Company.Name", "fund_weight",
    "DAII_3.5_Score", "DAII_Quartile",
    "R_D_Score", "Analyst_Score", "Patent_Score", "News_Score", "Growth_Score",
    "GICS.Ind.Grp.Name"
  )
  
  holding_cols <- intersect(holding_cols, names(integrated_data))
  portfolio_data <- integrated_data[, holding_cols]
  
  # Sort by fund and DAII score
  portfolio_data <- portfolio_data[order(portfolio_data$fund_name, 
                                        -portfolio_data$DAII_3.5_Score), ]
  
  write.csv(portfolio_data, file_path, row.names = FALSE)
  files$portfolio_holdings_csv <- file_path
  
  # 2. Fund-Level Analysis (CSV)
  if(!is.null(fund_analysis)) {
    file_name <- "fund_level_innovation_analysis.csv"
    file_path <- file.path(output_dir, file_name)
    
    write.csv(fund_analysis, file_path, row.names = FALSE)
    files$fund_analysis_csv <- file_path
  }
  
  # 3. Industry Exposure Analysis (CSV)
  if(!is.null(industry_analysis)) {
    file_name <- "portfolio_industry_innovation_exposure.csv"
    file_path <- file.path(output_dir, file_name)
    
    write.csv(industry_analysis, file_path, row.names = FALSE)
    files$industry_exposure_csv <- file_path
  }
  
  # 4. Portfolio Summary Report (CSV)
  file_name <- "portfolio_innovation_summary.csv"
  file_path <- file.path(output_dir, file_name)
  
  write.csv(portfolio_summary, file_path, row.names = FALSE)
  files$portfolio_summary_csv <- file_path
  
  # 5. Top and Bottom Holdings Report
  file_name <- "portfolio_innovation_extremes.csv"
  file_path <- file.path(output_dir, file_name)
  
  # Top 10 holdings by innovation contribution
  portfolio_data$innovation_contribution <- 
    portfolio_data$fund_weight * portfolio_data$DAII_3.5_Score
  
  top_10 <- portfolio_data %>%
    arrange(desc(innovation_contribution)) %>%
    head(10) %>%
    mutate(Rank = row_number(),
           Contribution_Percent = round(100 * innovation_contribution / 
                                        sum(portfolio_data$innovation_contribution, na.rm = TRUE), 2))
  
  bottom_10 <- portfolio_data %>%
    arrange(innovation_contribution) %>%
    head(10) %>%
    mutate(Rank = row_number(),
           Contribution_Percent = round(100 * innovation_contribution / 
                                        sum(portfolio_data$innovation_contribution, na.rm = TRUE), 2))
  
  extremes_report <- list(
    Top_10_Innovation_Contributors = top_10,
    Bottom_10_Innovation_Contributors = bottom_10
  )
  
  # Write to separate sheets in Excel
  file_name <- "portfolio_innovation_extremes.xlsx"
  file_path <- file.path(output_dir, file_name)
  
  wb <- createWorkbook()
  addWorksheet(wb, "Top_10_Contributors")
  addWorksheet(wb, "Bottom_10_Contributors")
  
  writeData(wb, "Top_10_Contributors", top_10)
  writeData(wb, "Bottom_10_Contributors", bottom_10)
  
  saveWorkbook(wb, file_path, overwrite = TRUE)
  files$extremes_excel <- file_path
  
  # 6. Complete Portfolio Analysis Workbook
  file_name <- "complete_portfolio_analysis.xlsx"
  file_path <- file.path(output_dir, file_name)
  
  wb <- createWorkbook()
  
  # Add sheets
  addWorksheet(wb, "Portfolio_Holdings")
  writeData(wb, "Portfolio_Holdings", portfolio_data)
  
  if(!is.null(fund_analysis)) {
    addWorksheet(wb, "Fund_Analysis")
    writeData(wb, "Fund_Analysis", fund_analysis)
  }
  
  if(!is.null(industry_analysis)) {
    addWorksheet(wb, "Industry_Exposure")
    writeData(wb, "Industry_Exposure", industry_analysis)
  }
  
  addWorksheet(wb, "Portfolio_Summary")
  writeData(wb, "Portfolio_Summary", portfolio_summary)
  
  addWorksheet(wb, "Top_Contributors")
  writeData(wb, "Top_Contributors", top_10)
  
  addWorksheet(wb, "Bottom_Contributors")
  writeData(wb, "Bottom_Contributors", bottom_10)
  
  # Add portfolio metrics sheet
  metrics_df <- data.frame(
    Metric = names(unlist(portfolio_metrics$overall)),
    Value = as.character(unlist(portfolio_metrics$overall))
  )
  
  addWorksheet(wb, "Portfolio_Metrics")
  writeData(wb, "Portfolio_Metrics", metrics_df)
  
  # Save workbook
  saveWorkbook(wb, file_path, overwrite = TRUE)
  files$complete_portfolio_excel <- file_path
  
  return(files)
}

generate_validation_outputs <- function(validation_results, output_dir) {
  #' Generate Validation Reports
  #' 
  #' @param validation_results Validation results
  #' @param output_dir Output directory
  #' @return List of generated files
  
  files <- list()
  
  validation_report <- validation_results$validation_report
  validation_template <- validation_results$validation_template
  validation_summary <- validation_results$validation_summary
  
  # 1. Validation Summary (CSV)
  file_name <- "daii_3.5_validation_summary.csv"
  file_path <- file.path(output_dir, file_name)
  
  write.csv(validation_summary, file_path, row.names = FALSE)
  files$validation_summary_csv <- file_path
  
  # 2. Validation Template (CSV)
  file_name <- "validation_review_template.csv"
  file_path <- file.path(output_dir, file_name)
  
  write.csv(validation_template, file_path, row.names = FALSE)
  files$validation_template_csv <- file_path
  
  # 3. Statistical Validation Report (CSV)
  if(!is.null(validation_report$statistical)) {
    # Distribution statistics
    file_name <- "statistical_validation_distributions.csv"
    file_path <- file.path(output_dir, file_name)
    
    write.csv(validation_report$statistical$distribution, 
              file_path, row.names = FALSE)
    files$statistical_distributions_csv <- file_path
    
    # Correlation matrix
    file_name <- "statistical_validation_correlations.csv"
    file_path <- file.path(output_dir, file_name)
    
    write.csv(as.data.frame(validation_report$statistical$correlation), 
              file_path, row.names = TRUE)
    files$statistical_correlations_csv <- file_path
    
    # Outlier analysis
    outlier_data <- do.call(rbind, lapply(
      names(validation_report$statistical$outliers),
      function(metric) {
        data.frame(
          Metric = metric,
          Outlier_Count = validation_report$statistical$outliers[[metric]]$outlier_count,
          Outlier_Percent = validation_report$statistical$outliers[[metric]]$outlier_percent,
          stringsAsFactors = FALSE
        )
      }
    ))
    
    file_name <- "statistical_validation_outliers.csv"
    file_path <- file.path(output_dir, file_name)
    
    write.csv(outlier_data, file_path, row.names = FALSE)
    files$statistical_outliers_csv <- file_path
  }
  
  # 4. Business Validation Report (CSV)
  if(!is.null(validation_report$business)) {
    # Known innovators
    file_name <- "business_validation_known_innovators.csv"
    file_path <- file.path(output_dir, file_name)
    
    write.csv(validation_report$business$face_validity$known_innovators,
              file_path, row.names = FALSE)
    files$known_innovators_csv <- file_path
    
    # Reasonableness flags
    file_name <- "business_validation_flags.csv"
    file_path <- file.path(output_dir, file_name)
    
    write.csv(validation_report$business$reasonableness_flags$flagged_companies,
              file_path, row.names = FALSE)
    files$reasonableness_flags_csv <- file_path
    
    # Quartile distribution test
    quartile_test <- data.frame(
      Test = "Chi-square quartile distribution",
      Chi_Square = validation_report$business$quartile_distribution$chi_square,
      P_Value = validation_report$business$quartile_distribution$p_value,
      Interpretation = ifelse(
        validation_report$business$quartile_distribution$p_value > 0.05,
        "Quartiles evenly distributed",
        "Quartiles not evenly distributed"
      ),
      stringsAsFactors = FALSE
    )
    
    file_name <- "business_validation_quartile_test.csv"
    file_path <- file.path(output_dir, file_name)
    
    write.csv(quartile_test, file_path, row.names = FALSE)
    files$quartile_test_csv <- file_path
  }
  
  # 5. Process Validation Report (CSV)
  if(!is.null(validation_report$process)) {
    # Imputation summary
    file_name <- "process_validation_imputation_summary.csv"
    file_path <- file.path(output_dir, file_name)
    
    write.csv(validation_report$process$imputation_summary,
              file_path, row.names = FALSE)
    files$imputation_summary_csv <- file_path
    
    # Impact analysis
    file_name <- "process_validation_imputation_impact.csv"
    file_path <- file.path(output_dir, file_name)
    
    write.csv(validation_report$process$impact_analysis,
              file_path, row.names = FALSE)
    files$imputation_impact_csv <- file_path
  }
  
  # 6. Complete Validation Workbook
  file_name <- "complete_validation_report.xlsx"
  file_path <- file.path(output_dir, file_name)
  
  wb <- createWorkbook()
  
  # Summary sheet
  addWorksheet(wb, "Validation_Summary")
  writeData(wb, "Validation_Summary", validation_summary)
  
  # Template sheet
  addWorksheet(wb, "Review_Template")
  writeData(wb, "Review_Template", validation_template)
  
  # Statistical validation sheets
  if(!is.null(validation_report$statistical)) {
    addWorksheet(wb, "Statistical_Distributions")
    writeData(wb, "Statistical_Distributions", 
              validation_report$statistical$distribution)
    
    addWorksheet(wb, "Statistical_Correlations")
    writeData(wb, "Statistical_Correlations", 
              as.data.frame(validation_report$statistical$correlation),
              rowNames = TRUE)
  }
  
  # Business validation sheets
  if(!is.null(validation_report$business)) {
    addWorksheet(wb, "Known_Innovators")
    writeData(wb, "Known_Innovators",
              validation_report$business$face_validity$known_innovators)
    
    addWorksheet(wb, "Reasonableness_Flags")
    writeData(wb, "Reasonableness_Flags",
              validation_report$business$reasonableness_flags$flagged_companies)
  }
  
  # Process validation sheets
  if(!is.null(validation_report$process)) {
    addWorksheet(wb, "Imputation_Summary")
    writeData(wb, "Imputation_Summary",
              validation_report$process$imputation_summary)
    
    addWorksheet(wb, "Imputation_Impact")
    writeData(wb, "Imputation_Impact",
              validation_report$process$impact_analysis)
  }
  
  saveWorkbook(wb, file_path, overwrite = TRUE)
  files$complete_validation_excel <- file_path
  
  return(files)
}

generate_executive_summaries <- function(daii_results,
                                         portfolio_results,
                                         validation_results,
                                         output_dir) {
  #' Generate Executive Summary Reports
  #' 
  #' @param daii_results DAII scoring results
  #' @param portfolio_results Portfolio integration results
  #' @param validation_results Validation results
  #' @param output_dir Output directory
  #' @return List of generated files
  
  files <- list()
  
  # 1. Executive Summary (Text Report)
  file_name <- "daii_3.5_executive_summary.txt"
  file_path <- file.path(output_dir, file_name)
  
  summary_text <- create_executive_summary_text(
    daii_results,
    portfolio_results,
    validation_results
  )
  
  writeLines(summary_text, file_path)
  files$executive_summary_txt <- file_path
  
  # 2. Key Findings (CSV)
  file_name <- "daii_3.5_key_findings.csv"
  file_path <- file.path(output_dir, file_name)
  
  key_findings <- create_key_findings_table(
    daii_results,
    portfolio_results,
    validation_results
  )
  
  write.csv(key_findings, file_path, row.names = FALSE)
  files$key_findings_csv <- file_path
  
  # 3. Recommendations Report (CSV)
  file_name <- "daii_3.5_recommendations.csv"
  file_path <- file.path(output_dir, file_name)
  
  recommendations <- create_recommendations_table(
    daii_results,
    portfolio_results
  )
  
  write.csv(recommendations, file_path, row.names = FALSE)
  files$recommendations_csv <- file_path
  
  # 4. Executive Dashboard (HTML)
  file_name <- "daii_3.5_executive_dashboard.html"
  file_path <- file.path(output_dir, file_name)
  
  dashboard_html <- create_executive_dashboard(
    daii_results,
    portfolio_results,
    validation_results
  )
  
  writeLines(dashboard_html, file_path)
  files$executive_dashboard_html <- file_path
  
  # 5. Presentation Slides (HTML)
  file_name <- "daii_3.5_presentation.html"
  file_path <- file.path(output_dir, file_name)
  
  presentation_html <- create_presentation_html(
    daii_results,
    portfolio_results
  )
  
  writeLines(presentation_html, file_path)
  files$presentation_html <- file_path
  
  return(files)
}

create_executive_summary_text <- function(daii_results,
                                          portfolio_results,
                                          validation_results) {
  #' Create Executive Summary Text Report
  
  summary_text <- paste(
    "========================================================================",
    "                    DAII 3.5 - EXECUTIVE SUMMARY",
    "========================================================================",
    "",
    paste("Report Date:", format(Sys.time(), "%Y-%m-%d %H:%M:%S")),
    "",
    "OVERVIEW",
    "---------",
    paste("Total Companies Analyzed:", nrow(daii_results$daii_data)),
    paste("Portfolio Holdings:", portfolio_results$portfolio_metrics$overall$total_holdings),
    paste("Unique Companies in Portfolio:", portfolio_results$portfolio_metrics$overall$unique_companies),
    "",
    "KEY RESULTS",
    "-----------",
    paste("Portfolio DAII Score (Weighted):", 
          round(portfolio_results$portfolio_metrics$overall$portfolio_daii, 1)),
    paste("Portfolio DAII Score (Equal Weight):", 
          round(portfolio_results$portfolio_metrics$overall$portfolio_daii_equal, 1)),
    paste("DAII Score Range:", portfolio_results$portfolio_metrics$overall$daii_range),
    "",
    "TOP INNOVATORS",
    "--------------",
    sep = "\n"
  )
  
  # Add top 5 companies
  top_5 <- daii_results$daii_data %>%
    arrange(desc(DAII_3.5_Score)) %>%
    head(5)
  
  for(i in 1:nrow(top_5)) {
    summary_text <- paste(summary_text,
                         paste(i, ".", top_5$Ticker[i], 
                               " - ", round(top_5$DAII_3.5_Score[i], 1),
                               " (", top_5$DAII_Quartile[i], ")", sep = ""),
                         sep = "\n")
  }
  
  # Add portfolio insights
  summary_text <- paste(summary_text,
                       "",
                       "PORTFOLIO INSIGHTS",
                       "-----------------",
                       sep = "\n")
  
  if(!is.null(portfolio_results$fund_analysis)) {
    top_fund <- portfolio_results$fund_analysis %>%
      arrange(desc(weighted_daii)) %>%
      head(1)
    
    summary_text <- paste(summary_text,
                         paste("Highest Innovation Fund:", top_fund$fund_name[1]),
                         paste("Fund DAII Score:", round(top_fund$weighted_daii[1], 1)),
                         "",
                         sep = "\n")
  }
  
  # Add validation status
  if(!is.null(validation_results$validation_summary)) {
    overall_status <- validation_results$validation_summary %>%
      filter(Metric == "Validation Status") %>%
      pull(Value)
    
    summary_text <- paste(summary_text,
                         "VALIDATION STATUS",
                         "-----------------",
                         paste("Overall Validation:", overall_status),
                         "",
                         sep = "\n")
  }
  
  # Add recommendations
  summary_text <- paste(summary_text,
                       "RECOMMENDATIONS",
                       "--------------",
                       "1. Review top innovators for potential investment opportunities",
                       "2. Analyze low-scoring holdings for innovation risk",
                       "3. Consider rebalancing to increase innovation exposure",
                       "4. Monitor innovation scores quarterly for trend analysis",
                       "",
                       "========================================================================",
                       sep = "\n")
  
  return(summary_text)
}

create_key_findings_table <- function(daii_results,
                                      portfolio_results,
                                      validation_results) {
  #' Create Key Findings Table
  
  key_findings <- data.frame(
    Category = character(),
    Finding = character(),
    Metric = character(),
    Value = character(),
    Interpretation = character(),
    stringsAsFactors = FALSE
  )
  
  # Overall portfolio innovation
  key_findings <- rbind(key_findings, data.frame(
    Category = "Portfolio Innovation",
    Finding = "Overall Innovation Score",
    Metric = "Weighted DAII",
    Value = as.character(round(portfolio_results$portfolio_metrics$overall$portfolio_daii, 1)),
    Interpretation = ifelse(portfolio_results$portfolio_metrics$overall$portfolio_daii > 50,
                           "Above average innovation", "Below average innovation"),
    stringsAsFactors = FALSE
  ))
  
  # Innovation concentration
  concentration <- portfolio_results$concentration_analysis
  key_findings <- rbind(key_findings, data.frame(
    Category = "Portfolio Innovation",
    Finding = "Innovation Concentration",
    Metric = "HHI Index",
    Value = as.character(round(concentration$hhi_daii, 0)),
    Interpretation = concentration$hhi_interpretation,
    stringsAsFactors = FALSE
  ))
  
  # Top industry
  if(!is.null(portfolio_results$industry_analysis)) {
    top_industry <- portfolio_results$industry_analysis %>%
      arrange(desc(industry_daii)) %>%
      head(1)
    
    key_findings <- rbind(key_findings, data.frame(
      Category = "Industry Analysis",
      Finding = "Most Innovative Industry",
      Metric = "Industry Name",
      Value = top_industry$GICS.Ind.Grp.Name[1],
      Interpretation = paste("Average DAII:", round(top_industry$industry_daii[1], 1)),
      stringsAsFactors = FALSE
    ))
  }
  
  # Validation status
  if(!is.null(validation_results$validation_summary)) {
    validation_status <- validation_results$validation_summary %>%
      filter(Metric == "Validation Status") %>%
      pull(Value)
    
    key_findings <- rbind(key_findings, data.frame(
      Category = "Data Quality",
      Finding = "Overall Validation",
      Metric = "Status",
      Value = validation_status,
      Interpretation = "Data quality and score reliability",
      stringsAsFactors = FALSE
    ))
  }
  
  # Component strengths
  component_strengths <- daii_results$daii_data %>%
    summarise(
      R_D_Strength = mean(R_D_Score, na.rm = TRUE),
      Analyst_Strength = mean(Analyst_Score, na.rm = TRUE),
      Patent_Strength = mean(Patent_Score, na.rm = TRUE)
    )
  
  strongest_component <- which.max(c(
    component_strengths$R_D_Strength,
    component_strengths$Analyst_Strength,
    component_strengths$Patent_Strength
  ))
  
  component_names <- c("R&D Capability", "Analyst Sentiment", "Patent Activity")
  
  key_findings <- rbind(key_findings, data.frame(
    Category = "Component Analysis",
    Finding = "Strongest Innovation Component",
    Metric = "Component",
    Value = component_names[strongest_component],
    Interpretation = "Driving overall innovation scores",
    stringsAsFactors = FALSE
  ))
  
  return(key_findings)
}

create_recommendations_table <- function(daii_results,
                                         portfolio_results) {
  #' Create Recommendations Table
  
  recommendations <- data.frame(
    Priority = character(),
    Recommendation = character(),
    Action = character(),
    Impact = character(),
    Timeline = character(),
    stringsAsFactors = FALSE
  )
  
  # Analyze portfolio for recommendations
  portfolio_data <- portfolio_results$integrated_data
  
  # Recommendation 1: Increase exposure to top innovators
  top_innovators <- portfolio_data %>%
    filter(DAII_Quartile == "Q1 (High)") %>%
    summarise(
      current_weight = sum(fund_weight, na.rm = TRUE),
      count = n()
    )
  
  if(top_innovators$current_weight < 0.25) { # Less than 25% in top quartile
    recommendations <- rbind(recommendations, data.frame(
      Priority = "High",
      Recommendation = "Increase exposure to top innovators",
      Action = "Rebalance portfolio to increase weight in Q1 companies",
      Impact = "Potentially increase portfolio innovation score by 10-15%",
      Timeline = "Next rebalancing cycle",
      stringsAsFactors = FALSE
    ))
  }
  
  # Recommendation 2: Reduce exposure to low innovators
  low_innovators <- portfolio_data %>%
    filter(DAII_Quartile == "Q4 (Low)") %>%
    summarise(
      current_weight = sum(fund_weight, na.rm = TRUE),
      count = n()
    )
  
  if(low_innovators$current_weight > 0.15) { # More than 15% in bottom quartile
    recommendations <- rbind(recommendations, data.frame(
      Priority = "Medium",
      Recommendation = "Reduce exposure to low innovators",
      Action = "Review Q4 holdings for divestment opportunities",
      Impact = "Reduce innovation risk in portfolio",
      Timeline = "6-12 months",
      stringsAsFactors = FALSE
    ))
  }
  
  # Recommendation 3: Diversify innovation sources
  hhi <- portfolio_results$concentration_analysis$hhi_daii
  if(hhi > 1500) { # Moderately concentrated
    recommendations <- rbind(recommendations, data.frame(
      Priority = "Medium",
      Recommendation = "Diversify innovation sources",
      Action = "Spread innovation exposure across more companies/industries",
      Impact = "Reduce concentration risk, improve stability",
      Timeline = "Next investment cycle",
      stringsAsFactors = FALSE
    ))
  }
  
  # Recommendation 4: Monitor specific industries
  if(!is.null(portfolio_results$industry_analysis)) {
    industry_gap <- portfolio_results$industry_analysis %>%
      filter(industry_weight > 0.05) %>% # Industries with >5% weight
      summarise(
        max_daii = max(industry_daii, na.rm = TRUE),
        min_daii = min(industry_daii, na.rm = TRUE),
        gap = max_daii - min_daii
      )
    
    if(industry_gap$gap > 20) { # Large innovation gap between industries
      recommendations <- rbind(recommendations, data.frame(
        Priority = "Low",
        Recommendation = "Address industry innovation gaps",
        Action = "Review industry allocation to balance innovation exposure",
        Impact = "More balanced innovation profile",
        Timeline = "Strategic review",
        stringsAsFactors = FALSE
      ))
    }
  }
  
  # Default recommendation if none generated
  if(nrow(recommendations) == 0) {
    recommendations <- rbind(recommendations, data.frame(
      Priority = "Low",
      Recommendation = "Maintain current innovation strategy",
      Action = "Continue monitoring innovation scores quarterly",
      Impact = "Ongoing innovation tracking",
      Timeline = "Continuous",
      stringsAsFactors = FALSE
    ))
  }
  
  return(recommendations)
}

create_executive_dashboard <- function(daii_results,
                                       portfolio_results,
                                       validation_results) {
  #' Create Executive Dashboard HTML
  
  dashboard_html <- '
  <!DOCTYPE html>
  <html lang="en">
  <head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>DAII 3.5 Executive Dashboard</title>
    <style>
      body {
        font-family: Arial, sans-serif;
        margin: 20px;
        background-color: #f5f5f5;
      }
      .header {
        background: linear-gradient(135deg, #1F4E79 0%, #2E8B57 100%);
        color: white;
        padding: 20px;
        border-radius: 10px;
        margin-bottom: 20px;
      }
      .kpi-container {
        display: grid;
        grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
        gap: 20px;
        margin-bottom: 30px;
      }
      .kpi-card {
        background-color: white;
        padding: 20px;
        border-radius: 8px;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        text-align: center;
      }
      .kpi-value {
        font-size: 36px;
        font-weight: bold;
        margin: 10px 0;
      }
      .kpi-label {
        color: #666;
        font-size: 14px;
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
      .table {
        width: 100%;
        border-collapse: collapse;
      }
      .table th, .table td {
        padding: 10px;
        text-align: left;
        border-bottom: 1px solid #ddd;
      }
      .table th {
        background-color: #f2f2f2;
        font-weight: bold;
      }
      .status-good { color: #2E8B57; }
      .status-warning { color: #FFD700; }
      .status-alert { color: #CD5C5C; }
      .visualization {
        text-align: center;
        margin: 20px 0;
      }
      .visualization img {
        max-width: 100%;
        height: auto;
        border-radius: 5px;
      }
      .timestamp {
        color: #666;
        font-size: 12px;
        margin-top: 20px;
        text-align: center;
      }
    </style>
  </head>
  <body>
    <div class="header">
      <h1>🚀 DAII 3.5 Executive Dashboard</h1>
      <p>Digital Acceleration & Innovation Index Analysis</p>
      <p>Generated: '
  
  dashboard_html <- paste0(dashboard_html, format(Sys.time(), "%Y-%m-%d %H:%M:%S"), '</p>
    </div>')
  
  # KPI Section
  dashboard_html <- paste0(dashboard_html, '
    <div class="kpi-container">
      <div class="kpi-card">
        <div class="kpi-label">Portfolio DAII Score</div>
        <div class="kpi-value">', 
        round(portfolio_results$portfolio_metrics$overall$portfolio_daii, 1), 
        '</div>
        <div>', 
        ifelse(portfolio_results$portfolio_metrics$overall$portfolio_daii > 50,
               '<span class="status-good">Above Average</span>',
               '<span class="status-alert">Below Average</span>'),
        '</div>
      </div>
      
      <div class="kpi-card">
        <div class="kpi-label">Top Quartile Holdings</div>
        <div class="kpi-value">',
        sum(portfolio_results$portfolio_metrics$overall$quartile_distribution["Q1 (High)"]),
        '</div>
        <div>Companies</div>
      </div>
      
      <div class="kpi-card">
        <div class="kpi-label">Innovation Concentration</div>
        <div class="kpi-value">',
        round(portfolio_results$concentration_analysis$hhi_daii, 0),
        '</div>
        <div>', portfolio_results$concentration_analysis$hhi_interpretation, '</div>
      </div>
      
      <div class="kpi-card">
        <div class="kpi-label">Validation Status</div>
        <div class="kpi-value">')
  
  if(!is.null(validation_results$validation_summary)) {
    overall_status <- validation_results$validation_summary %>%
      filter(Metric == "Validation Status") %>%
      pull(Value)
    
    dashboard_html <- paste0(dashboard_html, overall_status)
  } else {
    dashboard_html <- paste0(dashboard_html, "Pending")
  }
  
  dashboard_html <- paste0(dashboard_html, '</div>
        <div>Data Quality</div>
      </div>
    </div>')
  
  # Top Innovators Section
  dashboard_html <- paste0(dashboard_html, '
    <div class="section">
      <h2 class="section-title">🏆 Top 5 Innovators</h2>
      <table class="table">
        <thead>
          <tr>
            <th>Rank</th>
            <th>Ticker</th>
            <th>DAII Score</th>
            <th>Quartile</th>
            <th>R&D Score</th>
            <th>Industry</th>
          </tr>
        </thead>
        <tbody>')
  
  top_5 <- daii_results$daii_data %>%
    arrange(desc(DAII_3.5_Score)) %>%
    head(5)
  
  for(i in 1:nrow(top_5)) {
    dashboard_html <- paste0(dashboard_html, '
          <tr>
            <td>', i, '</td>
            <td><strong>', top_5$Ticker[i], '</strong></td>
            <td>', round(top_5$DAII_3.5_Score[i], 1), '</td>
            <td>', top_5$DAII_Quartile[i], '</td>
            <td>', round(top_5$R_D_Score[i], 1), '</td>
            <td>', 
            ifelse("GICS.Ind.Grp.Name" %in% names(top_5), 
                   top_5$GICS.Ind.Grp.Name[i], "N/A"),
            '</td>
          </tr>')
  }
  
  dashboard_html <- paste0(dashboard_html, '
        </tbody>
      </table>
    </div>')
  
  # Portfolio Insights Section
  dashboard_html <- paste0(dashboard_html, '
    <div class="section">
      <h2 class="section-title">📊 Portfolio Insights</h2>
      <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 20px;">')
  
  # Left column: Portfolio distribution
  dashboard_html <- paste0(dashboard_html, '
        <div>
          <h3>Quartile Distribution</h3>
          <table class="table">
            <thead>
              <tr>
                <th>Quartile</th>
                <th>Count</th>
                <th>% of Portfolio</th>
              </tr>
            </thead>
            <tbody>')
  
  quartile_dist <- portfolio_results$portfolio_metrics$overall$quartile_distribution
  quartile_perc <- portfolio_results$portfolio_metrics$overall$quartile_percentages
  
  for(q in names(quartile_dist)) {
    dashboard_html <- paste0(dashboard_html, '
              <tr>
                <td>', q, '</td>
                <td>', quartile_dist[q], '</td>
                <td>', round(quartile_perc[q], 1), '%</td>
              </tr>')
  }
  
  dashboard_html <- paste0(dashboard_html, '
            </tbody>
          </table>
        </div>')
  
  # Right column: Component contributions
  dashboard_html <- paste0(dashboard_html, '
        <div>
          <h3>Component Contributions</h3>
          <table class="table">
            <thead>
              <tr>
                <th>Component</th>
                <th>Weight</th>
                <th>Avg Score</th>
              </tr>
            </thead>
            <tbody>')
  
  component_weights <- c("R&D" = 0.30, "Analyst" = 0.20, "Patent" = 0.25, 
                        "News" = 0.10, "Growth" = 0.15)
  
  component_scores <- portfolio_results$portfolio_metrics$component_contributions
  
  for(comp in names(component_scores)) {
    comp_name <- gsub("_Score", "", comp)
    comp_name <- gsub("_", " ", comp_name)
    
    dashboard_html <- paste0(dashboard_html, '
              <tr>
                <td>', comp_name, '</td>
                <td>', component_weights[comp_name] * 100, '%</td>
                <td>', round(component_scores[comp], 1), '</td>
              </tr>')
  }
  
  dashboard_html <- paste0(dashboard_html, '
            </tbody>
          </table>
        </div>
      </div>
    </div>')
  
  # Recommendations Section
  dashboard_html <- paste0(dashboard_html, '
    <div class="section">
      <h2 class="section-title">🎯 Key Recommendations</h2>')
  
  recommendations <- create_recommendations_table(daii_results, portfolio_results)
  
  for(i in 1:min(3, nrow(recommendations))) {
    priority_class <- paste0("status-", 
                           tolower(gsub("\\s+", "", recommendations$Priority[i])))
    
    dashboard_html <- paste0(dashboard_html, '
      <div style="margin-bottom: 15px; padding: 15px; background-color: #f9f9f9; border-radius: 5px;">
        <div style="display: flex; justify-content: space-between; align-items: center;">
          <h3 style="margin: 0;">', recommendations$Recommendation[i], '</h3>
          <span class="', priority_class, '" style="font-weight: bold;">',
          recommendations$Priority[i], '</span>
        </div>
        <p><strong>Action:</strong> ', recommendations$Action[i], '</p>
        <p><strong>Impact:</strong> ', recommendations$Impact[i], '</p>
        <p><strong>Timeline:</strong> ', recommendations$Timeline[i], '</p>
      </div>')
  }
  
  dashboard_html <- paste0(dashboard_html, '
    </div>')
  
  # Close HTML
  dashboard_html <- paste0(dashboard_html, '
    <div class="timestamp">
      <p>DAII 3.5 Analysis completed on ', format(Sys.time(), "%Y-%m-%d"), '</p>
      <p>For detailed analysis, refer to the complete output package.</p>
    </div>
  </body>
  </html>')
  
  return(dashboard_html)
}

export_raw_data <- function(daii_results, portfolio_results, output_dir) {
  #' Export Raw Data Files
  #' 
  #' @param daii_results DAII scoring results
  #' @param portfolio_results Portfolio integration results
  #' @param output_dir Output directory
  #' @return List of generated files
  
  files <- list()
  
  # 1. Raw DAII Data (RDS format for reproducibility)
  file_name <- "daii_3.5_raw_data.rds"
  file_path <- file.path(output_dir, file_name)
  
  saveRDS(daii_results, file_path)
  files$raw_rds <- file_path
  
  # 2. Processed Data (CSV)
  file_name <- "daii_3.5_processed_data.csv"
  file_path <- file.path(output_dir, file_name)
  
  write.csv(daii_results$daii_data, file_path, row.names = FALSE)
  files$processed_csv <- file_path
  
  # 3. Portfolio Data (RDS)
  file_name <- "portfolio_analysis_raw.rds"
  file_path <- file.path(output_dir, file_name)
  
  saveRDS(portfolio_results, file_path)
  files$portfolio_rds <- file_path
  
  # 4. Configuration Settings
  file_name <- "daii_3.5_configuration.json"
  file_path <- file.path(output_dir, file_name)
  
  config <- list(
    version = "3.5",
    analysis_date = Sys.time(),
    weights = list(
      r_d = 0.30,
      analyst = 0.20,
      patent = 0.25,
      news = 0.10,
      growth = 0.15
    ),
    normalization_method = "min-max scaling",
    imputation_method = "median/mean with industry adjustment",
    quartile_method = "equal frequency"
  )
  
  writeLines(jsonlite::toJSON(config, pretty = TRUE), file_path)
  files$config_json <- file_path
  
  # 5. Session Information
  file_name <- "session_info.txt"
  file_path <- file.path(output_dir, file_name)
  
  sink(file_path)
  print(sessionInfo())
  sink()
  
  files$session_info <- file_path
  
  return(files)
}

create_output_summary <- function(output_files, run_dir) {
  #' Create Output Summary
  
  summary_df <- data.frame(
    File_Type = character(),
    File_Name = character(),
    Size_KB = numeric(),
    Description = character(),
    stringsAsFactors = FALSE
  )
  
  # Process all files
  for(category in names(output_files)) {
    for(file_type in names(output_files[[category]])) {
      file_path <- output_files[[category]][[file_type]]
      
      if(file.exists(file_path)) {
        file_size <- round(file.info(file_path)$size / 1024, 1)  # KB
        
        # Get relative path
        rel_path <- gsub(paste0(run_dir, "/"), "", file_path)
        
        # Create description
        description <- switch(category,
          "company_scores" = "Company-level DAII scores and rankings",
          "portfolio_reports" = "Portfolio analysis and holdings",
          "validation_reports" = "Validation and quality assurance",
          "executive_summaries" = "Executive summaries and dashboards",
          "raw_data" = "Raw data and configuration files",
          "Other outputs"
        )
        
        summary_df <- rbind(summary_df, data.frame(
          File_Type = category,
          File_Name = rel_path,
          Size_KB = file_size,
          Description = description,
          stringsAsFactors = FALSE
        ))
      }
    }
  }
  
  # Save summary
  summary_path <- file.path(run_dir, "output_summary.csv")
  write.csv(summary_df, summary_path, row.names = FALSE)
  
  # Create summary report
  summary_report <- paste(
    "=========================================",
    "       DAII 3.5 OUTPUT SUMMARY",
    "=========================================",
    "",
    paste("Output Directory:", run_dir),
    paste("Total Files Generated:", nrow(summary_df)),
    paste("Total Size:", round(sum(summary_df$Size_KB) / 1024, 1), "MB"),
    "",
    "FILE CATEGORIES:",
    "----------------",
    sep = "\n"
  )
  
  # Add category summaries
  category_summary <- summary_df %>%
    group_by(File_Type) %>%
    summarise(
      File_Count = n(),
      Total_Size_KB = sum(Size_KB)
    )
  
  for(i in 1:nrow(category_summary)) {
    summary_report <- paste(summary_report,
                           paste(category_summary$File_Type[i], ":",
                                 category_summary$File_Count[i], "files,",
                                 round(category_summary$Total_Size_KB[i] / 1024, 1), "MB"),
                           sep = "\n")
  }
  
  # Add largest files
  summary_report <- paste(summary_report,
                         "",
                         "LARGEST FILES:",
                         "-------------",
                         sep = "\n")
  
  largest_files <- summary_df %>%
    arrange(desc(Size_KB)) %>%
    head(5)
  
  for(i in 1:nrow(largest_files)) {
    summary_report <- paste(summary_report,
                           paste(i, ".", largest_files$File_Name[i],
                                 "(", round(largest_files$Size_KB[i] / 1024, 1), "MB)"),
                           sep = "\n")
  }
  
  # Write summary report
  report_path <- file.path(run_dir, "output_summary_report.txt")
  writeLines(summary_report, report_path)
  
  return(list(
    summary_data = summary_df,
    category_summary = category_summary,
    report_path = report_path
  ))
}

create_master_index <- function(run_dir, output_files, timestamp) {
  #' Create Master Index HTML File
  
  index_html <- '
  <!DOCTYPE html>
  <html lang="en">
  <head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>DAII 3.5 Output Package Index</title>
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
      .file-list {
        list-style-type: none;
        padding: 0;
      }
      .file-list li {
        padding: 10px;
        border-bottom: 1px solid #eee;
        display: flex;
        justify-content: space-between;
        align-items: center;
      }
      .file-list li:hover {
        background-color: #f9f9f9;
      }
      .file-link {
        color: #1F4E79;
        text-decoration: none;
        font-weight: bold;
      }
      .file-link:hover {
        text-decoration: underline;
      }
      .file-size {
        color: #666;
        font-size: 12px;
      }
      .category-icon {
        font-size: 24px;
        margin-right: 10px;
      }
      .quick-links {
        display: grid;
        grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
        gap: 20px;
        margin-bottom: 30px;
      }
      .quick-link-card {
        background-color: white;
        padding: 20px;
        border-radius: 8px;
        text-align: center;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        transition: transform 0.2s;
      }
      .quick-link-card:hover {
        transform: translateY(-5px);
        box-shadow: 0 4px 8px rgba(0,0,0,0.2);
      }
      .quick-link-card a {
        text-decoration: none;
        color: #1F4E79;
        font-weight: bold;
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
      <h1>📦 DAII 3.5 Output Package</h1>
      <p>Complete analysis results and deliverables</p>
      <p><strong>Run ID:</strong> '
  
  index_html <- paste0(index_html, timestamp, '</p>
      <p><strong>Generated:</strong> ', format(Sys.time(), "%Y-%m-%d %H:%M:%S"), '</p>
    </div>')
  
  # Quick Links
  index_html <- paste0(index_html, '
    <div class="quick-links">
      <div class="quick-link-card">
        <span class="category-icon">🏢</span>
        <h3><a href="01_Company_Scores/">Company Scores</a></h3>
        <p>Individual company DAII scores and rankings</p>
      </div>
      <div class="quick-link-card">
        <span class="category-icon">🏦</span>
        <h3><a href="02_Portfolio_Analysis/">Portfolio Analysis</a></h3>
        <p>Portfolio-level innovation metrics and insights</p>
      </div>
      <div class="quick-link-card">
        <span class="category-icon">🔍</span>
        <h3><a href="03_Validation_Reports/">Validation Reports</a></h3>
        <p>Data quality and validation results</p>
      </div>
      <div class="quick-link-card">
        <span class="category-icon">📈</span>
        <h3><a href="04_Executive_Summaries/">Executive Summaries</a></h3>
        <p>High-level insights and recommendations</p>
      </div>
    </div>')
  
  # File Index by Category
  for(category in names(output_files)) {
    category_name <- gsub("_", " ", category)
    category_name <- paste0(toupper(substr(category_name, 1, 1)), 
                           substr(category_name, 2, nchar(category_name)))
    
    category_icon <- switch(category,
      "company_scores" = "🏢",
      "portfolio_reports" = "🏦",
      "validation_reports" = "🔍",
      "executive_summaries" = "📈",
      "raw_data" = "💾",
      "📁"
    )
    
    index_html <- paste0(index_html, '
    <div class="section">
      <h2 class="section-title">', category_icon, ' ', category_name, '</h2>
      <ul class="file-list">')
    
    for(file_type in names(output_files[[category]])) {
      file_path <- output_files[[category]][[file_type]]
      rel_path <- gsub(paste0(run_dir, "/"), "", file_path)
      file_name <- basename(file_path)
      
      if(file.exists(file_path)) {
        file_size <- round(file.info(file_path)$size / 1024, 1)
        
        index_html <- paste0(index_html, '
        <li>
          <a href="', rel_path, '" class="file-link">', file_name, '</a>
          <span class="file-size">', file_size, ' KB</span>
        </li>')
      }
    }
    
    index_html <- paste0(index_html, '
      </ul>
    </div>')
  }
  
  # Instructions
  index_html <- paste0(index_html, '
    <div class="section">
      <h2 class="section-title">📋 How to Use This Package</h2>
      <h3>For Investment Analysts:</h3>
      <ul>
        <li>Review <strong>Company Scores</strong> for individual company innovation metrics</li>
        <li>Analyze <strong>Portfolio Analysis</strong> for fund-level insights</li>
        <li>Check <strong>Top Innovators</strong> for investment opportunities</li>
      </ul>
      
      <h3>For Portfolio Managers:</h3>
      <ul>
        <li>Examine <strong>Portfolio Summary</strong> for overall innovation health</li>
        <li>Review <strong>Recommendations</strong> for portfolio adjustments</li>
        <li>Monitor <strong>Industry Exposure</strong> for sector allocation</li>
      </ul>
      
      <h3>For Risk Management:</h3>
      <ul>
        <li>Check <strong>Validation Reports</strong> for data quality</li>
        <li>Review <strong>Concentration Analysis</strong> for risk assessment</li>
        <li>Monitor <strong>Low Innovators</strong> for potential divestment</li>
      </ul>
    </div>')
  
  # Close HTML
  index_html <- paste0(index_html, '
    <div class="timestamp">
      <p>DAII 3.5 Analysis Framework v3.5</p>
      <p>For questions or support, contact the Innovation Analytics Team</p>
    </div>
  </body>
  </html>')
  
  # Write index file
  index_path <- file.path(run_dir, "index.html")
  writeLines(index_html, index_path)
  
  return(index_path)
}