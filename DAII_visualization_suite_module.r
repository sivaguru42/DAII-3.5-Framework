# ============================================================================
# MODULE 7: VISUALIZATION SUITE - Data Representation & Insights
# ============================================================================
#
# PURPOSE: Generate comprehensive visualizations for DAII 3.5 analysis
#
# VISUALIZATION PHILOSOPHY:
# 1. Clarity: Clear communication of complex information
# 2. Accuracy: Faithful representation of data
# 3. Insight: Reveal patterns and relationships
# 4. Actionability: Support decision-making
#
# ============================================================================

create_daii_visualizations <- function(daii_data,
                                       portfolio_results = NULL,
                                       validation_report = NULL,
                                       output_dir = "05_Visualizations") {
  #' Create Comprehensive DAII 3.5 Visualizations
  #' 
  #' @param daii_data Company-level DAII data
  #' @param portfolio_results Portfolio integration results
  #' @param validation_report Validation results
  #' @param output_dir Output directory for plots
  #' @return List of generated visualizations
  
  cat("\n🎨 CREATING COMPREHENSIVE DAII 3.5 VISUALIZATIONS\n")
  cat(paste(rep("=", 60), collapse = ""), "\n")
  
  # Create output directory
  if(!dir.exists(output_dir)) {
    dir.create(output_dir, showWarnings = FALSE)
  }
  
  visualization_list <- list()
  
  # 1. DISTRIBUTION VISUALIZATIONS
  cat("\n1️⃣ DISTRIBUTION VISUALIZATIONS\n")
  distribution_plots <- create_distribution_visualizations(daii_data, output_dir)
  visualization_list$distributions <- distribution_plots
  
  # 2. RELATIONSHIP VISUALIZATIONS
  cat("\n2️⃣ RELATIONSHIP VISUALIZATIONS\n")
  relationship_plots <- create_relationship_visualizations(daii_data, output_dir)
  visualization_list$relationships <- relationship_plots
  
  # 3. COMPOSITION VISUALIZATIONS
  cat("\n3️⃣ COMPOSITION VISUALIZATIONS\n")
  composition_plots <- create_composition_visualizations(daii_data, output_dir)
  visualization_list$composition <- composition_plots
  
  # 4. PORTFOLIO VISUALIZATIONS (if portfolio data available)
  if(!is.null(portfolio_results)) {
    cat("\n4️⃣ PORTFOLIO VISUALIZATIONS\n")
    portfolio_plots <- create_portfolio_visualizations(
      daii_data, portfolio_results, output_dir
    )
    visualization_list$portfolio <- portfolio_plots
  }
  
  # 5. VALIDATION VISUALIZATIONS (if validation data available)
  if(!is.null(validation_report)) {
    cat("\n5️⃣ VALIDATION VISUALIZATIONS\n")
    validation_plots <- create_validation_visualizations(
      validation_report, output_dir
    )
    visualization_list$validation <- validation_plots
  }
  
  # 6. CREATE VISUALIZATION SUMMARY REPORT
  cat("\n6️⃣ CREATING VISUALIZATION SUMMARY REPORT\n")
  
  summary_report <- create_visualization_summary(
    visualization_list,
    output_dir
  )
  
  cat(sprintf("\n✅ Visualizations saved to: %s/\n", output_dir))
  cat(sprintf("   Total plots generated: %d\n", length(unlist(visualization_list))))
  
  return(list(
    plots = visualization_list,
    summary = summary_report,
    output_directory = output_dir
  ))
}

create_distribution_visualizations <- function(daii_data, output_dir) {
  #' Create Distribution Visualizations
  #' 
  #' @param daii_data Company-level DAII data
  #' @param output_dir Output directory
  #' @return List of distribution plots
  
  plots <- list()
  
  # Define color scheme
  daii_colors <- c(
    "Q1 (High)" = "#2E8B57",  # Sea Green
    "Q2" = "#87CEEB",         # Sky Blue
    "Q3" = "#FFD700",         # Gold
    "Q4 (Low)" = "#CD5C5C"    # Indian Red
  )
  
  # 1. DAII 3.5 Score Distribution (Histogram with Density)
  p1 <- ggplot(daii_data, aes(x = DAII_3.5_Score)) +
    geom_histogram(aes(y = ..density..), 
                   bins = 30, 
                   fill = "#4B9CD3", 
                   alpha = 0.7) +
    geom_density(color = "#1F4E79", size = 1.2) +
    geom_vline(aes(xintercept = mean(DAII_3.5_Score, na.rm = TRUE)),
               color = "#FF6B6B", 
               size = 1, 
               linetype = "dashed") +
    labs(
      title = "Distribution of DAII 3.5 Scores",
      subtitle = "Histogram with Density Curve",
      x = "DAII 3.5 Score",
      y = "Density"
    ) +
    theme_minimal() +
    theme(
      plot.title = element_text(size = 16, face = "bold"),
      plot.subtitle = element_text(size = 12, color = "gray50")
    )
  
  plots$daii_histogram <- p1
  ggsave(file.path(output_dir, "01_daii_distribution.png"), 
         p1, width = 10, height = 6, dpi = 300)
  
  # 2. Component Score Distributions (Faceted)
  component_data <- daii_data %>%
    select(Ticker, R_D_Score, Analyst_Score, Patent_Score, 
           News_Score, Growth_Score) %>%
    pivot_longer(cols = -Ticker, 
                 names_to = "Component", 
                 values_to = "Score")
  
  # Define component names
  component_labels <- c(
    "R_D_Score" = "R&D Score",
    "Analyst_Score" = "Analyst Score", 
    "Patent_Score" = "Patent Score",
    "News_Score" = "News Score",
    "Growth_Score" = "Growth Score"
  )
  
  component_data$Component <- factor(
    component_data$Component,
    levels = names(component_labels),
    labels = component_labels
  )
  
  p2 <- ggplot(component_data, aes(x = Score, fill = Component)) +
    geom_histogram(bins = 25, alpha = 0.7) +
    facet_wrap(~ Component, scales = "free", ncol = 3) +
    scale_fill_brewer(palette = "Set2") +
    labs(
      title = "Distribution of Component Scores",
      subtitle = "All scores normalized to 0-100 scale",
      x = "Score",
      y = "Count"
    ) +
    theme_minimal() +
    theme(
      plot.title = element_text(size = 16, face = "bold"),
      strip.text = element_text(size = 10, face = "bold"),
      legend.position = "none"
    )
  
  plots$component_distributions <- p2
  ggsave(file.path(output_dir, "02_component_distributions.png"), 
         p2, width = 12, height = 8, dpi = 300)
  
  # 3. Box Plot by Quartile
  p3 <- ggplot(daii_data, aes(x = DAII_Quartile, y = DAII_3.5_Score, fill = DAII_Quartile)) +
    geom_boxplot(alpha = 0.8, outlier.color = "#FF6B6B") +
    geom_jitter(width = 0.2, alpha = 0.3, size = 1) +
    scale_fill_manual(values = daii_colors) +
    labs(
      title = "DAII 3.5 Score Distribution by Quartile",
      subtitle = "Box plot with individual company points",
      x = "Innovation Quartile",
      y = "DAII 3.5 Score"
    ) +
    theme_minimal() +
    theme(
      plot.title = element_text(size = 16, face = "bold"),
      axis.text.x = element_text(angle = 45, hjust = 1)
    )
  
  plots$quartile_boxplot <- p3
  ggsave(file.path(output_dir, "03_quartile_boxplot.png"), 
         p3, width = 10, height = 7, dpi = 300)
  
  # 4. Violin Plot of Component Scores by DAII Quartile
  component_quartile_data <- daii_data %>%
    select(Ticker, DAII_Quartile, R_D_Score, Analyst_Score, 
           Patent_Score, News_Score, Growth_Score) %>%
    pivot_longer(cols = c(R_D_Score, Analyst_Score, Patent_Score, 
                         News_Score, Growth_Score),
                 names_to = "Component",
                 values_to = "Score")
  
  component_quartile_data$Component <- factor(
    component_quartile_data$Component,
    levels = c("R_D_Score", "Analyst_Score", "Patent_Score", 
               "News_Score", "Growth_Score"),
    labels = c("R&D", "Analyst", "Patent", "News", "Growth")
  )
  
  p4 <- ggplot(component_quartile_data, 
               aes(x = DAII_Quartile, y = Score, fill = DAII_Quartile)) +
    geom_violin(alpha = 0.7, scale = "width") +
    geom_boxplot(width = 0.1, fill = "white", alpha = 0.5) +
    facet_wrap(~ Component, ncol = 3) +
    scale_fill_manual(values = daii_colors) +
    labs(
      title = "Component Score Distributions by Innovation Quartile",
      subtitle = "Violin plots show density, box plots show quartiles",
      x = "DAII Quartile",
      y = "Component Score"
    ) +
    theme_minimal() +
    theme(
      plot.title = element_text(size = 16, face = "bold"),
      strip.text = element_text(size = 10, face = "bold"),
      axis.text.x = element_text(angle = 45, hjust = 1),
      legend.position = "none"
    )
  
  plots$component_violin <- p4
  ggsave(file.path(output_dir, "04_component_violin.png"), 
         p4, width = 14, height = 10, dpi = 300)
  
  return(plots)
}

create_relationship_visualizations <- function(daii_data, output_dir) {
  #' Create Relationship Visualizations
  #' 
  #' @param daii_data Company-level DAII data
  #' @param output_dir Output directory
  #' @return List of relationship plots
  
  plots <- list()
  
  # 1. Correlation Matrix Heatmap
  correlation_cols <- c("DAII_3.5_Score", "R_D_Score", "Analyst_Score", 
                       "Patent_Score", "News_Score", "Growth_Score")
  
  cor_matrix <- cor(daii_data[, correlation_cols], use = "complete.obs")
  
  # Melt correlation matrix
  cor_melted <- melt(cor_matrix)
  
  p1 <- ggplot(cor_melted, aes(x = Var1, y = Var2, fill = value)) +
    geom_tile(color = "white") +
    geom_text(aes(label = sprintf("%.2f", value)), 
              color = "black", size = 4) +
    scale_fill_gradient2(low = "#CD5C5C", mid = "white", high = "#2E8B57",
                        midpoint = 0, limit = c(-1, 1), 
                        name = "Correlation") +
    labs(
      title = "Correlation Matrix of DAII Components",
      subtitle = "Pearson correlation coefficients",
      x = "",
      y = ""
    ) +
    theme_minimal() +
    theme(
      plot.title = element_text(size = 16, face = "bold"),
      axis.text.x = element_text(angle = 45, hjust = 1),
      legend.position = "right"
    ) +
    coord_fixed()
  
  plots$correlation_heatmap <- p1
  ggsave(file.path(output_dir, "05_correlation_heatmap.png"), 
         p1, width = 10, height = 8, dpi = 300)
  
  # 2. Scatter Plot Matrix
  scatter_data <- daii_data[, correlation_cols]
  colnames(scatter_data) <- c("DAII", "R&D", "Analyst", "Patent", "News", "Growth")
  
  # Create custom panel functions
  panel.cor <- function(x, y, digits = 2, prefix = "", cex.cor, ...) {
    usr <- par("usr"); on.exit(par(usr))
    par(usr = c(0, 1, 0, 1))
    r <- abs(cor(x, y, use = "complete.obs"))
    txt <- format(c(r, 0.123456789), digits = digits)[1]
    txt <- paste0(prefix, txt)
    if(missing(cex.cor)) cex.cor <- 0.8/strwidth(txt)
    text(0.5, 0.5, txt, cex = cex.cor * r)
  }
  
  panel.hist <- function(x, ...) {
    usr <- par("usr"); on.exit(par(usr))
    par(usr = c(usr[1:2], 0, 1.5))
    h <- hist(x, plot = FALSE)
    breaks <- h$breaks; nB <- length(breaks)
    y <- h$counts; y <- y/max(y)
    rect(breaks[-nB], 0, breaks[-1], y, col = "#4B9CD3", ...)
  }
  
  panel.scatter <- function(x, y, ...) {
    points(x, y, pch = 19, col = rgb(0, 0, 0, 0.3), cex = 0.6)
    abline(lm(y ~ x), col = "#FF6B6B", lwd = 2)
  }
  
  # Save scatter plot matrix
  png(file.path(output_dir, "06_scatter_matrix.png"), 
      width = 12, height = 10, units = "in", res = 300)
  pairs(scatter_data,
        upper.panel = panel.cor,
        diag.panel = panel.hist,
        lower.panel = panel.scatter,
        main = "Scatter Plot Matrix of DAII Components")
  dev.off()
  
  plots$scatter_matrix <- "06_scatter_matrix.png"
  
  # 3. R&D vs Analyst Score with DAII Color
  p3 <- ggplot(daii_data, aes(x = R_D_Score, y = Analyst_Score, color = DAII_3.5_Score)) +
    geom_point(size = 3, alpha = 0.7) +
    geom_smooth(method = "lm", se = FALSE, color = "#1F4E79") +
    scale_color_gradient2(low = "#CD5C5C", mid = "#FFD700", high = "#2E8B57",
                         midpoint = median(daii_data$DAII_3.5_Score, na.rm = TRUE),
                         name = "DAII 3.5 Score") +
    labs(
      title = "R&D Score vs Analyst Score",
      subtitle = "Color represents DAII 3.5 Score",
      x = "R&D Score",
      y = "Analyst Score"
    ) +
    theme_minimal() +
    theme(
      plot.title = element_text(size = 16, face = "bold"),
      legend.position = "right"
    )
  
  plots$rd_vs_analyst <- p3
  ggsave(file.path(output_dir, "07_rd_vs_analyst.png"), 
         p3, width = 10, height = 7, dpi = 300)
  
  # 4. Parallel Coordinates Plot (Top 20 companies)
  top_20 <- daii_data %>%
    arrange(desc(DAII_3.5_Score)) %>%
    head(20)
  
  # Prepare data for parallel coordinates
  parallel_data <- top_20 %>%
    select(Ticker, R_D_Score, Analyst_Score, Patent_Score, 
           News_Score, Growth_Score, DAII_3.5_Score) %>%
    mutate(Company = paste0(Ticker, " (", round(DAII_3.5_Score, 1), ")")) %>%
    select(-Ticker, -DAII_3.5_Score)
  
  # Reshape for plotting
  parallel_long <- parallel_data %>%
    pivot_longer(cols = -Company, names_to = "Component", values_to = "Score")
  
  parallel_long$Component <- factor(
    parallel_long$Component,
    levels = c("R_D_Score", "Analyst_Score", "Patent_Score", "News_Score", "Growth_Score"),
    labels = c("R&D", "Analyst", "Patent", "News", "Growth")
  )
  
  p4 <- ggplot(parallel_long, aes(x = Component, y = Score, group = Company, color = Company)) +
    geom_line(size = 1, alpha = 0.7) +
    geom_point(size = 2) +
    scale_color_viridis_d(name = "Company (DAII Score)") +
    labs(
      title = "Parallel Coordinates: Top 20 Innovators",
      subtitle = "Component score profiles",
      x = "Component",
      y = "Score"
    ) +
    theme_minimal() +
    theme(
      plot.title = element_text(size = 16, face = "bold"),
      axis.text.x = element_text(angle = 45, hjust = 1),
      legend.position = "right",
      legend.text = element_text(size = 8)
    )
  
  plots$parallel_coordinates <- p4
  ggsave(file.path(output_dir, "08_parallel_coordinates.png"), 
         p4, width = 12, height = 8, dpi = 300)
  
  return(plots)
}

create_composition_visualizations <- function(daii_data, output_dir) {
  #' Create Composition Visualizations
  #' 
  #' @param daii_data Company-level DAII data
  #' @param output_dir Output directory
  #' @return List of composition plots
  
  plots <- list()
  
  # 1. Pie Chart of Quartile Distribution
  quartile_counts <- as.data.frame(table(daii_data$DAII_Quartile))
  colnames(quartile_counts) <- c("Quartile", "Count")
  
  # Calculate percentages
  quartile_counts$Percentage <- round(100 * quartile_counts$Count / sum(quartile_counts$Count), 1)
  quartile_counts$Label <- paste0(quartile_counts$Quartile, "\n", 
                                 quartile_counts$Count, " (", 
                                 quartile_counts$Percentage, "%)")
  
  p1 <- ggplot(quartile_counts, aes(x = "", y = Count, fill = Quartile)) +
    geom_bar(stat = "identity", width = 1) +
    coord_polar("y", start = 0) +
    geom_text(aes(label = Label), 
              position = position_stack(vjust = 0.5),
              color = "white", size = 4, fontface = "bold") +
    scale_fill_manual(values = c("Q1 (High)" = "#2E8B57",
                                "Q2" = "#87CEEB",
                                "Q3" = "#FFD700",
                                "Q4 (Low)" = "#CD5C5C")) +
    labs(
      title = "Distribution of Companies by Innovation Quartile",
      fill = "Innovation Quartile"
    ) +
    theme_void() +
    theme(
      plot.title = element_text(size = 16, face = "bold", hjust = 0.5),
      legend.position = "bottom"
    )
  
  plots$quartile_pie <- p1
  ggsave(file.path(output_dir, "09_quartile_pie.png"), 
         p1, width = 8, height = 8, dpi = 300)
  
  # 2. Stacked Bar Chart: Component Contribution by Quartile
  component_contributions <- daii_data %>%
    group_by(DAII_Quartile) %>%
    summarise(
      R_D = mean(R_D_Score, na.rm = TRUE) * 0.30,
      Analyst = mean(Analyst_Score, na.rm = TRUE) * 0.20,
      Patent = mean(Patent_Score, na.rm = TRUE) * 0.25,
      News = mean(News_Score, na.rm = TRUE) * 0.10,
      Growth = mean(Growth_Score, na.rm = TRUE) * 0.15,
      .groups = 'drop'
    ) %>%
    pivot_longer(cols = c(R_D, Analyst, Patent, News, Growth),
                 names_to = "Component",
                 values_to = "Contribution")
  
  p2 <- ggplot(component_contributions, 
               aes(x = DAII_Quartile, y = Contribution, fill = Component)) +
    geom_bar(stat = "identity", position = "stack") +
    scale_fill_brewer(palette = "Set2", 
                     labels = c("R&D", "Analyst", "Patent", "News", "Growth")) +
    labs(
      title = "Component Contribution to DAII Score by Quartile",
      subtitle = "Stacked by weighted component contributions",
      x = "Innovation Quartile",
      y = "Weighted Contribution"
    ) +
    theme_minimal() +
    theme(
      plot.title = element_text(size = 16, face = "bold"),
      axis.text.x = element_text(angle = 45, hjust = 1)
    )
  
  plots$component_stacked <- p2
  ggsave(file.path(output_dir, "10_component_stacked.png"), 
         p2, width = 10, height = 7, dpi = 300)
  
  # 3. Radar Chart for Top 5 Companies
  top_5 <- daii_data %>%
    arrange(desc(DAII_3.5_Score)) %>%
    head(5)
  
  # Prepare radar chart data
  radar_data <- top_5 %>%
    select(Ticker, R_D_Score, Analyst_Score, Patent_Score, 
           News_Score, Growth_Score) %>%
    mutate_at(vars(-Ticker), ~./100)  # Normalize to 0-1 for radar chart
  
  # Create radar chart
  png(file.path(output_dir, "11_radar_top5.png"), 
      width = 10, height = 8, units = "in", res = 300)
  
  # Set up radar chart parameters
  opar <- par()
  par(mar = c(1, 2, 2, 1))
  
  # Create empty radar chart
  radarchart(
    rbind(rep(1, 5), rep(0, 5), radar_data[, -1]),
    axistype = 1,
    pcol = c("#2E8B57", "#87CEEB", "#FFD700", "#CD5C5C", "#9370DB"),
    pfcol = c(rgb(46/255, 139/255, 87/255, 0.3),
              rgb(135/255, 206/255, 235/255, 0.3),
              rgb(255/255, 215/255, 0, 0.3),
              rgb(205/255, 92/255, 92/255, 0.3),
              rgb(147/255, 112/255, 219/255, 0.3)),
    plwd = 2,
    plty = 1,
    cglcol = "grey",
    cglty = 1,
    axislabcol = "grey",
    caxislabels = c("0", "25", "50", "75", "100"),
    cglwd = 0.8,
    vlcex = 0.8,
    title = "Radar Chart: Component Scores of Top 5 Innovators"
  )
  
  # Add legend
  legend(
    "topright",
    legend = paste0(radar_data$Ticker, " (", 
                   round(top_5$DAII_3.5_Score, 1), ")"),
    bty = "n",
    pch = 20,
    col = c("#2E8B57", "#87CEEB", "#FFD700", "#CD5C5C", "#9370DB"),
    text.col = "black",
    cex = 0.8,
    pt.cex = 1.5
  )
  
  par(opar)
  dev.off()
  
  plots$radar_top5 <- "11_radar_top5.png"
  
  # 4. Treemap of Industry Distribution
  if("GICS.Ind.Grp.Name" %in% names(daii_data)) {
    industry_summary <- daii_data %>%
      group_by(GICS.Ind.Grp.Name) %>%
      summarise(
        Count = n(),
        Avg_DAII = mean(DAII_3.5_Score, na.rm = TRUE),
        .groups = 'drop'
      ) %>%
      filter(Count >= 3)  # Only show industries with at least 3 companies
    
    if(require(treemap) && nrow(industry_summary) > 0) {
      png(file.path(output_dir, "12_industry_treemap.png"), 
          width = 12, height = 8, units = "in", res = 300)
      
      treemap(
        industry_summary,
        index = "GICS.Ind.Grp.Name",
        vSize = "Count",
        vColor = "Avg_DAII",
        type = "value",
        palette = "RdYlGn",
        title = "Industry Distribution by Company Count (Size) and DAII Score (Color)",
        title.legend = "Average DAII Score",
        fontsize.labels = 10,
        fontcolor.labels = "white",
        bg.labels = 0,
        align.labels = list(c("center", "center")),
        overlap.labels = 0.5,
        inflate.labels = TRUE
      )
      
      dev.off()
      
      plots$industry_treemap <- "12_industry_treemap.png"
    }
  }
  
  return(plots)
}

create_portfolio_visualizations <- function(daii_data, portfolio_results, output_dir) {
  #' Create Portfolio Visualizations
  #' 
  #' @param daii_data Company-level DAII data
  #' @param portfolio_results Portfolio integration results
  #' @param output_dir Output directory
  #' @return List of portfolio plots
  
  plots <- list()
  
  # Extract portfolio data
  integrated_data <- portfolio_results$integrated_data
  portfolio_metrics <- portfolio_results$portfolio_metrics
  
  # 1. Portfolio DAII Distribution (Weighted vs Equal)
  p1 <- ggplot(integrated_data, aes(x = DAII_3.5_Score)) +
    geom_histogram(aes(y = ..density.., weight = fund_weight),
                   bins = 30, fill = "#4B9CD3", alpha = 0.5,
                   position = "identity") +
    geom_density(aes(weight = fund_weight), 
                 color = "#1F4E79", size = 1.2) +
    geom_histogram(aes(y = ..density..), 
                   bins = 30, fill = "#FF6B6B", alpha = 0.3,
                   position = "identity") +
    geom_density(color = "#CD5C5C", size = 1.2, linetype = "dashed") +
    geom_vline(aes(xintercept = portfolio_metrics$overall$portfolio_daii),
               color = "#1F4E79", size = 1, linetype = "solid") +
    geom_vline(aes(xintercept = portfolio_metrics$overall$portfolio_daii_equal),
               color = "#CD5C5C", size = 1, linetype = "dashed") +
    annotate("text", 
             x = portfolio_metrics$overall$portfolio_daii,
             y = 0.02,
             label = paste("Weighted:", round(portfolio_metrics$overall$portfolio_daii, 1)),
             color = "#1F4E79", hjust = -0.1) +
    annotate("text",
             x = portfolio_metrics$overall$portfolio_daii_equal,
             y = 0.02,
             label = paste("Equal:", round(portfolio_metrics$overall$portfolio_daii_equal, 1)),
             color = "#CD5C5C", hjust = 1.1) +
    labs(
      title = "Portfolio DAII Distribution: Weighted vs Equal Weight",
      subtitle = "Solid: Weighted by position size | Dashed: Equal weight",
      x = "DAII 3.5 Score",
      y = "Density"
    ) +
    theme_minimal() +
    theme(
      plot.title = element_text(size = 16, face = "bold")
    )
  
  plots$portfolio_distribution <- p1
  ggsave(file.path(output_dir, "13_portfolio_distribution.png"), 
         p1, width = 10, height = 7, dpi = 300)
  
  # 2. Fund-Level Innovation Comparison
  if(!is.null(portfolio_metrics$fund_level)) {
    fund_data <- portfolio_metrics$fund_level %>%
      arrange(desc(fund_daii)) %>%
      head(10)  # Top 10 funds by DAII
    
    p2 <- ggplot(fund_data, aes(x = reorder(fund_name, fund_daii), y = fund_daii)) +
      geom_bar(stat = "identity", aes(fill = fund_daii), alpha = 0.8) +
      geom_hline(yintercept = portfolio_metrics$overall$portfolio_daii,
                 color = "#FF6B6B", size = 1, linetype = "dashed") +
      geom_text(aes(label = paste0(round(fund_daii, 1), "\n(", 
                                  round(fund_weight * 100, 1), "%)")),
                hjust = -0.1, size = 3) +
      scale_fill_gradient(low = "#FFD700", high = "#2E8B57", 
                         name = "Fund DAII") +
      labs(
        title = "Top 10 Funds by Innovation Score",
        subtitle = paste("Overall portfolio DAII:", 
                        round(portfolio_metrics$overall$portfolio_daii, 1)),
        x = "Fund",
        y = "Weighted DAII Score"
      ) +
      coord_flip(ylim = c(min(fund_data$fund_daii) * 0.9, 
                         max(fund_data$fund_daii) * 1.1)) +
      theme_minimal() +
      theme(
        plot.title = element_text(size = 16, face = "bold"),
        axis.text.y = element_text(size = 9)
      )
    
    plots$fund_comparison <- p2
    ggsave(file.path(output_dir, "14_fund_comparison.png"), 
           p2, width = 12, height = 8, dpi = 300)
  }
  
  # 3. Innovation Contribution Waterfall Chart
  if(!is.null(portfolio_results$industry_analysis)) {
    industry_data <- portfolio_results$industry_analysis %>%
      arrange(desc(innovation_contribution)) %>%
      head(10)  # Top 10 industries by contribution
    
    # Calculate cumulative sum for waterfall
    industry_data <- industry_data %>%
      mutate(
        end = cumsum(contribution_percent),
        start = c(0, head(end, -1)),
        Industry = reorder(GICS.Ind.Grp.Name, contribution_percent)
      )
    
    p3 <- ggplot(industry_data) +
      geom_rect(aes(xmin = as.numeric(Industry) - 0.4,
                   xmax = as.numeric(Industry) + 0.4,
                   ymin = start,
                   ymax = end,
                   fill = innovation_tilt)) +
      geom_text(aes(x = Industry, y = (start + end)/2,
                   label = paste0(GICS.Ind.Grp.Name, "\n",
                                 round(contribution_percent, 1), "%")),
                size = 3, color = "white", fontface = "bold") +
      scale_fill_manual(values = c("Innovation Leader" = "#2E8B57",
                                  "Innovation Lagger" = "#CD5C5C")) +
      labs(
        title = "Top 10 Industries: Innovation Contribution",
        subtitle = "Percentage of portfolio innovation contributed by each industry",
        x = "",
        y = "Cumulative Contribution (%)",
        fill = "Innovation Status"
      ) +
      theme_minimal() +
      theme(
        plot.title = element_text(size = 16, face = "bold"),
        axis.text.x = element_blank(),
        axis.ticks.x = element_blank()
      )
    
    plots$industry_waterfall <- p3
    ggsave(file.path(output_dir, "15_industry_waterfall.png"), 
           p3, width = 12, height = 8, dpi = 300)
  }
  
  # 4. Innovation Concentration Plot (Lorenz Curve)
  # Calculate Lorenz curve for innovation
  sorted_by_daii <- integrated_data[order(integrated_data$DAII_3.5_Score), ]
  sorted_by_daii$cumulative_weight <- cumsum(sorted_by_daii$fund_weight) / 
                                     sum(sorted_by_daii$fund_weight, na.rm = TRUE)
  sorted_by_daii$cumulative_daii <- cumsum(
    sorted_by_daii$DAII_3.5_Score * sorted_by_daii$fund_weight
  ) / sum(sorted_by_daii$DAII_3.5_Score * sorted_by_daii$fund_weight, na.rm = TRUE)
  
  # Perfect equality line
  equality_df <- data.frame(
    x = seq(0, 1, length.out = 100),
    y = seq(0, 1, length.out = 100)
  )
  
  p4 <- ggplot() +
    geom_line(data = equality_df, aes(x = x, y = y), 
              color = "gray50", linetype = "dashed", size = 1) +
    geom_line(data = sorted_by_daii, 
              aes(x = cumulative_weight, y = cumulative_daii),
              color = "#2E8B57", size = 1.5) +
    geom_ribbon(data = sorted_by_daii,
                aes(x = cumulative_weight, 
                    ymin = cumulative_weight, 
                    ymax = cumulative_daii),
                fill = "#2E8B57", alpha = 0.3) +
    labs(
      title = "Lorenz Curve: Innovation Concentration in Portfolio",
      subtitle = paste("Gini Coefficient:", 
                      round(portfolio_results$concentration_analysis$gini_coefficient, 3)),
      x = "Cumulative Proportion of Portfolio (by weight)",
      y = "Cumulative Proportion of Innovation"
    ) +
    theme_minimal() +
    theme(
      plot.title = element_text(size = 16, face = "bold")
    )
  
  plots$lorenz_curve <- p4
  ggsave(file.path(output_dir, "16_lorenz_curve.png"), 
         p4, width = 10, height = 8, dpi = 300)
  
  return(plots)
}

create_validation_visualizations <- function(validation_report, output_dir) {
  #' Create Validation Visualizations
  #' 
  #' @param validation_report Validation results
  #' @param output_dir Output directory
  #' @return List of validation plots
  
  plots <- list()
  
  # 1. Validation Summary Dashboard
  if(!is.null(validation_report$validation_summary)) {
    summary_data <- validation_report$validation_summary
    
    # Create validation status plot
    status_data <- summary_data %>%
      filter(Validation_Category != "OVERALL") %>%
      mutate(
        Status_Simple = case_when(
          grepl("✅", Status) ~ "Pass",
          grepl("🟡", Status) ~ "Warning",
          grepl("⚠️", Status) ~ "Review",
          TRUE ~ "Unknown"
        )
      )
    
    status_counts <- status_data %>%
      group_by(Validation_Category, Status_Simple) %>%
      summarise(Count = n(), .groups = 'drop')
    
    p1 <- ggplot(status_counts, aes(x = Validation_Category, y = Count, fill = Status_Simple)) +
      geom_bar(stat = "identity", position = "stack") +
      scale_fill_manual(values = c("Pass" = "#2E8B57",
                                  "Warning" = "#FFD700",
                                  "Review" = "#CD5C5C",
                                  "Unknown" = "gray50")) +
      geom_text(aes(label = Count), 
                position = position_stack(vjust = 0.5),
                color = "white", fontface = "bold") +
      labs(
        title = "Validation Status by Category",
        subtitle = "Count of validation metrics by status",
        x = "Validation Category",
        y = "Count",
        fill = "Status"
      ) +
      theme_minimal() +
      theme(
        plot.title = element_text(size = 16, face = "bold"),
        axis.text.x = element_text(angle = 45, hjust = 1)
      )
    
    plots$validation_status <- p1
    ggsave(file.path(output_dir, "17_validation_status.png"), 
           p1, width = 10, height = 7, dpi = 300)
  }
  
  # 2. Imputation Impact Visualization
  if(!is.null(validation_report$process)) {
    impact_data <- validation_report$process$impact_analysis
    
    if(nrow(impact_data) == 2) {
      p2 <- ggplot(impact_data, aes(x = Group, y = mean_daii, fill = Group)) +
        geom_bar(stat = "identity", alpha = 0.8) +
        geom_errorbar(aes(ymin = mean_daii - sd_daii,
                         ymax = mean_daii + sd_daii),
                     width = 0.2) +
        geom_text(aes(label = sprintf("%.1f", mean_daii)),
                  vjust = -0.5, fontface = "bold") +
        scale_fill_manual(values = c("Imputed" = "#FFD700",
                                    "Non-Imputed" = "#2E8B57")) +
        labs(
          title = "Imputation Impact on DAII Scores",
          subtitle = "Comparison of companies with and without imputed values",
          x = "Data Group",
          y = "Mean DAII Score (± SD)"
        ) +
        theme_minimal() +
        theme(
          plot.title = element_text(size = 16, face = "bold"),
          legend.position = "none"
        )
      
      plots$imputation_impact <- p2
      ggsave(file.path(output_dir, "18_imputation_impact.png"), 
             p2, width = 8, height = 7, dpi = 300)
    }
  }
  
  # 3. Component Correlation Validation
  if(!is.null(validation_report$statistical)) {
    cor_matrix <- validation_report$statistical$correlation
    
    # Create annotated correlation plot
    cor_melted <- melt(cor_matrix)
    
    p3 <- ggplot(cor_melted, aes(x = Var1, y = Var2, fill = value)) +
      geom_tile(color = "white") +
      geom_text(aes(label = sprintf("%.2f", value)), 
                color = ifelse(abs(cor_melted$value) > 0.5, "white", "black"),
                fontface = ifelse(abs(cor_melted$value) > 0.7, "bold", "plain")) +
      scale_fill_gradient2(low = "#CD5C5C", mid = "white", high = "#2E8B57",
                          midpoint = 0, limit = c(-1, 1),
                          name = "Correlation") +
      labs(
        title = "Validation: Component Correlation Matrix",
        subtitle = "Bold text indicates correlations > |0.7|",
        x = "",
        y = ""
      ) +
      theme_minimal() +
      theme(
        plot.title = element_text(size = 16, face = "bold"),
        axis.text.x = element_text(angle = 45, hjust = 1)
      ) +
      coord_fixed()
    
    plots$correlation_validation <- p3
    ggsave(file.path(output_dir, "19_correlation_validation.png"), 
           p3, width = 10, height = 8, dpi = 300)
  }
  
  # 4. Known Innovators Validation Plot
  if(!is.null(validation_report$business$face_validity)) {
    innovator_data <- validation_report$business$face_validity$known_innovators
    
    p4 <- ggplot(innovator_data, aes(x = reorder(Ticker, DAII_3.5_Score), 
                                      y = DAII_3.5_Score, 
                                      fill = DAII_Quartile)) +
      geom_bar(stat = "identity", alpha = 0.8) +
      geom_hline(yintercept = 50, color = "#FF6B6B", 
                 linetype = "dashed", size = 1) +
      geom_text(aes(label = round(DAII_3.5_Score, 1)), 
                vjust = -0.5, fontface = "bold") +
      scale_fill_manual(values = c("Q1 (High)" = "#2E8B57",
                                  "Q2" = "#87CEEB",
                                  "Q3" = "#FFD700",
                                  "Q4 (Low)" = "#CD5C5C")) +
      labs(
        title = "Known Technology Innovators: DAII Validation",
        subtitle = "Dashed line shows average score (50)",
        x = "Company",
        y = "DAII 3.5 Score",
        fill = "Quartile"
      ) +
      theme_minimal() +
      theme(
        plot.title = element_text(size = 16, face = "bold"),
        axis.text.x = element_text(angle = 45, hjust = 1)
      )
    
    plots$known_innovators <- p4
    ggsave(file.path(output_dir, "20_known_innovators.png"), 
           p4, width = 10, height = 7, dpi = 300)
  }
  
  # 5. Quartile Distribution Validation
  if(!is.null(validation_report$business$quartile_distribution)) {
    quartile_data <- as.data.frame(
      validation_report$business$quartile_distribution$observed
    )
    colnames(quartile_data) <- c("Quartile", "Observed")
    quartile_data$Expected <- validation_report$business$quartile_distribution$expected
    
    # Reshape for plotting
    quartile_long <- quartile_data %>%
      pivot_longer(cols = c(Observed, Expected),
                   names_to = "Type",
                   values_to = "Count")
    
    p5 <- ggplot(quartile_long, aes(x = Quartile, y = Count, fill = Type)) +
      geom_bar(stat = "identity", position = "dodge", alpha = 0.8) +
      geom_text(aes(label = Count), 
                position = position_dodge(width = 0.9),
                vjust = -0.5, fontface = "bold") +
      scale_fill_manual(values = c("Observed" = "#4B9CD3",
                                  "Expected" = "#FFD700")) +
      labs(
        title = "Quartile Distribution Validation",
        subtitle = paste("Chi-square p-value:", 
                        round(validation_report$business$quartile_distribution$p_value, 4)),
        x = "Innovation Quartile",
        y = "Count",
        fill = "Distribution"
      ) +
      theme_minimal() +
      theme(
        plot.title = element_text(size = 16, face = "bold"),
        axis.text.x = element_text(angle = 45, hjust = 1)
      )
    
    plots$quartile_validation <- p5
    ggsave(file.path(output_dir, "21_quartile_validation.png"), 
           p5, width = 10, height = 7, dpi = 300)
  }
  
  return(plots)
}

create_visualization_summary <- function(visualization_list, output_dir) {
  #' Create Visualization Summary Report
  #' 
  #' @param visualization_list List of all visualizations
  #' @param output_dir Output directory
  #' @return Summary report
  
  summary_report <- data.frame(
    Category = character(),
    Visualization_Type = character(),
    File_Name = character(),
    Description = character(),
    stringsAsFactors = FALSE
  )
  
  # Define visualization descriptions
  viz_descriptions <- list(
    "daii_histogram" = "Distribution of DAII 3.5 scores with density curve",
    "component_distributions" = "Histograms of individual component scores",
    "quartile_boxplot" = "Box plot of DAII scores by innovation quartile",
    "component_violin" = "Violin plots of component scores by quartile",
    "correlation_heatmap" = "Heatmap of component correlations",
    "scatter_matrix" = "Scatter plot matrix with correlations",
    "rd_vs_analyst" = "Scatter plot of R&D vs Analyst scores colored by DAII",
    "parallel_coordinates" = "Parallel coordinates plot of top 20 innovators",
    "quartile_pie" = "Pie chart of quartile distribution",
    "component_stacked" = "Stacked bar chart of component contributions by quartile",
    "radar_top5" = "Radar chart of top 5 innovators' component scores",
    "industry_treemap" = "Treemap of industry distribution",
    "portfolio_distribution" = "Portfolio DAII distribution (weighted vs equal)",
    "fund_comparison" = "Bar chart of top 10 funds by innovation score",
    "industry_waterfall" = "Waterfall chart of industry innovation contributions",
    "lorenz_curve" = "Lorenz curve showing innovation concentration",
    "validation_status" = "Dashboard of validation status by category",
    "imputation_impact" = "Bar chart comparing imputed vs non-imputed companies",
    "correlation_validation" = "Annotated correlation matrix for validation",
    "known_innovators" = "Bar chart of known technology innovators",
    "quartile_validation" = "Comparison of observed vs expected quartile distribution"
  )
  
  # Build summary
  for(category in names(visualization_list)) {
    for(viz_name in names(visualization_list[[category]])) {
      if(viz_name %in% names(viz_descriptions)) {
        file_name <- ifelse(
          is.character(visualization_list[[category]][[viz_name]]),
          visualization_list[[category]][[viz_name]],
          paste0(viz_name, ".png")
        )
        
        summary_report <- rbind(summary_report, data.frame(
          Category = category,
          Visualization_Type = viz_name,
          File_Name = file_name,
          Description = viz_descriptions[[viz_name]],
          stringsAsFactors = FALSE
        ))
      }
    }
  }
  
  # Save summary to CSV
  write.csv(summary_report, 
            file.path(output_dir, "visualization_summary.csv"),
            row.names = FALSE)
  
  # Create HTML gallery
  create_html_gallery(visualization_list, output_dir)
  
  return(summary_report)
}

create_html_gallery <- function(visualization_list, output_dir) {
  #' Create HTML Gallery of Visualizations
  #' 
  #' @param visualization_list List of visualizations
  #' @param output_dir Output directory
  
  html_file <- file.path(output_dir, "visualization_gallery.html")
  
  # Start HTML document
  html_content <- '
  <!DOCTYPE html>
  <html lang="en">
  <head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>DAII 3.5 Visualization Gallery</title>
    <style>
      body {
        font-family: Arial, sans-serif;
        margin: 40px;
        background-color: #f5f5f5;
      }
      h1 {
        color: #2c3e50;
        border-bottom: 2px solid #3498db;
        padding-bottom: 10px;
      }
      h2 {
        color: #34495e;
        margin-top: 30px;
        padding: 10px;
        background-color: #ecf0f1;
        border-radius: 5px;
      }
      .gallery {
        display: grid;
        grid-template-columns: repeat(auto-fill, minmax(300px, 1fr));
        gap: 20px;
        margin-top: 20px;
      }
      .viz-card {
        background-color: white;
        border-radius: 8px;
        padding: 15px;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        transition: transform 0.2s;
      }
      .viz-card:hover {
        transform: translateY(-5px);
        box-shadow: 0 4px 8px rgba(0,0,0,0.2);
      }
      .viz-card img {
        width: 100%;
        height: auto;
        border-radius: 4px;
      }
      .viz-title {
        font-weight: bold;
        margin-top: 10px;
        color: #2c3e50;
      }
      .viz-desc {
        font-size: 12px;
        color: #7f8c8d;
        margin-top: 5px;
      }
      .category-section {
        margin-bottom: 40px;
      }
      .timestamp {
        color: #95a5a6;
        font-size: 12px;
        margin-bottom: 20px;
      }
    </style>
  </head>
  <body>
    <h1>📊 DAII 3.5 Visualization Gallery</h1>
    <div class="timestamp">Generated: ' 
  
  html_content <- paste0(html_content, Sys.time(), '</div>')
  
  # Add each category
  for(category in names(visualization_list)) {
    html_content <- paste0(html_content, '
    <div class="category-section">
      <h2>', toupper(category), ' VISUALIZATIONS</h2>
      <div class="gallery">')
    
    for(viz_name in names(visualization_list[[category]])) {
      file_name <- ifelse(
        is.character(visualization_list[[category]][[viz_name]]),
        visualization_list[[category]][[viz_name]],
        paste0(viz_name, ".png")
      )
      
      # Create descriptive title
      viz_title <- gsub("_", " ", viz_name)
      viz_title <- paste0(toupper(substr(viz_title, 1, 1)), 
                         substr(viz_title, 2, nchar(viz_title)))
      
      html_content <- paste0(html_content, '
        <div class="viz-card">
          <img src="', file_name, '" alt="', viz_title, '">
          <div class="viz-title">', viz_title, '</div>
          <div class="viz-desc">', category, ' visualization</div>
        </div>')
    }
    
    html_content <- paste0(html_content, '
      </div>
    </div>')
  }
  
  # Close HTML
  html_content <- paste0(html_content, '
  </body>
  </html>')
  
  # Write to file
  writeLines(html_content, html_file)
  
  cat(sprintf("   HTML gallery created: %s\n", html_file))
}