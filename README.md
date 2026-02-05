# DAII 3.5 Framework

## Innovation Scoring Framework for Portfolio Analysis

**Status**: Phase 1 Complete (Data Pipeline Validated) | Ready for N200 Scaling

### Quick Start
1. Run the consolidated pipeline: `source("src/consolidated/DAII_3.5_Phase1_Complete_Codebase.R")`
2. For modular testing: See individual modules in `src/modules/`

### Project Structure
- `data/`: Raw and processed datasets
- `src/`: Source code (modules, consolidated scripts, utilities)
- `config/`: Configuration files
- `docs/`: Documentation and continuity packages
- `outputs/": Generated files (not tracked in Git)

### Critical Coding Standard
**NEVER use `+` for string concatenation in R* Always use `paste0()`:
```r
# WRONG: cat("\n" + "text")
# CORRECT: cat(paste0("\n", "text"))
```

### Current Phase
Phase 1 (Data Pipeline):  **COMPLETE AND VALIDATED**
Phase 2 (ML Modules):  **READY TO START**


## File Structure (After Renaming)

### Consolidated Scripts
- `src/consolidated/DAII_3.5_Phase1_Complete_Codebase.R` - **Primary script** (Modules 0-9)
- `src/archive/` - Older versions for reference

### Core Modules
- `src/modules/01_data_pipeline.r` - Module 1: Data ingestion and preparation
- `src/modules/02_imputation_engine.r` - Module 2: Missing data imputation  
- `src/modules/03_scoring_engine.r` - Module 3: DAII component scoring
- `src/modules/04_aggregation_framework.r` - Module 4: Portfolio aggregation
- `src/modules/05_portfolio_integration.r` - Module 5: DUMAC integration
- `src/modules/06_validation_system.r` - Module 6: Validation and QA
- `src/modules/07_visualization_suite.r` - Module 7: Reporting visuals
- `src/modules/08_reproducibility.r` - Module 8: Reproducibility package
- `src/modules/09_output_package.r` - Module 9: Final output generation

### Testing Suite
- `src/tests/test_module_integration.R` - Integration tests
- `src/tests/test_pipeline_n50.R` - N50 pipeline validation
- `src/tests/test_reassemble_n50.R` - Data reassembly test

### Documentation
- `docs/continuity_packages/2026-01-30_continuity_review.md` - Jan 30 continuity
- `docs/continuity_packages/2026-01-31_continuity_record.md` - Jan 31 continuity

### Data
- `data/raw/` - All raw datasets (N50, N200, samples)
- `data/processed/` - Processed/intermediate data (gitignored)
- `data/sample/` - Sample datasets for testing

## Naming Conventions
- **Modules**: Numbered two-digit prefix (`01_`, `02_`, etc.)
- **Tests**: `test_` prefix with descriptive name
- **Dates**: YYYY-MM-DD format for documentation
- **No spaces**: Use underscores instead of spaces
- **No special chars**: Avoid `:`, `(`, `)`, `&`, etc.
