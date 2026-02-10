DAII 3.5 Pipeline Execution Summary
Generated: 2026-02-09 15:46:00.034674
Version: 3.5.8 (Field Mapping Integrated + Company/Fund Separation)

EXECUTION RESULTS:
- Input rows (fund-level): 589
- Unique companies: 50
- Company-level rows: 50
- Output rows (fund-level with scores): 589
- Component scores calculated: 5/5
- Composite score calculated: YES
- Innovation quartiles assigned: YES

KEY IMPROVEMENTS:
1. Field mapping system integrated
2. Company/fund data separation applied
3. Scoring performed at company level (N=50)
4. Statistics reflect true company distribution
5. Scores correctly joined to fund holdings

DATA STRUCTURE:
- Company dataset: 50 rows × company metrics
- Fund dataset: 589 rows × fund-specific metrics
- Final output: 589 rows (scores repeated per fund holding)

FIELD MAPPING STATUS: APPLIED
COMPANY/FUND SEPARATION: APPLIED

NOTES:
- Statistics in 05_score_statistics.csv are based on 50 companies, not 589 fund holdings
- This fixes the Q1-Q3 clustering issue from previous runs
- Output ready for Modules 4-9 (portfolio construction)

