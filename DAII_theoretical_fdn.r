# ============================================================================
# DAII 3.5 - DUMAC AI Investment Intelligence
# ============================================================================
# 
# THEORETICAL FRAMEWORK:
# ----------------------
# DAII 3.5 measures company innovation across 5 dimensions:
# 
# 1. R&D INTENSITY (30%) - Core innovation investment
#    - Theory: Schumpeterian "Creative Destruction" - Innovation drives growth
#    - Metric: R&D Expense / Market Capitalization
#    - Transformation: Log(x + ε) to handle extreme skewness
#    - Why 30%?: Primary driver of long-term innovation capability
# 
# 2. ANALYST SENTIMENT (20%) - Market perception of innovation
#    - Theory: Efficient Market Hypothesis - Prices reflect available information
#    - Metric: Bloomberg Best Analyst Rating (1-5 scale, normalized to 0-100)
#    - Why 20%?: Reduced from 25% to balance market perception with actual R&D
# 
# 3. PATENT ACTIVITY (25%) - Intellectual property creation
#    - Theory: Endogenous Growth Theory - Knowledge creation drives growth
#    - Metric: Log(Patents + Trademarks + Copyrights + 1)
#    - Transformation: Log to normalize count data
#    - Why 25%?: Increased from 20% to emphasize formal IP protection
# 
# 4. NEWS SENTIMENT (10%) - Media perception of innovation
#    - Theory: Behavioral Finance - Media sentiment influences perceptions
#    - Metric: News sentiment score (normalized to 0-100)
#    - Why 10%?: Reduced from 15% as noisy, short-term signal
# 
# 5. GROWTH MOMENTUM (15%) - Business performance supporting innovation
#    - Theory: Innovation Diffusion Theory - Growth enables further innovation
#    - Metric: Revenue growth (1-year, normalized to 0-100)
#    - Why 15%?: Stable weighting - links innovation to business results
# 
# STATISTICAL PRINCIPLES APPLIED:
# --------------------------------
# 1. Normalization: Min-Max scaling to 0-100 for comparability
# 2. Log Transformation: For highly skewed distributions (R&D, Patents)
# 3. Winsorization: Implicit through normalization (handles extreme values)
# 4. Missing Data Imputation: Median/Mean imputation with tracking
# 5. Weighted Aggregation: Linear combination with sum to 100%
# 6. Quartile Classification: Equal-frequency bins for portfolio construction
# 
# INNOVATION THEORY INTEGRATION:
# ------------------------------
# - Schumpeter (1934): Innovation as new combinations
# - Romer (1990): Endogenous growth through knowledge accumulation
# - Teece (1997): Dynamic capabilities for innovation
# - Christensen (1997): Disruptive innovation theory
# 
# PRACTICAL DESIGN DECISIONS:
# ---------------------------
# 1. Company-level scoring: Innovation is a company attribute, not holding attribute
# 2. Holdings aggregation: Multiple fund holdings of same company get same score
# 3. Portfolio weighting: Market-value weighted innovation score
# 4. Industry benchmarking: Controls for sector innovation patterns
# 5. Validation framework: Combines statistical and business validation
# 
# ============================================================================
