# KernelStats

**The core of your analysis.** Free, open-source statistical analysis platform.

[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/licenses/MIT)
[![R](https://img.shields.io/badge/R-≥4.1-276DC3?logo=r)](https://www.r-project.org/)
[![Shiny](https://img.shields.io/badge/Shiny-app-blue?logo=rstudio)](https://shiny.posit.co/)

---

## What is KernelStats?

KernelStats is a free, open-source statistical analysis platform that does everything Minitab ($1,700/yr), JMP ($1,785/yr), and SAS ($8,700/yr) do -- plus features none of them have. Built in R Shiny by a publishing statistician, not a software company.

**Try it now:** [kernelstats.com](https://kernelstats.com)

---

## Features

### Variable Doctor
Intelligent diagnostics for every column in your dataset. Detects 15 types of data quality issues with one-click fixes and undo.

### Complete EDA
Univariate, bivariate, and multivariate analysis (PCA, MCA, FAMD, K-means, PAM/Gower, hierarchical clustering).

### 20+ Hypothesis Tests
Parametric and nonparametric tests with a Test Finder wizard, normality tests, and power analysis.

### Regression & GLMs
10 model families with automatic assumption checking (Shapiro-Wilk, Breusch-Pagan, Durbin-Watson, VIF).

### Time Series & GARCH
ARIMA, GARCH(1,1), GJR-GARCH, EGARCH with volatility plots, news impact curves, and forecasting.

### Survival Analysis
Kaplan-Meier, Cox PH, hazard ratio forest plots, concordance index, Schoenfeld tests.

### Machine Learning Pipeline
7 algorithms, auto-detect classification vs regression, model comparison, ROC curves, variable importance.

### Industry Templates
SPC control charts (Cp/Cpk), RFM segmentation, Pareto charts, heatmaps, volcano plots. 15 sample datasets.

### Professional Reports
Word (.docx) or HTML reports with customizable sections, data overview, diagnostics, and visualizations.

### Education Layer
Plain-English interpretations for every test result. Built to teach, not just compute.

---

## Comparison

| Feature | KernelStats | Minitab | JMP | SAS |
|---------|:-----------:|:-------:|:---:|:---:|
| Price | **Free** | $1,700/yr | $1,785/yr | $8,700/yr |
| Open source | Yes | No | No | No |
| Variable Doctor | Yes | No | No | No |
| Test Finder wizard | Yes | No | No | No |
| GARCH models | Yes | No | No | Yes |
| ML pipeline | Yes | No | Yes | Yes |
| Survival analysis | Yes | No | Yes | Yes |
| Plain-English explanations | Yes | No | No | No |
| Runs in browser | Yes | No | No | No |

---

## Quick Start

### Option 1: Use online
Visit [kernelstats.com](https://kernelstats.com)

### Option 2: Run locally
```r
source("install_packages.R")
shiny::runApp("app.R")
```

---

## Requirements

- R >= 4.1
- Core: shiny, bslib, DT, ggplot2, dplyr, tidyr, readr, readxl, stringr, purrr, scales
- Optional: forecast, tseries, rugarch, survival, survminer, rpart, ranger, xgboost, e1071, pROC, rmarkdown
- See `install_packages.R` for the complete list

---

## Built By

**[Anjana Yatawara, Ph.D.](https://www.yatawara.com)**
Assistant Professor of Mathematics & Statistics, California State University, Bakersfield

---

## License

MIT License

---

*KernelStats: The core of your analysis.*
