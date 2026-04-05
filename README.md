# 🌱 Kernel

**The core of your analysis.** Free, open-source statistical analysis platform.

[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/licenses/MIT)
[![R](https://img.shields.io/badge/R-≥4.1-276DC3?logo=r)](https://www.r-project.org/)
[![Shiny](https://img.shields.io/badge/Shiny-app-blue?logo=rstudio)](https://shiny.posit.co/)

---

## What is Kernel?

Kernel is a free, open-source statistical analysis platform that does everything Minitab ($1,700/yr), JMP ($1,785/yr), and SAS ($8,700/yr) do — plus features none of them have. Built in R Shiny by a publishing statistician, not a software company.

**🔗 Try it now:** [kernelstats.com](https://kernelstats.com)

---

## Features

### 🩺 Variable Doctor
Intelligent diagnostics for every column in your dataset. Detects 15 types of data quality issues — coded missing values, numeric-as-text, dates stored as strings, ID columns, outliers, whitespace problems — with one-click fixes and undo.

### 📊 Complete EDA
Univariate (histogram, density, boxplot, bar chart), bivariate (scatter + fit lines, group comparisons, correlation matrix with significance stars), and multivariate analysis (PCA, MCA, FAMD, K-means, PAM/Gower, hierarchical clustering).

### 🧪 20+ Hypothesis Tests
One-sample, two-sample, and multi-sample tests — parametric and nonparametric. Includes a **Test Finder wizard** that recommends the right test based on your data. Plus normality tests and power analysis with power curves.

### 📈 Regression & GLMs
10 model families: linear, logistic, probit, multinomial, ordinal, Poisson, negative binomial, zero-inflated, hurdle, and bias-reduced. With automatic assumption checking (Shapiro-Wilk, Breusch-Pagan, Durbin-Watson, VIF).

### 📉 Time Series & GARCH
ARIMA (auto-selection), seasonal decomposition, stationarity tests (ADF, KPSS, Phillips-Perron), forecasting with confidence intervals. **GARCH(1,1), GJR-GARCH, and EGARCH** with conditional volatility plots, news impact curves, and volatility forecasting.

### 🫀 Survival Analysis
Kaplan-Meier curves with log-rank tests, Cox proportional hazards regression with hazard ratio forest plots, concordance index, and Schoenfeld PH assumption tests.

### 🤖 Machine Learning Pipeline
7 algorithms (Decision Tree, Random Forest, XGBoost, SVM, KNN, Naive Bayes, Logistic Regression). Auto-detects classification vs. regression. Model comparison dashboard, ROC curves, confusion matrices, variable importance plots.

### 🏭 Industry Templates
**Quality/SPC:** Control charts (I-MR, X-bar R, P, C) with process capability (Cp/Cpk).
**Business:** RFM customer segmentation, Pareto charts.
**Healthcare:** Heatmaps, volcano plots for differential analysis.
**15 built-in sample datasets** for instant exploration.

### 📝 Report Generation
One-click HTML or Markdown reports with data overview, diagnostics, and analysis results. R code export for reproducibility.

### 🎓 Education Layer
Plain-English interpretations for every test. Method descriptions with assumptions and guidance. Built to teach, not just compute.

---

## Comparison

| Feature | Kernel | Minitab | JMP | SAS |
|---------|:------:|:-------:|:---:|:---:|
| Price | **Free** | $1,700/yr | $1,785/yr | $8,700/yr |
| Open source | ✅ | ❌ | ❌ | ❌ |
| Variable Doctor | ✅ | ❌ | ❌ | ❌ |
| Test Finder wizard | ✅ | ❌ | ❌ | ❌ |
| GARCH models | ✅ | ❌ | ❌ | ✅ |
| ML pipeline | ✅ | ❌ | ✅ | ✅ |
| Survival analysis | ✅ | ❌ | ✅ | ✅ |
| Plain-English explanations | ✅ | ❌ | ❌ | ❌ |
| Control charts / SPC | ✅ | ✅ | ✅ | ✅ |
| Report generation | ✅ | ✅ | ✅ | ✅ |
| Runs in browser | ✅ | ❌ | ❌ | ❌ |

---

## Quick Start

### Option 1: Run locally

```r
# Install required packages
source("install_packages.R")

# Run the app
shiny::runApp("app.R")
```

### Option 2: Use online

Visit [kernelstats.com](https://kernelstats.com) — no installation needed.

---

## Requirements

- R ≥ 4.1
- Core packages: shiny, bslib, DT, ggplot2, dplyr, tidyr, readr, readxl, stringr, purrr, scales

Optional packages (app works without them but enables more features):
- Time series: forecast, tseries, rugarch, xts, zoo
- Survival: survival, survminer
- ML: rpart, rpart.plot, ranger, xgboost, e1071, class, pROC, caret
- Reports: rmarkdown
- See `install_packages.R` for the complete list

---

## Project Structure

```
kernel/
├── app.R                  # Main application (4,908 lines)
├── methods_catalog.csv    # 132-method registry
├── report_template.qmd    # Quarto report template
├── install_packages.R     # Package installer
├── README.md
└── LICENSE
```

---

## Built By

**[Anjana Yatawara, Ph.D.](https://www.yatawara.com)**
Assistant Professor of Mathematics & Statistics
California State University, Bakersfield

Research: Financial volatility (GARCH models), environmental justice, AI in education.
Publications in *Environmental Research: Health*, *ACS ES&T Air*, IEEE FIE.
Papers targeting *Journal of Business & Economic Statistics* and *Journal of Forecasting*.

---

## Contributing

Contributions are welcome! Please open an issue or submit a pull request.

Areas where help is especially appreciated:
- Additional statistical methods
- UI/UX improvements
- Documentation and tutorials
- Testing with real-world datasets
- Translations (i18n)

---

## Roadmap

- [x] Phase 1: Variable Doctor
- [x] Phase 2: Bivariate EDA + Correlation
- [x] Phase 3: Model Diagnostics
- [x] Phase 4: Hypothesis Tests + Test Finder
- [x] Phase 5: Education Layer
- [x] Phase 6: Time Series + GARCH
- [x] Phase 7: Survival Analysis
- [x] Phase 8: Report Generation
- [x] Phase 9: ML Pipeline
- [x] Phase 10: Industry Templates
- [ ] Bayesian methods (brms)
- [ ] Mixed-effects models (lme4)
- [ ] Text mining (tidytext)
- [ ] Geospatial analysis (sf + leaflet)
- [ ] **Kernel Pro** — AI-powered commercial version

---

## License

MIT License — free to use, modify, and distribute.

---

*Kernel: The core of your analysis.* 🌱
