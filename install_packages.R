# ══════════════════════════════════════════════════════════════
# Kernel — Package Installer
# Run this script once to install all required and optional packages
# ══════════════════════════════════════════════════════════════

cat("Installing Kernel packages...\n\n")

core <- c("shiny", "bslib", "DT", "ggplot2", "dplyr", "tidyr",
          "readr", "readxl", "stringr", "purrr", "scales")
import <- c("rio", "haven", "janitor")
eda <- c("naniar", "FactoMineR", "factoextra", "ca", "vcd", "vcdExtra")
categorical <- c("DescTools", "epitools")
regression <- c("nnet", "ordinal", "VGAM", "pscl", "brglm2",
                "glmnet", "mgcv", "lme4", "geepack", "MASS")
doe <- c("FrF2", "DoE.base", "rsm", "lhs", "AlgDesign", "skpr", "mixexp")
timeseries <- c("forecast", "tseries", "rugarch", "xts", "zoo")
survival_pkgs <- c("survival", "survminer")
ml <- c("rpart", "rpart.plot", "ranger", "xgboost", "e1071", "class", "pROC", "caret")
reporting <- c("rmarkdown", "knitr")
other <- c("cluster", "poLCA", "survey")

all_packages <- unique(c(core, import, eda, categorical, regression,
                          doe, timeseries, survival_pkgs, ml, reporting, other))
installed <- installed.packages()[, "Package"]
to_install <- setdiff(all_packages, installed)

if (length(to_install) == 0) {
  cat("All", length(all_packages), "packages already installed!\n")
} else {
  cat("Installing", length(to_install), "packages...\n")
  install.packages(to_install, dependencies = TRUE)
  cat("Done!\n")
}
