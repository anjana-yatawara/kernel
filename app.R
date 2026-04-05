suppressPackageStartupMessages({
  library(shiny)
  library(bslib)
  library(DT)
  library(ggplot2)
  library(dplyr)
  library(tidyr)
  library(readr)
  library(readxl)
  library(stringr)
  library(purrr)
  library(scales)
})


locate_app_dir <- function() {
  frames <- sys.frames()
  for (i in rev(seq_along(frames))) {
    frame <- frames[[i]]
    if (!is.null(frame$ofile)) {
      return(dirname(normalizePath(frame$ofile, winslash = "/", mustWork = FALSE)))
    }
  }
  normalizePath(getwd(), winslash = "/", mustWork = FALSE)
}

APP_DIR <- locate_app_dir()

`%||%` <- function(x, y) {
  if (is.null(x) || length(x) == 0 || (length(x) == 1 && is.na(x)) || identical(x, "")) y else x
}

has_pkg <- function(pkg) {
  requireNamespace(pkg, quietly = TRUE)
}

clean_names_base <- function(x) {
  x <- tolower(x)
  x <- gsub("[^a-z0-9]+", "_", x)
  x <- gsub("(^_+|_+$)", "", x)
  make.unique(x, sep = "_")
}

friendly_type <- function(x) {
  if (inherits(x, "ordered")) return("ordered factor")
  if (is.factor(x)) return("factor")
  if (is.character(x)) return("character")
  if (is.logical(x)) return("logical")
  if (inherits(x, "Date")) return("date")
  if (inherits(x, c("POSIXct", "POSIXt"))) return("datetime")
  if (is.integer(x)) return("integer")
  if (is.numeric(x)) return("numeric")
  paste(class(x), collapse = "/")
}

guess_delim_base <- function(path) {
  first_line <- tryCatch(readLines(path, n = 1, warn = FALSE), error = function(e) "")
  if (!length(first_line) || identical(first_line, "")) return(",")
  candidates <- c(",", "\t", ";", "|")
  counts <- vapply(candidates, function(delim) length(strsplit(first_line, delim, fixed = TRUE)[[1]]), numeric(1))
  candidates[which.max(counts)]
}

list_rdata_frames <- function(path) {
  env <- new.env(parent = emptyenv())
  obj_names <- load(path, envir = env)
  df_names <- obj_names[vapply(obj_names, function(nm) {
    obj <- get(nm, envir = env)
    inherits(obj, c("data.frame", "tbl_df", "tbl"))
  }, logical(1))]
  df_names
}

import_data <- function(path, file_name, sheet = NULL, object_name = NULL) {
  ext <- tolower(tools::file_ext(file_name))

  if (has_pkg("rio") && ext %in% c("csv", "tsv", "txt", "xlsx", "xls", "rds", "rdata", "sav", "dta", "sas7bdat", "xpt", "feather", "parquet")) {
    imported <- tryCatch({
      if (ext %in% c("xlsx", "xls") && !is.null(sheet) && nzchar(sheet)) {
        rio::import(path, which = sheet)
      } else {
        rio::import(path)
      }
    }, error = function(e) NULL)

    if (!is.null(imported) && inherits(imported, c("data.frame", "tbl_df", "tbl"))) {
      return(as.data.frame(imported))
    }
  }

  if (ext == "csv") {
    return(as.data.frame(readr::read_csv(path, show_col_types = FALSE, progress = FALSE)))
  }
  if (ext == "tsv") {
    return(as.data.frame(readr::read_tsv(path, show_col_types = FALSE, progress = FALSE)))
  }
  if (ext == "txt") {
    delim <- guess_delim_base(path)
    return(as.data.frame(readr::read_delim(path, delim = delim, show_col_types = FALSE, progress = FALSE)))
  }
  if (ext %in% c("xlsx", "xls")) {
    return(as.data.frame(readxl::read_excel(path, sheet = sheet %||% 1)))
  }
  if (ext == "rds") {
    obj <- readRDS(path)
    if (!inherits(obj, c("data.frame", "tbl_df", "tbl"))) {
      stop("The RDS file does not contain a data frame.")
    }
    return(as.data.frame(obj))
  }
  if (ext == "rdata") {
    env <- new.env(parent = emptyenv())
    obj_names <- load(path, envir = env)
    if (is.null(object_name) || !nzchar(object_name)) {
      df_names <- obj_names[vapply(obj_names, function(nm) {
        obj <- get(nm, envir = env)
        inherits(obj, c("data.frame", "tbl_df", "tbl"))
      }, logical(1))]
      if (!length(df_names)) {
        stop("The RData file does not contain a data frame.")
      }
      object_name <- df_names[[1]]
    }
    obj <- get(object_name, envir = env)
    if (!inherits(obj, c("data.frame", "tbl_df", "tbl"))) {
      stop("The selected RData object is not a data frame.")
    }
    return(as.data.frame(obj))
  }

  stop("Unsupported file extension. Install the 'rio' package for broader import coverage.")
}

prep_data <- function(df, clean_names = TRUE, drop_empty = TRUE) {
  if (!is.data.frame(df)) stop("Imported object is not a data frame.")

  out <- as.data.frame(df, stringsAsFactors = FALSE)

  if (clean_names) {
    names(out) <- clean_names_base(names(out))
  }

  if (drop_empty && ncol(out) > 0) {
    keep_cols <- vapply(out, function(col) {
      vals <- as.character(col)
      any(!(is.na(vals) | trimws(vals) == ""))
    }, logical(1))
    out <- out[, keep_cols, drop = FALSE]
  }

  if (drop_empty && nrow(out) > 0 && ncol(out) > 0) {
    keep_rows <- apply(out, 1, function(row_vals) {
      vals <- as.character(row_vals)
      any(!(is.na(vals) | trimws(vals) == ""))
    })
    out <- out[keep_rows, , drop = FALSE]
  }

  out
}

column_profile <- function(df) {
  if (is.null(df) || !ncol(df)) return(data.frame())

  data.frame(
    variable = names(df),
    type = vapply(df, friendly_type, character(1)),
    missing_n = vapply(df, function(x) sum(is.na(x)), numeric(1)),
    missing_pct = round(vapply(df, function(x) mean(is.na(x)) * 100, numeric(1)), 2),
    distinct_n = vapply(df, function(x) dplyr::n_distinct(x, na.rm = TRUE), numeric(1)),
    zero_n = vapply(df, function(x) if (is.numeric(x)) sum(x == 0, na.rm = TRUE) else NA_real_, numeric(1)),
    min_value = vapply(df, function(x) {
      if (!is.numeric(x) || !any(!is.na(x))) return(NA_character_)
      format(signif(min(x, na.rm = TRUE), 5), scientific = FALSE)
    }, character(1)),
    max_value = vapply(df, function(x) {
      if (!is.numeric(x) || !any(!is.na(x))) return(NA_character_)
      format(signif(max(x, na.rm = TRUE), 5), scientific = FALSE)
    }, character(1)),
    example_or_top_level = vapply(df, function(x) {
      x_no_na <- x[!is.na(x)]
      if (!length(x_no_na)) return(NA_character_)
      if (is.numeric(x)) return(as.character(signif(stats::median(x_no_na), 5)))
      tab <- sort(table(as.character(x_no_na)), decreasing = TRUE)
      paste0(names(tab)[1], " (n=", as.integer(tab[1]), ")")
    }, character(1)),
    row.names = NULL,
    check.names = FALSE
  )
}


# ══════════════════════════════════════════════════════════════
# VARIABLE DOCTOR — Diagnostic Engine
# ══════════════════════════════════════════════════════════════

CODED_MISSING <- c("999", "-999", "9999", "-9999", "99", "-99",
                   ".", "..", "...", "-", "--", "N/A", "n/a", "NA",
                   "na", "NULL", "null", "None", "none", "missing",
                   "Missing", "MISSING", "unknown", "Unknown", "UNK",
                   "not available", "Not Available", "#N/A", "#NA",
                   "NaN", "nan", "inf", "-inf", "")

diagnose_variables <- function(df) {
  if (is.null(df) || ncol(df) == 0 || nrow(df) == 0) {
    return(data.frame(variable = character(), severity = character(), issue = character(),
                      detail = character(), fix_action = character(), fix_id = character(),
                      stringsAsFactors = FALSE))
  }
  findings <- list()
  add <- function(var, sev, iss, det, fa, fid) {
    findings[[length(findings) + 1]] <<- data.frame(
      variable = var, severity = sev, issue = iss, detail = det,
      fix_action = fa, fix_id = fid, stringsAsFactors = FALSE)
  }
  for (nm in names(df)) {
    x <- df[[nm]]; n <- length(x); n_na <- sum(is.na(x)); n_valid <- n - n_na
    x_nona <- x[!is.na(x)]; n_unique <- length(unique(x_nona))

    # Constant column
    if (n_unique <= 1) {
      add(nm, "critical", "Constant column",
          paste0("Only ", n_unique, " unique value(s). No analytical information."),
          "drop_column", paste0("const_", nm)); next
    }
    # Extreme missingness
    pct_na <- round(n_na / n * 100, 1)
    if (pct_na >= 50) {
      add(nm, "critical", "Mostly missing",
          paste0(pct_na, "% missing (", n_na, "/", n, " rows)."),
          "drop_column", paste0("highmiss_", nm))
    } else if (pct_na >= 20) {
      add(nm, "warning", "Notable missingness", paste0(pct_na, "% missing."), "none", paste0("modmiss_", nm))
    }
    # Coded missing (character)
    if (is.character(x) || is.factor(x)) {
      x_chr <- trimws(as.character(x_nona))
      coded <- intersect(x_chr, CODED_MISSING)
      if (length(coded) > 0) {
        counts <- table(x_chr[x_chr %in% coded])
        dp <- paste0('"', names(counts), '" (n=', as.integer(counts), ')')
        add(nm, "critical", "Coded missing values",
            paste0("Sentinel values found: ", paste(dp, collapse = ", ")),
            "recode_na", paste0("coded_", nm))
      }
    }
    # Coded missing (numeric)
    if (is.numeric(x)) {
      sn <- c(999, -999, 9999, -9999, 99, -99)
      found <- sn[sn %in% x_nona]
      if (length(found) > 0) {
        cnts <- sapply(found, function(v) sum(x_nona == v, na.rm = TRUE))
        if (any(cnts >= 3)) {
          fl <- found[cnts >= 3]
          add(nm, "warning", "Possible coded missing (numeric)",
              paste0("Values ", paste(fl, collapse = ", "), " appear frequently."),
              "recode_na_numeric", paste0("numcoded_", nm))
        }
      }
    }
    # Numeric stored as character
    if (is.character(x)) {
      xt <- trimws(x_nona); xt <- xt[!xt %in% CODED_MISSING]
      if (length(xt) > 0) {
        parseable <- suppressWarnings(!is.na(as.numeric(xt)))
        pn <- sum(parseable) / length(xt) * 100
        if (pn >= 90) {
          non_num <- xt[!parseable]
          if (length(non_num) > 0) {
            ex <- paste0('"', head(unique(non_num), 5), '"')
            add(nm, "critical", "Numeric stored as text",
                paste0(round(pn,1), "% numeric. Blockers: ", paste(ex, collapse=", ")),
                "convert_numeric", paste0("numchr_", nm))
          } else {
            add(nm, "critical", "Numeric stored as text",
                "All values are numeric but type is character.",
                "convert_numeric", paste0("numchr_", nm))
          }
        }
      }
    }
    # Date stored as character
    if (is.character(x)) {
      xs <- head(trimws(x_nona), 50)
      xs <- xs[!xs %in% CODED_MISSING & nchar(xs) >= 6]
      if (length(xs) >= 3) {
        pats <- c("^\\d{4}-\\d{2}-\\d{2}", "^\\d{2}/\\d{2}/\\d{4}",
                   "^\\d{2}-\\d{2}-\\d{4}", "^\\d{1,2}/\\d{1,2}/\\d{2,4}")
        for (p in pats) {
          if (sum(grepl(p, xs)) / length(xs) >= 0.8) {
            add(nm, "warning", "Date stored as text",
                paste0("Values like '", xs[grepl(p,xs)][1], "' look like dates."),
                "convert_date", paste0("date_", nm)); break
          }
        }
      }
    }
    # Low-cardinality numeric
    if (is.numeric(x) && !inherits(x, c("Date", "POSIXct")) && n_unique <= 7 && n_valid >= 10) {
      vals <- sort(unique(x_nona))
      add(nm, "info", "Low-cardinality numeric",
          paste0(n_unique, " unique values: ", paste(head(vals,7), collapse=", "), ". Factor?"),
          "convert_factor", paste0("lowcard_", nm))
      if (n_unique == 2 && all(sort(unique(x_nona)) == c(0,1))) {
        add(nm, "info", "Binary 0/1", "Convert to Yes/No factor?",
            "convert_binary_factor", paste0("binary_", nm))
      }
    }
    # ID-like column
    if (is.numeric(x) && n_unique == n_valid && n_valid >= 10) {
      s <- sort(x_nona); d <- diff(s)
      if (all(d >= 0) && all(d <= 2))
        add(nm, "info", "ID-like column", "Monotonic + all unique. Likely a row ID.",
            "drop_column", paste0("id_", nm))
    }
    # Code/ID by name
    if (is.numeric(x) && is.integer(x)) {
      nl <- tolower(nm)
      if (grepl("zip|postal|phone|fips|code|ssn|id$|_id$", nl))
        add(nm, "warning", "Code/ID stored as numeric",
            paste0("Name suggests '", nm, "' is a code, not a measurement."),
            "convert_factor", paste0("code_", nm))
    }
    # Whitespace
    if (is.character(x)) {
      xc <- as.character(x_nona)
      n_ws <- sum(xc != trimws(xc))
      if (n_ws > 0)
        add(nm, "warning", "Whitespace issues",
            paste0(n_ws, " values have leading/trailing spaces."),
            "trim_whitespace", paste0("ws_", nm))
    }
    # High cardinality
    if ((is.character(x) || is.factor(x)) && n_unique > 50)
      add(nm, "warning", "High-cardinality categorical",
          paste0(n_unique, " unique levels. May cause modeling issues."),
          "none", paste0("highcard_", nm))
    # Outliers
    if (is.numeric(x) && n_valid >= 20) {
      q1 <- stats::quantile(x, 0.25, na.rm=TRUE); q3 <- stats::quantile(x, 0.75, na.rm=TRUE)
      iqr <- q3 - q1
      if (iqr > 0) {
        lo <- q1 - 3*iqr; hi <- q3 + 3*iqr
        no <- sum(x_nona < lo | x_nona > hi)
        if (no > 0) add(nm, "info", "Extreme outliers",
                        paste0(no, " values beyond 3x IQR."), "none", paste0("out_", nm))
      }
    }
    # Logical
    if (is.logical(x))
      add(nm, "info", "Logical column", "Convert TRUE/FALSE to factor?",
          "convert_factor", paste0("log_", nm))
  }
  if (length(findings) == 0) return(data.frame(variable=character(), severity=character(),
    issue=character(), detail=character(), fix_action=character(), fix_id=character(), stringsAsFactors=FALSE))
  do.call(rbind, findings)
}

icon_for_fix <- function(action) {
  switch(action, "drop_column"="Drop column", "recode_na"="Fix to NA",
    "recode_na_numeric"="Fix to NA", "convert_numeric"="To numeric",
    "convert_date"="To date", "convert_factor"="To factor",
    "convert_binary_factor"="To Yes/No", "trim_whitespace"="Trim spaces", "Fix")
}

apply_fix <- function(df, variable, action) {
  if (!variable %in% names(df)) return(list(data=NULL, message=paste0("Column '", variable, "' not found.")))
  x <- df[[variable]]
  if (action == "drop_column") { df[[variable]] <- NULL; return(list(data=df, message=paste0("Dropped '", variable, "'."))) }
  if (action == "recode_na") {
    xc <- trimws(as.character(x)); nb <- sum(is.na(x))
    xc[xc %in% CODED_MISSING] <- NA
    rem <- xc[!is.na(xc)]
    if (length(rem) > 0 && all(!is.na(suppressWarnings(as.numeric(rem))))) df[[variable]] <- as.numeric(xc) else df[[variable]] <- xc
    return(list(data=df, message=paste0("'", variable, "': ", sum(is.na(df[[variable]]))-nb, " coded values to NA.")))
  }
  if (action == "recode_na_numeric") {
    sn <- c(999,-999,9999,-9999,99,-99); fd <- sn[sn %in% x[!is.na(x)]]
    nf <- sum(x %in% fd, na.rm=TRUE); df[[variable]][df[[variable]] %in% fd] <- NA
    return(list(data=df, message=paste0("'", variable, "': ", nf, " sentinel values to NA.")))
  }
  if (action == "convert_numeric") {
    xc <- trimws(as.character(x)); xc[xc %in% CODED_MISSING] <- NA
    df[[variable]] <- suppressWarnings(as.numeric(xc))
    return(list(data=df, message=paste0("'", variable, "': Converted to numeric.")))
  }
  if (action == "convert_date") {
    xc <- trimws(as.character(x))
    fmts <- c("%Y-%m-%d","%m/%d/%Y","%m-%d-%Y","%d/%m/%Y","%m/%d/%y","%B %d, %Y","%Y/%m/%d")
    for (fmt in fmts) { att <- as.Date(xc, format=fmt); if (sum(!is.na(att))/sum(!is.na(xc)) > 0.8) { df[[variable]] <- att; break } }
    return(list(data=df, message=paste0("'", variable, "': Converted to Date.")))
  }
  if (action == "convert_factor") { df[[variable]] <- factor(df[[variable]]); return(list(data=df, message=paste0("'", variable, "': To factor."))) }
  if (action == "convert_binary_factor") { df[[variable]] <- factor(df[[variable]], levels=c(0,1), labels=c("No","Yes")); return(list(data=df, message=paste0("'", variable, "': 0/1 to No/Yes."))) }
  if (action == "trim_whitespace") { df[[variable]] <- trimws(as.character(df[[variable]])); return(list(data=df, message=paste0("'", variable, "': Trimmed whitespace."))) }
  list(data=df, message=paste0("'", variable, "': Unknown fix."))
}



package_status <- function() {
  packages <- c(
    "rio", "haven", "janitor", "naniar", "FactoMineR", "factoextra", "ca",
    "vcd", "vcdExtra", "DescTools", "epitools", "nnet", "ordinal", "VGAM",
    "pscl", "brglm2", "glmnet", "mgcv", "lme4", "geepack", "survey",
    "FrF2", "DoE.base", "rsm", "lhs", "AlgDesign", "skpr", "mixexp",
    "cluster", "poLCA",
    "forecast", "tseries", "rugarch", "xts", "zoo",
    "survival", "survminer", "rmarkdown",
    "rpart", "rpart.plot", "ranger", "xgboost", "e1071", "class", "pROC", "caret"
  )
  purpose <- c(
    "Expanded file import",
    "SPSS/Stata/SAS-family import",
    "Name cleaning / data prep",
    "Missing-data visualization",
    "MCA/FAMD/CA alternatives",
    "Enhanced multivariate plots",
    "Correspondence analysis",
    "Mosaic graphics",
    "Advanced categorical graphics / loglinear extensions",
    "Effect sizes and association measures",
    "Odds ratios and risk metrics",
    "Multinomial logistic regression",
    "Ordinal regression",
    "Extended categorical/count model families",
    "Zero-inflated and hurdle models",
    "Bias-reduced logistic regression",
    "Regularized regression",
    "GAM / spline modeling",
    "Mixed-effects models",
    "GEE / correlated outcomes",
    "Survey-weighted models",
    "Fractional factorial designs",
    "Orthogonal arrays / screening designs",
    "Response-surface designs",
    "Latin hypercube designs",
    "Optimal design generation",
    "Custom DOE / power tooling",
    "Mixture-design ecosystem",
    "PAM/Gower clustering",
    "Latent class analysis",
    "ARIMA/ETS forecasting",
    "Stationarity tests (ADF/KPSS)",
    "GARCH/GJR-GARCH/EGARCH modeling",
    "Time series objects",
    "Time series infrastructure",
    "Kaplan-Meier / Cox PH survival models",
    "Publication-quality survival plots",
    "Report rendering (Quarto/RMarkdown)",
    "Decision trees",
    "Decision tree visualization",
    "Random forests (fast)",
    "Gradient boosting (XGBoost)",
    "SVM and Naive Bayes",
    "K-nearest neighbors",
    "ROC curves and AUC",
    "ML training utilities"
  )

  data.frame(
    package = packages,
    purpose = purpose,
    installed = ifelse(vapply(packages, has_pkg, logical(1)), "Yes", "No"),
    stringsAsFactors = FALSE
  )
}

methods_catalog <- function() {
  path <- file.path(APP_DIR, "methods_catalog.csv")
  if (file.exists(path)) {
    return(read.csv(path, stringsAsFactors = FALSE))
  }
  data.frame(
    domain = "Platform",
    method = "Method catalog",
    purpose = "Fallback catalog",
    sas_jmp_analogue = "Method registry",
    r_packages = "base",
    status = "Implemented",
    included_in_app = "Yes",
    stringsAsFactors = FALSE
  )
}

categorical_vars_of <- function(df) {
  names(df)[vapply(df, function(x) is.factor(x) || is.character(x) || is.logical(x) || inherits(x, "ordered"), logical(1))]
}

numeric_vars_of <- function(df) {
  names(df)[vapply(df, is.numeric, logical(1))]
}

coerce_categorical <- function(x) {
  if (is.logical(x)) return(factor(x, levels = c(FALSE, TRUE), labels = c("FALSE", "TRUE")))
  if (inherits(x, "ordered")) return(x)
  if (is.character(x)) return(factor(x))
  if (is.factor(x)) return(x)
  factor(x)
}

safe_complete <- function(df) {
  if (is.null(df) || !ncol(df)) return(df)
  df[stats::complete.cases(df), , drop = FALSE]
}

cramers_v_manual <- function(tbl) {
  chi <- suppressWarnings(chisq.test(tbl, correct = FALSE))
  n <- sum(tbl)
  if (n == 0) return(NA_real_)
  k <- min(nrow(tbl) - 1, ncol(tbl) - 1)
  if (k <= 0) return(NA_real_)
  sqrt(unname(chi$statistic) / (n * k))
}

odds_and_risk <- function(tbl) {
  if (!all(dim(tbl) == c(2, 2))) return(NULL)
  a <- tbl[1, 1]
  b <- tbl[1, 2]
  c <- tbl[2, 1]
  d <- tbl[2, 2]
  or <- ifelse(any(c(b, c) == 0), NA_real_, (a * d) / (b * c))
  rr <- ifelse((a + b) == 0 || (c + d) == 0, NA_real_, (a / (a + b)) / (c / (c + d)))
  data.frame(
    metric = c("Odds ratio", "Relative risk"),
    value = c(or, rr),
    stringsAsFactors = FALSE
  )
}

build_formula_text <- function(outcome, predictors, interactions = FALSE, poly_degree = 1) {
  predictors <- predictors[predictors != outcome]
  if (!length(predictors)) return(paste(outcome, "~ 1"))

  rhs <- paste(predictors, collapse = " + ")

  if (isTRUE(interactions) && length(predictors) > 1) {
    rhs <- paste0("(", rhs, ")^2")
  }

  if (!is.null(poly_degree) && poly_degree > 1 && length(predictors) == 1) {
    rhs <- paste0("poly(", predictors[[1]], ", ", poly_degree, ", raw = TRUE)")
  }

  paste(outcome, "~", rhs)
}

extract_coefficients <- function(fit) {
  if (inherits(fit, c("lm", "glm", "negbin"))) {
    out <- as.data.frame(summary(fit)$coefficients)
    names(out) <- c("estimate", "std_error", "statistic", "p_value")
    out$term <- rownames(out)
    rownames(out) <- NULL
    out$conf_low <- out$estimate - 1.96 * out$std_error
    out$conf_high <- out$estimate + 1.96 * out$std_error
    return(out[, c("term", "estimate", "std_error", "statistic", "p_value", "conf_low", "conf_high")])
  }

  if (inherits(fit, "multinom")) {
    sm <- summary(fit)
    coef_mat <- as.matrix(sm$coefficients)
    se_mat <- as.matrix(sm$standard.errors)
    out <- data.frame(
      class_level = rep(rownames(coef_mat), each = ncol(coef_mat)),
      term = rep(colnames(coef_mat), times = nrow(coef_mat)),
      estimate = as.vector(coef_mat),
      std_error = as.vector(se_mat),
      stringsAsFactors = FALSE
    )
    out$statistic <- out$estimate / out$std_error
    out$p_value <- 2 * pnorm(abs(out$statistic), lower.tail = FALSE)
    out$conf_low <- out$estimate - 1.96 * out$std_error
    out$conf_high <- out$estimate + 1.96 * out$std_error
    return(out)
  }

  if (inherits(fit, "clm")) {
    out <- as.data.frame(summary(fit)$coefficients)
    out$term <- rownames(out)
    rownames(out) <- NULL
    names(out) <- c("estimate", "std_error", "statistic", "p_value", "term")
    out$conf_low <- out$estimate - 1.96 * out$std_error
    out$conf_high <- out$estimate + 1.96 * out$std_error
    return(out[, c("term", "estimate", "std_error", "statistic", "p_value", "conf_low", "conf_high")])
  }

  if (inherits(fit, c("zeroinfl", "hurdle"))) {
    sm <- summary(fit)
    count <- as.data.frame(sm$coefficients$count)
    zero <- as.data.frame(sm$coefficients$zero)
    count$component <- "count"
    count$term <- rownames(count)
    zero$component <- "zero"
    zero$term <- rownames(zero)
    out <- bind_rows(count, zero)
    rownames(out) <- NULL
    names(out)[1:4] <- c("estimate", "std_error", "statistic", "p_value")
    out$conf_low <- out$estimate - 1.96 * out$std_error
    out$conf_high <- out$estimate + 1.96 * out$std_error
    return(out[, c("component", "term", "estimate", "std_error", "statistic", "p_value", "conf_low", "conf_high")])
  }

  data.frame()
}

model_metrics <- function(fit) {
  cls <- class(fit)[1]
  metrics <- data.frame(metric = "Model class", value = cls, stringsAsFactors = FALSE)

  add_metric <- function(name, value) {
    if (is.null(value) || length(value) == 0 || is.na(value)) return(NULL)
    data.frame(metric = name, value = format(signif(as.numeric(value), 5), scientific = FALSE), stringsAsFactors = FALSE)
  }

  n_obs <- tryCatch(stats::nobs(fit), error = function(e) NA_real_)
  metrics <- bind_rows(metrics, add_metric("Observations", n_obs))

  aic_val <- tryCatch(AIC(fit), error = function(e) NA_real_)
  bic_val <- tryCatch(BIC(fit), error = function(e) NA_real_)
  loglik_val <- tryCatch(as.numeric(logLik(fit)), error = function(e) NA_real_)
  metrics <- bind_rows(metrics, add_metric("AIC", aic_val), add_metric("BIC", bic_val), add_metric("LogLik", loglik_val))

  if (inherits(fit, "lm")) {
    sm <- summary(fit)
    metrics <- bind_rows(
      metrics,
      add_metric("R-squared", sm$r.squared),
      add_metric("Adj. R-squared", sm$adj.r.squared),
      add_metric("Sigma", sm$sigma)
    )
  }

  if (inherits(fit, "glm")) {
    pseudo_r2 <- tryCatch(1 - fit$deviance / fit$null.deviance, error = function(e) NA_real_)
    metrics <- bind_rows(
      metrics,
      add_metric("Residual deviance", fit$deviance),
      add_metric("Null deviance", fit$null.deviance),
      add_metric("Pseudo R-squared", pseudo_r2)
    )
  }

  metrics
}

make_method_note <- function(df) {
  if (is.null(df)) {
    return(tags$p("Upload data to unlock analysis tabs."))
  }

  profile <- column_profile(df)
  high_missing <- profile$variable[profile$missing_pct >= 30]
  constant_cols <- profile$variable[profile$distinct_n <= 1]
  dup_rows <- sum(duplicated(df))
  n_num <- length(numeric_vars_of(df))
  n_cat <- length(categorical_vars_of(df))

  items <- list(
    tags$li(paste0("Rows: ", nrow(df), " | Columns: ", ncol(df), " | Numeric: ", n_num, " | Categorical-like: ", n_cat, ".")),
    tags$li(paste0("Duplicate rows detected: ", dup_rows, "."))
  )

  if (length(high_missing)) {
    items <- c(items, tags$li(paste0("High missingness (>=30%): ", paste(high_missing, collapse = ", "), ".")))
  }
  if (length(constant_cols)) {
    items <- c(items, tags$li(paste0("Constant or near-empty columns to review: ", paste(constant_cols, collapse = ", "), ".")))
  }
  if (n_num < 2) {
    items <- c(items, tags$li("At least two numeric variables are needed for PCA and scaled clustering."))
  }
  if (n_cat < 2) {
    items <- c(items, tags$li("At least two categorical variables are needed for the contingency/association tab."))
  }

  tags$ul(items)
}

make_design <- function(type, vars, runs, levels, center_points, randomize) {
  if (type == "Full factorial") {
    vals <- seq(-1, 1, length.out = levels)
    design <- expand.grid(rep(list(vals), length(vars)), KEEP.OUT.ATTRS = FALSE, stringsAsFactors = FALSE)
    names(design) <- vars
    return(design)
  }

  if (type == "Fractional factorial (2-level)") {
    if (!has_pkg("FrF2")) stop("Install 'FrF2' to generate fractional factorial designs.")
    return(as.data.frame(FrF2::FrF2(nruns = runs, nfactors = length(vars), factor.names = vars, randomize = randomize)))
  }

  if (type == "Central composite") {
    if (!has_pkg("rsm")) stop("Install 'rsm' to generate central composite designs.")
    design <- as.data.frame(rsm::ccd(k = length(vars), n0 = c(center_points, center_points), randomize = randomize, coding = FALSE))
    names(design)[seq_along(vars)] <- vars
    return(design)
  }

  if (type == "Box-Behnken") {
    if (!has_pkg("rsm")) stop("Install 'rsm' to generate Box-Behnken designs.")
    if (length(vars) < 3) stop("Box-Behnken designs require at least 3 factors.")
    design <- as.data.frame(rsm::bbd(k = length(vars), n0 = center_points, randomize = randomize, coding = FALSE))
    names(design)[seq_along(vars)] <- vars
    return(design)
  }

  if (type == "Latin hypercube") {
    if (!has_pkg("lhs")) stop("Install 'lhs' to generate Latin hypercube designs.")
    design <- as.data.frame(lhs::randomLHS(runs, length(vars)))
    names(design) <- vars
    return(design)
  }

  if (type == "D-optimal") {
    if (!has_pkg("AlgDesign")) stop("Install 'AlgDesign' to generate D-optimal designs.")
    candidate <- AlgDesign::gen.factorial(levels = levels, nVars = length(vars), factors = "all", varNames = vars)
    formula_text <- paste("~ (", paste(vars, collapse = " + "), ")^2")
    design <- AlgDesign::optFederov(stats::as.formula(formula_text), data = candidate, nTrials = runs)$design
    return(as.data.frame(design))
  }

  stop("Unknown design type.")
}



# ══════════════════════════════════════════════════════════════
# BIVARIATE & MULTIVARIATE HELPER FUNCTIONS
# ══════════════════════════════════════════════════════════════

recommend_test <- function(x_type, y_type, n_groups = 2, paired = FALSE) {
  if (x_type == "numeric" && y_type == "numeric") {
    return(data.frame(
      test = c("Pearson correlation", "Spearman correlation", "Simple linear regression"),
      when_to_use = c("Both variables roughly normal, linear relationship",
                      "Non-normal or ordinal data, monotonic relationship",
                      "Predict Y from X, assess slope significance"),
      stringsAsFactors = FALSE))
  }
  if ((x_type == "numeric" && y_type == "categorical") ||
      (x_type == "categorical" && y_type == "numeric")) {
    if (n_groups == 2) {
      tests <- c("Welch's t-test", "Mann-Whitney U test", "Cohen's d")
      whens <- c("Compare means of two groups (assumes normality)",
                 "Compare two groups without normality assumption",
                 "Effect size: how large is the difference?")
    } else {
      tests <- c("One-way ANOVA", "Kruskal-Wallis test", "Tukey HSD post-hoc", "Eta-squared")
      whens <- c("Compare means across 3+ groups (assumes normality + equal variance)",
                 "Compare 3+ groups without normality assumption",
                 "Which specific pairs differ? (after significant ANOVA)",
                 "Effect size: proportion of variance explained by groups")
    }
    return(data.frame(test = tests, when_to_use = whens, stringsAsFactors = FALSE))
  }
  if (x_type == "categorical" && y_type == "categorical") {
    return(data.frame(
      test = c("Chi-square test", "Fisher exact test", "Cramer's V"),
      when_to_use = c("Test independence (expected counts >= 5)",
                      "Test independence (small samples or sparse cells)",
                      "Effect size for categorical association"),
      stringsAsFactors = FALSE))
  }
  data.frame(test = "Unknown", when_to_use = "Could not determine appropriate test.", stringsAsFactors = FALSE)
}

run_bivariate_test <- function(df, xvar, yvar, test_name) {
  x <- df[[xvar]]; y <- df[[yvar]]
  cc <- stats::complete.cases(x, y); x <- x[cc]; y <- y[cc]
  result <- tryCatch({
    switch(test_name,
      "Pearson correlation" = {
        ct <- cor.test(x, y, method = "pearson")
        data.frame(statistic = c("r", "t-statistic", "p-value", "95% CI lower", "95% CI upper", "n"),
                   value = c(round(ct$estimate, 4), round(ct$statistic, 4), signif(ct$p.value, 4),
                             round(ct$conf.int[1], 4), round(ct$conf.int[2], 4), length(x)),
                   stringsAsFactors = FALSE)
      },
      "Spearman correlation" = {
        ct <- cor.test(x, y, method = "spearman", exact = FALSE)
        data.frame(statistic = c("rho", "S-statistic", "p-value", "n"),
                   value = c(round(ct$estimate, 4), round(ct$statistic, 2), signif(ct$p.value, 4), length(x)),
                   stringsAsFactors = FALSE)
      },
      "Welch's t-test" = {
        tt <- t.test(y ~ x)
        d <- abs(diff(tt$estimate)) / sqrt(mean(c(var(y[x == levels(factor(x))[1]]), var(y[x == levels(factor(x))[2]]))))
        data.frame(statistic = c("t-statistic", "df", "p-value", "Mean group 1", "Mean group 2", "Cohen's d"),
                   value = c(round(tt$statistic, 4), round(tt$parameter, 2), signif(tt$p.value, 4),
                             round(tt$estimate[1], 4), round(tt$estimate[2], 4), round(d, 4)),
                   stringsAsFactors = FALSE)
      },
      "Mann-Whitney U test" = {
        wt <- wilcox.test(y ~ x, exact = FALSE)
        data.frame(statistic = c("W-statistic", "p-value", "n"),
                   value = c(round(wt$statistic, 2), signif(wt$p.value, 4), length(x)),
                   stringsAsFactors = FALSE)
      },
      "One-way ANOVA" = {
        av <- summary(aov(y ~ factor(x)))
        f_val <- av[[1]]$`F value`[1]; p_val <- av[[1]]$`Pr(>F)`[1]
        ss_b <- av[[1]]$`Sum Sq`[1]; ss_t <- sum(av[[1]]$`Sum Sq`)
        eta2 <- ss_b / ss_t
        data.frame(statistic = c("F-statistic", "p-value", "Eta-squared", "df between", "df within"),
                   value = c(round(f_val, 4), signif(p_val, 4), round(eta2, 4),
                             av[[1]]$Df[1], av[[1]]$Df[2]),
                   stringsAsFactors = FALSE)
      },
      "Kruskal-Wallis test" = {
        kt <- kruskal.test(y ~ factor(x))
        data.frame(statistic = c("Chi-squared", "df", "p-value"),
                   value = c(round(kt$statistic, 4), kt$parameter, signif(kt$p.value, 4)),
                   stringsAsFactors = FALSE)
      },
      "Tukey HSD post-hoc" = {
        tk <- TukeyHSD(aov(y ~ factor(x)))
        out <- as.data.frame(tk[[1]])
        out$comparison <- rownames(out); rownames(out) <- NULL
        out[, c("comparison", names(out)[1:4])]
      },
      data.frame(statistic = "Error", value = "Test not implemented", stringsAsFactors = FALSE)
    )
  }, error = function(e) {
    data.frame(statistic = "Error", value = as.character(e$message), stringsAsFactors = FALSE)
  })
  result
}

compute_cor_matrix <- function(df, method = "pearson") {
  nums <- df[, vapply(df, is.numeric, logical(1)), drop = FALSE]
  nums <- nums[, vapply(nums, function(x) stats::sd(x, na.rm = TRUE) > 0, logical(1)), drop = FALSE]
  if (ncol(nums) < 2) return(NULL)
  cor_mat <- cor(nums, use = "pairwise.complete.obs", method = method)
  # P-values
  n <- ncol(nums)
  p_mat <- matrix(NA, n, n, dimnames = list(colnames(nums), colnames(nums)))
  for (i in 1:(n-1)) {
    for (j in (i+1):n) {
      ct <- tryCatch(cor.test(nums[[i]], nums[[j]], method = method), error = function(e) NULL)
      if (!is.null(ct)) { p_mat[i,j] <- ct$p.value; p_mat[j,i] <- ct$p.value }
    }
  }
  list(cor = cor_mat, pval = p_mat, vars = colnames(nums))
}

check_assumptions_lm <- function(fit) {
  res <- stats::residuals(fit); fitted_vals <- stats::fitted(fit)
  checks <- list()
  # Shapiro-Wilk on residuals
  n <- length(res)
  sw <- if (n >= 3 && n <= 5000) tryCatch(shapiro.test(res), error = function(e) NULL) else NULL
  if (!is.null(sw)) {
    checks$normality <- data.frame(test = "Shapiro-Wilk normality", statistic = round(sw$statistic, 4),
                                   p_value = signif(sw$p.value, 4),
                                   verdict = if (sw$p.value < 0.05) "FAIL: Residuals may not be normal" else "PASS: No evidence against normality",
                                   stringsAsFactors = FALSE)
  }
  # Breusch-Pagan style (simple)
  bp_fit <- tryCatch({
    res2 <- res^2
    bp_lm <- lm(res2 ~ fitted_vals)
    bp_sm <- summary(bp_lm)
    f_p <- pf(bp_sm$fstatistic[1], bp_sm$fstatistic[2], bp_sm$fstatistic[3], lower.tail = FALSE)
    data.frame(test = "Heteroscedasticity (residuals vs fitted)", statistic = round(bp_sm$fstatistic[1], 4),
               p_value = signif(f_p, 4),
               verdict = if (f_p < 0.05) "FAIL: Variance may not be constant" else "PASS: No strong evidence of heteroscedasticity",
               stringsAsFactors = FALSE)
  }, error = function(e) NULL)
  if (!is.null(bp_fit)) checks$homoscedasticity <- bp_fit
  # VIF
  if (length(coef(fit)) > 2) {
    vif_vals <- tryCatch({
      X <- model.matrix(fit)[, -1, drop = FALSE]
      if (ncol(X) >= 2) {
        R2 <- sapply(seq_len(ncol(X)), function(j) {
          summary(lm(X[,j] ~ X[,-j]))$r.squared
        })
        vif <- 1 / (1 - R2)
        data.frame(predictor = colnames(X), VIF = round(vif, 2),
                   verdict = ifelse(vif > 10, "HIGH: Severe multicollinearity",
                                    ifelse(vif > 5, "MODERATE: Check for collinearity", "OK")),
                   stringsAsFactors = FALSE)
      } else NULL
    }, error = function(e) NULL)
    if (!is.null(vif_vals)) checks$vif <- vif_vals
  }
  # Durbin-Watson approximation
  dw <- tryCatch({
    d <- sum(diff(res)^2) / sum(res^2)
    data.frame(test = "Durbin-Watson autocorrelation", statistic = round(d, 4), p_value = NA,
               verdict = if (d < 1.5) "WARNING: Possible positive autocorrelation"
                         else if (d > 2.5) "WARNING: Possible negative autocorrelation"
                         else "PASS: No strong autocorrelation signal",
               stringsAsFactors = FALSE)
  }, error = function(e) NULL)
  if (!is.null(dw)) checks$autocorrelation <- dw
  checks
}



reshape2_melt_cor <- function(cor_mat, p_mat) {
  vars <- rownames(cor_mat)
  n <- length(vars)
  rows <- list()
  for (i in seq_len(n)) {
    for (j in seq_len(n)) {
      sig <- ""
      if (i != j && !is.na(p_mat[i,j])) {
        if (p_mat[i,j] < 0.001) sig <- "***"
        else if (p_mat[i,j] < 0.01) sig <- "**"
        else if (p_mat[i,j] < 0.05) sig <- "*"
      }
      rows[[length(rows) + 1]] <- data.frame(
        Var1 = vars[i], Var2 = vars[j],
        value = cor_mat[i, j],
        label = paste0(round(cor_mat[i, j], 2), sig),
        stringsAsFactors = FALSE)
    }
  }
  out <- do.call(rbind, rows)
  out$Var1 <- factor(out$Var1, levels = vars)
  out$Var2 <- factor(out$Var2, levels = rev(vars))
  out
}




# ══════════════════════════════════════════════════════════════
# PHASE 4: HYPOTHESIS TESTING ENGINE
# ══════════════════════════════════════════════════════════════

run_hypothesis_test <- function(df, test_name, var1, var2 = NULL, mu0 = 0, p0 = 0.5, conf_level = 0.95, paired = FALSE) {
  x <- df[[var1]]; x <- x[!is.na(x)]
  y <- if (!is.null(var2) && var2 != "" && var2 %in% names(df)) { v <- df[[var2]]; v[!is.na(v)] } else NULL
  alpha <- 1 - conf_level

  tryCatch({
    switch(test_name,

    # ── One-sample ──
    "One-sample t-test" = {
      tt <- t.test(x, mu = mu0, conf.level = conf_level)
      data.frame(statistic = c("t", "df", "p-value", "Mean", "95% CI lower", "95% CI upper", "H0: mu =", "n"),
                 value = c(round(tt$statistic,4), round(tt$parameter,2), signif(tt$p.value,4),
                           round(tt$estimate,4), round(tt$conf.int[1],4), round(tt$conf.int[2],4), mu0, length(x)),
                 stringsAsFactors = FALSE)
    },
    "Wilcoxon signed-rank" = {
      wt <- wilcox.test(x, mu = mu0, conf.int = TRUE, conf.level = conf_level, exact = FALSE)
      data.frame(statistic = c("V", "p-value", "Pseudomedian", "CI lower", "CI upper", "H0: median =", "n"),
                 value = c(round(wt$statistic,2), signif(wt$p.value,4), round(wt$estimate,4),
                           round(wt$conf.int[1],4), round(wt$conf.int[2],4), mu0, length(x)),
                 stringsAsFactors = FALSE)
    },
    "Sign test" = {
      pos <- sum(x > mu0); neg <- sum(x < mu0); nn <- pos + neg
      p_val <- 2 * min(pbinom(min(pos,neg), nn, 0.5), 1 - pbinom(min(pos,neg)-1, nn, 0.5))
      data.frame(statistic = c("Positive", "Negative", "Ties", "p-value (two-sided)", "H0: median ="),
                 value = c(pos, neg, sum(x == mu0), signif(min(p_val,1),4), mu0),
                 stringsAsFactors = FALSE)
    },
    "Binomial test" = {
      successes <- sum(x == 1 | x == TRUE | toupper(as.character(x)) %in% c("YES","Y","TRUE","SUCCESS","1"))
      n_total <- length(x)
      bt <- binom.test(successes, n_total, p = p0, conf.level = conf_level)
      data.frame(statistic = c("Successes", "Trials", "Proportion", "p-value", "CI lower", "CI upper", "H0: p ="),
                 value = c(bt$statistic, bt$parameter, round(bt$estimate,4), signif(bt$p.value,4),
                           round(bt$conf.int[1],4), round(bt$conf.int[2],4), p0),
                 stringsAsFactors = FALSE)
    },
    "Chi-square goodness of fit" = {
      tab <- table(factor(x))
      ct <- chisq.test(tab)
      data.frame(statistic = c("Chi-squared", "df", "p-value", "n", "Categories"),
                 value = c(round(ct$statistic,4), ct$parameter, signif(ct$p.value,4), length(x), length(tab)),
                 stringsAsFactors = FALSE)
    },

    # ── Two-sample ──
    "Independent t-test (Welch)" = {
      validate_two(x, y)
      grps <- split_by_group(x, y)
      tt <- t.test(grps$a, grps$b, var.equal = FALSE, conf.level = conf_level)
      d_val <- abs(diff(tt$estimate)) / sqrt((var(grps$a) + var(grps$b)) / 2)
      data.frame(statistic = c("t", "df", "p-value", "Mean 1", "Mean 2", "Diff", "CI lower", "CI upper", "Cohen's d", "n1", "n2"),
                 value = c(round(tt$statistic,4), round(tt$parameter,2), signif(tt$p.value,4),
                           round(tt$estimate[1],4), round(tt$estimate[2],4), round(diff(tt$estimate),4),
                           round(tt$conf.int[1],4), round(tt$conf.int[2],4), round(d_val,4),
                           length(grps$a), length(grps$b)),
                 stringsAsFactors = FALSE)
    },
    "Paired t-test" = {
      validate_two(x, y); cc <- complete.cases(df[[var1]], df[[var2]])
      xx <- df[[var1]][cc]; yy <- df[[var2]][cc]
      tt <- t.test(xx, yy, paired = TRUE, conf.level = conf_level)
      data.frame(statistic = c("t", "df", "p-value", "Mean diff", "CI lower", "CI upper", "n pairs"),
                 value = c(round(tt$statistic,4), round(tt$parameter,2), signif(tt$p.value,4),
                           round(tt$estimate,4), round(tt$conf.int[1],4), round(tt$conf.int[2],4), sum(cc)),
                 stringsAsFactors = FALSE)
    },
    "Mann-Whitney U" = {
      validate_two(x, y); grps <- split_by_group(x, y)
      wt <- wilcox.test(grps$a, grps$b, exact = FALSE, conf.int = TRUE, conf.level = conf_level)
      data.frame(statistic = c("W", "p-value", "Location shift", "CI lower", "CI upper", "n1", "n2"),
                 value = c(round(wt$statistic,2), signif(wt$p.value,4), round(wt$estimate,4),
                           round(wt$conf.int[1],4), round(wt$conf.int[2],4), length(grps$a), length(grps$b)),
                 stringsAsFactors = FALSE)
    },
    "Wilcoxon signed-rank (paired)" = {
      validate_two(x, y); cc <- complete.cases(df[[var1]], df[[var2]])
      xx <- df[[var1]][cc]; yy <- df[[var2]][cc]
      wt <- wilcox.test(xx, yy, paired = TRUE, exact = FALSE, conf.int = TRUE, conf.level = conf_level)
      data.frame(statistic = c("V", "p-value", "Pseudomedian diff", "CI lower", "CI upper", "n pairs"),
                 value = c(round(wt$statistic,2), signif(wt$p.value,4), round(wt$estimate,4),
                           round(wt$conf.int[1],4), round(wt$conf.int[2],4), sum(cc)),
                 stringsAsFactors = FALSE)
    },
    "Two-proportion z-test" = {
      validate_two(x, y)
      x1 <- sum(x == 1 | x == TRUE, na.rm=T); n1 <- sum(!is.na(x))
      x2 <- sum(y == 1 | y == TRUE, na.rm=T); n2 <- sum(!is.na(y))
      pt <- prop.test(c(x1, x2), c(n1, n2), conf.level = conf_level)
      data.frame(statistic = c("Chi-squared", "p-value", "Prop 1", "Prop 2", "Diff", "CI lower", "CI upper"),
                 value = c(round(pt$statistic,4), signif(pt$p.value,4), round(pt$estimate[1],4),
                           round(pt$estimate[2],4), round(diff(pt$estimate),4),
                           round(pt$conf.int[1],4), round(pt$conf.int[2],4)),
                 stringsAsFactors = FALSE)
    },

    # ── Multi-sample ──
    "One-way ANOVA" = {
      validate_two(x, y); grps <- factor(y)
      av <- summary(aov(x ~ grps))
      f_val <- av[[1]]$`F value`[1]; p_val <- av[[1]]$`Pr(>F)`[1]
      ss_b <- av[[1]]$`Sum Sq`[1]; ss_t <- sum(av[[1]]$`Sum Sq`); eta2 <- ss_b/ss_t
      data.frame(statistic = c("F", "df1", "df2", "p-value", "Eta-squared", "Groups", "n"),
                 value = c(round(f_val,4), av[[1]]$Df[1], av[[1]]$Df[2], signif(p_val,4),
                           round(eta2,4), nlevels(grps), length(x)),
                 stringsAsFactors = FALSE)
    },
    "Welch's ANOVA" = {
      validate_two(x, y)
      ow <- oneway.test(x ~ factor(y), var.equal = FALSE)
      data.frame(statistic = c("F", "df1", "df2", "p-value"),
                 value = c(round(ow$statistic,4), round(ow$parameter[1],2), round(ow$parameter[2],2), signif(ow$p.value,4)),
                 stringsAsFactors = FALSE)
    },
    "Kruskal-Wallis" = {
      validate_two(x, y)
      kt <- kruskal.test(x ~ factor(y))
      data.frame(statistic = c("H (chi-squared)", "df", "p-value", "Groups"),
                 value = c(round(kt$statistic,4), kt$parameter, signif(kt$p.value,4), length(unique(y))),
                 stringsAsFactors = FALSE)
    },
    "Tukey HSD" = {
      validate_two(x, y)
      tk <- TukeyHSD(aov(x ~ factor(y)))
      out <- as.data.frame(tk[[1]]); out$comparison <- rownames(out); rownames(out) <- NULL
      names(out)[1:4] <- c("diff", "ci_lower", "ci_upper", "p_adj")
      out[, c("comparison", "diff", "ci_lower", "ci_upper", "p_adj")]
    },
    "Friedman test" = {
      validate_two(x, y)
      ft <- friedman.test(as.matrix(df[, c(var1, var2)]))
      data.frame(statistic = c("Chi-squared", "df", "p-value"),
                 value = c(round(ft$statistic,4), ft$parameter, signif(ft$p.value,4)),
                 stringsAsFactors = FALSE)
    },

    # ── Normality tests ──
    "Shapiro-Wilk" = {
      xs <- if(length(x) > 5000) sample(x, 5000) else x
      sw <- shapiro.test(xs)
      data.frame(statistic = c("W", "p-value", "n", "Verdict"),
                 value = c(round(sw$statistic,5), signif(sw$p.value,4), length(xs),
                           if(sw$p.value < 0.05) "Evidence against normality" else "No evidence against normality"),
                 stringsAsFactors = FALSE)
    },
    "Anderson-Darling" = {
      xs <- sort(x); nn <- length(xs); mn <- mean(xs); sd <- sd(xs)
      z <- pnorm((xs - mn) / sd)
      z <- pmax(pmin(z, 1 - 1e-10), 1e-10)
      S <- sum((2 * seq_len(nn) - 1) * (log(z) + log(1 - rev(z)))) / nn
      A2 <- -nn - S
      A2_star <- A2 * (1 + 0.75/nn + 2.25/nn^2)
      p_approx <- if(A2_star >= 0.6) exp(1.2937 - 5.709*A2_star + 0.0186*A2_star^2)
                  else if(A2_star >= 0.34) exp(0.9177 - 4.279*A2_star - 1.38*A2_star^2)
                  else if(A2_star >= 0.2) 1 - exp(-8.318 + 42.796*A2_star - 59.938*A2_star^2)
                  else 1 - exp(-13.436 + 101.14*A2_star - 223.73*A2_star^2)
      data.frame(statistic = c("A-squared", "A-squared*", "p-value (approx)", "n", "Verdict"),
                 value = c(round(A2,4), round(A2_star,4), signif(max(min(p_approx,1),0),4), nn,
                           if(p_approx < 0.05) "Evidence against normality" else "No evidence against normality"),
                 stringsAsFactors = FALSE)
    },
    "Kolmogorov-Smirnov" = {
      ks <- ks.test(x, "pnorm", mean(x), sd(x))
      data.frame(statistic = c("D", "p-value", "n", "Verdict"),
                 value = c(round(ks$statistic,5), signif(ks$p.value,4), length(x),
                           if(ks$p.value < 0.05) "Evidence against normality" else "No evidence against normality"),
                 stringsAsFactors = FALSE)
    },

    # ── Power analysis ──
    "Power: two-sample t-test" = {
      d_vals <- c(0.2, 0.5, 0.8)
      results <- sapply(d_vals, function(d) {
        ceiling(power.t.test(delta = d, sd = 1, sig.level = alpha, power = 0.80)$n)
      })
      data.frame(statistic = c("Small effect (d=0.2)", "Medium effect (d=0.5)", "Large effect (d=0.8)", "Significance level", "Target power"),
                 value = c(paste0("n = ", results), alpha, 0.80),
                 stringsAsFactors = FALSE)
    },
    "Power: one-way ANOVA" = {
      f_vals <- c(0.1, 0.25, 0.4)
      k <- if(!is.null(y)) length(unique(y[!is.na(y)])) else 3
      results <- sapply(f_vals, function(f) {
        ceiling(power.anova.test(groups = k, between.var = f^2, within.var = 1, sig.level = alpha, power = 0.80)$n)
      })
      data.frame(statistic = c("Small (f=0.1)", "Medium (f=0.25)", "Large (f=0.4)", "Groups", "Significance", "Power"),
                 value = c(paste0("n/group = ", results), k, alpha, 0.80),
                 stringsAsFactors = FALSE)
    },
    "Power: chi-square" = {
      w_vals <- c(0.1, 0.3, 0.5)
      df_chi <- if(!is.null(y)) (length(unique(x))-1) * (length(unique(y))-1) else 1
      results <- sapply(w_vals, function(w) ceiling((qchisq(1-alpha, df_chi) + qnorm(0.80)*sqrt(df_chi))^2 / w^2))
      data.frame(statistic = c("Small (w=0.1)", "Medium (w=0.3)", "Large (w=0.5)", "df", "Significance", "Power"),
                 value = c(paste0("n = ", results), df_chi, alpha, 0.80),
                 stringsAsFactors = FALSE)
    },

    # Default
    data.frame(statistic = "Error", value = paste0("Test '", test_name, "' not implemented."), stringsAsFactors = FALSE)
    )
  }, error = function(e) {
    data.frame(statistic = "Error", value = as.character(e$message), stringsAsFactors = FALSE)
  })
}

validate_two <- function(x, y) {
  if (is.null(y) || length(y) == 0) stop("Second variable required for this test.")
  invisible(TRUE)
}

split_by_group <- function(numeric_var, group_var) {
  g <- factor(group_var)
  levs <- levels(g)
  list(a = numeric_var[g == levs[1]], b = numeric_var[g == levs[2]])
}

# Test Finder logic
find_test <- function(n_groups, paired, data_type) {
  if (data_type == "continuous") {
    if (n_groups == 1) {
      return(data.frame(
        test = c("One-sample t-test", "Wilcoxon signed-rank", "Sign test"),
        assumption = c("Normal distribution", "Symmetric distribution", "None (weakest)"),
        use_when = c("Data roughly normal, want to test mean", "Non-normal but symmetric", "Very few assumptions needed"),
        stringsAsFactors = FALSE))
    }
    if (n_groups == 2 && !paired) {
      return(data.frame(
        test = c("Independent t-test (Welch)", "Mann-Whitney U"),
        assumption = c("Both groups roughly normal", "No normality needed"),
        use_when = c("Compare two group means", "Compare two group distributions/ranks"),
        stringsAsFactors = FALSE))
    }
    if (n_groups == 2 && paired) {
      return(data.frame(
        test = c("Paired t-test", "Wilcoxon signed-rank (paired)"),
        assumption = c("Differences roughly normal", "No normality needed"),
        use_when = c("Before/after, matched pairs (parametric)", "Before/after, matched pairs (nonparametric)"),
        stringsAsFactors = FALSE))
    }
    if (n_groups >= 3 && !paired) {
      return(data.frame(
        test = c("One-way ANOVA", "Welch's ANOVA", "Kruskal-Wallis", "Tukey HSD"),
        assumption = c("Normal + equal variance", "Normal, unequal variance OK", "No normality needed", "Post-hoc after ANOVA"),
        use_when = c("Compare 3+ group means (classic)", "Safer when variances differ", "Non-normal data, 3+ groups", "Which pairs differ?"),
        stringsAsFactors = FALSE))
    }
  }
  if (data_type == "categorical") {
    if (n_groups == 1) {
      return(data.frame(
        test = c("Chi-square goodness of fit", "Binomial test"),
        assumption = c("Expected counts >= 5", "Binary outcome"),
        use_when = c("Do observed proportions match expected?", "Test a single proportion"),
        stringsAsFactors = FALSE))
    }
    return(data.frame(
      test = c("Chi-square test", "Fisher exact test", "Two-proportion z-test"),
      assumption = c("Expected counts >= 5", "Any sample size", "Two binary outcomes"),
      use_when = c("Test independence (large samples)", "Test independence (small/sparse)", "Compare two proportions"),
      stringsAsFactors = FALSE))
  }
  data.frame(test = "See bivariate tab", assumption = "-", use_when = "Use the Bivariate EDA tab for mixed types", stringsAsFactors = FALSE)
}




# ══════════════════════════════════════════════════════════════
# PHASE 5: EDUCATION LAYER — Interpretation Engine
# ══════════════════════════════════════════════════════════════

interpret_p_value <- function(p, alpha = 0.05) {
  if (is.na(p)) return("p-value not available.")
  if (p < 0.001) paste0("The p-value is very small (p < 0.001). There is very strong evidence against the null hypothesis at the ", alpha*100, "% significance level.")
  else if (p < 0.01) paste0("The p-value is ", signif(p,3), ". There is strong evidence against the null hypothesis.")
  else if (p < alpha) paste0("The p-value is ", signif(p,3), ". There is sufficient evidence to reject the null hypothesis at the ", alpha*100, "% level.")
  else if (p < 0.10) paste0("The p-value is ", signif(p,3), ". The result is not statistically significant at ", alpha*100, "%, but is marginally suggestive. Consider increasing sample size.")
  else paste0("The p-value is ", signif(p,3), ". There is not enough evidence to reject the null hypothesis. The observed result is consistent with the null.")
}

interpret_correlation <- function(r, p) {
  strength <- if (abs(r) >= 0.8) "very strong"
    else if (abs(r) >= 0.6) "strong"
    else if (abs(r) >= 0.4) "moderate"
    else if (abs(r) >= 0.2) "weak"
    else "very weak or negligible"
  direction <- if (r > 0) "positive" else "negative"
  sig <- if (!is.na(p) && p < 0.05) "statistically significant" else "not statistically significant"
  paste0("There is a ", strength, " ", direction, " linear relationship (r = ", round(r,3),
         "). This correlation is ", sig, ". ",
         if (abs(r) >= 0.4) paste0("About ", round(r^2 * 100), "% of the variance in one variable is explained by the other.")
         else "The variables share little linear information.")
}

interpret_effect_size <- function(d) {
  if (is.na(d)) return("")
  size <- if (abs(d) >= 0.8) "large" else if (abs(d) >= 0.5) "medium" else if (abs(d) >= 0.2) "small" else "negligible"
  paste0("Cohen's d = ", round(d, 3), " indicates a ", size, " effect size. ",
         switch(size,
           "large" = "The difference between groups is substantial and likely practically meaningful.",
           "medium" = "The difference is moderate and may be practically meaningful depending on context.",
           "small" = "The difference is small. Statistical significance does not guarantee practical importance.",
           "The effect is too small to be meaningful in most practical contexts."))
}

interpret_r_squared <- function(r2, adj_r2 = NULL) {
  pct <- round(r2 * 100, 1)
  quality <- if (r2 >= 0.9) "excellent" else if (r2 >= 0.7) "good" else if (r2 >= 0.5) "moderate" else if (r2 >= 0.3) "weak" else "very weak"
  msg <- paste0("R-squared = ", round(r2, 4), ". The model explains ", pct, "% of the variance in the outcome. This is a ", quality, " fit.")
  if (!is.null(adj_r2)) msg <- paste0(msg, " Adjusted R-squared = ", round(adj_r2, 4), " (penalizes for number of predictors).")
  msg
}

method_info <- list(
  "One-sample t-test" = list(
    what = "Tests whether the population mean equals a specified value.",
    assumptions = "Data approximately normal (robust with n > 30). Random sample.",
    interpret = "If p < 0.05, the sample mean is significantly different from the hypothesized value."
  ),
  "Independent t-test (Welch)" = list(
    what = "Compares means of two independent groups. Welch's version does not assume equal variances.",
    assumptions = "Both groups roughly normal (robust with n > 30 each). Independent observations.",
    interpret = "If p < 0.05, the two groups have significantly different means. Check Cohen's d for practical significance."
  ),
  "Mann-Whitney U" = list(
    what = "Nonparametric alternative to the independent t-test. Compares distributions/ranks of two groups.",
    assumptions = "Independent observations. Similar distribution shapes (tests location shift).",
    interpret = "If p < 0.05, the two groups differ significantly in their distributions."
  ),
  "One-way ANOVA" = list(
    what = "Tests whether three or more group means are equal.",
    assumptions = "Normal distribution within each group. Equal variances (homoscedasticity). Independent observations.",
    interpret = "If p < 0.05, at least one group mean differs. Use Tukey HSD to find which pairs differ."
  ),
  "Kruskal-Wallis" = list(
    what = "Nonparametric alternative to one-way ANOVA. Compares distributions across 3+ groups.",
    assumptions = "Independent observations. Similar distribution shapes across groups.",
    interpret = "If p < 0.05, at least one group differs. Follow up with pairwise comparisons."
  ),
  "Pearson correlation" = list(
    what = "Measures the strength and direction of the linear relationship between two continuous variables.",
    assumptions = "Both variables continuous. Linear relationship. Bivariate normality (approximately).",
    interpret = "r ranges from -1 to 1. |r| > 0.7 is strong, 0.4-0.7 moderate, < 0.4 weak."
  ),
  "Spearman correlation" = list(
    what = "Measures monotonic (not necessarily linear) association between two variables using ranks.",
    assumptions = "Ordinal or continuous data. Monotonic relationship.",
    interpret = "Similar interpretation to Pearson but based on ranks. More robust to outliers."
  ),
  "Linear regression" = list(
    what = "Models the relationship between a continuous outcome and one or more predictors.",
    assumptions = "Linearity. Independence of errors. Normality of residuals. Constant variance (homoscedasticity).",
    interpret = "R-squared measures fit. Check p-values of coefficients. Always check diagnostic plots."
  ),
  "Logistic regression" = list(
    what = "Models the probability of a binary outcome as a function of predictors.",
    assumptions = "Binary outcome. Independence. No multicollinearity. Linear relationship between predictors and log-odds.",
    interpret = "Coefficients are log-odds. Exponentiate for odds ratios. AUC measures discrimination."
  )
)

# ══════════════════════════════════════════════════════════════
# PHASE 6: TIME SERIES ENGINE
# ══════════════════════════════════════════════════════════════

detect_date_column <- function(df) {
  for (nm in names(df)) {
    x <- df[[nm]]
    if (inherits(x, c("Date", "POSIXct", "POSIXt"))) return(nm)
  }
  for (nm in names(df)) {
    if (grepl("date|time|day|month|year|period|quarter", tolower(nm))) {
      x <- df[[nm]]
      if (is.character(x)) {
        xs <- head(trimws(x[!is.na(x)]), 20)
        for (fmt in c("%Y-%m-%d", "%m/%d/%Y", "%d/%m/%Y", "%Y/%m/%d", "%m-%d-%Y")) {
          att <- as.Date(xs, format = fmt)
          if (sum(!is.na(att)) / length(xs) > 0.8) return(nm)
        }
      }
    }
  }
  NULL
}

detect_frequency <- function(dates) {
  if (length(dates) < 3) return(1)
  d <- as.numeric(diff(sort(dates)))
  med_diff <- stats::median(d, na.rm = TRUE)
  if (med_diff <= 1.5) return(365)      # daily
  if (med_diff <= 8) return(52)          # weekly
  if (med_diff <= 35) return(12)         # monthly
  if (med_diff <= 100) return(4)         # quarterly
  return(1)                               # annual
}

freq_label <- function(freq) {
  switch(as.character(freq),
    "365" = "Daily", "252" = "Trading days", "52" = "Weekly",
    "12" = "Monthly", "4" = "Quarterly", "1" = "Annual",
    paste0("Custom (", freq, ")"))
}

make_ts_object <- function(values, dates, freq) {
  ord <- order(dates)
  values <- values[ord]; dates <- dates[ord]
  start_year <- as.numeric(format(dates[1], "%Y"))
  if (freq == 12) {
    start_period <- as.numeric(format(dates[1], "%m"))
  } else if (freq == 4) {
    start_period <- ceiling(as.numeric(format(dates[1], "%m")) / 3)
  } else if (freq == 52) {
    start_period <- as.numeric(format(dates[1], "%U"))
  } else {
    start_period <- 1
  }
  ts(values, start = c(start_year, start_period), frequency = freq)
}

run_stationarity_tests <- function(x) {
  results <- list()
  # ADF
  if (has_pkg("tseries")) {
    adf <- tryCatch({
      t <- tseries::adf.test(x)
      data.frame(test = "Augmented Dickey-Fuller", statistic = round(t$statistic, 4),
                 p_value = signif(t$p.value, 4),
                 verdict = if (t$p.value < 0.05) "STATIONARY (reject unit root)" else "NON-STATIONARY (cannot reject unit root)",
                 stringsAsFactors = FALSE)
    }, error = function(e) NULL)
    if (!is.null(adf)) results$adf <- adf
  }
  # KPSS
  if (has_pkg("tseries")) {
    kpss <- tryCatch({
      t <- tseries::kpss.test(x)
      data.frame(test = "KPSS", statistic = round(t$statistic, 4),
                 p_value = signif(t$p.value, 4),
                 verdict = if (t$p.value < 0.05) "NON-STATIONARY (reject stationarity)" else "STATIONARY (cannot reject stationarity)",
                 stringsAsFactors = FALSE)
    }, error = function(e) NULL)
    if (!is.null(kpss)) results$kpss <- kpss
  }
  # PP
  if (has_pkg("tseries")) {
    pp <- tryCatch({
      t <- tseries::pp.test(x)
      data.frame(test = "Phillips-Perron", statistic = round(t$statistic, 4),
                 p_value = signif(t$p.value, 4),
                 verdict = if (t$p.value < 0.05) "STATIONARY (reject unit root)" else "NON-STATIONARY (cannot reject unit root)",
                 stringsAsFactors = FALSE)
    }, error = function(e) NULL)
    if (!is.null(pp)) results$pp <- pp
  }
  if (length(results) == 0) return(data.frame(test = "No tests available", statistic = NA, p_value = NA, verdict = "Install 'tseries' package", stringsAsFactors = FALSE))
  do.call(rbind, results)
}

fit_arima_model <- function(ts_obj) {
  if (!has_pkg("forecast")) stop("Install 'forecast' package for ARIMA modeling.")
  forecast::auto.arima(ts_obj, stepwise = TRUE, approximation = TRUE, trace = FALSE)
}

fit_garch_model <- function(returns, model_type = "sGARCH", garch_order = c(1,1)) {
  if (!has_pkg("rugarch")) stop("Install 'rugarch' package for GARCH modeling.")
  variance_model <- if (model_type == "EGARCH") "eGARCH"
                    else if (model_type == "GJR-GARCH") "gjrGARCH"
                    else "sGARCH"
  spec <- rugarch::ugarchspec(
    variance.model = list(model = variance_model, garchOrder = garch_order),
    mean.model = list(armaOrder = c(0, 0), include.mean = TRUE),
    distribution.model = "std"
  )
  rugarch::ugarchfit(spec, data = returns, solver = "hybrid")
}




# ══════════════════════════════════════════════════════════════
# PHASE 7: SURVIVAL ANALYSIS ENGINE
# ══════════════════════════════════════════════════════════════

detect_event_column <- function(df) {
  for (nm in names(df)) {
    x <- df[[nm]][!is.na(df[[nm]])]
    if ((is.numeric(x) || is.integer(x)) && all(x %in% c(0, 1)) && length(unique(x)) == 2) {
      if (grepl("event|status|dead|death|censor|fail|outcome|indicator", tolower(nm))) return(nm)
    }
  }
  for (nm in names(df)) {
    x <- df[[nm]][!is.na(df[[nm]])]
    if ((is.numeric(x) || is.integer(x)) && all(x %in% c(0, 1)) && length(unique(x)) == 2) return(nm)
  }
  NULL
}

detect_time_column <- function(df) {
  for (nm in names(df)) {
    if (grepl("time|duration|survival|follow|tenure|days|months|years|weeks|los|length", tolower(nm))) {
      if (is.numeric(df[[nm]])) return(nm)
    }
  }
  nums <- names(df)[vapply(df, is.numeric, logical(1))]
  if (length(nums)) return(nums[1])
  NULL
}

fit_km <- function(df, time_var, event_var, group_var = NULL) {
  if (!has_pkg("survival")) stop("Install 'survival' package.")
  surv_obj <- survival::Surv(df[[time_var]], df[[event_var]])
  if (!is.null(group_var) && group_var != "" && group_var %in% names(df)) {
    form <- as.formula(paste0("surv_obj ~ factor(df[['", group_var, "']])"))
    fit <- survival::survfit(surv_obj ~ factor(df[[group_var]]))
    logrank <- survival::survdiff(surv_obj ~ factor(df[[group_var]]))
  } else {
    fit <- survival::survfit(surv_obj ~ 1)
    logrank <- NULL
  }
  list(fit = fit, logrank = logrank, surv = surv_obj, group = group_var)
}

fit_cox <- function(df, time_var, event_var, predictors) {
  if (!has_pkg("survival")) stop("Install 'survival' package.")
  surv_obj <- survival::Surv(df[[time_var]], df[[event_var]])
  rhs <- paste(predictors, collapse = " + ")
  form <- as.formula(paste("surv_obj ~", rhs))
  survival::coxph(form, data = df)
}

# ══════════════════════════════════════════════════════════════
# PHASE 8: REPORT GENERATION ENGINE
# ══════════════════════════════════════════════════════════════

generate_analysis_summary <- function(df, doctor_findings_df, fix_log) {
  lines <- c(
    "# Kernel — Analysis Report",
    paste0("**Generated:** ", Sys.time()),
    "",
    "## Data Overview",
    paste0("- **Rows:** ", nrow(df)),
    paste0("- **Columns:** ", ncol(df)),
    paste0("- **Numeric variables:** ", length(numeric_vars_of(df))),
    paste0("- **Categorical variables:** ", length(categorical_vars_of(df))),
    ""
  )
  if (nrow(doctor_findings_df) > 0) {
    lines <- c(lines,
      "## Variable Doctor Findings",
      paste0("- Critical issues: ", sum(doctor_findings_df$severity == "critical")),
      paste0("- Warnings: ", sum(doctor_findings_df$severity == "warning")),
      paste0("- Suggestions: ", sum(doctor_findings_df$severity == "info")),
      "")
  }
  if (length(fix_log) > 0) {
    lines <- c(lines, "## Fixes Applied", paste0("- ", fix_log), "")
  }
  lines <- c(lines,
    "## Column Profile",
    "",
    knitr::kable(column_profile(df), format = "markdown"),
    ""
  )
  paste(lines, collapse = "\n")
}




# ══════════════════════════════════════════════════════════════
# PHASE 9: MACHINE LEARNING PIPELINE ENGINE
# ══════════════════════════════════════════════════════════════

ml_detect_task <- function(y) {
  if (is.factor(y) || is.character(y) || is.logical(y)) return("classification")
  if (is.numeric(y) && length(unique(y[!is.na(y)])) <= 10) return("classification")
  "regression"
}

ml_split_data <- function(df, outcome, predictors, train_pct = 0.8, seed = 42) {
  set.seed(seed)
  keep <- unique(c(outcome, predictors))
  d <- df[, keep, drop = FALSE]
  d <- d[stats::complete.cases(d), , drop = FALSE]
  n <- nrow(d)
  idx <- sample(n, floor(n * train_pct))
  list(train = d[idx, , drop = FALSE], test = d[-idx, , drop = FALSE], all = d)
}

ml_prep <- function(train, test, outcome, task) {
  if (task == "classification") {
    train[[outcome]] <- factor(train[[outcome]])
    test[[outcome]] <- factor(test[[outcome]], levels = levels(train[[outcome]]))
  }
  list(train = train, test = test)
}

ml_fit_model <- function(train, outcome, predictors, method, task) {
  form <- as.formula(paste(outcome, "~", paste(predictors, collapse = " + ")))
  tryCatch({
    switch(method,
      "Decision Tree" = {
        if (!has_pkg("rpart")) stop("Install 'rpart'")
        rpart::rpart(form, data = train, method = if(task == "classification") "class" else "anova")
      },
      "Random Forest" = {
        if (!has_pkg("ranger")) stop("Install 'ranger'")
        ranger::ranger(form, data = train, num.trees = 500, importance = "impurity",
                       probability = (task == "classification"), seed = 42)
      },
      "XGBoost" = {
        if (!has_pkg("xgboost")) stop("Install 'xgboost'")
        x_train <- model.matrix(form, data = train)[, -1, drop = FALSE]
        y_train <- if (task == "classification") as.numeric(factor(train[[outcome]])) - 1 else train[[outcome]]
        nclass <- if (task == "classification") length(unique(y_train)) else NULL
        obj <- if (task == "classification") { if (nclass == 2) "binary:logistic" else "multi:softprob" } else "reg:squarederror"
        params <- list(objective = obj, max_depth = 6, eta = 0.1, nrounds = 100, verbose = 0)
        if (!is.null(nclass) && nclass > 2) params$num_class <- nclass
        dtrain <- xgboost::xgb.DMatrix(data = x_train, label = y_train)
        xgboost::xgb.train(params = params, data = dtrain, nrounds = 100, verbose = 0)
      },
      "SVM" = {
        if (!has_pkg("e1071")) stop("Install 'e1071'")
        e1071::svm(form, data = train, probability = TRUE, kernel = "radial")
      },
      "KNN" = {
        # Store training data for prediction
        list(train = train, outcome = outcome, predictors = predictors, k = min(5, nrow(train) - 1))
      },
      "Naive Bayes" = {
        if (!has_pkg("e1071")) stop("Install 'e1071'")
        e1071::naiveBayes(form, data = train)
      },
      "Logistic Regression" = {
        if (task != "classification") stop("Logistic regression requires a classification task.")
        stats::glm(form, data = train, family = binomial(link = "logit"))
      },
      stop(paste("Unknown method:", method))
    )
  }, error = function(e) {
    list(error = TRUE, message = e$message)
  })
}

ml_predict <- function(fit, test, outcome, predictors, method, task, train = NULL) {
  tryCatch({
    switch(method,
      "Decision Tree" = {
        if (task == "classification") {
          probs <- predict(fit, test, type = "prob")
          preds <- predict(fit, test, type = "class")
          list(class = as.character(preds), prob = probs)
        } else {
          list(value = predict(fit, test))
        }
      },
      "Random Forest" = {
        p <- predict(fit, data = test)
        if (task == "classification") {
          list(class = as.character(p$predictions[, 1] < 0.5 + 0), prob = p$predictions,
               class = levels(factor(train[[outcome]]))[apply(p$predictions, 1, which.max)])
        } else {
          list(value = p$predictions)
        }
      },
      "XGBoost" = {
        form <- as.formula(paste(outcome, "~", paste(predictors, collapse = " + ")))
        x_test <- model.matrix(form, data = test)[, -1, drop = FALSE]
        dtest <- xgboost::xgb.DMatrix(data = x_test)
        raw <- predict(fit, dtest)
        if (task == "classification") {
          levs <- levels(factor(train[[outcome]]))
          if (length(levs) == 2) {
            list(class = levs[(raw > 0.5) + 1], prob = cbind(1 - raw, raw))
          } else {
            prob_mat <- matrix(raw, ncol = length(levs), byrow = TRUE)
            list(class = levs[apply(prob_mat, 1, which.max)], prob = prob_mat)
          }
        } else {
          list(value = raw)
        }
      },
      "SVM" = {
        if (task == "classification") {
          p <- predict(fit, test, probability = TRUE)
          probs <- attr(p, "probabilities")
          list(class = as.character(p), prob = if(!is.null(probs)) probs else NULL)
        } else {
          list(value = as.numeric(predict(fit, test)))
        }
      },
      "KNN" = {
        if (!has_pkg("class")) stop("Install 'class'")
        train_x <- model.matrix(~ . - 1, data = fit$train[, fit$predictors, drop = FALSE])
        test_x <- model.matrix(~ . - 1, data = test[, fit$predictors, drop = FALSE])
        train_y <- fit$train[[fit$outcome]]
        p <- class::knn(train_x, test_x, train_y, k = fit$k, prob = TRUE)
        list(class = as.character(p), prob = attr(p, "prob"))
      },
      "Naive Bayes" = {
        p <- predict(fit, test, type = "class")
        probs <- predict(fit, test, type = "raw")
        list(class = as.character(p), prob = probs)
      },
      "Logistic Regression" = {
        probs <- predict(fit, test, type = "response")
        levs <- levels(factor(train[[outcome]]))
        list(class = levs[(probs > 0.5) + 1], prob = cbind(1 - probs, probs))
      },
      list(error = TRUE)
    )
  }, error = function(e) list(error = TRUE, message = e$message))
}

ml_metrics_classification <- function(actual, predicted, prob = NULL) {
  actual <- factor(actual); predicted <- factor(predicted, levels = levels(actual))
  cm <- table(Predicted = predicted, Actual = actual)
  accuracy <- sum(diag(cm)) / sum(cm)
  metrics <- data.frame(metric = "Accuracy", value = round(accuracy, 4), stringsAsFactors = FALSE)

  if (nlevels(actual) == 2) {
    tp <- cm[2,2]; fp <- cm[2,1]; fn <- cm[1,2]; tn <- cm[1,1]
    prec <- if ((tp+fp) > 0) tp/(tp+fp) else 0
    rec <- if ((tp+fn) > 0) tp/(tp+fn) else 0
    spec <- if ((tn+fp) > 0) tn/(tn+fp) else 0
    f1 <- if ((prec+rec) > 0) 2*prec*rec/(prec+rec) else 0
    metrics <- rbind(metrics, data.frame(
      metric = c("Precision (PPV)", "Recall (Sensitivity)", "Specificity", "F1 Score"),
      value = round(c(prec, rec, spec, f1), 4), stringsAsFactors = FALSE))
    if (!is.null(prob) && has_pkg("pROC")) {
      prob_vec <- if (is.matrix(prob)) prob[, ncol(prob)] else prob
      auc_val <- tryCatch(as.numeric(pROC::auc(pROC::roc(actual, prob_vec, quiet = TRUE))), error = function(e) NA)
      if (!is.na(auc_val)) metrics <- rbind(metrics, data.frame(metric = "AUC", value = round(auc_val, 4), stringsAsFactors = FALSE))
    }
  }
  list(metrics = metrics, confusion = cm)
}

ml_metrics_regression <- function(actual, predicted) {
  residuals <- actual - predicted
  mae <- mean(abs(residuals), na.rm = TRUE)
  rmse <- sqrt(mean(residuals^2, na.rm = TRUE))
  ss_res <- sum(residuals^2, na.rm = TRUE)
  ss_tot <- sum((actual - mean(actual, na.rm = TRUE))^2, na.rm = TRUE)
  r2 <- if (ss_tot > 0) 1 - ss_res / ss_tot else NA
  mape <- if (all(actual != 0)) mean(abs(residuals / actual), na.rm = TRUE) * 100 else NA
  data.frame(
    metric = c("MAE", "RMSE", "R-squared", "MAPE (%)"),
    value = round(c(mae, rmse, r2, mape), 4),
    stringsAsFactors = FALSE)
}

ml_variable_importance <- function(fit, method, predictors) {
  tryCatch({
    switch(method,
      "Decision Tree" = {
        imp <- fit$variable.importance
        if (is.null(imp)) return(NULL)
        data.frame(variable = names(imp), importance = as.numeric(imp), stringsAsFactors = FALSE)
      },
      "Random Forest" = {
        imp <- ranger::importance(fit)
        data.frame(variable = names(imp), importance = as.numeric(imp), stringsAsFactors = FALSE)
      },
      "XGBoost" = {
        imp <- xgboost::xgb.importance(model = fit)
        data.frame(variable = imp$Feature, importance = imp$Gain, stringsAsFactors = FALSE)
      },
      "Logistic Regression" = {
        cf <- abs(coef(fit))[-1]
        data.frame(variable = names(cf), importance = as.numeric(cf), stringsAsFactors = FALSE)
      },
      NULL
    )
  }, error = function(e) NULL)
}




# ══════════════════════════════════════════════════════════════
# PHASE 10: INDUSTRY TEMPLATES ENGINE
# ══════════════════════════════════════════════════════════════

# Pareto chart (universal)
make_pareto <- function(x, title = "Pareto Chart") {
  tab <- sort(table(x), decreasing = TRUE)
  df <- data.frame(category = factor(names(tab), levels = names(tab)),
                   count = as.integer(tab), stringsAsFactors = FALSE)
  df$pct <- df$count / sum(df$count) * 100
  df$cum_pct <- cumsum(df$pct)
  list(data = df, title = title)
}

# Control chart calculations
control_chart_stats <- function(x, type = "xbar", subgroup_size = 5) {
  x <- x[!is.na(x)]
  n <- length(x)
  if (type == "individuals") {
    cl <- mean(x)
    mr <- mean(abs(diff(x)))
    ucl <- cl + 2.66 * mr
    lcl <- cl - 2.66 * mr
    return(list(values = x, cl = cl, ucl = ucl, lcl = lcl, index = seq_along(x),
                mr = mr, type = "Individuals (I)", sigma = mr / 1.128))
  }
  if (type == "xbar") {
    n_groups <- floor(n / subgroup_size)
    if (n_groups < 2) return(NULL)
    groups <- split(x[1:(n_groups * subgroup_size)], rep(1:n_groups, each = subgroup_size))
    means <- sapply(groups, mean)
    ranges <- sapply(groups, function(g) diff(range(g)))
    xbar <- mean(means)
    rbar <- mean(ranges)
    d2 <- c(0, 0, 1.128, 1.693, 2.059, 2.326, 2.534, 2.704, 2.847, 2.970)[min(subgroup_size, 10)]
    A2 <- 3 / (d2 * sqrt(subgroup_size))
    ucl <- xbar + A2 * rbar
    lcl <- xbar - A2 * rbar
    return(list(values = means, cl = xbar, ucl = ucl, lcl = lcl, index = seq_along(means),
                rbar = rbar, type = paste0("X-bar (n=", subgroup_size, ")"), sigma = rbar / d2))
  }
  if (type == "p") {
    cl <- mean(x)
    n_each <- subgroup_size
    ucl <- cl + 3 * sqrt(cl * (1 - cl) / n_each)
    lcl <- max(0, cl - 3 * sqrt(cl * (1 - cl) / n_each))
    return(list(values = x, cl = cl, ucl = ucl, lcl = lcl, index = seq_along(x),
                type = "P chart (proportion)", sigma = sqrt(cl * (1 - cl) / n_each)))
  }
  if (type == "c") {
    cl <- mean(x)
    ucl <- cl + 3 * sqrt(cl)
    lcl <- max(0, cl - 3 * sqrt(cl))
    return(list(values = x, cl = cl, ucl = ucl, lcl = lcl, index = seq_along(x),
                type = "C chart (count)", sigma = sqrt(cl)))
  }
  NULL
}

# Process capability
process_capability <- function(x, lsl = NULL, usl = NULL) {
  x <- x[!is.na(x)]
  mu <- mean(x); sigma <- sd(x)
  results <- data.frame(metric = c("Mean", "Std Dev", "n"), value = c(round(mu,4), round(sigma,4), length(x)), stringsAsFactors = FALSE)
  if (!is.null(usl) && !is.null(lsl) && usl > lsl && sigma > 0) {
    cp <- (usl - lsl) / (6 * sigma)
    cpu <- (usl - mu) / (3 * sigma)
    cpl <- (mu - lsl) / (3 * sigma)
    cpk <- min(cpu, cpl)
    pp <- (usl - lsl) / (6 * sd(x))
    ppm_above <- pnorm(usl, mu, sigma, lower.tail = FALSE) * 1e6
    ppm_below <- pnorm(lsl, mu, sigma) * 1e6
    results <- rbind(results, data.frame(
      metric = c("LSL", "USL", "Cp", "Cpk", "Cpu", "Cpl", "PPM above USL", "PPM below LSL", "Total PPM defective"),
      value = c(round(lsl,4), round(usl,4), round(cp,4), round(cpk,4), round(cpu,4), round(cpl,4),
                round(ppm_above,1), round(ppm_below,1), round(ppm_above + ppm_below, 1)),
      stringsAsFactors = FALSE))
    verdict <- if (cpk >= 1.33) "CAPABLE (Cpk >= 1.33)" else if (cpk >= 1.0) "MARGINAL (1.0 <= Cpk < 1.33)" else "NOT CAPABLE (Cpk < 1.0)"
    results <- rbind(results, data.frame(metric = "Verdict", value = verdict, stringsAsFactors = FALSE))
  }
  results
}

# RFM scoring
compute_rfm <- function(df, customer_col, date_col, amount_col, ref_date = Sys.Date()) {
  dates <- df[[date_col]]
  if (is.character(dates)) {
    for (fmt in c("%Y-%m-%d","%m/%d/%Y","%d/%m/%Y")) {
      att <- as.Date(dates, format = fmt)
      if (sum(!is.na(att)) / sum(!is.na(dates)) > 0.7) { dates <- att; break }
    }
  }
  if (!inherits(dates, "Date")) dates <- tryCatch(as.Date(dates), error = function(e) NULL)
  if (is.null(dates)) stop("Cannot parse date column.")

  rfm <- df |>
    mutate(.date = dates, .amount = as.numeric(df[[amount_col]])) |>
    group_by(across(all_of(customer_col))) |>
    summarise(
      recency = as.numeric(ref_date - max(.date, na.rm = TRUE)),
      frequency = n(),
      monetary = sum(.amount, na.rm = TRUE),
      .groups = "drop"
    )
  # Score 1-5
  rfm$R_score <- as.integer(cut(-rfm$recency, breaks = 5, labels = FALSE))
  rfm$F_score <- as.integer(cut(rfm$frequency, breaks = quantile(rfm$frequency, probs = seq(0,1,0.2), na.rm = TRUE), labels = FALSE, include.lowest = TRUE))
  rfm$M_score <- as.integer(cut(rfm$monetary, breaks = quantile(rfm$monetary, probs = seq(0,1,0.2), na.rm = TRUE), labels = FALSE, include.lowest = TRUE))
  rfm$RFM_score <- rfm$R_score * 100 + rfm$F_score * 10 + rfm$M_score
  rfm$segment <- ifelse(rfm$R_score >= 4 & rfm$F_score >= 4, "Champions",
    ifelse(rfm$R_score >= 4 & rfm$F_score <= 2, "New customers",
    ifelse(rfm$R_score <= 2 & rfm$F_score >= 4, "At risk",
    ifelse(rfm$R_score <= 2 & rfm$F_score <= 2, "Lost", "Regular"))))
  rfm
}


ui <- fluidPage(
  theme = bs_theme(version = 5, bootswatch = "flatly"),
  tags$head(
    tags$style(HTML("\n      .stat-card {background: #ffffff; border: 1px solid #e9ecef; border-radius: 14px; padding: 16px 18px; margin-bottom: 12px; box-shadow: 0 4px 16px rgba(0,0,0,0.05);}\n      .stat-card .label {font-size: 0.82rem; text-transform: uppercase; color: #6c757d; letter-spacing: 0.04em;}\n      .stat-card .value {font-size: 1.65rem; font-weight: 700; color: #212529;}\n      .tab-note {background: #f8f9fa; border-left: 4px solid #0d6efd; padding: 12px 14px; border-radius: 6px; margin-bottom: 12px;}\n      .section-gap {margin-top: 16px;}\n      .control-label {font-weight: 600;}\n      .btn {margin-bottom: 8px;}\n    "))
  ),
  titlePanel("Kernel"),
  sidebarLayout(
    sidebarPanel(
      width = 3,
      h4("Import and export"),
      fileInput(
        "file",
        "Choose a data file",
        accept = c(
          ".csv", ".tsv", ".txt", ".xlsx", ".xls", ".rds", ".RData",
          ".sav", ".dta", ".sas7bdat", ".xpt", ".feather", ".parquet"
        )
      ),
      uiOutput("import_detail_ui"),
      checkboxInput("clean_names", "Clean column names", TRUE),
      checkboxInput("drop_empty", "Drop empty rows and columns", TRUE),
      tags$hr(),
      downloadButton("download_clean_data", "Download clean data (.csv)", class = "btn-primary"),
      downloadButton("download_profile", "Download column profile (.csv)"),
      downloadButton("download_methods", "Download method catalog (.csv)"),
      downloadButton("download_design", "Download DOE design (.csv)"),
      downloadButton("download_qmd", "Download Quarto report template"),
      tags$hr(),
      p(
        class = "text-muted",
        "The app degrades gracefully when optional packages are missing. The Methods Catalog tab shows what is installed and what is still roadmap."
      )
    ),
    mainPanel(
      width = 9,
      tabsetPanel(
        id = "main_tabs",
        tabPanel(
          "Overview",
          br(),
          fluidRow(
            column(3, uiOutput("card_rows")),
            column(3, uiOutput("card_cols")),
            column(3, uiOutput("card_numeric")),
            column(3, uiOutput("card_categorical"))
          ),
          div(class = "tab-note", uiOutput("insight_notes")),
          fluidRow(
            column(7, h4("Data preview"), DTOutput("data_preview")),
            column(5, h4("Optional package status"), DTOutput("package_status_tbl"))
          )
        ),
        tabPanel(
          "Data Quality",
          br(),
          fluidRow(
            column(7, h4("Column profile"), DTOutput("column_profile_tbl")),
            column(5, h4("Missingness by variable"), plotOutput("missingness_plot", height = 360))
          ),
          fluidRow(
            column(12, h4("Variable-type composition"), plotOutput("type_plot", height = 280))
          )
        ),
        tabPanel(
          "Univariate",
          br(),
          fluidRow(
            column(4, selectInput("univariate_var", "Variable", choices = NULL)),
            column(4, selectInput("univariate_plot", "Plot style", choices = c("Auto", "Histogram", "Density", "Boxplot", "Bar chart"), selected = "Auto")),
            column(4, checkboxInput("univariate_drop_na", "Drop missing values before plotting", TRUE))
          ),
          fluidRow(
            column(7, plotOutput("univariate_plot_out", height = 420)),
            column(5, h4("Summary"), DTOutput("univariate_summary_tbl"))
          )
        ),
        tabPanel(

        
        tabPanel(
          "Bivariate",
          icon = icon("arrows-left-right"),
          br(),
          div(class = "tab-note", style = "border-left-color: #6f42c1;",
              p(strong("Bivariate analysis"), " — explore relationships between two variables. ",
                "The app auto-detects variable types and suggests appropriate tests and visualizations.")),
          fluidRow(
            column(3, selectInput("bi_x", "X variable", choices = NULL)),
            column(3, selectInput("bi_y", "Y variable", choices = NULL)),
            column(3, selectInput("bi_plot_type", "Plot type", choices = c("Auto", "Scatter", "Scatter + Linear fit", "Scatter + Loess", "Boxplot by group", "Violin by group", "Grouped bar chart"), selected = "Auto")),
            column(3, selectInput("bi_test", "Statistical test", choices = c("Auto-recommend"), selected = "Auto-recommend"))
          ),
          fluidRow(
            column(7, plotOutput("bi_plot", height = 440)),
            column(5,
              h4("Recommended tests"),
              DTOutput("bi_recommend_tbl"),
              br(),
              h4("Test results"),
              DTOutput("bi_test_tbl")
            )
          ),
          tags$hr(),
          h4("Correlation matrix", style = "margin-top: 12px;"),
          fluidRow(
            column(3, selectInput("cor_method", "Method", choices = c("pearson", "spearman", "kendall"), selected = "pearson")),
            column(9, p(class = "text-muted", style = "margin-top: 28px;", "Heatmap of pairwise correlations across all numeric variables. Stars indicate significance: * p<0.05, ** p<0.01, *** p<0.001"))
          ),
          fluidRow(
            column(7, plotOutput("cor_heatmap", height = 500)),
            column(5, h4("Correlation table"), DTOutput("cor_table"))
          )
        ),


        tabPanel(
          "Hypothesis Tests",
          icon = icon("flask-vial"),
          br(),
          div(class = "tab-note", style = "border-left-color: #d63384;",
              h4(style = "margin-top:0;", "Hypothesis Testing Suite"),
              p("20+ tests from one-sample through multi-sample, parametric and nonparametric, ",
                "plus normality checks and power analysis. Use the ", strong("Test Finder"), " to get started.")),
          fluidRow(
            column(12,
              tabsetPanel(id = "hyp_subtabs", type = "pills",

                tabPanel("Test Finder",
                  br(),
                  div(style = "background:#f8f0fc; border:1px solid #d63384; border-radius:10px; padding:20px; max-width:600px;",
                    h4(style = "color:#d63384; margin-top:0;", "Answer 3 questions to find the right test"),
                    fluidRow(
                      column(4, selectInput("finder_groups", "How many groups?",
                        choices = c("1 (one-sample)" = "1", "2 (two-sample)" = "2", "3 or more" = "3"), selected = "2")),
                      column(4, selectInput("finder_paired", "Paired or independent?",
                        choices = c("Independent" = "no", "Paired / matched" = "yes"), selected = "no")),
                      column(4, selectInput("finder_type", "Data type?",
                        choices = c("Continuous (numeric)" = "continuous", "Categorical (counts)" = "categorical"), selected = "continuous"))
                    ),
                    DTOutput("finder_result_tbl")
                  )
                ),

                tabPanel("Run a Test",
                  br(),
                  fluidRow(
                    column(3, selectInput("hyp_test_name", "Test", choices = c(
                      "--- One-sample ---" = "",
                      "One-sample t-test", "Wilcoxon signed-rank", "Sign test", "Binomial test",
                      "Chi-square goodness of fit",
                      "--- Two-sample ---" = "",
                      "Independent t-test (Welch)", "Paired t-test",
                      "Mann-Whitney U", "Wilcoxon signed-rank (paired)",
                      "Two-proportion z-test",
                      "--- Multi-sample ---" = "",
                      "One-way ANOVA", "Welch's ANOVA", "Kruskal-Wallis", "Tukey HSD",
                      "Friedman test"
                    ), selected = "Independent t-test (Welch)")),
                    column(3, selectInput("hyp_var1", "Variable 1 (or numeric)", choices = NULL)),
                    column(3, selectInput("hyp_var2", "Variable 2 (or grouping)", choices = c("None" = ""), selected = "")),
                    column(3, numericInput("hyp_mu0", "H0 value (one-sample)", value = 0))
                  ),
                  fluidRow(
                    column(3, numericInput("hyp_p0", "H0 proportion (binomial)", value = 0.5, min = 0, max = 1, step = 0.05)),
                    column(3, numericInput("hyp_conf", "Confidence level", value = 0.95, min = 0.80, max = 0.99, step = 0.01)),
                    column(3, actionButton("run_hyp_test", "Run test", class = "btn-primary", icon = icon("play"))),
                    column(3, tags$p(class = "text-muted", style = "margin-top:28px;", "Results appear below."))
                  ),
                  fluidRow(
                    column(7, h4("Test results"), DTOutput("hyp_result_tbl")),
                    column(5, h4("Visual"), plotOutput("hyp_plot", height = 340))
                  )
                ),

                tabPanel("Normality Tests",
                  br(),
                  fluidRow(
                    column(4, selectInput("norm_var", "Variable to test", choices = NULL)),
                    column(4, actionButton("run_norm", "Run all normality tests", class = "btn-primary", icon = icon("chart-area"))),
                    column(4, tags$p(class = "text-muted", style = "margin-top:28px;", "Tests Shapiro-Wilk, Anderson-Darling, and Kolmogorov-Smirnov."))
                  ),
                  fluidRow(
                    column(6, plotOutput("norm_qq_plot", height = 380)),
                    column(6, plotOutput("norm_hist_plot", height = 380))
                  ),
                  fluidRow(column(12, DTOutput("norm_results_tbl")))
                ),

                tabPanel("Power Analysis",
                  br(),
                  fluidRow(
                    column(4, selectInput("power_test", "Test type",
                      choices = c("Power: two-sample t-test", "Power: one-way ANOVA", "Power: chi-square"),
                      selected = "Power: two-sample t-test")),
                    column(4, numericInput("power_alpha", "Significance level", value = 0.05, min = 0.001, max = 0.10, step = 0.01)),
                    column(4, actionButton("run_power", "Calculate sample size", class = "btn-primary", icon = icon("calculator")))
                  ),
                  fluidRow(
                    column(6, DTOutput("power_result_tbl")),
                    column(6, plotOutput("power_curve_plot", height = 360))
                  )
                )
              )
            )
          )
        ),

tabPanel(
          "Variable Doctor",
          icon = icon("stethoscope"),
          br(),
          div(class = "tab-note", style = "border-left-color: #198754;",
              h4(style = "margin-top:0;", "Variable Doctor"),
              p("Intelligent diagnostics for every column. Issues ranked by severity. Click Fix to apply, Undo All to revert.")),
          fluidRow(
            column(3, uiOutput("doctor_card_critical")),
            column(3, uiOutput("doctor_card_warning")),
            column(3, uiOutput("doctor_card_info")),
            column(3, uiOutput("doctor_card_healthy"))
          ),
          fluidRow(column(12,
            actionButton("doctor_fix_all", "Fix all critical issues", class = "btn-danger", icon = icon("wrench"), style = "margin-right:8px;"),
            actionButton("doctor_undo", "Undo all fixes", class = "btn-outline-secondary", icon = icon("rotate-left")),
            downloadButton("doctor_download_report", "Download report", class = "btn-outline-primary", style = "margin-left:8px;")
          )),
          br(),
          fluidRow(column(12, DTOutput("doctor_table"))),
          br(),
          fluidRow(
            column(6, h4("Column health overview"), plotOutput("doctor_health_plot", height = 300)),
            column(6, h4("Fix log"), uiOutput("doctor_fix_log"))
          )
        ),

          "Categorical Association",
          br(),
          fluidRow(
            column(4, selectInput("assoc_x", "Row variable", choices = NULL)),
            column(4, selectInput("assoc_y", "Column variable", choices = NULL)),
            column(4, selectInput("assoc_strata", "Optional strata variable", choices = c("None" = ""), selected = ""))
          ),
          fluidRow(
            column(6, h4("Contingency table"), DTOutput("assoc_table")),
            column(6, h4("Test summary"), DTOutput("assoc_stats"))
          ),
          fluidRow(
            column(12, h4("Graphical view"), plotOutput("assoc_plot", height = 420))
          )
        ),
        tabPanel(
          "Regression and GLMs",
          br(),
          div(class = "tab-note", textOutput("model_guidance")),
          fluidRow(
            column(3, selectInput("model_family", "Model family", choices = c(
              "Linear (Gaussian)",
              "Binary logistic",
              "Binary probit",
              "Multinomial logistic",
              "Ordinal logistic",
              "Poisson",
              "Negative binomial",
              "Zero-inflated Poisson",
              "Hurdle Poisson",
              "Bias-reduced logistic"
            ), selected = "Linear (Gaussian)")),
            column(3, selectInput("model_outcome", "Outcome variable", choices = NULL)),
            column(4, selectizeInput("model_predictors", "Predictors", choices = NULL, multiple = TRUE)),
            column(2, numericInput("poly_degree", "Polynomial degree", value = 1, min = 1, max = 5))
          ),
          fluidRow(
            column(3, checkboxInput("model_interactions", "Include two-way interactions", FALSE)),
            column(3, checkboxInput("ordered_outcome", "Treat outcome as ordered (ordinal tab)", FALSE)),
            column(3, actionButton("fit_model", "Fit model", class = "btn-primary")),
            column(3, tags$p(strong("Formula")), textOutput("model_formula"))
          ),
          fluidRow(
            column(6, h4("Coefficient table"), DTOutput("model_coef_tbl")),
            column(6, h4("Model metrics"), DTOutput("model_metrics_tbl"))
          ),
          fluidRow(
            column(12, h4("Coefficient plot"), plotOutput("model_coef_plot", height = 420))
          ),
          fluidRow(
            column(12, h4("Predictions preview"), DTOutput("model_pred_tbl"))
          )
        ),
        
          tags$hr(),
          h4("Model diagnostics", style = "margin-top: 12px;"),
          fluidRow(
            column(6, plotOutput("diag_resid_fitted", height = 300)),
            column(6, plotOutput("diag_qq", height = 300))
          ),
          fluidRow(
            column(6, plotOutput("diag_scale_loc", height = 300)),
            column(6, h4("Assumption checks"), DTOutput("diag_checks_tbl"))
          ),
          conditionalPanel(
            condition = "true",
            fluidRow(column(12, DTOutput("diag_vif_tbl")))
          ),

tabPanel(
          "Multivariate",
          br(),
          fluidRow(
            column(4, selectInput("multi_method", "Method", choices = c("PCA", "MCA", "FAMD", "K-means", "PAM (Gower)"), selected = "PCA")),
            column(2, numericInput("multi_clusters", "Clusters (if needed)", value = 3, min = 2, max = 20)),
            column(2, actionButton("run_multi", "Run analysis", class = "btn-primary"))
          ),
          fluidRow(
            column(8, plotOutput("multi_plot", height = 440)),
            column(4, h4("Summary table"), DTOutput("multi_summary_tbl"))
          )
        ),
        tabPanel(
          "DOE",
          br(),
          h4("Generate a design"),
          fluidRow(
            column(3, selectInput("design_type", "Design type", choices = c("Full factorial", "Fractional factorial (2-level)", "Central composite", "Box-Behnken", "Latin hypercube", "D-optimal"), selected = "Full factorial")),
            column(2, numericInput("design_factors", "Number of factors", value = 3, min = 2, max = 12)),
            column(3, textInput("design_names", "Factor names (comma-separated)", value = "A, B, C")),
            column(2, numericInput("design_runs", "Runs (when applicable)", value = 12, min = 4, max = 200)),
            column(2, numericInput("design_levels", "Levels (full factorial)", value = 2, min = 2, max = 5))
          ),
          fluidRow(
            column(2, numericInput("design_center", "Center points", value = 4, min = 0, max = 20)),
            column(2, checkboxInput("design_randomize", "Randomize design", TRUE)),
            column(2, actionButton("generate_design", "Generate design", class = "btn-primary"))
          ),
          fluidRow(
            column(7, h4("Generated design"), DTOutput("design_tbl")),
            column(5, h4("Design plot"), plotOutput("design_plot", height = 320))
          ),
          tags$hr(),
          h4("Analyze an existing experimental data set"),
          fluidRow(
            column(3, selectInput("doe_response", "Response variable", choices = NULL)),
            column(5, selectizeInput("doe_factors_existing", "Factors in uploaded data", choices = NULL, multiple = TRUE)),
            column(2, checkboxInput("doe_interactions", "Two-way interactions", TRUE)),
            column(2, actionButton("analyze_doe", "Analyze DOE", class = "btn-primary"))
          ),
          fluidRow(
            column(6, h4("ANOVA / effect table"), DTOutput("doe_anova_tbl")),
            column(6, h4("Main-effects plot"), plotOutput("doe_effect_plot", height = 340))
          )
        ),
        
          fluidRow(
            column(6, h4("Scree / Elbow plot"), plotOutput("multi_scree", height = 300)),
            column(6, h4("Dendrogram (hierarchical)"), plotOutput("multi_dendro", height = 300))
          ),


        tabPanel(
          "Time Series",
          icon = icon("chart-line"),
          br(),
          div(class = "tab-note", style = "border-left-color: #0d6efd;",
              h4(style = "margin-top:0;", "Time Series Analysis"),
              p("Visualization, stationarity testing, ARIMA modeling, GARCH volatility modeling, and forecasting. ",
                "Select a date column and a numeric variable to begin.")),
          fluidRow(
            column(3, selectInput("ts_date_col", "Date column", choices = NULL)),
            column(3, selectInput("ts_value_col", "Value column (numeric)", choices = NULL)),
            column(2, selectInput("ts_freq", "Frequency", choices = c("Auto-detect" = "auto", "Daily (365)" = "365", "Trading days (252)" = "252", "Weekly (52)" = "52", "Monthly (12)" = "12", "Quarterly (4)" = "4", "Annual (1)" = "1"), selected = "auto")),
            column(2, checkboxInput("ts_use_returns", "Use log-returns (for GARCH)", FALSE)),
            column(2, checkboxInput("ts_use_diff", "First difference (for stationarity)", FALSE))
          ),
          tabsetPanel(id = "ts_subtabs", type = "pills",

            tabPanel("Visualization",
              br(),
              fluidRow(column(12, plotOutput("ts_line_plot", height = 360))),
              fluidRow(
                column(6, plotOutput("ts_acf_plot", height = 300)),
                column(6, plotOutput("ts_pacf_plot", height = 300))
              ),
              fluidRow(column(12, plotOutput("ts_decomp_plot", height = 400)))
            ),

            tabPanel("Stationarity",
              br(),
              fluidRow(
                column(3, actionButton("run_stationarity", "Run stationarity tests", class = "btn-primary", icon = icon("microscope"))),
                column(9, div(class = "tab-note", p("ADF and PP test H0: unit root (non-stationary). KPSS tests H0: stationary. ",
                  "If ADF rejects AND KPSS does not reject, the series is likely stationary.")))
              ),
              fluidRow(column(12, DTOutput("ts_stationarity_tbl"))),
              fluidRow(column(12, uiOutput("ts_stationarity_interpretation")))
            ),

            tabPanel("ARIMA",
              br(),
              fluidRow(
                column(3, actionButton("fit_arima", "Fit auto.arima", class = "btn-primary", icon = icon("wand-magic-sparkles"))),
                column(3, numericInput("arima_horizon", "Forecast horizon", value = 12, min = 1, max = 365)),
                column(6, div(class = "tab-note", p("Uses the ", code("forecast::auto.arima()"), " function to automatically select the best ARIMA(p,d,q) model by AIC.")))
              ),
              fluidRow(
                column(6, h4("Model summary"), verbatimTextOutput("arima_summary")),
                column(6, h4("Model metrics"), DTOutput("arima_metrics_tbl"))
              ),
              fluidRow(column(12, h4("Forecast"), plotOutput("arima_forecast_plot", height = 400))),
              fluidRow(
                column(6, h4("Residual diagnostics"), plotOutput("arima_resid_plot", height = 300)),
                column(6, h4("Ljung-Box test"), DTOutput("arima_ljung_tbl"))
              )
            ),

            tabPanel("GARCH",
              br(),
              div(class = "tab-note", style = "border-left-color: #dc3545;",
                  p(strong("Anjana Yatawara's domain."), " GARCH models capture volatility clustering — ",
                    "periods of high volatility followed by periods of low volatility. ",
                    "Check 'Use log-returns' above, then select a model below.")),
              fluidRow(
                column(3, selectInput("garch_type", "Model type", choices = c("GARCH(1,1)" = "sGARCH", "GJR-GARCH" = "GJR-GARCH", "EGARCH" = "EGARCH"), selected = "sGARCH")),
                column(2, numericInput("garch_p", "GARCH p", value = 1, min = 1, max = 3)),
                column(2, numericInput("garch_q", "GARCH q", value = 1, min = 1, max = 3)),
                column(3, actionButton("fit_garch", "Fit GARCH model", class = "btn-danger", icon = icon("fire"))),
                column(2, numericInput("garch_forecast_h", "Forecast h", value = 20, min = 1, max = 100))
              ),
              fluidRow(
                column(12, h4("Model summary"), verbatimTextOutput("garch_summary"))
              ),
              fluidRow(
                column(6, h4("Conditional volatility"), plotOutput("garch_vol_plot", height = 350)),
                column(6, h4("Standardized residuals"), plotOutput("garch_resid_plot", height = 350))
              ),
              fluidRow(
                column(6, h4("News impact curve"), plotOutput("garch_nic_plot", height = 300)),
                column(6, h4("Volatility forecast"), plotOutput("garch_forecast_plot", height = 300))
              ),
              fluidRow(column(12, DTOutput("garch_coef_tbl")))
            )
          )
        ),


        tabPanel(
          "Survival",
          icon = icon("heart-pulse"),
          br(),
          div(class = "tab-note", style = "border-left-color: #dc3545;",
              h4(style = "margin-top:0;", "Survival Analysis"),
              p("Kaplan-Meier curves, log-rank tests, Cox proportional hazards regression, and diagnostics. ",
                "Select a time variable and an event/status indicator (0 = censored, 1 = event).")),
          fluidRow(
            column(3, selectInput("surv_time", "Time variable", choices = NULL)),
            column(3, selectInput("surv_event", "Event indicator (0/1)", choices = NULL)),
            column(3, selectInput("surv_group", "Grouping variable (optional)", choices = c("None" = ""), selected = "")),
            column(3, actionButton("run_km", "Fit Kaplan-Meier", class = "btn-danger", icon = icon("play")))
          ),
          fluidRow(
            column(8, plotOutput("km_plot", height = 440)),
            column(4,
              h4("Survival summary"),
              DTOutput("km_summary_tbl"),
              br(),
              h4("Log-rank test"),
              DTOutput("km_logrank_tbl")
            )
          ),
          tags$hr(),
          h4("Cox Proportional Hazards Regression"),
          fluidRow(
            column(5, selectizeInput("cox_predictors", "Predictors", choices = NULL, multiple = TRUE)),
            column(3, actionButton("run_cox", "Fit Cox PH", class = "btn-danger", icon = icon("play"))),
            column(4, div(class = "tab-note", p("Cox PH models the hazard rate as a function of covariates. ",
              "Hazard ratio > 1 means higher risk; < 1 means lower risk.")))
          ),
          fluidRow(
            column(6, h4("Coefficient table (hazard ratios)"), DTOutput("cox_coef_tbl")),
            column(6, h4("Hazard ratio forest plot"), plotOutput("cox_forest_plot", height = 350))
          ),
          fluidRow(
            column(6, h4("Cox model metrics"), DTOutput("cox_metrics_tbl")),
            column(6, h4("PH assumption test (Schoenfeld)"), DTOutput("cox_ph_test_tbl"))
          )
        ),


        tabPanel(
          "Report",
          icon = icon("file-lines"),
          br(),
          div(class = "tab-note", style = "border-left-color: #198754;",
              h4(style = "margin-top:0;", "Analysis Report Generator"),
              p("Generate a downloadable report summarizing your data, diagnostics, and analyses. ",
                "The report includes the Variable Doctor findings, column profile, and all fixes applied.")),
          fluidRow(
            column(3, selectInput("report_format", "Format", choices = c("HTML" = "html", "Markdown (.md)" = "md"), selected = "html")),
            column(3, textInput("report_title", "Report title", value = "Kernel Report")),
            column(3, textInput("report_author", "Author", value = "")),
            column(3, downloadButton("generate_report", "Generate & Download Report", class = "btn-success", icon = icon("download")))
          ),
          tags$hr(),
          h4("Report preview"),
          fluidRow(column(12, uiOutput("report_preview"))),
          tags$hr(),
          h4("R Code Export"),
          fluidRow(
            column(12,
              div(class = "tab-note",
                p("Copy the R code below to reproduce your analysis outside of this app. ",
                  "This code uses the same packages and functions available in the workbench.")),
              verbatimTextOutput("code_export")
            )
          ),
          fluidRow(column(12,
            downloadButton("download_code", "Download R script (.R)", class = "btn-outline-primary")
          ))
        ),


        tabPanel(
          "Machine Learning",
          icon = icon("robot"),
          br(),
          div(class = "tab-note", style = "border-left-color: #6f42c1;",
              h4(style = "margin-top:0;", "Machine Learning Pipeline"),
              p("Train, evaluate, and compare multiple ML models. Supports classification and regression. ",
                "The pipeline auto-detects the task type from your outcome variable.")),
          fluidRow(
            column(3, selectInput("ml_outcome", "Outcome variable", choices = NULL)),
            column(4, selectizeInput("ml_predictors", "Predictor variables", choices = NULL, multiple = TRUE)),
            column(2, sliderInput("ml_split", "Train %", min = 50, max = 90, value = 80, step = 5)),
            column(3, numericInput("ml_seed", "Random seed", value = 42, min = 1))
          ),
          fluidRow(
            column(3, uiOutput("ml_task_display")),
            column(9,
              checkboxGroupInput("ml_methods", "Models to train:", inline = TRUE,
                choices = c("Decision Tree", "Random Forest", "XGBoost", "SVM", "KNN", "Naive Bayes", "Logistic Regression"),
                selected = c("Decision Tree", "Random Forest", "XGBoost"))
            )
          ),
          fluidRow(
            column(3, actionButton("ml_train", "Train all models", class = "btn-primary btn-lg", icon = icon("play"),
                                   style = "margin-top:4px;")),
            column(9, uiOutput("ml_status"))
          ),
          tags$hr(),
          tabsetPanel(id = "ml_subtabs", type = "pills",
            tabPanel("Model comparison",
              br(),
              fluidRow(column(12, h4("Performance comparison"), DTOutput("ml_comparison_tbl"))),
              fluidRow(
                column(6, h4("Metrics bar chart"), plotOutput("ml_comparison_plot", height = 360)),
                column(6, h4("ROC curves (classification)"), plotOutput("ml_roc_plot", height = 360))
              )
            ),
            tabPanel("Variable importance",
              br(),
              fluidRow(
                column(4, selectInput("ml_imp_model", "Select model", choices = NULL)),
                column(8, plotOutput("ml_importance_plot", height = 400))
              )
            ),
            tabPanel("Predictions",
              br(),
              fluidRow(
                column(4, selectInput("ml_pred_model", "Select model", choices = NULL)),
                column(8, plotOutput("ml_pred_plot", height = 400))
              ),
              fluidRow(column(12, DTOutput("ml_pred_tbl")))
            ),
            tabPanel("Confusion matrix",
              br(),
              fluidRow(
                column(4, selectInput("ml_cm_model", "Select model", choices = NULL)),
                column(4, plotOutput("ml_cm_plot", height = 350)),
                column(4, DTOutput("ml_cm_tbl"))
              )
            ),
            tabPanel("Decision tree",
              br(),
              fluidRow(column(12, plotOutput("ml_tree_plot", height = 500)))
            )
          )
        ),


        tabPanel(
          "Templates",
          icon = icon("swatchbook"),
          br(),
          div(class = "tab-note", style = "border-left-color: #20c997;",
              h4(style = "margin-top:0;", "Industry Analysis Templates"),
              p("Pre-built analysis workflows for common use cases. Select a template, map your columns, and get instant results. ",
                "Works with your uploaded data.")),
          tabsetPanel(id = "template_subtabs", type = "pills",

            tabPanel("Quality / SPC",
              icon = icon("industry"),
              br(),
              h4("Control Charts"),
              fluidRow(
                column(3, selectInput("spc_var", "Measurement variable", choices = NULL)),
                column(2, selectInput("spc_type", "Chart type", choices = c("Individuals (I-MR)" = "individuals", "X-bar & R" = "xbar", "P chart" = "p", "C chart" = "c"), selected = "individuals")),
                column(2, numericInput("spc_subgroup", "Subgroup size", value = 5, min = 2, max = 25)),
                column(3, actionButton("run_spc", "Generate chart", class = "btn-primary", icon = icon("chart-line"))),
                column(2, uiOutput("spc_ooc_count"))
              ),
              fluidRow(column(12, plotOutput("spc_chart", height = 400))),
              fluidRow(column(12, DTOutput("spc_stats_tbl"))),
              tags$hr(),
              h4("Process Capability (Cp/Cpk)"),
              fluidRow(
                column(3, selectInput("cap_var", "Measurement variable", choices = NULL)),
                column(2, numericInput("cap_lsl", "Lower spec limit (LSL)", value = NA)),
                column(2, numericInput("cap_usl", "Upper spec limit (USL)", value = NA)),
                column(3, actionButton("run_cap", "Calculate capability", class = "btn-primary", icon = icon("gauge-high"))),
                column(2)
              ),
              fluidRow(
                column(6, plotOutput("cap_plot", height = 350)),
                column(6, DTOutput("cap_tbl"))
              )
            ),

            tabPanel("Business / RFM",
              icon = icon("shop"),
              br(),
              h4("RFM Customer Segmentation"),
              div(class = "tab-note", p("RFM (Recency, Frequency, Monetary) analysis segments customers by purchase behavior. ",
                "Map your customer ID, transaction date, and amount columns below.")),
              fluidRow(
                column(3, selectInput("rfm_customer", "Customer ID column", choices = NULL)),
                column(3, selectInput("rfm_date", "Transaction date column", choices = NULL)),
                column(3, selectInput("rfm_amount", "Amount column", choices = NULL)),
                column(3, actionButton("run_rfm", "Run RFM analysis", class = "btn-primary", icon = icon("users")))
              ),
              fluidRow(
                column(7, DTOutput("rfm_tbl")),
                column(5, plotOutput("rfm_segment_plot", height = 350))
              ),
              fluidRow(
                column(6, plotOutput("rfm_scatter_plot", height = 350)),
                column(6, plotOutput("rfm_heatmap", height = 350))
              ),
              tags$hr(),
              h4("Pareto Chart (80/20 Analysis)"),
              fluidRow(
                column(4, selectInput("pareto_var", "Category variable", choices = NULL)),
                column(4, actionButton("run_pareto", "Generate Pareto chart", class = "btn-primary", icon = icon("chart-bar"))),
                column(4)
              ),
              fluidRow(column(12, plotOutput("pareto_plot", height = 400)))
            ),

            tabPanel("Healthcare",
              icon = icon("heart-pulse"),
              br(),
              h4("Heatmap (Expression / Correlation Matrix)"),
              fluidRow(
                column(4, selectizeInput("heat_vars", "Numeric variables for heatmap", choices = NULL, multiple = TRUE)),
                column(2, selectInput("heat_scale", "Scale", choices = c("None" = "none", "Row" = "row", "Column" = "column"), selected = "row")),
                column(2, numericInput("heat_max_vars", "Max variables", value = 30, min = 5, max = 100)),
                column(2, actionButton("run_heatmap", "Generate heatmap", class = "btn-primary", icon = icon("grip"))),
                column(2)
              ),
              fluidRow(column(12, plotOutput("template_heatmap", height = 550))),
              tags$hr(),
              h4("Volcano Plot (Differential Analysis)"),
              div(class = "tab-note", p("Upload data with columns for log2 fold-change and p-values (e.g., from DESeq2, limma, or similar). ",
                "Or use any two numeric columns to create a volcano-style plot.")),
              fluidRow(
                column(3, selectInput("volcano_fc", "Log2 fold-change column", choices = NULL)),
                column(3, selectInput("volcano_p", "P-value column", choices = NULL)),
                column(2, numericInput("volcano_fc_thresh", "FC threshold", value = 1, min = 0, step = 0.5)),
                column(2, numericInput("volcano_p_thresh", "P-value threshold", value = 0.05, min = 0.001, max = 0.1, step = 0.01)),
                column(2, actionButton("run_volcano", "Generate plot", class = "btn-primary", icon = icon("mountain")))
              ),
              fluidRow(column(12, plotOutput("volcano_plot", height = 450)))
            ),

            tabPanel("Sample datasets",
              icon = icon("database"),
              br(),
              h4("Load a sample dataset to explore"),
              fluidRow(
                column(4, selectInput("sample_dataset", "Dataset", choices = c(
                  "mtcars (cars)" = "mtcars", "iris (flowers)" = "iris", "airquality (environment)" = "airquality",
                  "ToothGrowth (health)" = "ToothGrowth", "PlantGrowth (experiment)" = "PlantGrowth",
                  "ChickWeight (growth)" = "ChickWeight", "CO2 (environment)" = "CO2",
                  "USArrests (social)" = "USArrests", "faithful (geology)" = "faithful",
                  "swiss (demographics)" = "swiss", "sleep (health)" = "sleep",
                  "chickwts (experiment)" = "chickwts", "InsectSprays (DOE)" = "InsectSprays",
                  "warpbreaks (quality)" = "warpbreaks", "Titanic (survival)" = "titanic_df"
                ), selected = "mtcars")),
                column(3, actionButton("load_sample", "Load dataset", class = "btn-success", icon = icon("download"))),
                column(5, uiOutput("sample_info"))
              )
            )
          )
        ),

tabPanel(
          "Methods Catalog",
          br(),
          div(class = "tab-note", "This registry maps methods you would expect in SAS/JMP-style workflows to R packages, and marks whether the current app implements them directly or treats them as optional/roadmap."),
          DTOutput("methods_tbl")
        )
      )
    )
  )
)

server <- function(input, output, session) {
  raw_data <- reactive({
    req(input$file)

    ext <- tolower(tools::file_ext(input$file$name))
    sheet <- if (ext %in% c("xlsx", "xls")) input$excel_sheet %||% NULL else NULL
    object_name <- if (ext == "rdata") input$rdata_object %||% NULL else NULL

    tryCatch(
      import_data(input$file$datapath, input$file$name, sheet = sheet, object_name = object_name),
      error = function(e) {
        showNotification(paste("Import error:", e$message), type = "error")
        NULL
      }
    )
  })

  analysis_data <- reactive({
    df <- raw_data()
    req(df)

    tryCatch(
      prep_data(df, clean_names = isTRUE(input$clean_names), drop_empty = isTRUE(input$drop_empty)),
      error = function(e) {
        showNotification(paste("Preparation error:", e$message), type = "error")
        NULL
      }
    )
  })

  output$import_detail_ui <- renderUI({
    req(input$file)
    ext <- tolower(tools::file_ext(input$file$name))

    if (ext %in% c("xlsx", "xls")) {
      sheets <- tryCatch(readxl::excel_sheets(input$file$datapath), error = function(e) character())
      if (length(sheets)) {
        return(selectInput("excel_sheet", "Excel sheet", choices = sheets, selected = sheets[1]))
      }
    }

    if (ext == "rdata") {
      objs <- tryCatch(list_rdata_frames(input$file$datapath), error = function(e) character())
      if (length(objs)) {
        return(selectInput("rdata_object", "Data frame inside RData", choices = objs, selected = objs[1]))
      }
    }

    NULL
  })

  observeEvent(analysis_data(), {
    df <- doctor_data()
    req(df)

    vars <- names(df)
    nums <- numeric_vars_of(df)
    cats <- categorical_vars_of(df)

    updateSelectInput(session, "univariate_var", choices = vars, selected = vars[1] %||% NULL)
    updateSelectInput(session, "assoc_x", choices = cats, selected = cats[1] %||% NULL)
    updateSelectInput(session, "assoc_y", choices = cats, selected = cats[min(2, length(cats))] %||% cats[1] %||% NULL)
    updateSelectInput(session, "assoc_strata", choices = c("None" = "", cats), selected = "")
    updateSelectInput(session, "model_outcome", choices = vars, selected = vars[1] %||% NULL)
    updateSelectizeInput(session, "model_predictors", choices = vars, selected = vars[vars != vars[1]][seq_len(min(2, max(0, length(vars) - 1)))] %||% character(0), server = TRUE)
    updateSelectInput(session, "doe_response", choices = nums, selected = nums[1] %||% NULL)
    updateSelectizeInput(session, "doe_factors_existing", choices = vars, selected = vars[seq_len(min(3, length(vars)))] %||% character(0), server = TRUE)
  }, ignoreNULL = FALSE)

  observeEvent(input$model_outcome, {
    df <- doctor_data()
    req(df, input$model_outcome)
    vars <- setdiff(names(df), input$model_outcome)
    selected <- intersect(input$model_predictors %||% character(0), vars)
    if (!length(selected)) selected <- vars[seq_len(min(2, length(vars)))] %||% character(0)
    updateSelectizeInput(session, "model_predictors", choices = vars, selected = selected, server = TRUE)
  }, ignoreNULL = FALSE)

  output$card_rows <- renderUI({
    df <- doctor_data()
    value <- if (is.null(df)) "-" else format(nrow(df), big.mark = ",")
    div(class = "stat-card", div(class = "label", "Rows"), div(class = "value", value))
  })

  output$card_cols <- renderUI({
    df <- doctor_data()
    value <- if (is.null(df)) "-" else format(ncol(df), big.mark = ",")
    div(class = "stat-card", div(class = "label", "Columns"), div(class = "value", value))
  })

  output$card_numeric <- renderUI({
    df <- doctor_data()
    value <- if (is.null(df)) "-" else length(numeric_vars_of(df))
    div(class = "stat-card", div(class = "label", "Numeric variables"), div(class = "value", value))
  })

  output$card_categorical <- renderUI({
    df <- doctor_data()
    value <- if (is.null(df)) "-" else length(categorical_vars_of(df))
    div(class = "stat-card", div(class = "label", "Categorical-like variables"), div(class = "value", value))
  })

  output$insight_notes <- renderUI({
    make_method_note(doctor_data())
  })

  output$data_preview <- renderDT({
    df <- doctor_data()
    req(df)
    datatable(df, options = list(pageLength = 10, scrollX = TRUE), filter = "top")
  })

  output$package_status_tbl <- renderDT({
    datatable(package_status(), options = list(dom = "tip", pageLength = 15, scrollX = TRUE), rownames = FALSE)
  })

  output$column_profile_tbl <- renderDT({
    df <- doctor_data()
    req(df)
    datatable(column_profile(df), options = list(pageLength = 10, scrollX = TRUE), rownames = FALSE)
  })

  output$missingness_plot <- renderPlot({
    df <- doctor_data()
    req(df)
    prof <- column_profile(df)
    validate(need(nrow(prof) > 0, "No columns available."))

    ggplot(prof, aes(x = reorder(variable, missing_pct), y = missing_pct)) +
      geom_col() +
      coord_flip() +
      labs(x = NULL, y = "Missing (%)", title = "Missingness by variable") +
      scale_y_continuous(labels = label_percent(scale = 1)) +
      theme_minimal(base_size = 12)
  })

  output$type_plot <- renderPlot({
    df <- doctor_data()
    req(df)
    prof <- column_profile(df)
    type_counts <- prof |>
      count(type, sort = TRUE)

    ggplot(type_counts, aes(x = reorder(type, n), y = n)) +
      geom_col() +
      coord_flip() +
      labs(x = NULL, y = "Variables", title = "Variable types") +
      theme_minimal(base_size = 12)
  })


  # ══ Variable Doctor Server Logic ══
  doctor_state <- reactiveValues(original_data = NULL, current_data = NULL, fix_log = character(), fix_count = 0)

  observeEvent(analysis_data(), {
    df <- analysis_data(); if (!is.null(df)) { doctor_state$original_data <- df; doctor_state$current_data <- df; doctor_state$fix_log <- character(); doctor_state$fix_count <- 0 }
  })

  doctor_data <- reactive({ if (!is.null(doctor_state$current_data)) doctor_state$current_data else analysis_data() })
  doctor_findings <- reactive({ df <- doctor_data(); req(df); diagnose_variables(df) })

  output$doctor_card_critical <- renderUI({ f <- doctor_findings(); n <- sum(f$severity=="critical"); cl <- if(n>0) "#dc3545" else "#198754"
    div(class="stat-card", style=paste0("border-left:4px solid ",cl,";"), div(class="label","Critical"), div(class="value",style=paste0("color:",cl,";"),n)) })
  output$doctor_card_warning <- renderUI({ f <- doctor_findings(); n <- sum(f$severity=="warning"); cl <- if(n>0) "#fd7e14" else "#198754"
    div(class="stat-card", style=paste0("border-left:4px solid ",cl,";"), div(class="label","Warnings"), div(class="value",style=paste0("color:",cl,";"),n)) })
  output$doctor_card_info <- renderUI({ f <- doctor_findings(); n <- sum(f$severity=="info")
    div(class="stat-card", style="border-left:4px solid #0d6efd;", div(class="label","Suggestions"), div(class="value",style="color:#0d6efd;",n)) })
  output$doctor_card_healthy <- renderUI({ df <- doctor_data(); f <- doctor_findings(); req(df); af <- length(unique(f$variable)); hl <- ncol(df)-af
    div(class="stat-card", style="border-left:4px solid #198754;", div(class="label","Healthy"), div(class="value",style="color:#198754;",hl)) })

  output$doctor_table <- renderDT({
    f <- doctor_findings()
    if (nrow(f)==0) return(datatable(data.frame(Message="All columns healthy!"), options=list(dom="t"), rownames=FALSE))
    dd <- f[, c("variable","severity","issue","detail"), drop=FALSE]
    dd$severity <- ifelse(f$severity=="critical", '<span style="color:#dc3545;font-weight:700;">CRITICAL</span>',
      ifelse(f$severity=="warning", '<span style="color:#fd7e14;font-weight:700;">WARNING</span>',
             '<span style="color:#0d6efd;font-weight:600;">INFO</span>'))
    dd$action <- ifelse(f$fix_action != "none",
      paste0('<button class="btn btn-sm btn-outline-primary" onclick="Shiny.setInputValue('doctor_fix_single','',
             f$fix_id, '',{priority:'event'})" style="font-size:0.75rem;padding:2px 8px;">',
             sapply(f$fix_action, icon_for_fix), '</button>'),
      '<span style="color:#999;font-size:0.8rem;">Manual review</span>')
    datatable(dd, escape=FALSE, options=list(pageLength=20, scrollX=TRUE, order=list(list(1,"asc")), dom="ftip"), rownames=FALSE,
              colnames=c("Variable","Severity","Issue","Details","Action"))
  })

  output$doctor_health_plot <- renderPlot({
    df <- doctor_data(); f <- doctor_findings(); req(df)
    av <- data.frame(variable=names(df), stringsAsFactors=FALSE)
    if (nrow(f)>0) {
      worst <- f |> mutate(sr=case_when(severity=="critical"~1,severity=="warning"~2,TRUE~3)) |>
        group_by(variable) |> summarise(ws=severity[which.min(sr)], .groups="drop")
      av <- av |> left_join(worst, by="variable") |> mutate(status=ifelse(is.na(ws),"healthy",ws))
    } else av$status <- "healthy"
    av$status <- factor(av$status, levels=c("critical","warning","info","healthy"), labels=c("Critical","Warning","Info","Healthy"))
    ggplot(av, aes(x=reorder(variable,as.numeric(status)), fill=status)) + geom_bar(width=0.7) + coord_flip() +
      scale_fill_manual(values=c("Critical"="#dc3545","Warning"="#fd7e14","Info"="#0d6efd","Healthy"="#198754"), drop=FALSE) +
      labs(x=NULL, y=NULL, fill="Status") + theme_minimal(base_size=12) +
      theme(legend.position="bottom", axis.text.x=element_blank(), axis.ticks.x=element_blank(), panel.grid=element_blank())
  })

  output$doctor_fix_log <- renderUI({
    lg <- doctor_state$fix_log
    if (length(lg)==0) return(tags$p(class="text-muted","No fixes applied yet."))
    tags$ul(style="font-size:0.85rem;", lapply(rev(lg), function(e) tags$li(style="margin-bottom:4px;", e)))
  })

  observeEvent(input$doctor_fix_single, {
    fid <- input$doctor_fix_single; f <- doctor_findings(); row <- f[f$fix_id==fid,,drop=FALSE]
    if (nrow(row)==0) return()
    result <- apply_fix(doctor_state$current_data, row$variable[1], row$fix_action[1])
    if (!is.null(result$data)) {
      doctor_state$current_data <- result$data; doctor_state$fix_count <- doctor_state$fix_count+1
      doctor_state$fix_log <- c(doctor_state$fix_log, result$message)
      showNotification(result$message, type="message", duration=3)
    }
  })

  observeEvent(input$doctor_fix_all, {
    f <- doctor_findings(); cr <- f[f$severity=="critical" & f$fix_action!="none",,drop=FALSE]
    if (nrow(cr)==0) { showNotification("No critical auto-fixes.", type="warning"); return() }
    df <- doctor_state$current_data; fc <- 0
    for (i in seq_len(nrow(cr))) {
      if (cr$variable[i] %in% names(df)) {
        r <- apply_fix(df, cr$variable[i], cr$fix_action[i])
        if (!is.null(r$data)) { df <- r$data; doctor_state$fix_log <- c(doctor_state$fix_log, r$message); fc <- fc+1 }
      }
    }
    doctor_state$current_data <- df; doctor_state$fix_count <- doctor_state$fix_count+fc
    showNotification(paste0("Applied ", fc, " critical fixes."), type="message")
  })

  observeEvent(input$doctor_undo, {
    if (!is.null(doctor_state$original_data)) {
      doctor_state$current_data <- doctor_state$original_data
      doctor_state$fix_log <- c(doctor_state$fix_log, paste0("UNDO: Reverted all ", doctor_state$fix_count, " fixes."))
      doctor_state$fix_count <- 0; showNotification("All fixes undone.", type="warning")
    }
  })

  output$doctor_download_report <- downloadHandler(
    filename = function() paste0("variable_doctor_", Sys.Date(), ".csv"),
    content = function(file) { f <- doctor_findings(); readr::write_csv(f[,c("variable","severity","issue","detail")], file) }
  )



  output$univariate_plot_out <- renderPlot({
    df <- doctor_data()
    req(df, input$univariate_var)
    var <- input$univariate_var
    x <- df[[var]]
    if (isTRUE(input$univariate_drop_na)) {
      df <- df[!is.na(df[[var]]), , drop = FALSE]
      x <- df[[var]]
    }

    validate(need(length(x) > 0, "No data available after filtering missing values."))

    var_type <- friendly_type(x)
    chosen <- input$univariate_plot
    if (chosen == "Auto") {
      chosen <- if (is.numeric(x)) "Histogram" else "Bar chart"
    }

    if (chosen == "Histogram") {
      validate(need(is.numeric(x), "Histogram requires a numeric variable."))
      print(
        ggplot(df, aes(x = .data[[var]])) +
          geom_histogram(bins = 30, color = "white") +
          labs(title = paste("Histogram of", var), x = var, y = "Count") +
          theme_minimal(base_size = 12)
      )
    } else if (chosen == "Density") {
      validate(need(is.numeric(x), "Density plot requires a numeric variable."))
      print(
        ggplot(df, aes(x = .data[[var]])) +
          geom_density(alpha = 0.6) +
          labs(title = paste("Density of", var), x = var, y = "Density") +
          theme_minimal(base_size = 12)
      )
    } else if (chosen == "Boxplot") {
      validate(need(is.numeric(x), "Boxplot requires a numeric variable."))
      print(
        ggplot(df, aes(y = .data[[var]], x = "")) +
          geom_boxplot() +
          labs(title = paste("Boxplot of", var), x = NULL, y = var) +
          theme_minimal(base_size = 12)
      )
    } else {
      x_fac <- coerce_categorical(x)
      plot_df <- data.frame(value = x_fac, stringsAsFactors = FALSE)
      print(
        ggplot(plot_df, aes(x = value)) +
          geom_bar() +
          coord_flip() +
          labs(title = paste("Bar chart of", var), x = var_type, y = "Count") +
          theme_minimal(base_size = 12)
      )
    }
  })

  output$univariate_summary_tbl <- renderDT({
    df <- doctor_data()
    req(df, input$univariate_var)
    x <- df[[input$univariate_var]]

    if (is.numeric(x)) {
      out <- data.frame(
        statistic = c("n", "missing", "mean", "sd", "min", "q25", "median", "q75", "max"),
        value = c(
          sum(!is.na(x)),
          sum(is.na(x)),
          mean(x, na.rm = TRUE),
          stats::sd(x, na.rm = TRUE),
          min(x, na.rm = TRUE),
          stats::quantile(x, 0.25, na.rm = TRUE),
          stats::median(x, na.rm = TRUE),
          stats::quantile(x, 0.75, na.rm = TRUE),
          max(x, na.rm = TRUE)
        )
      )
    } else {
      tab <- sort(table(coerce_categorical(x), useNA = "ifany"), decreasing = TRUE)
      out <- data.frame(level = names(tab), count = as.integer(tab), proportion = as.numeric(tab) / sum(tab))
    }

    datatable(out, options = list(dom = "tip", pageLength = 10, scrollX = TRUE), rownames = FALSE)
  })

  assoc_table_reactive <- reactive({
    df <- doctor_data()
    req(df, input$assoc_x, input$assoc_y)
    x <- coerce_categorical(df[[input$assoc_x]])
    y <- coerce_categorical(df[[input$assoc_y]])
    table(x, y, useNA = "no")
  })

  output$assoc_table <- renderDT({
    tbl <- assoc_table_reactive()
    datatable(as.data.frame.matrix(tbl), options = list(dom = "tip", scrollX = TRUE))
  })

  output$assoc_stats <- renderDT({
    df <- doctor_data()
    req(df, input$assoc_x, input$assoc_y)
    tbl <- assoc_table_reactive()
    validate(need(all(dim(tbl) >= 2), "Choose variables with at least 2 levels each."))

    chi <- suppressWarnings(chisq.test(tbl, correct = FALSE))
    fisher_p <- tryCatch(fisher.test(tbl)$p.value, error = function(e) NA_real_)
    mcnemar_p <- if (all(dim(tbl) == c(2, 2))) tryCatch(mcnemar.test(tbl)$p.value, error = function(e) NA_real_) else NA_real_
    cmh_p <- NA_real_

    if (!is.null(input$assoc_strata) && nzchar(input$assoc_strata)) {
      z <- coerce_categorical(df[[input$assoc_strata]])
      arr <- xtabs(~ x + y + z, data = data.frame(x = coerce_categorical(df[[input$assoc_x]]), y = coerce_categorical(df[[input$assoc_y]]), z = z))
      cmh_p <- tryCatch(mantelhaen.test(arr)$p.value, error = function(e) NA_real_)
    }

    v <- if (has_pkg("DescTools")) {
      tryCatch(DescTools::CramerV(tbl), error = function(e) cramers_v_manual(tbl))
    } else {
      cramers_v_manual(tbl)
    }

    effect_tbl <- odds_and_risk(tbl)
    out <- data.frame(
      metric = c("Chi-square statistic", "Chi-square p-value", "Fisher exact p-value", "McNemar p-value", "CMH p-value", "Cramer's V"),
      value = c(unname(chi$statistic), chi$p.value, fisher_p, mcnemar_p, cmh_p, v),
      stringsAsFactors = FALSE
    )

    if (!is.null(effect_tbl)) {
      out <- bind_rows(out, effect_tbl)
    }

    datatable(out, options = list(dom = "tip", scrollX = TRUE), rownames = FALSE)
  })

  output$assoc_plot <- renderPlot({
    tbl <- assoc_table_reactive()
    validate(need(all(dim(tbl) >= 2), "Choose variables with at least 2 levels each."))

    if (has_pkg("vcd")) {
      vcd::mosaic(tbl, shade = TRUE, legend = TRUE, main = "Mosaic plot with residual shading")
    } else {
      mosaicplot(tbl, color = TRUE, main = "Mosaic plot")
    }
  })

  output$model_guidance <- renderText({
    fam <- input$model_family %||% "Linear (Gaussian)"
    switch(
      fam,
      "Linear (Gaussian)" = "Use for continuous outcomes. Polynomial degree >1 is most meaningful with a single predictor.",
      "Binary logistic" = "Use for two-level outcomes. Coefficients are on the log-odds scale.",
      "Binary probit" = "Like logistic regression but with a probit link.",
      "Multinomial logistic" = "Use for nominal outcomes with more than two classes.",
      "Ordinal logistic" = "Use for ordered outcomes. You can coerce the selected outcome to ordered.",
      "Poisson" = "Use for non-negative counts when variance is roughly comparable to the mean.",
      "Negative binomial" = "Use for overdispersed counts.",
      "Zero-inflated Poisson" = "Use when counts have more zeros than a standard Poisson model expects.",
      "Hurdle Poisson" = "Two-part count model: zero process plus positive-count process.",
      "Bias-reduced logistic" = "Useful when separation or rare events make standard logistic estimates unstable."
    )
  })

  output$model_formula <- renderText({
    req(input$model_outcome)
    build_formula_text(input$model_outcome, input$model_predictors %||% character(0), input$model_interactions, input$poly_degree)
  })

  fitted_model <- eventReactive(input$fit_model, {
    df <- doctor_data()
    req(df, input$model_outcome)
    validate(need(length(input$model_predictors) >= 1 || input$model_family == "Linear (Gaussian)", "Select at least one predictor."))

    model_df <- df[, unique(c(input$model_outcome, input$model_predictors)), drop = FALSE]
    model_df <- safe_complete(model_df)
    validate(need(nrow(model_df) >= 5, "Not enough complete rows to fit a model."))

    formula_text <- build_formula_text(input$model_outcome, input$model_predictors %||% character(0), input$model_interactions, input$poly_degree)
    form <- stats::as.formula(formula_text)
    outcome <- model_df[[input$model_outcome]]

    if (input$model_family == "Linear (Gaussian)") {
      validate(need(is.numeric(outcome), "Linear regression requires a numeric outcome."))
      return(stats::lm(form, data = model_df))
    }

    if (input$model_family == "Binary logistic") {
      validate(need((is.factor(outcome) && nlevels(outcome) == 2) || (is.numeric(outcome) && all(stats::na.omit(outcome) %in% c(0, 1))), "Binary logistic requires a 2-level outcome or 0/1 numeric outcome."))
      return(stats::glm(form, data = model_df, family = binomial(link = "logit")))
    }

    if (input$model_family == "Binary probit") {
      validate(need((is.factor(outcome) && nlevels(outcome) == 2) || (is.numeric(outcome) && all(stats::na.omit(outcome) %in% c(0, 1))), "Binary probit requires a 2-level outcome or 0/1 numeric outcome."))
      return(stats::glm(form, data = model_df, family = binomial(link = "probit")))
    }

    if (input$model_family == "Multinomial logistic") {
      validate(need(has_pkg("nnet"), "Install 'nnet' for multinomial logistic regression."))
      model_df[[input$model_outcome]] <- factor(outcome)
      validate(need(nlevels(model_df[[input$model_outcome]]) >= 3, "Multinomial logistic requires 3 or more outcome classes."))
      return(nnet::multinom(form, data = model_df, trace = FALSE))
    }

    if (input$model_family == "Ordinal logistic") {
      validate(need(has_pkg("ordinal"), "Install 'ordinal' for ordinal logistic regression."))
      model_df[[input$model_outcome]] <- if (isTRUE(input$ordered_outcome)) ordered(outcome) else outcome
      validate(need(is.ordered(model_df[[input$model_outcome]]), "The selected outcome must be an ordered factor, or enable 'Treat outcome as ordered'."))
      return(ordinal::clm(form, data = model_df))
    }

    if (input$model_family == "Poisson") {
      validate(need(is.numeric(outcome) && all(stats::na.omit(outcome) >= 0), "Poisson regression requires a non-negative numeric count outcome."))
      return(stats::glm(form, data = model_df, family = poisson(link = "log")))
    }

    if (input$model_family == "Negative binomial") {
      validate(need(has_pkg("MASS"), "Install 'MASS' for negative binomial regression."))
      validate(need(is.numeric(outcome) && all(stats::na.omit(outcome) >= 0), "Negative binomial regression requires a non-negative numeric count outcome."))
      return(MASS::glm.nb(form, data = model_df))
    }

    if (input$model_family == "Zero-inflated Poisson") {
      validate(need(has_pkg("pscl"), "Install 'pscl' for zero-inflated Poisson models."))
      validate(need(is.numeric(outcome) && all(stats::na.omit(outcome) >= 0), "Zero-inflated Poisson requires a non-negative numeric count outcome."))
      rhs <- paste(input$model_predictors, collapse = " + ")
      zi_form <- stats::as.formula(paste(input$model_outcome, "~", rhs, "|", rhs))
      return(pscl::zeroinfl(zi_form, data = model_df, dist = "poisson"))
    }

    if (input$model_family == "Hurdle Poisson") {
      validate(need(has_pkg("pscl"), "Install 'pscl' for hurdle models."))
      validate(need(is.numeric(outcome) && all(stats::na.omit(outcome) >= 0), "Hurdle Poisson requires a non-negative numeric count outcome."))
      rhs <- paste(input$model_predictors, collapse = " + ")
      hu_form <- stats::as.formula(paste(input$model_outcome, "~", rhs, "|", rhs))
      return(pscl::hurdle(hu_form, data = model_df, dist = "poisson"))
    }

    if (input$model_family == "Bias-reduced logistic") {
      validate(need(has_pkg("brglm2"), "Install 'brglm2' for bias-reduced logistic regression."))
      validate(need((is.factor(outcome) && nlevels(outcome) == 2) || (is.numeric(outcome) && all(stats::na.omit(outcome) %in% c(0, 1))), "Bias-reduced logistic requires a 2-level outcome or 0/1 numeric outcome."))
      return(stats::glm(form, data = model_df, family = binomial(link = "logit"), method = brglm2::brglmFit))
    }

    stop("Unsupported model family.")
  })

  output$model_coef_tbl <- renderDT({
    fit <- fitted_model()
    coef_tbl <- extract_coefficients(fit)
    datatable(coef_tbl, options = list(pageLength = 10, scrollX = TRUE), rownames = FALSE)
  })

  output$model_metrics_tbl <- renderDT({
    fit <- fitted_model()
    datatable(model_metrics(fit), options = list(dom = "tip", scrollX = TRUE), rownames = FALSE)
  })

  output$model_coef_plot <- renderPlot({
    fit <- fitted_model()
    coef_tbl <- extract_coefficients(fit)
    validate(need(nrow(coef_tbl) > 0, "No coefficients available."))

    coef_tbl <- coef_tbl |>
      filter(!grepl("Intercept|\\|", term))

    validate(need(nrow(coef_tbl) > 0, "No non-intercept coefficients available to plot."))

    if ("class_level" %in% names(coef_tbl)) {
      print(
        ggplot(coef_tbl, aes(x = reorder(term, estimate), y = estimate, ymin = conf_low, ymax = conf_high)) +
          geom_pointrange() +
          coord_flip() +
          facet_wrap(~ class_level, scales = "free_y") +
          labs(x = NULL, y = "Estimate", title = "Coefficient plot") +
          theme_minimal(base_size = 12)
      )
    } else if ("component" %in% names(coef_tbl)) {
      print(
        ggplot(coef_tbl, aes(x = reorder(term, estimate), y = estimate, ymin = conf_low, ymax = conf_high)) +
          geom_pointrange() +
          coord_flip() +
          facet_wrap(~ component, scales = "free_y") +
          labs(x = NULL, y = "Estimate", title = "Coefficient plot") +
          theme_minimal(base_size = 12)
      )
    } else {
      print(
        ggplot(coef_tbl, aes(x = reorder(term, estimate), y = estimate, ymin = conf_low, ymax = conf_high)) +
          geom_pointrange() +
          coord_flip() +
          labs(x = NULL, y = "Estimate", title = "Coefficient plot") +
          theme_minimal(base_size = 12)
      )
    }
  })

  output$model_pred_tbl <- renderDT({
    fit <- fitted_model()
    df <- doctor_data()
    req(df, input$model_outcome)
    pred_df <- df[, unique(c(input$model_outcome, input$model_predictors)), drop = FALSE]
    pred_df <- safe_complete(pred_df)

    pred <- tryCatch({
      if (inherits(fit, "multinom")) {
        predict(fit, newdata = pred_df, type = "class")
      } else if (inherits(fit, "clm")) {
        as.vector(predict(fit, newdata = pred_df, type = "prob")$fit[, 1])
      } else if (inherits(fit, c("glm", "zeroinfl", "hurdle"))) {
        predict(fit, newdata = pred_df, type = "response")
      } else {
        predict(fit, newdata = pred_df)
      }
    }, error = function(e) rep(NA, nrow(pred_df)))

    out <- pred_df |>
      mutate(.prediction = pred) |>
      head(25)

    datatable(out, options = list(pageLength = 10, scrollX = TRUE), rownames = FALSE)
  })

  multi_result <- eventReactive(input$run_multi, {
    df <- doctor_data()
    req(df)

    if (input$multi_method == "PCA") {
      num <- df[, numeric_vars_of(df), drop = FALSE]
      num <- num[, vapply(num, function(x) stats::sd(x, na.rm = TRUE) > 0, logical(1)), drop = FALSE]
      num <- safe_complete(num)
      validate(need(ncol(num) >= 2, "PCA requires at least two non-constant numeric variables."))
      validate(need(nrow(num) >= 3, "PCA requires at least three complete rows."))
      fit <- stats::prcomp(num, center = TRUE, scale. = TRUE)
      return(list(method = "PCA", fit = fit, data = num))
    }

    if (input$multi_method == "MCA") {
      validate(need(has_pkg("FactoMineR"), "Install 'FactoMineR' for MCA."))
      cat_df <- df[, categorical_vars_of(df), drop = FALSE]
      cat_df <- cat_df |>
        mutate(across(everything(), coerce_categorical)) |>
        safe_complete()
      validate(need(ncol(cat_df) >= 2, "MCA requires at least two categorical variables."))
      fit <- FactoMineR::MCA(cat_df, graph = FALSE)
      return(list(method = "MCA", fit = fit, data = cat_df))
    }

    if (input$multi_method == "FAMD") {
      validate(need(has_pkg("FactoMineR"), "Install 'FactoMineR' for FAMD."))
      keep <- df[, vapply(df, function(x) is.numeric(x) || is.factor(x) || is.character(x) || is.logical(x) || is.ordered(x), logical(1)), drop = FALSE]
      keep <- keep |>
        mutate(across(where(function(x) is.character(x) || is.logical(x)), coerce_categorical)) |>
        safe_complete()
      validate(need(length(numeric_vars_of(keep)) >= 1, "FAMD requires at least one numeric variable."))
      validate(need(length(categorical_vars_of(keep)) >= 1, "FAMD requires at least one categorical variable."))
      fit <- FactoMineR::FAMD(keep, graph = FALSE)
      return(list(method = "FAMD", fit = fit, data = keep))
    }

    if (input$multi_method == "K-means") {
      num <- df[, numeric_vars_of(df), drop = FALSE]
      num <- num[, vapply(num, function(x) stats::sd(x, na.rm = TRUE) > 0, logical(1)), drop = FALSE]
      num <- safe_complete(num)
      validate(need(ncol(num) >= 2, "K-means requires at least two non-constant numeric variables."))
      validate(need(nrow(num) >= input$multi_clusters, "Number of clusters must be smaller than the number of complete rows."))
      scaled <- scale(num)
      fit <- stats::kmeans(scaled, centers = input$multi_clusters, nstart = 25)
      return(list(method = "K-means", fit = fit, data = as.data.frame(scaled)))
    }

    if (input$multi_method == "PAM (Gower)") {
      validate(need(has_pkg("cluster"), "Install 'cluster' for PAM/Gower clustering."))
      keep <- df[, vapply(df, function(x) is.numeric(x) || is.factor(x) || is.character(x) || is.logical(x), logical(1)), drop = FALSE]
      keep <- keep |>
        mutate(across(where(function(x) is.character(x) || is.logical(x)), coerce_categorical)) |>
        safe_complete()
      validate(need(ncol(keep) >= 2, "PAM (Gower) requires at least two usable variables."))
      diss <- cluster::daisy(keep, metric = "gower")
      fit <- cluster::pam(diss, k = input$multi_clusters, diss = TRUE)
      return(list(method = "PAM", fit = fit, data = keep, diss = diss))
    }
  })

  output$multi_plot <- renderPlot({
    res <- multi_result()

    if (res$method == "PCA") {
      scores <- as.data.frame(res$fit$x[, 1:2, drop = FALSE])
      ggplot(scores, aes(x = PC1, y = PC2)) +
        geom_point(alpha = 0.7) +
        labs(title = "PCA scores plot", x = "PC1", y = "PC2") +
        theme_minimal(base_size = 12)
    } else if (res$method == "MCA") {
      coords <- as.data.frame(res$fit$ind$coord[, 1:2, drop = FALSE])
      names(coords) <- c("Dim1", "Dim2")
      ggplot(coords, aes(x = Dim1, y = Dim2)) +
        geom_point(alpha = 0.7) +
        labs(title = "MCA individual map", x = "Dim 1", y = "Dim 2") +
        theme_minimal(base_size = 12)
    } else if (res$method == "FAMD") {
      coords <- as.data.frame(res$fit$ind$coord[, 1:2, drop = FALSE])
      names(coords) <- c("Dim1", "Dim2")
      ggplot(coords, aes(x = Dim1, y = Dim2)) +
        geom_point(alpha = 0.7) +
        labs(title = "FAMD individual map", x = "Dim 1", y = "Dim 2") +
        theme_minimal(base_size = 12)
    } else if (res$method == "K-means") {
      pc <- stats::prcomp(res$data, center = TRUE, scale. = TRUE)
      scores <- as.data.frame(pc$x[, 1:2, drop = FALSE])
      scores$cluster <- factor(res$fit$cluster)
      ggplot(scores, aes(x = PC1, y = PC2, color = cluster)) +
        geom_point(alpha = 0.8) +
        labs(title = "K-means clustering projected onto first two PCs", x = "PC1", y = "PC2") +
        theme_minimal(base_size = 12)
    } else {
      coords <- as.data.frame(stats::cmdscale(res$diss, k = 2))
      names(coords) <- c("Dim1", "Dim2")
      coords$cluster <- factor(res$fit$clustering)
      ggplot(coords, aes(x = Dim1, y = Dim2, color = cluster)) +
        geom_point(alpha = 0.8) +
        labs(title = "PAM/Gower clustering map", x = "Coordinate 1", y = "Coordinate 2") +
        theme_minimal(base_size = 12)
    }
  })

  output$multi_summary_tbl <- renderDT({
    res <- multi_result()

    if (res$method == "PCA") {
      imp <- summary(res$fit)$importance
      out <- data.frame(
        component = colnames(imp),
        std_dev = as.numeric(imp[1, ]),
        prop_var = as.numeric(imp[2, ]),
        cum_prop = as.numeric(imp[3, ]),
        row.names = NULL,
        check.names = FALSE
      )
    } else if (res$method %in% c("MCA", "FAMD")) {
      eig <- as.data.frame(res$fit$eig)
      eig$dimension <- paste0("Dim ", seq_len(nrow(eig)))
      out <- eig[, c("dimension", names(eig)[1:3])]
    } else {
      tab <- table(if (res$method == "K-means") res$fit$cluster else res$fit$clustering)
      out <- data.frame(cluster = names(tab), size = as.integer(tab), row.names = NULL)
    }

    datatable(out, options = list(dom = "tip", pageLength = 10, scrollX = TRUE), rownames = FALSE)
  })

  parsed_design_names <- reactive({
    nm <- strsplit(input$design_names %||% "", ",")[[1]]
    nm <- trimws(nm)
    nm <- nm[nm != ""]
    n <- input$design_factors %||% 3
    if (length(nm) < n) {
      nm <- c(nm, paste0("X", seq(length(nm) + 1, n)))
    }
    nm[seq_len(n)]
  })

  generated_design <- eventReactive(input$generate_design, {
    vars <- parsed_design_names()
    tryCatch(
      make_design(
        type = input$design_type,
        vars = vars,
        runs = input$design_runs,
        levels = input$design_levels,
        center_points = input$design_center,
        randomize = isTRUE(input$design_randomize)
      ),
      error = function(e) {
        showNotification(paste("Design generation error:", e$message), type = "error")
        NULL
      }
    )
  })

  output$design_tbl <- renderDT({
    des <- generated_design()
    req(des)
    datatable(des, options = list(pageLength = 10, scrollX = TRUE), rownames = FALSE)
  })

  output$design_plot <- renderPlot({
    des <- generated_design()
    req(des)
    validate(need(ncol(des) >= 2, "Need at least two columns to plot the design."))

    plot_df <- des
    plot_df$.run_order <- seq_len(nrow(plot_df))
    ggplot(plot_df, aes(x = .data[[names(plot_df)[1]]], y = .data[[names(plot_df)[2]]], label = .run_order)) +
      geom_point(size = 3, alpha = 0.8) +
      geom_text(vjust = -0.7, size = 3) +
      labs(title = "Design map (first two factors)", x = names(plot_df)[1], y = names(plot_df)[2]) +
      theme_minimal(base_size = 12)
  })

  doe_analysis <- eventReactive(input$analyze_doe, {
    df <- doctor_data()
    req(df, input$doe_response, input$doe_factors_existing)
    validate(need(length(input$doe_factors_existing) >= 1, "Select at least one DOE factor."))

    keep <- unique(c(input$doe_response, input$doe_factors_existing))
    doe_df <- df[, keep, drop = FALSE]
    doe_df <- safe_complete(doe_df)
    validate(need(nrow(doe_df) >= 5, "Not enough complete rows for DOE analysis."))

    rhs <- paste(input$doe_factors_existing, collapse = " + ")
    if (isTRUE(input$doe_interactions) && length(input$doe_factors_existing) > 1) {
      rhs <- paste0("(", rhs, ")^2")
    }
    form <- stats::as.formula(paste(input$doe_response, "~", rhs))
    fit <- stats::lm(form, data = doe_df)
    list(fit = fit, data = doe_df)
  })

  output$doe_anova_tbl <- renderDT({
    res <- doe_analysis()
    out <- as.data.frame(stats::anova(res$fit))
    out$term <- rownames(out)
    rownames(out) <- NULL
    out <- out[, c("term", setdiff(names(out), "term")), drop = FALSE]
    datatable(out, options = list(dom = "tip", scrollX = TRUE), rownames = FALSE)
  })

  output$doe_effect_plot <- renderPlot({
    res <- doe_analysis()
    df <- res$data
    response <- input$doe_response
    factors <- input$doe_factors_existing

    effect_df <- purrr::map_dfr(factors, function(fct) {
      df |>
        mutate(.level = as.factor(.data[[fct]])) |>
        group_by(.level) |>
        summarise(mean_response = mean(.data[[response]], na.rm = TRUE), .groups = "drop") |>
        mutate(factor = fct)
    })

    ggplot(effect_df, aes(x = .level, y = mean_response, group = 1)) +
      geom_line() +
      geom_point(size = 2.5) +
      facet_wrap(~ factor, scales = "free_x") +
      labs(title = "Main-effects plot", x = "Factor level", y = paste("Mean", response)) +
      theme_minimal(base_size = 12)
  })

  output$methods_tbl <- renderDT({
    datatable(methods_catalog(), options = list(pageLength = 15, scrollX = TRUE), filter = "top", rownames = FALSE)
  })

  output$download_clean_data <- downloadHandler(
    filename = function() paste0("clean_data_", Sys.Date(), ".csv"),
    content = function(file) {
      df <- doctor_data()
      req(df)
      readr::write_csv(df, file)
    }
  )

  output$download_profile <- downloadHandler(
    filename = function() paste0("column_profile_", Sys.Date(), ".csv"),
    content = function(file) {
      df <- doctor_data()
      req(df)
      readr::write_csv(column_profile(df), file)
    }
  )

  output$download_methods <- downloadHandler(
    filename = function() paste0("method_catalog_", Sys.Date(), ".csv"),
    content = function(file) {
      readr::write_csv(methods_catalog(), file)
    }
  )

  output$download_design <- downloadHandler(
    filename = function() paste0("doe_design_", Sys.Date(), ".csv"),
    content = function(file) {
      des <- generated_design()
      req(des)
      readr::write_csv(des, file)
    }
  )

  output$download_qmd <- downloadHandler(
    filename = function() "report_template.qmd",
    content = function(file) {
      src <- file.path(APP_DIR, "report_template.qmd")
      if (!file.exists(src)) stop("report_template.qmd was not found in the app directory.")
      file.copy(src, file, overwrite = TRUE)
    }
  )
}



  # ══════════════════════════════════════════════════════════════
  # BIVARIATE EDA SERVER
  # ══════════════════════════════════════════════════════════════

  observe({
    df <- doctor_data(); req(df)
    vars <- names(df)
    updateSelectInput(session, "bi_x", choices = vars, selected = vars[1])
    updateSelectInput(session, "bi_y", choices = vars, selected = if (length(vars) >= 2) vars[2] else vars[1])
  })

  bi_types <- reactive({
    df <- doctor_data(); req(df, input$bi_x, input$bi_y)
    x <- df[[input$bi_x]]; y <- df[[input$bi_y]]
    x_type <- if (is.numeric(x)) "numeric" else "categorical"
    y_type <- if (is.numeric(y)) "numeric" else "categorical"
    n_groups <- if (x_type == "categorical") length(unique(x[!is.na(x)])) else if (y_type == "categorical") length(unique(y[!is.na(y)])) else NA
    list(x = x_type, y = y_type, n_groups = n_groups)
  })

  observe({
    bt <- bi_types(); req(bt)
    rec <- recommend_test(bt$x, bt$y, bt$n_groups %||% 2)
    choices <- c("Auto-recommend", rec$test)
    updateSelectInput(session, "bi_test", choices = choices, selected = "Auto-recommend")
    # Auto plot type
    if (bt$x == "numeric" && bt$y == "numeric") {
      updateSelectInput(session, "bi_plot_type",
        choices = c("Auto", "Scatter", "Scatter + Linear fit", "Scatter + Loess"),
        selected = "Auto")
    } else if ((bt$x == "numeric" && bt$y == "categorical") || (bt$x == "categorical" && bt$y == "numeric")) {
      updateSelectInput(session, "bi_plot_type",
        choices = c("Auto", "Boxplot by group", "Violin by group"),
        selected = "Auto")
    } else {
      updateSelectInput(session, "bi_plot_type",
        choices = c("Auto", "Grouped bar chart"),
        selected = "Auto")
    }
  })

  output$bi_plot <- renderPlot({
    df <- doctor_data(); req(df, input$bi_x, input$bi_y)
    xvar <- input$bi_x; yvar <- input$bi_y
    bt <- bi_types(); pt <- input$bi_plot_type

    plot_df <- df[stats::complete.cases(df[[xvar]], df[[yvar]]), , drop = FALSE]
    validate(need(nrow(plot_df) >= 3, "Not enough complete cases to plot."))

    if (bt$x == "numeric" && bt$y == "numeric") {
      p <- ggplot(plot_df, aes(x = .data[[xvar]], y = .data[[yvar]])) +
        geom_point(alpha = 0.5, size = 2, color = "#6f42c1")
      if (pt %in% c("Auto", "Scatter + Linear fit"))
        p <- p + geom_smooth(method = "lm", se = TRUE, color = "#dc3545", linewidth = 1)
      if (pt == "Scatter + Loess")
        p <- p + geom_smooth(method = "loess", se = TRUE, color = "#0d6efd", linewidth = 1)
      p + labs(title = paste(yvar, "vs.", xvar), x = xvar, y = yvar) + theme_minimal(base_size = 13)

    } else if ((bt$x == "categorical" && bt$y == "numeric") || (bt$x == "numeric" && bt$y == "categorical")) {
      cat_var <- if (bt$x == "categorical") xvar else yvar
      num_var <- if (bt$x == "numeric") xvar else yvar
      plot_df[[cat_var]] <- factor(plot_df[[cat_var]])
      if (pt %in% c("Auto", "Boxplot by group")) {
        ggplot(plot_df, aes(x = .data[[cat_var]], y = .data[[num_var]], fill = .data[[cat_var]])) +
          geom_boxplot(alpha = 0.7, show.legend = FALSE) +
          stat_summary(fun = mean, geom = "point", shape = 18, size = 3, color = "#dc3545") +
          labs(title = paste(num_var, "by", cat_var), x = cat_var, y = num_var) +
          theme_minimal(base_size = 13) + theme(axis.text.x = element_text(angle = 45, hjust = 1))
      } else {
        ggplot(plot_df, aes(x = .data[[cat_var]], y = .data[[num_var]], fill = .data[[cat_var]])) +
          geom_violin(alpha = 0.6, show.legend = FALSE) +
          geom_boxplot(width = 0.15, fill = "white", alpha = 0.8) +
          labs(title = paste(num_var, "by", cat_var), x = cat_var, y = num_var) +
          theme_minimal(base_size = 13) + theme(axis.text.x = element_text(angle = 45, hjust = 1))
      }
    } else {
      plot_df[[xvar]] <- factor(plot_df[[xvar]]); plot_df[[yvar]] <- factor(plot_df[[yvar]])
      ggplot(plot_df, aes(x = .data[[xvar]], fill = .data[[yvar]])) +
        geom_bar(position = "dodge") +
        labs(title = paste(yvar, "by", xvar), x = xvar, fill = yvar) +
        theme_minimal(base_size = 13) + theme(axis.text.x = element_text(angle = 45, hjust = 1))
    }
  })

  output$bi_recommend_tbl <- renderDT({
    bt <- bi_types(); req(bt)
    rec <- recommend_test(bt$x, bt$y, bt$n_groups %||% 2)
    datatable(rec, options = list(dom = "t", pageLength = 10, scrollX = TRUE), rownames = FALSE)
  })

  output$bi_test_tbl <- renderDT({
    df <- doctor_data(); req(df, input$bi_x, input$bi_y, input$bi_test)
    bt <- bi_types()
    test_name <- input$bi_test
    if (test_name == "Auto-recommend") {
      rec <- recommend_test(bt$x, bt$y, bt$n_groups %||% 2)
      test_name <- rec$test[1]
    }
    result <- run_bivariate_test(df, input$bi_x, input$bi_y, test_name)
    datatable(result, options = list(dom = "t", pageLength = 10, scrollX = TRUE), rownames = FALSE)
  })

  # Correlation matrix
  output$cor_heatmap <- renderPlot({
    df <- doctor_data(); req(df)
    cm <- compute_cor_matrix(df, method = input$cor_method %||% "pearson")
    validate(need(!is.null(cm), "Need at least 2 numeric variables for correlation matrix."))

    cor_long <- reshape2_melt_cor(cm$cor, cm$pval)
    ggplot(cor_long, aes(x = Var1, y = Var2, fill = value)) +
      geom_tile(color = "white", linewidth = 0.5) +
      geom_text(aes(label = label), size = 3.2, color = "black") +
      scale_fill_gradient2(low = "#2166AC", mid = "white", high = "#B2182B", midpoint = 0,
                           limits = c(-1, 1), name = "Correlation") +
      labs(x = NULL, y = NULL, title = paste0(tools::toTitleCase(input$cor_method %||% "pearson"), " correlation matrix")) +
      theme_minimal(base_size = 12) +
      theme(axis.text.x = element_text(angle = 45, hjust = 1),
            panel.grid = element_blank())
  })

  output$cor_table <- renderDT({
    df <- doctor_data(); req(df)
    cm <- compute_cor_matrix(df, method = input$cor_method %||% "pearson")
    validate(need(!is.null(cm), "Need at least 2 numeric variables."))
    out <- as.data.frame(round(cm$cor, 3))
    out$variable <- rownames(out); rownames(out) <- NULL
    out <- out[, c("variable", setdiff(names(out), "variable"))]
    datatable(out, options = list(dom = "tip", scrollX = TRUE, pageLength = 20), rownames = FALSE)
  })

  # ══════════════════════════════════════════════════════════════
  # MODEL DIAGNOSTICS SERVER
  # ══════════════════════════════════════════════════════════════

  output$diag_resid_fitted <- renderPlot({
    fit <- fitted_model(); req(fit, inherits(fit, "lm"))
    plot_df <- data.frame(fitted = fitted(fit), residuals = residuals(fit))
    ggplot(plot_df, aes(x = fitted, y = residuals)) +
      geom_point(alpha = 0.5, color = "#6f42c1") +
      geom_hline(yintercept = 0, linetype = "dashed", color = "#dc3545") +
      geom_smooth(method = "loess", se = FALSE, color = "#0d6efd", linewidth = 0.8) +
      labs(title = "Residuals vs. Fitted", x = "Fitted values", y = "Residuals") +
      theme_minimal(base_size = 12)
  })

  output$diag_qq <- renderPlot({
    fit <- fitted_model(); req(fit, inherits(fit, "lm"))
    res <- residuals(fit)
    plot_df <- data.frame(sample = sort(res), theoretical = qnorm(ppoints(length(res))))
    ggplot(plot_df, aes(x = theoretical, y = sample)) +
      geom_point(alpha = 0.5, color = "#6f42c1") +
      geom_abline(slope = sd(res), intercept = mean(res), linetype = "dashed", color = "#dc3545") +
      labs(title = "Normal Q-Q plot of residuals", x = "Theoretical quantiles", y = "Sample quantiles") +
      theme_minimal(base_size = 12)
  })

  output$diag_scale_loc <- renderPlot({
    fit <- fitted_model(); req(fit, inherits(fit, "lm"))
    plot_df <- data.frame(fitted = fitted(fit), sqrt_abs_res = sqrt(abs(rstandard(fit))))
    ggplot(plot_df, aes(x = fitted, y = sqrt_abs_res)) +
      geom_point(alpha = 0.5, color = "#6f42c1") +
      geom_smooth(method = "loess", se = FALSE, color = "#0d6efd", linewidth = 0.8) +
      labs(title = "Scale-Location", x = "Fitted values", y = expression(sqrt("|Standardized residuals|"))) +
      theme_minimal(base_size = 12)
  })

  output$diag_checks_tbl <- renderDT({
    fit <- fitted_model(); req(fit, inherits(fit, "lm"))
    checks <- check_assumptions_lm(fit)
    test_rows <- list()
    for (nm in c("normality", "homoscedasticity", "autocorrelation")) {
      if (!is.null(checks[[nm]])) test_rows[[length(test_rows) + 1]] <- checks[[nm]]
    }
    if (length(test_rows) == 0) return(datatable(data.frame(message = "No diagnostics available."), options = list(dom = "t"), rownames = FALSE))
    out <- do.call(rbind, test_rows)
    datatable(out, options = list(dom = "t", scrollX = TRUE), rownames = FALSE)
  })

  output$diag_vif_tbl <- renderDT({
    fit <- fitted_model(); req(fit, inherits(fit, "lm"))
    checks <- check_assumptions_lm(fit)
    if (is.null(checks$vif)) return(datatable(data.frame(message = "VIF requires 2+ predictors."), options = list(dom = "t"), rownames = FALSE))
    datatable(checks$vif, options = list(dom = "t", scrollX = TRUE), rownames = FALSE)
  })

  # ══════════════════════════════════════════════════════════════
  # ENHANCED MULTIVARIATE SERVER
  # ══════════════════════════════════════════════════════════════

  output$multi_scree <- renderPlot({
    res <- multi_result(); req(res)
    if (res$method == "PCA") {
      ve <- summary(res$fit)$importance[2, ]
      plot_df <- data.frame(component = seq_along(ve), variance = as.numeric(ve) * 100)
      ggplot(plot_df, aes(x = component, y = variance)) +
        geom_line(color = "#6f42c1", linewidth = 1) +
        geom_point(color = "#6f42c1", size = 3) +
        geom_hline(yintercept = 100 / nrow(plot_df), linetype = "dashed", color = "#999") +
        labs(title = "Scree plot (PCA)", x = "Component", y = "% Variance explained") +
        scale_x_continuous(breaks = plot_df$component) +
        theme_minimal(base_size = 12)
    } else if (res$method == "K-means") {
      # Elbow plot: try 2-10 clusters
      scaled_data <- res$data
      max_k <- min(10, nrow(scaled_data) - 1)
      if (max_k < 2) return(NULL)
      wss <- sapply(2:max_k, function(k) kmeans(scaled_data, k, nstart = 10)$tot.withinss)
      plot_df <- data.frame(k = 2:max_k, wss = wss)
      ggplot(plot_df, aes(x = k, y = wss)) +
        geom_line(color = "#6f42c1", linewidth = 1) +
        geom_point(color = "#6f42c1", size = 3) +
        labs(title = "Elbow plot (K-means)", x = "Number of clusters (k)", y = "Total within-cluster SS") +
        scale_x_continuous(breaks = 2:max_k) +
        theme_minimal(base_size = 12)
    } else {
      plot.new(); text(0.5, 0.5, "Scree/elbow plot not applicable for this method.", cex = 1.2)
    }
  })

  output$multi_dendro <- renderPlot({
    df <- doctor_data(); req(df)
    num <- df[, vapply(df, is.numeric, logical(1)), drop = FALSE]
    num <- num[, vapply(num, function(x) stats::sd(x, na.rm = TRUE) > 0, logical(1)), drop = FALSE]
    num <- safe_complete(num)
    validate(need(ncol(num) >= 2, "Need 2+ numeric variables."))
    n_use <- min(nrow(num), 200)
    if (nrow(num) > 200) num <- num[sample(nrow(num), 200), ]
    d <- dist(scale(num))
    hc <- hclust(d, method = "ward.D2")
    plot(hc, labels = FALSE, main = "Hierarchical clustering (Ward's method)",
         xlab = paste0("Observations (n=", n_use, ")"), sub = "", hang = -1)
    if (!is.null(input$multi_clusters)) {
      rect.hclust(hc, k = input$multi_clusters, border = c("#dc3545", "#0d6efd", "#198754", "#fd7e14",
                                                             "#6f42c1", "#20c997", "#e83e8c", "#17a2b8"))
    }
  })




  # ══════════════════════════════════════════════════════════════
  # PHASE 4: HYPOTHESIS TESTS SERVER
  # ══════════════════════════════════════════════════════════════

  observe({
    df <- doctor_data(); req(df)
    vars <- names(df); nums <- numeric_vars_of(df)
    updateSelectInput(session, "hyp_var1", choices = vars, selected = vars[1])
    updateSelectInput(session, "hyp_var2", choices = c("None" = "", vars), selected = "")
    updateSelectInput(session, "norm_var", choices = nums, selected = if(length(nums)) nums[1] else NULL)
  })

  # Test Finder
  output$finder_result_tbl <- renderDT({
    n_g <- as.integer(input$finder_groups %||% "2")
    paired <- (input$finder_paired %||% "no") == "yes"
    dtype <- input$finder_type %||% "continuous"
    result <- find_test(n_g, paired, dtype)
    datatable(result, options = list(dom = "t", scrollX = TRUE), rownames = FALSE,
              colnames = c("Recommended test", "Assumptions", "When to use"))
  })

  # Run hypothesis test
  hyp_result <- eventReactive(input$run_hyp_test, {
    df <- doctor_data(); req(df, input$hyp_test_name, input$hyp_var1)
    validate(need(input$hyp_test_name != "", "Select a test."))
    run_hypothesis_test(df, input$hyp_test_name, input$hyp_var1,
                        if (input$hyp_var2 != "") input$hyp_var2 else NULL,
                        mu0 = input$hyp_mu0 %||% 0, p0 = input$hyp_p0 %||% 0.5,
                        conf_level = input$hyp_conf %||% 0.95)
  })

  output$hyp_result_tbl <- renderDT({
    res <- hyp_result()
    datatable(res, options = list(dom = "t", scrollX = TRUE, pageLength = 20), rownames = FALSE)
  })

  output$hyp_plot <- renderPlot({
    df <- doctor_data(); req(df, input$hyp_var1, input$hyp_test_name)
    x <- df[[input$hyp_var1]]; x <- x[!is.na(x)]
    tn <- input$hyp_test_name

    if (grepl("t-test|Wilcoxon|Sign|ANOVA|Kruskal|Mann-Whitney|Welch", tn) && is.numeric(x)) {
      if (!is.null(input$hyp_var2) && input$hyp_var2 != "" && input$hyp_var2 %in% names(df)) {
        g <- df[[input$hyp_var2]][!is.na(df[[input$hyp_var1]])]
        plot_df <- data.frame(value = x, group = factor(g))
        ggplot(plot_df, aes(x = group, y = value, fill = group)) +
          geom_boxplot(alpha = 0.7, show.legend = FALSE) +
          stat_summary(fun = mean, geom = "point", shape = 18, size = 3, color = "#dc3545") +
          labs(title = paste("Distribution by group:", input$hyp_var2), x = input$hyp_var2, y = input$hyp_var1) +
          theme_minimal(base_size = 13) + theme(axis.text.x = element_text(angle = 45, hjust = 1))
      } else {
        plot_df <- data.frame(value = x)
        ggplot(plot_df, aes(x = value)) +
          geom_histogram(aes(y = after_stat(density)), bins = 30, fill = "#d63384", alpha = 0.5, color = "white") +
          geom_density(linewidth = 1, color = "#d63384") +
          geom_vline(xintercept = input$hyp_mu0 %||% 0, linetype = "dashed", color = "#dc3545", linewidth = 1) +
          geom_vline(xintercept = mean(x), linetype = "solid", color = "#0d6efd", linewidth = 1) +
          labs(title = paste("Distribution of", input$hyp_var1),
               subtitle = paste0("Blue = sample mean (", round(mean(x), 3), "), Red dashed = H0 value"),
               x = input$hyp_var1, y = "Density") +
          theme_minimal(base_size = 13)
      }
    } else if (grepl("Binomial|Chi-square|proportion", tn)) {
      tab <- table(factor(x))
      plot_df <- data.frame(level = names(tab), count = as.integer(tab))
      ggplot(plot_df, aes(x = reorder(level, -count), y = count, fill = level)) +
        geom_col(show.legend = FALSE, alpha = 0.8) +
        labs(title = paste("Counts of", input$hyp_var1), x = input$hyp_var1, y = "Count") +
        theme_minimal(base_size = 13)
    } else {
      plot.new(); text(0.5, 0.5, "Select a test and click Run.", cex = 1.2)
    }
  })

  # Normality tests
  norm_results <- eventReactive(input$run_norm, {
    df <- doctor_data(); req(df, input$norm_var)
    x <- df[[input$norm_var]]; x <- x[!is.na(x)]
    validate(need(length(x) >= 8, "Need at least 8 non-missing values."))
    tests <- c("Shapiro-Wilk", "Anderson-Darling", "Kolmogorov-Smirnov")
    results <- lapply(tests, function(tn) run_hypothesis_test(df, tn, input$norm_var))
    list(results = results, tests = tests, x = x, var = input$norm_var)
  })

  output$norm_qq_plot <- renderPlot({
    nr <- norm_results(); x <- nr$x
    n <- length(x); qq_df <- data.frame(theoretical = qnorm(ppoints(n)), sample = sort(x))
    ggplot(qq_df, aes(x = theoretical, y = sample)) +
      geom_point(alpha = 0.5, color = "#d63384", size = 2) +
      geom_abline(intercept = mean(x), slope = sd(x), linetype = "dashed", color = "#0d6efd") +
      labs(title = paste("Normal Q-Q plot:", nr$var), x = "Theoretical quantiles", y = "Sample quantiles") +
      theme_minimal(base_size = 13)
  })

  output$norm_hist_plot <- renderPlot({
    nr <- norm_results(); x <- nr$x
    plot_df <- data.frame(x = x)
    ggplot(plot_df, aes(x = x)) +
      geom_histogram(aes(y = after_stat(density)), bins = 30, fill = "#d63384", alpha = 0.4, color = "white") +
      geom_density(linewidth = 1.2, color = "#d63384") +
      stat_function(fun = dnorm, args = list(mean = mean(x), sd = sd(x)),
                    linewidth = 1, linetype = "dashed", color = "#0d6efd") +
      labs(title = paste("Histogram + density:", nr$var),
           subtitle = "Pink = observed density, Blue dashed = normal fit",
           x = nr$var, y = "Density") +
      theme_minimal(base_size = 13)
  })

  output$norm_results_tbl <- renderDT({
    nr <- norm_results()
    out <- do.call(rbind, lapply(seq_along(nr$tests), function(i) {
      r <- nr$results[[i]]; r$test_name <- nr$tests[i]; r
    }))
    datatable(out, options = list(dom = "t", scrollX = TRUE), rownames = FALSE)
  })

  # Power analysis
  power_result <- eventReactive(input$run_power, {
    df <- doctor_data()
    y <- if (!is.null(input$hyp_var2) && input$hyp_var2 != "" && input$hyp_var2 %in% names(df)) df[[input$hyp_var2]] else NULL
    run_hypothesis_test(df, input$power_test, input$hyp_var1 %||% names(df)[1],
                        if(!is.null(y)) input$hyp_var2 else NULL,
                        conf_level = 1 - (input$power_alpha %||% 0.05))
  })

  output$power_result_tbl <- renderDT({
    res <- power_result()
    datatable(res, options = list(dom = "t", scrollX = TRUE), rownames = FALSE)
  })

  output$power_curve_plot <- renderPlot({
    alpha <- input$power_alpha %||% 0.05
    tn <- input$power_test %||% "Power: two-sample t-test"
    if (grepl("t-test", tn)) {
      n_seq <- seq(5, 200, by = 5)
      d_vals <- c(0.2, 0.5, 0.8)
      plot_data <- do.call(rbind, lapply(d_vals, function(d) {
        pwr <- sapply(n_seq, function(n) power.t.test(n = n, delta = d, sd = 1, sig.level = alpha)$power)
        data.frame(n = n_seq, power = pwr, effect = paste0("d = ", d))
      }))
      ggplot(plot_data, aes(x = n, y = power, color = effect)) +
        geom_line(linewidth = 1.2) +
        geom_hline(yintercept = 0.80, linetype = "dashed", color = "#999") +
        annotate("text", x = 180, y = 0.82, label = "80% power", color = "#999", size = 3.5) +
        scale_y_continuous(labels = scales::percent, limits = c(0, 1)) +
        labs(title = "Power curve: two-sample t-test", x = "Sample size per group", y = "Power", color = "Effect size") +
        theme_minimal(base_size = 13)
    } else {
      plot.new(); text(0.5, 0.5, "Power curve available for t-test.", cex = 1.2)
    }
  })




  # ══════════════════════════════════════════════════════════════
  # PHASE 5: EDUCATION LAYER — Interpretation panels
  # ══════════════════════════════════════════════════════════════

  output$model_interpretation <- renderUI({
    fit <- tryCatch(fitted_model(), error = function(e) NULL)
    if (is.null(fit)) return(NULL)
    if (inherits(fit, "lm") && !inherits(fit, "glm")) {
      sm <- summary(fit)
      msg <- interpret_r_squared(sm$r.squared, sm$adj.r.squared)
      f_p <- pf(sm$fstatistic[1], sm$fstatistic[2], sm$fstatistic[3], lower.tail = FALSE)
      msg2 <- interpret_p_value(f_p)
      div(class = "tab-note", style = "border-left-color: #198754; margin-top: 12px;",
          h5("Interpretation"), p(msg), p(msg2))
    }
  })

  # ══════════════════════════════════════════════════════════════
  # PHASE 6: TIME SERIES SERVER
  # ══════════════════════════════════════════════════════════════

  observe({
    df <- doctor_data(); req(df)
    all_vars <- names(df)
    nums <- numeric_vars_of(df)
    date_col <- detect_date_column(df)
    date_choices <- c(all_vars)
    updateSelectInput(session, "ts_date_col", choices = date_choices, selected = date_col %||% all_vars[1])
    updateSelectInput(session, "ts_value_col", choices = nums, selected = if (length(nums)) nums[1] else NULL)
  })

  ts_data <- reactive({
    df <- doctor_data(); req(df, input$ts_date_col, input$ts_value_col)
    date_col <- input$ts_date_col; val_col <- input$ts_value_col
    validate(need(val_col %in% names(df), "Select a numeric variable."))
    validate(need(date_col %in% names(df), "Select a date column."))

    dates <- df[[date_col]]
    if (is.character(dates)) {
      for (fmt in c("%Y-%m-%d","%m/%d/%Y","%d/%m/%Y","%Y/%m/%d","%m-%d-%Y")) {
        att <- as.Date(dates, format = fmt)
        if (sum(!is.na(att)) / sum(!is.na(dates)) > 0.7) { dates <- att; break }
      }
    }
    if (!inherits(dates, "Date")) dates <- tryCatch(as.Date(dates), error = function(e) NULL)
    validate(need(!is.null(dates), "Could not parse date column. Try a different column or format."))

    values <- as.numeric(df[[val_col]])
    cc <- !is.na(dates) & !is.na(values)
    dates <- dates[cc]; values <- values[cc]
    ord <- order(dates); dates <- dates[ord]; values <- values[ord]

    freq <- if (input$ts_freq == "auto") detect_frequency(dates) else as.integer(input$ts_freq)

    if (isTRUE(input$ts_use_returns) && length(values) > 1) {
      values <- c(NA, diff(log(values))) * 100
      dates <- dates; values[1] <- 0
    }
    if (isTRUE(input$ts_use_diff) && length(values) > 1) {
      values <- c(NA, diff(values))
      values[1] <- 0
    }

    ts_obj <- tryCatch(make_ts_object(values, dates, freq), error = function(e) ts(values, frequency = freq))
    list(dates = dates, values = values, ts = ts_obj, freq = freq, name = val_col)
  })

  output$ts_line_plot <- renderPlot({
    td <- ts_data(); req(td)
    plot_df <- data.frame(date = td$dates, value = td$values)
    suffix <- if (isTRUE(input$ts_use_returns)) " (log-returns %)" else if (isTRUE(input$ts_use_diff)) " (first difference)" else ""
    ggplot(plot_df, aes(x = date, y = value)) +
      geom_line(color = "#0d6efd", linewidth = 0.6) +
      geom_smooth(method = "loess", se = FALSE, color = "#dc3545", linewidth = 0.8, linetype = "dashed") +
      labs(title = paste0("Time series: ", td$name, suffix), subtitle = paste0("Frequency: ", freq_label(td$freq), " | n = ", length(td$values)),
           x = "Date", y = td$name) +
      theme_minimal(base_size = 13)
  })

  output$ts_acf_plot <- renderPlot({
    td <- ts_data(); req(td)
    acf_obj <- acf(td$values, lag.max = min(40, length(td$values)/3), plot = FALSE)
    acf_df <- data.frame(lag = acf_obj$lag[-1], acf = acf_obj$acf[-1])
    ci <- 1.96 / sqrt(length(td$values))
    ggplot(acf_df, aes(x = lag, y = acf)) +
      geom_hline(yintercept = 0, color = "gray40") +
      geom_hline(yintercept = c(-ci, ci), linetype = "dashed", color = "#dc3545") +
      geom_segment(aes(xend = lag, yend = 0), color = "#0d6efd", linewidth = 0.8) +
      labs(title = "ACF", x = "Lag", y = "Autocorrelation") +
      theme_minimal(base_size = 12)
  })

  output$ts_pacf_plot <- renderPlot({
    td <- ts_data(); req(td)
    pacf_obj <- pacf(td$values, lag.max = min(40, length(td$values)/3), plot = FALSE)
    pacf_df <- data.frame(lag = pacf_obj$lag, acf = pacf_obj$acf)
    ci <- 1.96 / sqrt(length(td$values))
    ggplot(pacf_df, aes(x = lag, y = acf)) +
      geom_hline(yintercept = 0, color = "gray40") +
      geom_hline(yintercept = c(-ci, ci), linetype = "dashed", color = "#dc3545") +
      geom_segment(aes(xend = lag, yend = 0), color = "#6f42c1", linewidth = 0.8) +
      labs(title = "PACF", x = "Lag", y = "Partial autocorrelation") +
      theme_minimal(base_size = 12)
  })

  output$ts_decomp_plot <- renderPlot({
    td <- ts_data(); req(td)
    validate(need(td$freq > 1, "Decomposition requires frequency > 1 (not annual)."))
    validate(need(length(td$values) >= 2 * td$freq, "Need at least 2 full cycles for decomposition."))
    decomp <- tryCatch(stl(td$ts, s.window = "periodic"), error = function(e) {
      tryCatch(decompose(td$ts), error = function(e2) NULL)
    })
    validate(need(!is.null(decomp), "Could not decompose. Check frequency setting."))
    if (inherits(decomp, "stl")) plot(decomp, main = "STL Decomposition") else plot(decomp)
  })

  # Stationarity
  stationarity_results <- eventReactive(input$run_stationarity, {
    td <- ts_data(); req(td)
    validate(need(length(td$values) >= 20, "Need at least 20 observations."))
    run_stationarity_tests(td$values)
  })

  output$ts_stationarity_tbl <- renderDT({
    res <- stationarity_results()
    datatable(res, options = list(dom = "t", scrollX = TRUE), rownames = FALSE)
  })

  output$ts_stationarity_interpretation <- renderUI({
    res <- stationarity_results()
    adf_p <- res$p_value[res$test == "Augmented Dickey-Fuller"]
    kpss_p <- res$p_value[res$test == "KPSS"]
    msg <- if (length(adf_p) && length(kpss_p)) {
      if (adf_p < 0.05 && kpss_p >= 0.05) "The series appears STATIONARY. ADF rejects the unit root and KPSS does not reject stationarity. You can proceed with ARIMA modeling."
      else if (adf_p >= 0.05 && kpss_p < 0.05) "The series appears NON-STATIONARY. Try first-differencing (check the box above) or use log-returns."
      else if (adf_p < 0.05 && kpss_p < 0.05) "Tests disagree. The series may be trend-stationary. Consider differencing."
      else "Both tests suggest non-stationarity. Differencing is strongly recommended."
    } else "Could not interpret. Check that 'tseries' package is installed."
    div(class = "tab-note", style = "border-left-color: #0d6efd; margin-top: 12px;", p(strong("Interpretation: "), msg))
  })

  # ARIMA
  arima_fit <- eventReactive(input$fit_arima, {
    td <- ts_data(); req(td)
    validate(need(has_pkg("forecast"), "Install 'forecast' package: install.packages('forecast')"))
    validate(need(length(td$values) >= 20, "Need at least 20 observations."))
    fit <- fit_arima_model(td$ts)
    h <- input$arima_horizon %||% 12
    fc <- forecast::forecast(fit, h = h)
    list(fit = fit, forecast = fc, data = td)
  })

  output$arima_summary <- renderPrint({ af <- arima_fit(); summary(af$fit) })

  output$arima_metrics_tbl <- renderDT({
    af <- arima_fit()
    acc <- tryCatch(forecast::accuracy(af$fit), error = function(e) NULL)
    if (!is.null(acc)) {
      out <- data.frame(metric = colnames(acc), value = round(as.numeric(acc[1,]), 4), stringsAsFactors = FALSE)
    } else {
      out <- data.frame(metric = c("AIC", "BIC"), value = c(round(AIC(af$fit),2), round(BIC(af$fit),2)), stringsAsFactors = FALSE)
    }
    datatable(out, options = list(dom = "t"), rownames = FALSE)
  })

  output$arima_forecast_plot <- renderPlot({
    af <- arima_fit()
    plot(af$forecast, main = paste("ARIMA Forecast:", af$data$name),
         xlab = "Time", ylab = af$data$name, col = "#0d6efd", fcol = "#dc3545",
         shadecols = c("#fce4ec", "#ffcdd2"), lwd = 2)
  })

  output$arima_resid_plot <- renderPlot({
    af <- arima_fit(); res <- residuals(af$fit)
    plot_df <- data.frame(x = res)
    ggplot(plot_df, aes(x = x)) +
      geom_histogram(aes(y = after_stat(density)), bins = 30, fill = "#0d6efd", alpha = 0.5, color = "white") +
      geom_density(linewidth = 1, color = "#0d6efd") +
      stat_function(fun = dnorm, args = list(mean = mean(res), sd = sd(res)), linetype = "dashed", color = "#dc3545") +
      labs(title = "ARIMA residual distribution", x = "Residuals", y = "Density") +
      theme_minimal(base_size = 12)
  })

  output$arima_ljung_tbl <- renderDT({
    af <- arima_fit(); res <- residuals(af$fit)
    lags <- c(10, 15, 20)
    out <- do.call(rbind, lapply(lags, function(lag) {
      bt <- Box.test(res, lag = lag, type = "Ljung-Box")
      data.frame(lag = lag, statistic = round(bt$statistic, 4), p_value = signif(bt$p.value, 4),
                 verdict = if (bt$p.value > 0.05) "PASS: No autocorrelation" else "FAIL: Residual autocorrelation",
                 stringsAsFactors = FALSE)
    }))
    datatable(out, options = list(dom = "t"), rownames = FALSE)
  })

  # GARCH
  garch_fit <- eventReactive(input$fit_garch, {
    td <- ts_data(); req(td)
    validate(need(has_pkg("rugarch"), "Install 'rugarch' package: install.packages('rugarch')"))
    validate(need(length(td$values) >= 50, "GARCH needs at least 50 observations. Use log-returns of a price series."))
    values <- td$values
    values <- values[!is.na(values)]
    fit <- fit_garch_model(values, model_type = input$garch_type, garch_order = c(input$garch_p, input$garch_q))
    h <- input$garch_forecast_h %||% 20
    fc <- rugarch::ugarchforecast(fit, n.ahead = h)
    list(fit = fit, forecast = fc, data = td, values = values)
  })

  output$garch_summary <- renderPrint({ gf <- garch_fit(); show(gf$fit) })

  output$garch_coef_tbl <- renderDT({
    gf <- garch_fit()
    cf <- rugarch::coef(gf$fit)
    se <- sqrt(diag(rugarch::vcov(gf$fit)))
    out <- data.frame(parameter = names(cf), estimate = round(cf, 6), std_error = round(se, 6),
                      t_value = round(cf/se, 4), p_value = signif(2*pnorm(-abs(cf/se)), 4),
                      stringsAsFactors = FALSE)
    datatable(out, options = list(dom = "t", scrollX = TRUE), rownames = FALSE)
  })

  output$garch_vol_plot <- renderPlot({
    gf <- garch_fit()
    sigma <- rugarch::sigma(gf$fit)
    plot_df <- data.frame(t = seq_along(sigma), sigma = as.numeric(sigma))
    ggplot(plot_df, aes(x = t, y = sigma)) +
      geom_line(color = "#dc3545", linewidth = 0.7) +
      labs(title = paste(input$garch_type, "— Conditional volatility"),
           x = "Time", y = expression(sigma[t])) +
      theme_minimal(base_size = 13)
  })

  output$garch_resid_plot <- renderPlot({
    gf <- garch_fit()
    std_res <- as.numeric(rugarch::residuals(gf$fit, standardize = TRUE))
    plot_df <- data.frame(t = seq_along(std_res), r = std_res)
    ggplot(plot_df, aes(x = t, y = r)) +
      geom_point(alpha = 0.3, color = "#6f42c1", size = 1) +
      geom_hline(yintercept = c(-2, 2), linetype = "dashed", color = "#dc3545") +
      labs(title = "Standardized residuals", x = "Time", y = "Std. residuals") +
      theme_minimal(base_size = 12)
  })

  output$garch_nic_plot <- renderPlot({
    gf <- garch_fit()
    ni <- tryCatch(rugarch::newsimpact(gf$fit), error = function(e) NULL)
    if (is.null(ni)) { plot.new(); text(0.5, 0.5, "NIC not available for this model.", cex = 1.2); return() }
    plot_df <- data.frame(z = ni$zx, h = ni$zy)
    ggplot(plot_df, aes(x = z, y = h)) +
      geom_line(color = "#dc3545", linewidth = 1.2) +
      geom_vline(xintercept = 0, linetype = "dashed", color = "#999") +
      labs(title = "News impact curve", x = expression(epsilon[t-1]), y = expression(sigma[t]^2)) +
      theme_minimal(base_size = 13)
  })

  output$garch_forecast_plot <- renderPlot({
    gf <- garch_fit()
    fc_sigma <- as.numeric(rugarch::sigma(gf$forecast))
    plot_df <- data.frame(h = seq_along(fc_sigma), sigma = fc_sigma)
    ggplot(plot_df, aes(x = h, y = sigma)) +
      geom_line(color = "#dc3545", linewidth = 1.2) +
      geom_point(color = "#dc3545", size = 2) +
      labs(title = paste(input$garch_type, "— Volatility forecast"),
           x = "Horizon (periods ahead)", y = expression(sigma[t+h])) +
      theme_minimal(base_size = 13)
  })




  # ══════════════════════════════════════════════════════════════
  # PHASE 7: SURVIVAL ANALYSIS SERVER
  # ══════════════════════════════════════════════════════════════

  observe({
    df <- doctor_data(); req(df)
    vars <- names(df); nums <- numeric_vars_of(df); cats <- categorical_vars_of(df)
    time_col <- detect_time_column(df)
    event_col <- detect_event_column(df)
    updateSelectInput(session, "surv_time", choices = nums, selected = time_col %||% (if(length(nums)) nums[1] else NULL))
    updateSelectInput(session, "surv_event", choices = vars, selected = event_col %||% (if(length(vars)>=2) vars[2] else NULL))
    updateSelectInput(session, "surv_group", choices = c("None" = "", cats, nums), selected = "")
    updateSelectizeInput(session, "cox_predictors", choices = vars, selected = NULL, server = TRUE)
  })

  km_result <- eventReactive(input$run_km, {
    df <- doctor_data(); req(df, input$surv_time, input$surv_event)
    validate(need(has_pkg("survival"), "Install 'survival': install.packages('survival')"))
    surv_df <- df[, unique(c(input$surv_time, input$surv_event,
                  if(input$surv_group != "") input$surv_group else NULL)), drop = FALSE]
    surv_df <- safe_complete(surv_df)
    validate(need(nrow(surv_df) >= 5, "Need at least 5 complete observations."))
    validate(need(all(surv_df[[input$surv_event]] %in% c(0, 1)), "Event variable must be 0 (censored) or 1 (event)."))
    grp <- if (input$surv_group != "") input$surv_group else NULL
    fit_km(surv_df, input$surv_time, input$surv_event, grp)
  })

  output$km_plot <- renderPlot({
    km <- km_result()
    if (has_pkg("survminer")) {
      p <- survminer::ggsurvplot(km$fit, data = NULL, risk.table = TRUE, pval = !is.null(km$logrank),
        conf.int = TRUE, xlab = input$surv_time, ylab = "Survival probability",
        title = "Kaplan-Meier survival curve", ggtheme = theme_minimal(base_size = 13),
        palette = c("#0d6efd", "#dc3545", "#198754", "#fd7e14", "#6f42c1", "#20c997"))
      print(p)
    } else {
      plot(km$fit, col = c("#0d6efd", "#dc3545", "#198754", "#fd7e14"), lwd = 2,
           xlab = input$surv_time, ylab = "Survival probability", main = "Kaplan-Meier curve")
      if (!is.null(km$group)) legend("topright", legend = names(km$fit$strata), col = c("#0d6efd", "#dc3545", "#198754", "#fd7e14"), lwd = 2)
    }
  })

  output$km_summary_tbl <- renderDT({
    km <- km_result()
    sm <- summary(km$fit)$table
    if (is.matrix(sm)) {
      out <- as.data.frame(sm)
      out$group <- rownames(out); rownames(out) <- NULL
      out <- out[, c("group", setdiff(names(out), "group"))]
    } else {
      out <- data.frame(metric = names(sm), value = as.character(round(as.numeric(sm), 3)), stringsAsFactors = FALSE)
    }
    datatable(out, options = list(dom = "t", scrollX = TRUE, pageLength = 10), rownames = FALSE)
  })

  output$km_logrank_tbl <- renderDT({
    km <- km_result()
    if (is.null(km$logrank)) return(datatable(data.frame(message = "No grouping variable selected. Add a group to compare survival curves."), options = list(dom = "t"), rownames = FALSE))
    lr <- km$logrank
    p_val <- 1 - pchisq(lr$chisq, length(lr$n) - 1)
    out <- data.frame(
      statistic = c("Chi-squared", "df", "p-value", "Interpretation"),
      value = c(round(lr$chisq, 4), length(lr$n) - 1, signif(p_val, 4),
        if (p_val < 0.05) "Significant difference in survival between groups" else "No significant difference in survival"),
      stringsAsFactors = FALSE)
    datatable(out, options = list(dom = "t"), rownames = FALSE)
  })

  cox_result <- eventReactive(input$run_cox, {
    df <- doctor_data(); req(df, input$surv_time, input$surv_event, input$cox_predictors)
    validate(need(has_pkg("survival"), "Install 'survival' package."))
    validate(need(length(input$cox_predictors) >= 1, "Select at least one predictor."))
    keep <- unique(c(input$surv_time, input$surv_event, input$cox_predictors))
    cox_df <- df[, keep, drop = FALSE]
    cox_df <- safe_complete(cox_df)
    validate(need(nrow(cox_df) >= 10, "Need at least 10 complete observations for Cox PH."))
    validate(need(all(cox_df[[input$surv_event]] %in% c(0, 1)), "Event must be 0/1."))
    fit <- fit_cox(cox_df, input$surv_time, input$surv_event, input$cox_predictors)
    list(fit = fit, data = cox_df)
  })

  output$cox_coef_tbl <- renderDT({
    cx <- cox_result()
    sm <- summary(cx$fit)
    cf <- as.data.frame(sm$coefficients)
    cf$term <- rownames(cf); rownames(cf) <- NULL
    cf$HR <- round(exp(cf$coef), 4)
    cf$HR_lower <- round(exp(cf$coef - 1.96 * cf$`se(coef)`), 4)
    cf$HR_upper <- round(exp(cf$coef + 1.96 * cf$`se(coef)`), 4)
    cf$coef <- round(cf$coef, 4); cf$`se(coef)` <- round(cf$`se(coef)`, 4)
    cf$`Pr(>|z|)` <- signif(cf$`Pr(>|z|)`, 4)
    display <- cf[, c("term", "coef", "se(coef)", "HR", "HR_lower", "HR_upper", "Pr(>|z|)")]
    names(display) <- c("Term", "Coef (log-HR)", "SE", "Hazard Ratio", "HR 95% Lower", "HR 95% Upper", "p-value")
    datatable(display, options = list(dom = "t", scrollX = TRUE), rownames = FALSE)
  })

  output$cox_forest_plot <- renderPlot({
    cx <- cox_result()
    sm <- summary(cx$fit)
    cf <- as.data.frame(sm$coefficients)
    cf$term <- rownames(cf)
    cf$HR <- exp(cf$coef)
    cf$HR_lower <- exp(cf$coef - 1.96 * cf$`se(coef)`)
    cf$HR_upper <- exp(cf$coef + 1.96 * cf$`se(coef)`)
    ggplot(cf, aes(x = HR, y = reorder(term, HR), xmin = HR_lower, xmax = HR_upper)) +
      geom_pointrange(color = "#dc3545", linewidth = 0.8, size = 0.8) +
      geom_vline(xintercept = 1, linetype = "dashed", color = "#999") +
      scale_x_log10() +
      labs(title = "Hazard ratio forest plot", x = "Hazard Ratio (log scale)", y = NULL,
           subtitle = "HR > 1 = higher risk | HR < 1 = protective") +
      theme_minimal(base_size = 13)
  })

  output$cox_metrics_tbl <- renderDT({
    cx <- cox_result()
    sm <- summary(cx$fit)
    out <- data.frame(
      metric = c("n", "Events", "Concordance (C-index)", "Likelihood ratio test p", "Wald test p", "Score test p"),
      value = c(sm$n, sm$nevent, round(sm$concordance[1], 4),
                signif(sm$logtest[3], 4), signif(sm$waldtest[3], 4), signif(sm$sctest[3], 4)),
      stringsAsFactors = FALSE)
    datatable(out, options = list(dom = "t"), rownames = FALSE)
  })

  output$cox_ph_test_tbl <- renderDT({
    cx <- cox_result()
    ph <- tryCatch({
      zt <- survival::cox.zph(cx$fit)
      out <- as.data.frame(zt$table)
      out$term <- rownames(out); rownames(out) <- NULL
      out <- out[, c("term", names(out)[1:3])]
      names(out) <- c("Term", "Chi-squared", "df", "p-value")
      out$Verdict <- ifelse(out$`p-value` < 0.05, "VIOLATION: PH may not hold", "OK: PH assumption holds")
      out
    }, error = function(e) data.frame(message = paste("Could not test:", e$message), stringsAsFactors = FALSE))
    datatable(ph, options = list(dom = "t", scrollX = TRUE), rownames = FALSE)
  })

  # ══════════════════════════════════════════════════════════════
  # PHASE 8: REPORT GENERATION SERVER
  # ══════════════════════════════════════════════════════════════

  output$report_preview <- renderUI({
    df <- doctor_data()
    if (is.null(df)) return(div(class = "tab-note", p("Upload data to generate a report.")))
    f <- doctor_findings()
    fl <- doctor_state$fix_log

    preview_items <- list(
      tags$h5("Data Overview"),
      tags$ul(
        tags$li(paste0("Rows: ", nrow(df), " | Columns: ", ncol(df))),
        tags$li(paste0("Numeric: ", length(numeric_vars_of(df)), " | Categorical: ", length(categorical_vars_of(df)))),
        tags$li(paste0("Total missing values: ", sum(is.na(df))))
      )
    )
    if (nrow(f) > 0) {
      preview_items <- c(preview_items, list(
        tags$h5("Variable Doctor"),
        tags$ul(
          tags$li(paste0("Critical: ", sum(f$severity == "critical"))),
          tags$li(paste0("Warnings: ", sum(f$severity == "warning"))),
          tags$li(paste0("Suggestions: ", sum(f$severity == "info")))
        )
      ))
    }
    if (length(fl) > 0) {
      preview_items <- c(preview_items, list(
        tags$h5(paste0("Fixes Applied (", length(fl), ")")),
        tags$ul(lapply(fl, tags$li))
      ))
    }
    preview_items <- c(preview_items, list(
      tags$h5("Column Profile"),
      tags$p(class = "text-muted", "Full column-by-column statistics will be included in the download.")
    ))
    div(style = "background: #f8f9fa; border: 1px solid #dee2e6; border-radius: 10px; padding: 16px;",
        preview_items)
  })

  output$generate_report <- downloadHandler(
    filename = function() {
      ext <- input$report_format %||% "html"
      paste0("analysis_report_", Sys.Date(), ".", ext)
    },
    content = function(file) {
      df <- doctor_data(); req(df)
      f <- doctor_findings()
      fl <- doctor_state$fix_log
      title <- input$report_title %||% "Analysis Report"
      author <- input$report_author %||% ""
      fmt <- input$report_format %||% "html"

      # Build markdown content
      md <- c(
        paste0("# ", title),
        if (nzchar(author)) paste0("**Author:** ", author) else NULL,
        paste0("**Date:** ", Sys.time()),
        paste0("**Tool:** Kernel"),
        "",
        "---",
        "",
        "## Data Overview",
        "",
        paste0("| Metric | Value |"),
        paste0("|--------|-------|"),
        paste0("| Rows | ", nrow(df), " |"),
        paste0("| Columns | ", ncol(df), " |"),
        paste0("| Numeric variables | ", length(numeric_vars_of(df)), " |"),
        paste0("| Categorical variables | ", length(categorical_vars_of(df)), " |"),
        paste0("| Total missing values | ", sum(is.na(df)), " |"),
        paste0("| Duplicate rows | ", sum(duplicated(df)), " |"),
        ""
      )

      if (nrow(f) > 0) {
        md <- c(md, "## Variable Doctor Findings", "",
          paste0("| Variable | Severity | Issue | Detail |"),
          paste0("|----------|----------|-------|--------|"),
          apply(f[, c("variable","severity","issue","detail")], 1, function(row) paste0("| ", paste(row, collapse = " | "), " |")),
          "")
      }

      if (length(fl) > 0) {
        md <- c(md, "## Fixes Applied", "", paste0("1. ", fl), "")
      }

      md <- c(md, "## Column Profile", "")
      prof <- column_profile(df)
      md <- c(md,
        paste0("| ", paste(names(prof), collapse = " | "), " |"),
        paste0("| ", paste(rep("---", ncol(prof)), collapse = " | "), " |"),
        apply(prof, 1, function(row) paste0("| ", paste(row, collapse = " | "), " |")),
        "")

      md <- c(md,
        "## Numeric Summary", "")
      nums <- numeric_vars_of(df)
      if (length(nums) > 0) {
        summ <- do.call(rbind, lapply(nums, function(nm) {
          x <- df[[nm]]
          data.frame(variable = nm, n = sum(!is.na(x)), mean = round(mean(x, na.rm=T), 4),
                     sd = round(sd(x, na.rm=T), 4), min = round(min(x, na.rm=T), 4),
                     median = round(median(x, na.rm=T), 4), max = round(max(x, na.rm=T), 4),
                     stringsAsFactors = FALSE)
        }))
        md <- c(md,
          paste0("| ", paste(names(summ), collapse = " | "), " |"),
          paste0("| ", paste(rep("---", ncol(summ)), collapse = " | "), " |"),
          apply(summ, 1, function(row) paste0("| ", paste(row, collapse = " | "), " |")),
          "")
      }

      md <- c(md, "", "---", "", paste0("*Generated by Kernel — ", Sys.time(), "*"))

      md_text <- paste(md, collapse = "\n")

      if (fmt == "md") {
        writeLines(md_text, file)
      } else {
        # HTML: convert markdown to HTML
        tmp_md <- tempfile(fileext = ".md")
        writeLines(md_text, tmp_md)
        if (has_pkg("rmarkdown")) {
          tmp_rmd <- tempfile(fileext = ".Rmd")
          writeLines(c("---", paste0("title: '", title, "'"), "output: html_document", "---", "", md_text), tmp_rmd)
          tryCatch({
            rmarkdown::render(tmp_rmd, output_file = file, quiet = TRUE)
          }, error = function(e) {
            # Fallback: simple HTML
            html <- paste0("<html><head><meta charset='utf-8'><title>", title,
              "</title><style>body{font-family:system-ui;max-width:800px;margin:40px auto;padding:0 20px;line-height:1.6;color:#333;}",
              "table{border-collapse:collapse;width:100%;}th,td{border:1px solid #ddd;padding:6px 10px;text-align:left;}",
              "th{background:#f5f5f5;}</style></head><body>",
              gsub("\n", "<br>", md_text), "</body></html>")
            writeLines(html, file)
          })
        } else {
          html <- paste0("<html><head><meta charset='utf-8'><title>", title,
            "</title><style>body{font-family:system-ui;max-width:800px;margin:40px auto;padding:0 20px;line-height:1.6;color:#333;}",
            "table{border-collapse:collapse;width:100%;}th,td{border:1px solid #ddd;padding:6px 10px;text-align:left;}",
            "th{background:#f5f5f5;}</style></head><body>",
            gsub("\n", "<br>", md_text), "</body></html>")
          writeLines(html, file)
        }
      }
    }
  )

  # Code export
  output$code_export <- renderText({
    df <- doctor_data()
    if (is.null(df)) return("# Upload data to generate reproducible R code.")
    fl <- doctor_state$fix_log
    lines <- c(
      "# ══════════════════════════════════════════════════",
      "# Kernel — Reproducible R Code",
      paste0("# Generated: ", Sys.time()),
      "# ══════════════════════════════════════════════════",
      "",
      "library(readr)",
      "library(dplyr)",
      "library(ggplot2)",
      "",
      "# 1. Load your data",
      '# df <- read_csv("your_data.csv")',
      paste0("# Data dimensions: ", nrow(df), " rows x ", ncol(df), " columns"),
      ""
    )
    if (length(fl) > 0) {
      lines <- c(lines, "# 2. Fixes applied:", paste0("#    ", fl), "")
    }
    lines <- c(lines,
      "# 3. Column profile",
      "# summary(df)",
      "# str(df)",
      "",
      "# 4. Univariate EDA",
      "# ggplot(df, aes(x = your_variable)) + geom_histogram(bins = 30) + theme_minimal()",
      "",
      "# 5. Bivariate analysis",
      "# cor.test(df$var1, df$var2)",
      "# t.test(value ~ group, data = df)",
      "",
      "# 6. Regression",
      "# fit <- lm(outcome ~ predictor1 + predictor2, data = df)",
      "# summary(fit)",
      "# plot(fit)",
      "",
      "# 7. Time series (if applicable)",
      "# library(forecast)",
      "# ts_obj <- ts(df$value, frequency = 12)",
      "# fit <- auto.arima(ts_obj)",
      "# forecast(fit, h = 12) |> plot()",
      "",
      "# 8. GARCH (if applicable)",
      "# library(rugarch)",
      '# spec <- ugarchspec(variance.model = list(model = "sGARCH"),',
      '#                    mean.model = list(armaOrder = c(0,0)),',
      '#                    distribution.model = "std")',
      "# fit <- ugarchfit(spec, data = returns)",
      ""
    )
    paste(lines, collapse = "\n")
  })

  output$download_code <- downloadHandler(
    filename = function() paste0("analysis_code_", Sys.Date(), ".R"),
    content = function(file) writeLines(output$code_export(), file)
  )




  # ══════════════════════════════════════════════════════════════
  # PHASE 9: MACHINE LEARNING PIPELINE SERVER
  # ══════════════════════════════════════════════════════════════

  observe({
    df <- doctor_data(); req(df)
    vars <- names(df)
    updateSelectInput(session, "ml_outcome", choices = vars, selected = vars[1])
    updateSelectizeInput(session, "ml_predictors", choices = vars, selected = vars[-1][seq_len(min(4, length(vars)-1))], server = TRUE)
  })

  observeEvent(input$ml_outcome, {
    df <- doctor_data(); req(df, input$ml_outcome)
    vars <- setdiff(names(df), input$ml_outcome)
    sel <- intersect(input$ml_predictors %||% character(0), vars)
    if (!length(sel)) sel <- vars[seq_len(min(4, length(vars)))]
    updateSelectizeInput(session, "ml_predictors", choices = vars, selected = sel, server = TRUE)
  })

  output$ml_task_display <- renderUI({
    df <- doctor_data(); req(df, input$ml_outcome)
    task <- ml_detect_task(df[[input$ml_outcome]])
    color <- if (task == "classification") "#6f42c1" else "#0d6efd"
    icon_nm <- if (task == "classification") "tags" else "chart-line"
    div(class = "stat-card", style = paste0("border-left: 4px solid ", color, ";"),
        div(class = "label", "Detected task"),
        div(class = "value", style = paste0("color:", color, "; font-size: 1.2rem;"),
            icon(icon_nm), toupper(task)))
  })

  ml_results <- eventReactive(input$ml_train, {
    df <- doctor_data(); req(df, input$ml_outcome, input$ml_predictors, input$ml_methods)
    validate(need(length(input$ml_predictors) >= 1, "Select at least one predictor."))
    validate(need(length(input$ml_methods) >= 1, "Select at least one model."))

    task <- ml_detect_task(df[[input$ml_outcome]])
    split <- ml_split_data(df, input$ml_outcome, input$ml_predictors, input$ml_split / 100, input$ml_seed)
    prepped <- ml_prep(split$train, split$test, input$ml_outcome, task)
    train <- prepped$train; test <- prepped$test

    results <- list()
    withProgress(message = "Training models...", value = 0, {
      methods <- input$ml_methods
      for (i in seq_along(methods)) {
        m <- methods[i]
        incProgress(1/length(methods), detail = m)

        # Skip logistic for regression
        if (m == "Logistic Regression" && task == "regression") next
        # Skip Naive Bayes for regression
        if (m == "Naive Bayes" && task == "regression") next

        fit <- ml_fit_model(train, input$ml_outcome, input$ml_predictors, m, task)
        if (is.list(fit) && isTRUE(fit$error)) {
          results[[m]] <- list(error = TRUE, message = fit$message)
          next
        }

        pred <- ml_predict(fit, test, input$ml_outcome, input$ml_predictors, m, task, train)
        if (is.list(pred) && isTRUE(pred$error)) {
          results[[m]] <- list(error = TRUE, message = pred$message %||% "Prediction failed")
          next
        }

        if (task == "classification") {
          metrics <- ml_metrics_classification(test[[input$ml_outcome]], pred$class, pred$prob)
          results[[m]] <- list(fit = fit, pred = pred, metrics = metrics$metrics,
                               confusion = metrics$confusion, task = task, error = FALSE)
        } else {
          metrics <- ml_metrics_regression(test[[input$ml_outcome]], pred$value)
          results[[m]] <- list(fit = fit, pred = pred, metrics = metrics, task = task, error = FALSE)
        }
      }
    })

    list(results = results, task = task, train = train, test = test,
         outcome = input$ml_outcome, predictors = input$ml_predictors)
  })

  observe({
    res <- tryCatch(ml_results(), error = function(e) NULL)
    if (!is.null(res)) {
      valid <- names(res$results)[!sapply(res$results, function(r) isTRUE(r$error))]
      updateSelectInput(session, "ml_imp_model", choices = valid, selected = valid[1])
      updateSelectInput(session, "ml_pred_model", choices = valid, selected = valid[1])
      updateSelectInput(session, "ml_cm_model", choices = valid, selected = valid[1])
    }
  })

  output$ml_status <- renderUI({
    res <- tryCatch(ml_results(), error = function(e) NULL)
    if (is.null(res)) return(NULL)
    n_ok <- sum(!sapply(res$results, function(r) isTRUE(r$error)))
    n_fail <- sum(sapply(res$results, function(r) isTRUE(r$error)))
    errs <- sapply(res$results[sapply(res$results, function(r) isTRUE(r$error))], function(r) r$message)
    tags$div(
      tags$span(style = "color:#198754; font-weight:600;", paste0(n_ok, " models trained successfully. ")),
      if (n_fail > 0) tags$span(style = "color:#dc3545;", paste0(n_fail, " failed: ", paste(errs, collapse = "; "))) else NULL
    )
  })

  output$ml_comparison_tbl <- renderDT({
    res <- ml_results(); req(res)
    valid <- res$results[!sapply(res$results, function(r) isTRUE(r$error))]
    if (length(valid) == 0) return(datatable(data.frame(message = "No models trained successfully."), options = list(dom="t"), rownames = FALSE))
    all_metrics <- do.call(rbind, lapply(names(valid), function(m) {
      df <- valid[[m]]$metrics; df$model <- m; df
    }))
    wide <- tidyr::pivot_wider(all_metrics, names_from = metric, values_from = value)
    datatable(wide, options = list(dom = "t", scrollX = TRUE), rownames = FALSE)
  })

  output$ml_comparison_plot <- renderPlot({
    res <- ml_results(); req(res)
    valid <- res$results[!sapply(res$results, function(r) isTRUE(r$error))]
    if (length(valid) == 0) return(NULL)
    primary_metric <- if (res$task == "classification") "Accuracy" else "R-squared"
    vals <- sapply(valid, function(r) {
      v <- r$metrics$value[r$metrics$metric == primary_metric]
      if (length(v)) as.numeric(v) else NA
    })
    plot_df <- data.frame(model = names(vals), value = vals, stringsAsFactors = FALSE)
    plot_df <- plot_df[!is.na(plot_df$value), ]
    ggplot(plot_df, aes(x = reorder(model, value), y = value, fill = model)) +
      geom_col(show.legend = FALSE, alpha = 0.85, width = 0.6) +
      geom_text(aes(label = round(value, 3)), hjust = -0.1, size = 4) +
      coord_flip() +
      labs(title = paste(primary_metric, "by model"), x = NULL, y = primary_metric) +
      theme_minimal(base_size = 13) +
      scale_fill_manual(values = c("#6f42c1", "#0d6efd", "#dc3545", "#198754", "#fd7e14", "#d63384", "#20c997"))
  })

  output$ml_roc_plot <- renderPlot({
    res <- ml_results(); req(res, res$task == "classification")
    if (!has_pkg("pROC")) { plot.new(); text(0.5, 0.5, "Install 'pROC' for ROC curves.", cex = 1.2); return() }
    valid <- res$results[!sapply(res$results, function(r) isTRUE(r$error))]
    actual <- res$test[[res$outcome]]

    colors <- c("#6f42c1", "#0d6efd", "#dc3545", "#198754", "#fd7e14", "#d63384", "#20c997")
    plot(NULL, xlim = c(1, 0), ylim = c(0, 1), xlab = "Specificity", ylab = "Sensitivity",
         main = "ROC curves (test set)")
    abline(a = 1, b = -1, lty = 2, col = "#999")
    legend_labels <- c(); legend_cols <- c()

    for (i in seq_along(valid)) {
      m <- names(valid)[i]; r <- valid[[m]]
      prob <- r$pred$prob
      if (is.null(prob) || !is.matrix(prob) && !is.numeric(prob)) next
      prob_vec <- if (is.matrix(prob)) prob[, ncol(prob)] else prob
      roc_obj <- tryCatch(pROC::roc(actual, prob_vec, quiet = TRUE), error = function(e) NULL)
      if (!is.null(roc_obj)) {
        lines(roc_obj, col = colors[i], lwd = 2)
        auc_val <- round(pROC::auc(roc_obj), 3)
        legend_labels <- c(legend_labels, paste0(m, " (AUC=", auc_val, ")"))
        legend_cols <- c(legend_cols, colors[i])
      }
    }
    if (length(legend_labels)) legend("bottomright", legend = legend_labels, col = legend_cols, lwd = 2, cex = 0.8)
  })

  output$ml_importance_plot <- renderPlot({
    res <- ml_results(); req(res, input$ml_imp_model)
    r <- res$results[[input$ml_imp_model]]
    if (is.null(r) || isTRUE(r$error)) return(NULL)
    imp <- ml_variable_importance(r$fit, input$ml_imp_model, res$predictors)
    if (is.null(imp) || nrow(imp) == 0) { plot.new(); text(0.5, 0.5, "Variable importance not available for this model.", cex = 1.2); return() }
    imp <- imp[order(imp$importance, decreasing = TRUE), ]
    imp <- head(imp, 20)
    ggplot(imp, aes(x = reorder(variable, importance), y = importance)) +
      geom_col(fill = "#6f42c1", alpha = 0.8, width = 0.6) +
      coord_flip() +
      labs(title = paste("Variable importance:", input$ml_imp_model), x = NULL, y = "Importance") +
      theme_minimal(base_size = 13)
  })

  output$ml_pred_plot <- renderPlot({
    res <- ml_results(); req(res, input$ml_pred_model)
    r <- res$results[[input$ml_pred_model]]
    if (is.null(r) || isTRUE(r$error)) return(NULL)

    if (res$task == "regression") {
      actual <- res$test[[res$outcome]]
      predicted <- r$pred$value
      plot_df <- data.frame(actual = actual, predicted = predicted)
      ggplot(plot_df, aes(x = actual, y = predicted)) +
        geom_point(alpha = 0.5, color = "#6f42c1", size = 2) +
        geom_abline(slope = 1, intercept = 0, linetype = "dashed", color = "#dc3545") +
        labs(title = paste("Actual vs. Predicted:", input$ml_pred_model),
             x = "Actual", y = "Predicted") +
        theme_minimal(base_size = 13)
    } else {
      actual <- factor(res$test[[res$outcome]])
      predicted <- factor(r$pred$class, levels = levels(actual))
      plot_df <- data.frame(actual = actual, predicted = predicted, correct = actual == predicted)
      ggplot(plot_df, aes(x = actual, fill = correct)) +
        geom_bar(position = "fill") +
        scale_fill_manual(values = c("TRUE" = "#198754", "FALSE" = "#dc3545"), labels = c("Incorrect", "Correct")) +
        labs(title = paste("Classification accuracy:", input$ml_pred_model),
             x = "Actual class", y = "Proportion", fill = NULL) +
        theme_minimal(base_size = 13)
    }
  })

  output$ml_pred_tbl <- renderDT({
    res <- ml_results(); req(res, input$ml_pred_model)
    r <- res$results[[input$ml_pred_model]]
    if (is.null(r) || isTRUE(r$error)) return(NULL)
    out <- res$test[, c(res$outcome, res$predictors), drop = FALSE]
    if (res$task == "classification") { out$.predicted <- r$pred$class; out$.correct <- out[[res$outcome]] == r$pred$class }
    else out$.predicted <- r$pred$value
    datatable(head(out, 50), options = list(pageLength = 10, scrollX = TRUE, dom = "tip"), rownames = FALSE)
  })

  output$ml_cm_plot <- renderPlot({
    res <- ml_results(); req(res, input$ml_cm_model, res$task == "classification")
    r <- res$results[[input$ml_cm_model]]
    if (is.null(r) || isTRUE(r$error) || is.null(r$confusion)) return(NULL)
    cm <- r$confusion
    cm_df <- as.data.frame(cm)
    names(cm_df) <- c("Predicted", "Actual", "Freq")
    ggplot(cm_df, aes(x = Actual, y = Predicted, fill = Freq)) +
      geom_tile(color = "white", linewidth = 1) +
      geom_text(aes(label = Freq), size = 6, fontface = "bold") +
      scale_fill_gradient(low = "white", high = "#6f42c1") +
      labs(title = paste("Confusion matrix:", input$ml_cm_model), fill = "Count") +
      theme_minimal(base_size = 13) +
      theme(panel.grid = element_blank())
  })

  output$ml_cm_tbl <- renderDT({
    res <- ml_results(); req(res, input$ml_cm_model, res$task == "classification")
    r <- res$results[[input$ml_cm_model]]
    if (is.null(r) || isTRUE(r$error)) return(NULL)
    datatable(r$metrics, options = list(dom = "t"), rownames = FALSE)
  })

  output$ml_tree_plot <- renderPlot({
    res <- ml_results(); req(res)
    dt_result <- res$results[["Decision Tree"]]
    if (is.null(dt_result) || isTRUE(dt_result$error)) { plot.new(); text(0.5, 0.5, "Train a Decision Tree first.", cex = 1.2); return() }
    if (has_pkg("rpart.plot")) {
      rpart.plot::rpart.plot(dt_result$fit, main = "Decision Tree", roundint = FALSE,
                             box.palette = c("#EBF1F9", "#C5D6EB", "#8BAED4", "#4271AE"),
                             shadow.col = "gray90", nn = TRUE)
    } else {
      plot(dt_result$fit, uniform = TRUE, main = "Decision Tree")
      text(dt_result$fit, use.n = TRUE, cex = 0.8)
    }
  })




  # ══════════════════════════════════════════════════════════════
  # PHASE 10: INDUSTRY TEMPLATES SERVER
  # ══════════════════════════════════════════════════════════════

  observe({
    df <- doctor_data(); req(df)
    vars <- names(df); nums <- numeric_vars_of(df); cats <- categorical_vars_of(df)
    updateSelectInput(session, "spc_var", choices = nums, selected = if(length(nums)) nums[1] else NULL)
    updateSelectInput(session, "cap_var", choices = nums, selected = if(length(nums)) nums[1] else NULL)
    updateSelectInput(session, "rfm_customer", choices = vars, selected = vars[1])
    updateSelectInput(session, "rfm_date", choices = vars, selected = if(length(vars)>=2) vars[2] else vars[1])
    updateSelectInput(session, "rfm_amount", choices = nums, selected = if(length(nums)) nums[1] else NULL)
    updateSelectInput(session, "pareto_var", choices = c(cats, vars), selected = if(length(cats)) cats[1] else vars[1])
    updateSelectizeInput(session, "heat_vars", choices = nums, selected = head(nums, min(20, length(nums))), server = TRUE)
    updateSelectInput(session, "volcano_fc", choices = nums, selected = if(length(nums)) nums[1] else NULL)
    updateSelectInput(session, "volcano_p", choices = nums, selected = if(length(nums)>=2) nums[2] else NULL)
  })

  # ── SPC Control Charts ──
  spc_result <- eventReactive(input$run_spc, {
    df <- doctor_data(); req(df, input$spc_var)
    x <- df[[input$spc_var]]; x <- x[!is.na(x)]
    validate(need(length(x) >= 10, "Need at least 10 observations."))
    control_chart_stats(x, type = input$spc_type, subgroup_size = input$spc_subgroup)
  })

  output$spc_chart <- renderPlot({
    cc <- spc_result(); req(cc)
    ooc <- cc$values > cc$ucl | cc$values < cc$lcl
    plot_df <- data.frame(index = cc$index, value = cc$values, ooc = ooc)
    ggplot(plot_df, aes(x = index, y = value)) +
      geom_line(color = "#0d6efd", linewidth = 0.7) +
      geom_point(aes(color = ooc), size = 2, show.legend = FALSE) +
      scale_color_manual(values = c("FALSE" = "#0d6efd", "TRUE" = "#dc3545")) +
      geom_hline(yintercept = cc$cl, linetype = "solid", color = "#198754", linewidth = 1) +
      geom_hline(yintercept = cc$ucl, linetype = "dashed", color = "#dc3545", linewidth = 0.8) +
      geom_hline(yintercept = cc$lcl, linetype = "dashed", color = "#dc3545", linewidth = 0.8) +
      annotate("text", x = max(cc$index), y = cc$cl, label = paste("CL =", round(cc$cl, 3)), hjust = 1.1, vjust = -0.5, color = "#198754", size = 3.5) +
      annotate("text", x = max(cc$index), y = cc$ucl, label = paste("UCL =", round(cc$ucl, 3)), hjust = 1.1, vjust = -0.5, color = "#dc3545", size = 3.5) +
      annotate("text", x = max(cc$index), y = cc$lcl, label = paste("LCL =", round(cc$lcl, 3)), hjust = 1.1, vjust = 1.5, color = "#dc3545", size = 3.5) +
      labs(title = paste("Control Chart:", cc$type), subtitle = paste("Red points = out of control"), x = "Observation / Subgroup", y = input$spc_var) +
      theme_minimal(base_size = 13)
  })

  output$spc_ooc_count <- renderUI({
    cc <- spc_result(); req(cc)
    n_ooc <- sum(cc$values > cc$ucl | cc$values < cc$lcl)
    color <- if (n_ooc > 0) "#dc3545" else "#198754"
    div(class = "stat-card", style = paste0("border-left:4px solid ", color, ";"),
        div(class = "label", "Out of control"), div(class = "value", style = paste0("color:", color, ";"), n_ooc))
  })

  output$spc_stats_tbl <- renderDT({
    cc <- spc_result(); req(cc)
    n_ooc <- sum(cc$values > cc$ucl | cc$values < cc$lcl)
    out <- data.frame(
      metric = c("Center line (CL)", "Upper control limit (UCL)", "Lower control limit (LCL)", "Sigma estimate", "n observations", "Out of control points", "% out of control"),
      value = c(round(cc$cl, 4), round(cc$ucl, 4), round(cc$lcl, 4), round(cc$sigma, 4), length(cc$values), n_ooc, round(n_ooc/length(cc$values)*100, 1)),
      stringsAsFactors = FALSE)
    datatable(out, options = list(dom = "t"), rownames = FALSE)
  })

  # ── Process Capability ──
  cap_result <- eventReactive(input$run_cap, {
    df <- doctor_data(); req(df, input$cap_var)
    validate(need(!is.na(input$cap_lsl) && !is.na(input$cap_usl), "Enter both LSL and USL."))
    validate(need(input$cap_usl > input$cap_lsl, "USL must be greater than LSL."))
    x <- df[[input$cap_var]]; x <- x[!is.na(x)]
    validate(need(length(x) >= 10, "Need at least 10 observations."))
    list(stats = process_capability(x, input$cap_lsl, input$cap_usl), x = x, lsl = input$cap_lsl, usl = input$cap_usl)
  })

  output$cap_plot <- renderPlot({
    cr <- cap_result(); req(cr)
    plot_df <- data.frame(x = cr$x)
    ggplot(plot_df, aes(x = x)) +
      geom_histogram(aes(y = after_stat(density)), bins = 30, fill = "#0d6efd", alpha = 0.5, color = "white") +
      geom_density(linewidth = 1.2, color = "#0d6efd") +
      geom_vline(xintercept = cr$lsl, linetype = "dashed", color = "#dc3545", linewidth = 1) +
      geom_vline(xintercept = cr$usl, linetype = "dashed", color = "#dc3545", linewidth = 1) +
      geom_vline(xintercept = mean(cr$x), linetype = "solid", color = "#198754", linewidth = 1) +
      annotate("text", x = cr$lsl, y = 0, label = "LSL", vjust = 1.5, color = "#dc3545", fontface = "bold") +
      annotate("text", x = cr$usl, y = 0, label = "USL", vjust = 1.5, color = "#dc3545", fontface = "bold") +
      labs(title = "Process capability histogram", x = input$cap_var, y = "Density") +
      theme_minimal(base_size = 13)
  })

  output$cap_tbl <- renderDT({
    cr <- cap_result(); datatable(cr$stats, options = list(dom = "t"), rownames = FALSE)
  })

  # ── RFM Analysis ──
  rfm_result <- eventReactive(input$run_rfm, {
    df <- doctor_data(); req(df, input$rfm_customer, input$rfm_date, input$rfm_amount)
    compute_rfm(df, input$rfm_customer, input$rfm_date, input$rfm_amount)
  })

  output$rfm_tbl <- renderDT({
    rfm <- rfm_result()
    datatable(rfm, options = list(pageLength = 10, scrollX = TRUE, dom = "ftip"), rownames = FALSE)
  })

  output$rfm_segment_plot <- renderPlot({
    rfm <- rfm_result()
    seg_counts <- rfm |> count(segment, sort = TRUE)
    ggplot(seg_counts, aes(x = reorder(segment, n), y = n, fill = segment)) +
      geom_col(show.legend = FALSE, alpha = 0.85) +
      coord_flip() +
      labs(title = "Customer segments", x = NULL, y = "Count") +
      scale_fill_manual(values = c("Champions" = "#198754", "Regular" = "#0d6efd", "New customers" = "#20c997", "At risk" = "#fd7e14", "Lost" = "#dc3545")) +
      theme_minimal(base_size = 13)
  })

  output$rfm_scatter_plot <- renderPlot({
    rfm <- rfm_result()
    ggplot(rfm, aes(x = recency, y = monetary, size = frequency, color = segment)) +
      geom_point(alpha = 0.6) +
      scale_size_continuous(range = c(2, 10)) +
      scale_color_manual(values = c("Champions" = "#198754", "Regular" = "#0d6efd", "New customers" = "#20c997", "At risk" = "#fd7e14", "Lost" = "#dc3545")) +
      labs(title = "RFM scatter", x = "Recency (days since last)", y = "Monetary (total spend)", size = "Frequency") +
      theme_minimal(base_size = 12)
  })

  output$rfm_heatmap <- renderPlot({
    rfm <- rfm_result()
    heat_data <- rfm |> group_by(R_score, F_score) |> summarise(avg_monetary = mean(monetary, na.rm = TRUE), .groups = "drop")
    ggplot(heat_data, aes(x = factor(F_score), y = factor(R_score), fill = avg_monetary)) +
      geom_tile(color = "white") +
      geom_text(aes(label = round(avg_monetary, 0)), size = 3.5) +
      scale_fill_gradient(low = "#EBF1F9", high = "#0d6efd", name = "Avg $") +
      labs(title = "RFM heatmap", x = "Frequency score", y = "Recency score") +
      theme_minimal(base_size = 12) + theme(panel.grid = element_blank())
  })

  # ── Pareto Chart ──
  pareto_result <- eventReactive(input$run_pareto, {
    df <- doctor_data(); req(df, input$pareto_var)
    make_pareto(df[[input$pareto_var]], title = paste("Pareto:", input$pareto_var))
  })

  output$pareto_plot <- renderPlot({
    pr <- pareto_result(); req(pr)
    d <- pr$data
    coeff <- max(d$count) / 100
    ggplot(d, aes(x = category)) +
      geom_col(aes(y = count), fill = "#0d6efd", alpha = 0.8, width = 0.7) +
      geom_line(aes(y = cum_pct * coeff, group = 1), color = "#dc3545", linewidth = 1.2) +
      geom_point(aes(y = cum_pct * coeff), color = "#dc3545", size = 3) +
      geom_hline(yintercept = 80 * coeff, linetype = "dashed", color = "#999") +
      annotate("text", x = nrow(d), y = 80 * coeff, label = "80%", hjust = 1.2, vjust = -0.5, color = "#999") +
      scale_y_continuous(name = "Count", sec.axis = sec_axis(~ . / coeff, name = "Cumulative %", labels = function(x) paste0(x, "%"))) +
      labs(title = pr$title, x = NULL) +
      theme_minimal(base_size = 13) + theme(axis.text.x = element_text(angle = 45, hjust = 1))
  })

  # ── Heatmap ──
  heatmap_result <- eventReactive(input$run_heatmap, {
    df <- doctor_data(); req(df, input$heat_vars)
    validate(need(length(input$heat_vars) >= 2, "Select at least 2 variables."))
    nums <- df[, input$heat_vars, drop = FALSE]
    nums <- nums[stats::complete.cases(nums), , drop = FALSE]
    validate(need(nrow(nums) >= 3, "Need at least 3 complete rows."))
    if (nrow(nums) > input$heat_max_vars) nums <- nums[1:input$heat_max_vars, ]
    list(data = nums, scale = input$heat_scale)
  })

  output$template_heatmap <- renderPlot({
    hr <- heatmap_result(); req(hr)
    mat <- as.matrix(hr$data)
    if (hr$scale == "row") mat <- t(scale(t(mat)))
    else if (hr$scale == "column") mat <- scale(mat)
    heatmap(mat, scale = "none", col = colorRampPalette(c("#2166AC", "white", "#B2182B"))(50),
            margins = c(8, 8), main = "Heatmap", cexRow = 0.7, cexCol = 0.8)
  })

  # ── Volcano Plot ──
  volcano_result <- eventReactive(input$run_volcano, {
    df <- doctor_data(); req(df, input$volcano_fc, input$volcano_p)
    data.frame(fc = df[[input$volcano_fc]], pval = df[[input$volcano_p]], stringsAsFactors = FALSE)
  })

  output$volcano_plot <- renderPlot({
    vr <- volcano_result(); req(vr)
    vr <- vr[!is.na(vr$fc) & !is.na(vr$pval) & vr$pval > 0, ]
    vr$log10p <- -log10(vr$pval)
    fc_thresh <- input$volcano_fc_thresh; p_thresh <- input$volcano_p_thresh
    vr$sig <- ifelse(abs(vr$fc) >= fc_thresh & vr$pval < p_thresh,
      ifelse(vr$fc > 0, "Up", "Down"), "NS")
    colors <- c("Up" = "#dc3545", "Down" = "#0d6efd", "NS" = "#cccccc")
    n_up <- sum(vr$sig == "Up"); n_down <- sum(vr$sig == "Down")
    ggplot(vr, aes(x = fc, y = log10p, color = sig)) +
      geom_point(alpha = 0.6, size = 1.5) +
      scale_color_manual(values = colors) +
      geom_vline(xintercept = c(-fc_thresh, fc_thresh), linetype = "dashed", color = "#999") +
      geom_hline(yintercept = -log10(p_thresh), linetype = "dashed", color = "#999") +
      labs(title = "Volcano plot", subtitle = paste0("Up: ", n_up, " | Down: ", n_down, " | NS: ", sum(vr$sig == "NS")),
           x = "Log2 fold change", y = "-Log10 p-value", color = "Significance") +
      theme_minimal(base_size = 13)
  })

  # ── Sample Datasets ──
  observeEvent(input$load_sample, {
    ds <- input$sample_dataset
    df <- tryCatch({
      if (ds == "titanic_df") {
        d <- as.data.frame(Titanic)
        d <- d[rep(seq_len(nrow(d)), d$Freq), 1:4]
        d$Survived <- ifelse(d$Survived == "Yes", 1, 0)
        rownames(d) <- NULL; d
      } else {
        as.data.frame(get(ds))
      }
    }, error = function(e) NULL)
    if (!is.null(df)) {
      doctor_state$original_data <- df
      doctor_state$current_data <- df
      doctor_state$fix_log <- c(doctor_state$fix_log, paste0("Loaded sample dataset: ", ds))
      doctor_state$fix_count <- 0
      showNotification(paste0("Loaded '", ds, "' (", nrow(df), " rows x ", ncol(df), " cols)"), type = "message")
    }
  })

  output$sample_info <- renderUI({
    ds <- input$sample_dataset
    info <- switch(ds,
      "mtcars" = "32 cars with 11 variables: mpg, cylinders, horsepower, weight, etc.",
      "iris" = "150 flowers, 3 species, 4 measurements. Classic classification dataset.",
      "airquality" = "153 daily air quality readings in NYC. Has missing values.",
      "ToothGrowth" = "60 guinea pigs, vitamin C dose vs. tooth length. DOE example.",
      "PlantGrowth" = "30 plants in 3 conditions. One-way ANOVA example.",
      "ChickWeight" = "Chick weights over time by diet. Repeated measures / growth curves.",
      "CO2" = "CO2 uptake in plants. Split-plot experiment.",
      "USArrests" = "50 US states, 4 crime variables. PCA / clustering example.",
      "faithful" = "Old Faithful geyser: eruption time vs. waiting. Bivariate / clustering.",
      "swiss" = "47 Swiss provinces, fertility + socioeconomic indicators. Regression.",
      "sleep" = "20 patients, 2 drugs, sleep increase. Paired t-test example.",
      "chickwts" = "71 chicks, 6 feed types, weight. ANOVA example.",
      "InsectSprays" = "72 observations, 6 sprays. DOE / ANOVA example.",
      "warpbreaks" = "54 observations, breaks by wool/tension. Two-way ANOVA / quality.",
      "titanic_df" = "2,201 passengers. Survival by class, sex, age. Classification / survival.",
      "Select a dataset.")
    div(class = "tab-note", p(info))
  })


shinyApp(ui = ui, server = server)
