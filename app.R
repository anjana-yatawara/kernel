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
    if (!is.null(frame$ofile)) return(dirname(normalizePath(frame$ofile, winslash = "/", mustWork = FALSE)))
  }
  normalizePath(getwd(), winslash = "/", mustWork = FALSE)
}
APP_DIR <- locate_app_dir()

`%||%` <- function(x, y) if (is.null(x) || length(x) == 0 || (length(x) == 1 && is.na(x)) || identical(x, "")) y else x
has_pkg <- function(pkg) requireNamespace(pkg, quietly = TRUE)

clean_names_base <- function(x) {
  x <- tolower(x); x <- gsub("[^a-z0-9]+", "_", x); x <- gsub("(^_+|_+$)", "", x); make.unique(x, sep = "_")
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
  env <- new.env(parent = emptyenv()); obj_names <- load(path, envir = env)
  obj_names[vapply(obj_names, function(nm) inherits(get(nm, envir = env), c("data.frame", "tbl_df", "tbl")), logical(1))]
}

import_data <- function(path, file_name, sheet = NULL, object_name = NULL) {
  ext <- tolower(tools::file_ext(file_name))
  if (has_pkg("rio") && ext %in% c("csv","tsv","txt","xlsx","xls","rds","rdata","sav","dta","sas7bdat","xpt","feather","parquet")) {
    imported <- tryCatch({
      if (ext %in% c("xlsx","xls") && !is.null(sheet) && nzchar(sheet)) rio::import(path, which = sheet)
      else rio::import(path)
    }, error = function(e) NULL)
    if (!is.null(imported) && inherits(imported, c("data.frame","tbl_df","tbl"))) return(as.data.frame(imported))
  }
  if (ext == "csv") return(as.data.frame(readr::read_csv(path, show_col_types = FALSE, progress = FALSE)))
  if (ext == "tsv") return(as.data.frame(readr::read_tsv(path, show_col_types = FALSE, progress = FALSE)))
  if (ext == "txt") { delim <- guess_delim_base(path); return(as.data.frame(readr::read_delim(path, delim = delim, show_col_types = FALSE, progress = FALSE))) }
  if (ext %in% c("xlsx","xls")) return(as.data.frame(readxl::read_excel(path, sheet = sheet %||% 1)))
  if (ext == "rds") { obj <- readRDS(path); stopifnot(inherits(obj, "data.frame")); return(as.data.frame(obj)) }
  if (ext == "rdata") {
    env <- new.env(parent = emptyenv()); obj_names <- load(path, envir = env)
    if (is.null(object_name) || !nzchar(object_name)) { df_names <- list_rdata_frames(path); object_name <- df_names[[1]] }
    return(as.data.frame(get(object_name, envir = env)))
  }
  stop("Unsupported file extension.")
}

prep_data <- function(df, clean_names = TRUE, drop_empty = TRUE) {
  out <- as.data.frame(df, stringsAsFactors = FALSE)
  if (clean_names) names(out) <- clean_names_base(names(out))
  if (drop_empty && ncol(out) > 0) {
    keep_cols <- vapply(out, function(col) any(!(is.na(as.character(col)) | trimws(as.character(col)) == "")), logical(1))
    out <- out[, keep_cols, drop = FALSE]
  }
  if (drop_empty && nrow(out) > 0 && ncol(out) > 0) {
    keep_rows <- apply(out, 1, function(r) any(!(is.na(as.character(r)) | trimws(as.character(r)) == "")))
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
    example = vapply(df, function(x) {
      xn <- x[!is.na(x)]; if (!length(xn)) return(NA_character_)
      if (is.numeric(x)) return(as.character(signif(stats::median(xn), 5)))
      tab <- sort(table(as.character(xn)), decreasing = TRUE); paste0(names(tab)[1], " (n=", as.integer(tab[1]), ")")
    }, character(1)),
    row.names = NULL, check.names = FALSE
  )
}

categorical_vars_of <- function(df) names(df)[vapply(df, function(x) is.factor(x) || is.character(x) || is.logical(x) || inherits(x, "ordered"), logical(1))]
numeric_vars_of <- function(df) names(df)[vapply(df, is.numeric, logical(1))]
coerce_categorical <- function(x) { if (is.logical(x)) return(factor(x)); if (is.character(x)) return(factor(x)); if (is.factor(x)) return(x); factor(x) }
safe_complete <- function(df) { if (is.null(df) || !ncol(df)) return(df); df[stats::complete.cases(df), , drop = FALSE] }

cramers_v_manual <- function(tbl) {
  chi <- suppressWarnings(chisq.test(tbl, correct = FALSE)); n <- sum(tbl); k <- min(nrow(tbl) - 1, ncol(tbl) - 1)
  if (k <= 0 || n == 0) return(NA_real_); sqrt(unname(chi$statistic) / (n * k))
}

odds_and_risk <- function(tbl) {
  if (!all(dim(tbl) == c(2, 2))) return(NULL)
  a <- tbl[1,1]; b <- tbl[1,2]; cc <- tbl[2,1]; d <- tbl[2,2]
  or <- ifelse(any(c(b, cc) == 0), NA_real_, (a * d) / (b * cc))
  rr <- ifelse((a + b) == 0 || (cc + d) == 0, NA_real_, (a / (a + b)) / (cc / (cc + d)))
  data.frame(metric = c("Odds ratio", "Relative risk"), value = c(or, rr), stringsAsFactors = FALSE)
}

build_formula_text <- function(outcome, predictors, interactions = FALSE, poly_degree = 1) {
  predictors <- predictors[predictors != outcome]
  if (!length(predictors)) return(paste(outcome, "~ 1"))
  rhs <- paste(predictors, collapse = " + ")
  if (isTRUE(interactions) && length(predictors) > 1) rhs <- paste0("(", rhs, ")^2")
  if (!is.null(poly_degree) && poly_degree > 1 && length(predictors) == 1) rhs <- paste0("poly(", predictors[[1]], ", ", poly_degree, ", raw = TRUE)")
  paste(outcome, "~", rhs)
}

extract_coefficients <- function(fit) {
  if (inherits(fit, c("lm", "glm", "negbin"))) {
    out <- as.data.frame(summary(fit)$coefficients); names(out) <- c("estimate","std_error","statistic","p_value")
    out$term <- rownames(out); rownames(out) <- NULL
    out$conf_low <- out$estimate - 1.96 * out$std_error; out$conf_high <- out$estimate + 1.96 * out$std_error
    return(out[, c("term","estimate","std_error","statistic","p_value","conf_low","conf_high")])
  }
  if (inherits(fit, "multinom")) {
    sm <- summary(fit); coef_mat <- as.matrix(sm$coefficients); se_mat <- as.matrix(sm$standard.errors)
    out <- data.frame(class_level = rep(rownames(coef_mat), each = ncol(coef_mat)),
      term = rep(colnames(coef_mat), times = nrow(coef_mat)),
      estimate = as.vector(coef_mat), std_error = as.vector(se_mat), stringsAsFactors = FALSE)
    out$statistic <- out$estimate / out$std_error; out$p_value <- 2 * pnorm(abs(out$statistic), lower.tail = FALSE)
    out$conf_low <- out$estimate - 1.96 * out$std_error; out$conf_high <- out$estimate + 1.96 * out$std_error; return(out)
  }
  if (inherits(fit, "clm")) {
    out <- as.data.frame(summary(fit)$coefficients); out$term <- rownames(out); rownames(out) <- NULL
    names(out) <- c("estimate","std_error","statistic","p_value","term")
    out$conf_low <- out$estimate - 1.96 * out$std_error; out$conf_high <- out$estimate + 1.96 * out$std_error
    return(out[, c("term","estimate","std_error","statistic","p_value","conf_low","conf_high")])
  }
  if (inherits(fit, c("zeroinfl", "hurdle"))) {
    sm <- summary(fit); count <- as.data.frame(sm$coefficients$count); zero <- as.data.frame(sm$coefficients$zero)
    count$component <- "count"; count$term <- rownames(count); zero$component <- "zero"; zero$term <- rownames(zero)
    out <- bind_rows(count, zero); rownames(out) <- NULL; names(out)[1:4] <- c("estimate","std_error","statistic","p_value")
    out$conf_low <- out$estimate - 1.96 * out$std_error; out$conf_high <- out$estimate + 1.96 * out$std_error
    return(out[, c("component","term","estimate","std_error","statistic","p_value","conf_low","conf_high")])
  }
  data.frame()
}

model_metrics <- function(fit) {
  metrics <- data.frame(metric = "Model class", value = class(fit)[1], stringsAsFactors = FALSE)
  add_m <- function(nm, val) { if (!is.null(val) && !is.na(val)) data.frame(metric = nm, value = format(signif(as.numeric(val), 5), scientific = FALSE), stringsAsFactors = FALSE) else NULL }
  metrics <- bind_rows(metrics, add_m("Observations", tryCatch(stats::nobs(fit), error = function(e) NA)),
    add_m("AIC", tryCatch(AIC(fit), error = function(e) NA)),
    add_m("BIC", tryCatch(BIC(fit), error = function(e) NA)),
    add_m("LogLik", tryCatch(as.numeric(logLik(fit)), error = function(e) NA)))
  if (inherits(fit, "lm") && !inherits(fit, "glm")) {
    sm <- summary(fit); metrics <- bind_rows(metrics, add_m("R-squared", sm$r.squared), add_m("Adj. R-squared", sm$adj.r.squared), add_m("Sigma", sm$sigma))
  }
  if (inherits(fit, "glm")) {
    metrics <- bind_rows(metrics, add_m("Residual deviance", fit$deviance), add_m("Pseudo R-squared", tryCatch(1 - fit$deviance / fit$null.deviance, error = function(e) NA)))
  }
  metrics
}

methods_catalog <- function() {
  path <- file.path(APP_DIR, "methods_catalog.csv")
  if (file.exists(path)) return(read.csv(path, stringsAsFactors = FALSE))
  data.frame(domain = "Platform", method = "Catalog", purpose = "Fallback", stringsAsFactors = FALSE)
}

package_status <- function() {
  packages <- c("rio","haven","janitor","naniar","FactoMineR","factoextra","ca","vcd","vcdExtra","DescTools","epitools",
    "nnet","ordinal","VGAM","pscl","brglm2","glmnet","mgcv","lme4","geepack","survey","FrF2","DoE.base","rsm","lhs",
    "AlgDesign","skpr","mixexp","cluster","poLCA","forecast","tseries","rugarch","xts","zoo","survival","survminer",
    "rmarkdown","rpart","rpart.plot","ranger","xgboost","e1071","class","pROC","caret")
  data.frame(package = packages, installed = ifelse(vapply(packages, has_pkg, logical(1)), "Yes", "No"), stringsAsFactors = FALSE)
}

make_design <- function(type, vars, runs, levels, center_points, randomize) {
  if (type == "Full factorial") { vals <- seq(-1, 1, length.out = levels); design <- expand.grid(rep(list(vals), length(vars))); names(design) <- vars; return(design) }
  if (type == "Fractional factorial (2-level)") { stopifnot(has_pkg("FrF2")); return(as.data.frame(FrF2::FrF2(nruns = runs, nfactors = length(vars), factor.names = vars, randomize = randomize))) }
  if (type == "Central composite") { stopifnot(has_pkg("rsm")); d <- as.data.frame(rsm::ccd(k = length(vars), n0 = c(center_points, center_points), randomize = randomize, coding = FALSE)); names(d)[seq_along(vars)] <- vars; return(d) }
  if (type == "Box-Behnken") { stopifnot(has_pkg("rsm"), length(vars) >= 3); d <- as.data.frame(rsm::bbd(k = length(vars), n0 = center_points, randomize = randomize, coding = FALSE)); names(d)[seq_along(vars)] <- vars; return(d) }
  if (type == "Latin hypercube") { stopifnot(has_pkg("lhs")); d <- as.data.frame(lhs::randomLHS(runs, length(vars))); names(d) <- vars; return(d) }
  if (type == "D-optimal") { stopifnot(has_pkg("AlgDesign")); cand <- AlgDesign::gen.factorial(levels = levels, nVars = length(vars), factors = "all", varNames = vars); ft <- paste("~ (", paste(vars, collapse = " + "), ")^2"); return(as.data.frame(AlgDesign::optFederov(as.formula(ft), data = cand, nTrials = runs)$design)) }
  stop("Unknown design type.")
}

# ══════════════════════════════════════════════════════════════
# PHASE 1: VARIABLE DOCTOR
# ══════════════════════════════════════════════════════════════

CODED_MISSING <- c("999","-999","9999","-9999","99","-99",".","..","-","--",
  "N/A","n/a","NA","na","NULL","null","None","none","missing","Missing",
  "MISSING","unknown","Unknown","not available","#N/A","#NA","NaN","nan","")

diagnose_variables <- function(df) {
  if (is.null(df) || ncol(df) == 0) return(data.frame(variable=character(), severity=character(), issue=character(), detail=character(), fix_action=character(), fix_id=character(), stringsAsFactors=FALSE))
  findings <- list()
  add <- function(v,s,i,d,fa,fid) { findings[[length(findings)+1]] <<- data.frame(variable=v, severity=s, issue=i, detail=d, fix_action=fa, fix_id=fid, stringsAsFactors=FALSE) }
  for (nm in names(df)) {
    x <- df[[nm]]; n <- length(x); n_na <- sum(is.na(x)); n_valid <- n - n_na; x_nona <- x[!is.na(x)]; n_unique <- length(unique(x_nona))
    if (n_unique <= 1) { add(nm,"critical","Constant column",paste0(n_unique," unique value(s)."),"drop_column",paste0("const_",nm)); next }
    pct_na <- round(n_na/n*100,1)
    if (pct_na >= 50) add(nm,"critical","Mostly missing",paste0(pct_na,"% missing."),"drop_column",paste0("hmiss_",nm))
    else if (pct_na >= 20) add(nm,"warning","Notable missingness",paste0(pct_na,"% missing."),"none",paste0("mmiss_",nm))
    if (is.character(x) || is.factor(x)) {
      xc <- trimws(as.character(x_nona)); coded <- intersect(xc, CODED_MISSING)
      if (length(coded) > 0) { ct <- table(xc[xc %in% coded]); dp <- paste0('"',names(ct),'" (n=',as.integer(ct),')'); add(nm,"critical","Coded missing values",paste0("Found: ",paste(dp,collapse=", ")),"recode_na",paste0("coded_",nm)) }
    }
    if (is.numeric(x)) {
      sn <- c(999,-999,9999,-9999); fd <- sn[sn %in% x_nona]
      if (length(fd) > 0) { cnts <- sapply(fd, function(v) sum(x_nona==v)); if (any(cnts>=3)) add(nm,"warning","Possible coded missing",paste0("Values ",paste(fd[cnts>=3],collapse=", ")," appear often."),"recode_na_numeric",paste0("nc_",nm)) }
    }
    if (is.character(x)) {
      xt <- trimws(x_nona); xt <- xt[!xt %in% CODED_MISSING]
      if (length(xt) > 0) { pn <- sum(suppressWarnings(!is.na(as.numeric(xt))))/length(xt)*100; if (pn >= 90) add(nm,"critical","Numeric stored as text",paste0(round(pn,1),"% parseable as numeric."),"convert_numeric",paste0("nchr_",nm)) }
    }
    if (is.character(x)) {
      xs <- head(trimws(x_nona),50); xs <- xs[!xs %in% CODED_MISSING & nchar(xs)>=6]
      if (length(xs) >= 3) for (p in c("^\\d{4}-\\d{2}-\\d{2}","^\\d{2}/\\d{2}/\\d{4}","^\\d{1,2}/\\d{1,2}/\\d{2,4}")) {
        if (sum(grepl(p,xs))/length(xs) >= 0.8) { add(nm,"warning","Date stored as text","Values look like dates.","convert_date",paste0("dt_",nm)); break }
      }
    }
    if (is.numeric(x) && !inherits(x,c("Date","POSIXct")) && n_unique<=7 && n_valid>=10) add(nm,"info","Low-cardinality numeric",paste0(n_unique," unique values. Factor?"),"convert_factor",paste0("lc_",nm))
    if (is.numeric(x) && n_unique==2 && all(sort(unique(x_nona))==c(0,1))) add(nm,"info","Binary 0/1","Convert to Yes/No?","convert_binary_factor",paste0("bin_",nm))
    if (is.numeric(x) && n_unique==n_valid && n_valid>=10) { s <- sort(x_nona); d <- diff(s); if (all(d>=0) && all(d<=2)) add(nm,"info","ID-like column","Monotonic + all unique.","drop_column",paste0("id_",nm)) }
    if (is.character(x)) { xc <- as.character(x_nona); nw <- sum(xc!=trimws(xc)); if (nw>0) add(nm,"warning","Whitespace issues",paste0(nw," values affected."),"trim_whitespace",paste0("ws_",nm)) }
    if ((is.character(x)||is.factor(x)) && n_unique>50) add(nm,"warning","High cardinality",paste0(n_unique," levels."),"none",paste0("hc_",nm))
    if (is.numeric(x) && n_valid>=20) { q1<-quantile(x,.25,na.rm=T); q3<-quantile(x,.75,na.rm=T); iqr<-q3-q1; if(iqr>0){no<-sum(x_nona<q1-3*iqr|x_nona>q3+3*iqr); if(no>0) add(nm,"info","Extreme outliers",paste0(no," values beyond 3xIQR."),"none",paste0("out_",nm))} }
    if (is.logical(x)) add(nm,"info","Logical column","Convert to factor?","convert_factor",paste0("log_",nm))
  }
  if (length(findings)==0) return(data.frame(variable=character(),severity=character(),issue=character(),detail=character(),fix_action=character(),fix_id=character(),stringsAsFactors=FALSE))
  do.call(rbind, findings)
}

icon_for_fix <- function(action) switch(action, "drop_column"="Drop","recode_na"="Fix NA","recode_na_numeric"="Fix NA","convert_numeric"="Numeric","convert_date"="Date","convert_factor"="Factor","convert_binary_factor"="Yes/No","trim_whitespace"="Trim","Fix")

apply_fix <- function(df, variable, action) {
  if (!variable %in% names(df)) return(list(data=NULL, message=paste0("'",variable,"' not found.")))
  x <- df[[variable]]
  if (action=="drop_column") { df[[variable]]<-NULL; return(list(data=df,message=paste0("Dropped '",variable,"'."))) }
  if (action=="recode_na") { xc<-trimws(as.character(x)); nb<-sum(is.na(x)); xc[xc %in% CODED_MISSING]<-NA; rem<-xc[!is.na(xc)]; if(length(rem)>0 && all(!is.na(suppressWarnings(as.numeric(rem))))) df[[variable]]<-as.numeric(xc) else df[[variable]]<-xc; return(list(data=df,message=paste0("'",variable,"': coded values to NA."))) }
  if (action=="recode_na_numeric") { sn<-c(999,-999,9999,-9999); fd<-sn[sn %in% x[!is.na(x)]]; nf<-sum(x %in% fd,na.rm=T); df[[variable]][df[[variable]] %in% fd]<-NA; return(list(data=df,message=paste0("'",variable,"': ",nf," sentinels to NA."))) }
  if (action=="convert_numeric") { xc<-trimws(as.character(x)); xc[xc %in% CODED_MISSING]<-NA; df[[variable]]<-suppressWarnings(as.numeric(xc)); return(list(data=df,message=paste0("'",variable,"': to numeric."))) }
  if (action=="convert_date") { xc<-trimws(as.character(x)); for(fmt in c("%Y-%m-%d","%m/%d/%Y","%d/%m/%Y","%m-%d-%Y","%Y/%m/%d")){att<-as.Date(xc,format=fmt); if(sum(!is.na(att))/sum(!is.na(xc))>0.8){df[[variable]]<-att;break}}; return(list(data=df,message=paste0("'",variable,"': to Date."))) }
  if (action=="convert_factor") { df[[variable]]<-factor(df[[variable]]); return(list(data=df,message=paste0("'",variable,"': to factor."))) }
  if (action=="convert_binary_factor") { df[[variable]]<-factor(df[[variable]],levels=c(0,1),labels=c("No","Yes")); return(list(data=df,message=paste0("'",variable,"': 0/1 to Yes/No."))) }
  if (action=="trim_whitespace") { df[[variable]]<-trimws(as.character(df[[variable]])); return(list(data=df,message=paste0("'",variable,"': trimmed."))) }
  list(data=df, message="Unknown fix.")
}

# ══════════════════════════════════════════════════════════════
# PHASE 2-3: BIVARIATE + CORRELATION + DIAGNOSTICS
# ══════════════════════════════════════════════════════════════

compute_cor_matrix <- function(df, method = "pearson") {
  nums <- df[, vapply(df, is.numeric, logical(1)), drop=FALSE]
  nums <- nums[, vapply(nums, function(x) stats::sd(x,na.rm=T)>0, logical(1)), drop=FALSE]
  if (ncol(nums)<2) return(NULL)
  cor_mat <- cor(nums, use="pairwise.complete.obs", method=method)
  n <- ncol(nums); p_mat <- matrix(NA,n,n,dimnames=list(colnames(nums),colnames(nums)))
  for (i in 1:(n-1)) for (j in (i+1):n) { ct<-tryCatch(cor.test(nums[[i]],nums[[j]],method=method),error=function(e)NULL); if(!is.null(ct)){p_mat[i,j]<-ct$p.value;p_mat[j,i]<-ct$p.value} }
  list(cor=cor_mat, pval=p_mat, vars=colnames(nums))
}

reshape_cor <- function(cor_mat, p_mat) {
  vars <- rownames(cor_mat); n <- length(vars); rows <- list()
  for (i in seq_len(n)) for (j in seq_len(n)) {
    sig <- ""; if (i!=j && !is.na(p_mat[i,j])) { if (p_mat[i,j]<0.001) sig<-"***" else if (p_mat[i,j]<0.01) sig<-"**" else if (p_mat[i,j]<0.05) sig<-"*" }
    rows[[length(rows)+1]] <- data.frame(Var1=vars[i],Var2=vars[j],value=cor_mat[i,j],label=paste0(round(cor_mat[i,j],2),sig),stringsAsFactors=FALSE)
  }
  out <- do.call(rbind,rows); out$Var1<-factor(out$Var1,levels=vars); out$Var2<-factor(out$Var2,levels=rev(vars)); out
}

check_assumptions_lm <- function(fit) {
  res <- residuals(fit); fv <- fitted(fit); checks <- list()
  n <- length(res)
  sw <- if (n>=3 && n<=5000) tryCatch(shapiro.test(res),error=function(e)NULL) else NULL
  if (!is.null(sw)) checks$normality <- data.frame(test="Shapiro-Wilk",statistic=round(sw$statistic,4),p_value=signif(sw$p.value,4),verdict=ifelse(sw$p.value<0.05,"FAIL: Non-normal residuals","PASS: Normality OK"),stringsAsFactors=FALSE)
  bp <- tryCatch({res2<-res^2;bp_lm<-lm(res2~fv);sm<-summary(bp_lm);fp<-pf(sm$fstatistic[1],sm$fstatistic[2],sm$fstatistic[3],lower.tail=FALSE)
    data.frame(test="Heteroscedasticity",statistic=round(sm$fstatistic[1],4),p_value=signif(fp,4),verdict=ifelse(fp<0.05,"FAIL: Non-constant variance","PASS: Homoscedasticity OK"),stringsAsFactors=FALSE)},error=function(e)NULL)
  if (!is.null(bp)) checks$homoscedasticity <- bp
  dw <- tryCatch({d<-sum(diff(res)^2)/sum(res^2); data.frame(test="Durbin-Watson",statistic=round(d,4),p_value=NA,verdict=ifelse(d<1.5,"WARNING: Positive autocorrelation",ifelse(d>2.5,"WARNING: Negative autocorrelation","PASS: No autocorrelation")),stringsAsFactors=FALSE)},error=function(e)NULL)
  if (!is.null(dw)) checks$autocorrelation <- dw
  if (length(coef(fit))>2) {
    vif_vals <- tryCatch({X<-model.matrix(fit)[,-1,drop=FALSE]; if(ncol(X)>=2){R2<-sapply(seq_len(ncol(X)),function(j)summary(lm(X[,j]~X[,-j]))$r.squared); vif<-1/(1-R2)
      data.frame(predictor=colnames(X),VIF=round(vif,2),verdict=ifelse(vif>10,"HIGH",ifelse(vif>5,"MODERATE","OK")),stringsAsFactors=FALSE)} else NULL},error=function(e)NULL)
    if (!is.null(vif_vals)) checks$vif <- vif_vals
  }
  checks
}

# ══════════════════════════════════════════════════════════════
# PHASE 4: HYPOTHESIS TESTING ENGINE
# ══════════════════════════════════════════════════════════════

find_test <- function(n_groups, paired, data_type) {
  if (data_type=="continuous") {
    if (n_groups==1) return(data.frame(test=c("One-sample t-test","Wilcoxon signed-rank","Sign test"),assumption=c("Normal","Symmetric","None"),use_when=c("Test mean","Non-normal symmetric","Minimal assumptions"),stringsAsFactors=FALSE))
    if (n_groups==2 && !paired) return(data.frame(test=c("Welch t-test","Mann-Whitney U"),assumption=c("Normal","Any"),use_when=c("Compare means","Compare distributions"),stringsAsFactors=FALSE))
    if (n_groups==2 && paired) return(data.frame(test=c("Paired t-test","Wilcoxon paired"),assumption=c("Normal diffs","Any"),use_when=c("Before/after parametric","Before/after nonparametric"),stringsAsFactors=FALSE))
    return(data.frame(test=c("One-way ANOVA","Welch ANOVA","Kruskal-Wallis","Tukey HSD"),assumption=c("Normal+equal var","Normal","Any","Post-hoc"),use_when=c("Compare 3+ means","Unequal variance","Nonparametric","Which pairs differ"),stringsAsFactors=FALSE))
  }
  if (data_type=="categorical") {
    if (n_groups==1) return(data.frame(test=c("Chi-square GOF","Binomial test"),assumption=c("Expected>=5","Binary"),use_when=c("Test proportions","Single proportion"),stringsAsFactors=FALSE))
    return(data.frame(test=c("Chi-square","Fisher exact","Two-proportion z"),assumption=c("Expected>=5","Any size","Binary"),use_when=c("Test independence","Small samples","Compare proportions"),stringsAsFactors=FALSE))
  }
  data.frame(test="See bivariate tab",assumption="-",use_when="Mixed types",stringsAsFactors=FALSE)
}

run_hypothesis_test <- function(df, test_name, var1, var2=NULL, mu0=0, p0=0.5, conf_level=0.95) {
  x <- df[[var1]]; x <- x[!is.na(x)]; alpha <- 1 - conf_level
  y <- if (!is.null(var2) && var2 != "" && var2 %in% names(df)) df[[var2]][!is.na(df[[var2]])] else NULL
  tryCatch({
    switch(test_name,
      "One-sample t-test"={tt<-t.test(x,mu=mu0,conf.level=conf_level); data.frame(statistic=c("t","df","p-value","Mean","CI low","CI high","n"),value=c(round(tt$statistic,4),round(tt$parameter,2),signif(tt$p.value,4),round(tt$estimate,4),round(tt$conf.int[1],4),round(tt$conf.int[2],4),length(x)),stringsAsFactors=FALSE)},
      "Wilcoxon signed-rank"={wt<-wilcox.test(x,mu=mu0,exact=FALSE,conf.int=TRUE); data.frame(statistic=c("V","p-value","Pseudomedian","n"),value=c(round(wt$statistic,2),signif(wt$p.value,4),round(wt$estimate,4),length(x)),stringsAsFactors=FALSE)},
      "Sign test"={pos<-sum(x>mu0);neg<-sum(x<mu0);nn<-pos+neg;pv<-2*min(pbinom(min(pos,neg),nn,0.5),1); data.frame(statistic=c("Positive","Negative","p-value"),value=c(pos,neg,signif(min(pv,1),4)),stringsAsFactors=FALSE)},
      "Binomial test"={s<-sum(x==1|x==TRUE);n<-length(x);bt<-binom.test(s,n,p=p0); data.frame(statistic=c("Successes","Trials","Proportion","p-value"),value=c(bt$statistic,bt$parameter,round(bt$estimate,4),signif(bt$p.value,4)),stringsAsFactors=FALSE)},
      "Chi-square GOF"={tab<-table(factor(x));ct<-chisq.test(tab); data.frame(statistic=c("Chi-sq","df","p-value"),value=c(round(ct$statistic,4),ct$parameter,signif(ct$p.value,4)),stringsAsFactors=FALSE)},
      "Welch t-test"={stopifnot(!is.null(y));g<-factor(y);levs<-levels(g);tt<-t.test(x[g==levs[1]],x[g==levs[2]]); data.frame(statistic=c("t","df","p-value","Mean1","Mean2","Cohen d"),value=c(round(tt$statistic,4),round(tt$parameter,2),signif(tt$p.value,4),round(tt$estimate[1],4),round(tt$estimate[2],4),round(abs(diff(tt$estimate))/sqrt(mean(c(var(x[g==levs[1]]),var(x[g==levs[2]])))),4)),stringsAsFactors=FALSE)},
      "Paired t-test"={stopifnot(!is.null(y));cc<-complete.cases(df[[var1]],df[[var2]]);tt<-t.test(df[[var1]][cc],df[[var2]][cc],paired=TRUE); data.frame(statistic=c("t","df","p-value","Mean diff"),value=c(round(tt$statistic,4),round(tt$parameter,2),signif(tt$p.value,4),round(tt$estimate,4)),stringsAsFactors=FALSE)},
      "Mann-Whitney U"={stopifnot(!is.null(y));g<-factor(y);levs<-levels(g);wt<-wilcox.test(x[g==levs[1]],x[g==levs[2]],exact=FALSE); data.frame(statistic=c("W","p-value"),value=c(round(wt$statistic,2),signif(wt$p.value,4)),stringsAsFactors=FALSE)},
      "Wilcoxon paired"={stopifnot(!is.null(y));cc<-complete.cases(df[[var1]],df[[var2]]);wt<-wilcox.test(df[[var1]][cc],df[[var2]][cc],paired=TRUE,exact=FALSE); data.frame(statistic=c("V","p-value"),value=c(round(wt$statistic,2),signif(wt$p.value,4)),stringsAsFactors=FALSE)},
      "One-way ANOVA"={stopifnot(!is.null(y));av<-summary(aov(x~factor(y))); data.frame(statistic=c("F","p-value","Eta-sq"),value=c(round(av[[1]]$`F value`[1],4),signif(av[[1]]$`Pr(>F)`[1],4),round(av[[1]]$`Sum Sq`[1]/sum(av[[1]]$`Sum Sq`),4)),stringsAsFactors=FALSE)},
      "Welch ANOVA"={stopifnot(!is.null(y));ow<-oneway.test(x~factor(y),var.equal=FALSE); data.frame(statistic=c("F","df1","df2","p-value"),value=c(round(ow$statistic,4),round(ow$parameter[1],2),round(ow$parameter[2],2),signif(ow$p.value,4)),stringsAsFactors=FALSE)},
      "Kruskal-Wallis"={stopifnot(!is.null(y));kt<-kruskal.test(x~factor(y)); data.frame(statistic=c("H","df","p-value"),value=c(round(kt$statistic,4),kt$parameter,signif(kt$p.value,4)),stringsAsFactors=FALSE)},
      "Tukey HSD"={stopifnot(!is.null(y));tk<-TukeyHSD(aov(x~factor(y)));out<-as.data.frame(tk[[1]]);out$comparison<-rownames(out);rownames(out)<-NULL;out},
      "Shapiro-Wilk"={xs<-if(length(x)>5000) sample(x,5000) else x;sw<-shapiro.test(xs); data.frame(statistic=c("W","p-value","Verdict"),value=c(round(sw$statistic,5),signif(sw$p.value,4),ifelse(sw$p.value<0.05,"Non-normal","Normal OK")),stringsAsFactors=FALSE)},
      "Anderson-Darling"={xs<-sort(x);nn<-length(xs);mn<-mean(xs);sd<-sd(xs);z<-pnorm((xs-mn)/sd);z<-pmax(pmin(z,1-1e-10),1e-10);S<-sum((2*seq_len(nn)-1)*(log(z)+log(1-rev(z))))/nn;A2<--nn-S; data.frame(statistic=c("A-squared","p (approx)"),value=c(round(A2,4),"See tables"),stringsAsFactors=FALSE)},
      "Kolmogorov-Smirnov"={ks<-ks.test(x,"pnorm",mean(x),sd(x)); data.frame(statistic=c("D","p-value"),value=c(round(ks$statistic,5),signif(ks$p.value,4)),stringsAsFactors=FALSE)},
      "Power: t-test"={d_vals<-c(0.2,0.5,0.8);ns<-sapply(d_vals,function(d)ceiling(power.t.test(delta=d,sd=1,sig.level=alpha,power=0.8)$n)); data.frame(statistic=paste0("d=",d_vals),value=paste0("n=",ns),stringsAsFactors=FALSE)},
      data.frame(statistic="Error",value=paste0("Unknown test: ",test_name),stringsAsFactors=FALSE)
    )
  }, error=function(e) data.frame(statistic="Error",value=e$message,stringsAsFactors=FALSE))
}

# ══════════════════════════════════════════════════════════════
# PHASE 5: EDUCATION LAYER
# ══════════════════════════════════════════════════════════════

interpret_p_value <- function(p, alpha=0.05) {
  if (is.na(p)) return("p-value not available.")
  if (p < 0.001) "Very strong evidence against H0 (p < 0.001)."
  else if (p < alpha) paste0("Significant at ",alpha*100,"% level (p = ",signif(p,3),"). Reject H0.")
  else paste0("Not significant (p = ",signif(p,3),"). Cannot reject H0.")
}

interpret_r_squared <- function(r2, adj_r2=NULL) {
  pct <- round(r2*100,1)
  q <- if(r2>=0.9)"excellent" else if(r2>=0.7)"good" else if(r2>=0.5)"moderate" else "weak"
  msg <- paste0("R² = ",round(r2,4),". Model explains ",pct,"% of variance (",q," fit).")
  if (!is.null(adj_r2)) msg <- paste0(msg," Adjusted R² = ",round(adj_r2,4),".")
  msg
}

# ══════════════════════════════════════════════════════════════
# PHASE 6: TIME SERIES ENGINE
# ══════════════════════════════════════════════════════════════

detect_date_column <- function(df) {
  for (nm in names(df)) if (inherits(df[[nm]], c("Date","POSIXct"))) return(nm)
  for (nm in names(df)) if (grepl("date|time|day|month|year",tolower(nm)) && is.character(df[[nm]])) return(nm)
  NULL
}

detect_frequency <- function(dates) {
  if (length(dates)<3) return(1); d <- stats::median(as.numeric(diff(sort(dates))),na.rm=TRUE)
  if (d<=1.5) 365 else if (d<=8) 52 else if (d<=35) 12 else if (d<=100) 4 else 1
}

freq_label <- function(f) switch(as.character(f),"365"="Daily","252"="Trading","52"="Weekly","12"="Monthly","4"="Quarterly","1"="Annual",paste0("f=",f))

make_ts_object <- function(values, dates, freq) {
  ord<-order(dates); values<-values[ord]; dates<-dates[ord]
  sy <- as.numeric(format(dates[1],"%Y")); sp <- if(freq==12) as.numeric(format(dates[1],"%m")) else if(freq==4) ceiling(as.numeric(format(dates[1],"%m"))/3) else 1
  ts(values, start=c(sy,sp), frequency=freq)
}

run_stationarity_tests <- function(x) {
  results <- list()
  if (has_pkg("tseries")) {
    adf<-tryCatch({t<-tseries::adf.test(x);data.frame(test="ADF",statistic=round(t$statistic,4),p_value=signif(t$p.value,4),verdict=ifelse(t$p.value<0.05,"STATIONARY","NON-STATIONARY"),stringsAsFactors=FALSE)},error=function(e)NULL); if(!is.null(adf)) results$adf<-adf
    kpss<-tryCatch({t<-tseries::kpss.test(x);data.frame(test="KPSS",statistic=round(t$statistic,4),p_value=signif(t$p.value,4),verdict=ifelse(t$p.value<0.05,"NON-STATIONARY","STATIONARY"),stringsAsFactors=FALSE)},error=function(e)NULL); if(!is.null(kpss)) results$kpss<-kpss
    pp<-tryCatch({t<-tseries::pp.test(x);data.frame(test="Phillips-Perron",statistic=round(t$statistic,4),p_value=signif(t$p.value,4),verdict=ifelse(t$p.value<0.05,"STATIONARY","NON-STATIONARY"),stringsAsFactors=FALSE)},error=function(e)NULL); if(!is.null(pp)) results$pp<-pp
  }
  if (length(results)==0) return(data.frame(test="N/A",statistic=NA,p_value=NA,verdict="Install tseries",stringsAsFactors=FALSE))
  do.call(rbind,results)
}

# ══════════════════════════════════════════════════════════════
# PHASE 7: SURVIVAL HELPERS
# ══════════════════════════════════════════════════════════════

detect_event_column <- function(df) {
  for (nm in names(df)) { x<-df[[nm]][!is.na(df[[nm]])]; if(is.numeric(x) && all(x %in% c(0,1)) && length(unique(x))==2 && grepl("event|status|dead|censor|fail",tolower(nm))) return(nm) }
  for (nm in names(df)) { x<-df[[nm]][!is.na(df[[nm]])]; if(is.numeric(x) && all(x %in% c(0,1)) && length(unique(x))==2) return(nm) }
  NULL
}
detect_time_column <- function(df) {
  for (nm in names(df)) if (grepl("time|duration|survival|follow|tenure|days|months|los",tolower(nm)) && is.numeric(df[[nm]])) return(nm)
  nums <- numeric_vars_of(df); if(length(nums)) nums[1] else NULL
}

# ══════════════════════════════════════════════════════════════
# PHASE 9: ML ENGINE
# ══════════════════════════════════════════════════════════════

ml_detect_task <- function(y) if (is.factor(y)||is.character(y)||is.logical(y)||(is.numeric(y)&&length(unique(y[!is.na(y)]))<=10)) "classification" else "regression"

ml_split_data <- function(df, outcome, predictors, pct=0.8, seed=42) {
  set.seed(seed); d<-df[,unique(c(outcome,predictors)),drop=FALSE]; d<-d[complete.cases(d),,drop=FALSE]
  idx<-sample(nrow(d),floor(nrow(d)*pct)); list(train=d[idx,,drop=FALSE],test=d[-idx,,drop=FALSE])
}

ml_fit <- function(train, outcome, predictors, method, task) {
  form <- as.formula(paste(outcome,"~",paste(predictors,collapse="+")))
  if (task=="classification") train[[outcome]] <- factor(train[[outcome]])
  tryCatch(switch(method,
    "Decision Tree"={stopifnot(has_pkg("rpart")); rpart::rpart(form,data=train,method=ifelse(task=="classification","class","anova"))},
    "Random Forest"={stopifnot(has_pkg("ranger")); ranger::ranger(form,data=train,num.trees=500,importance="impurity",probability=(task=="classification"),seed=42)},
    "XGBoost"={stopifnot(has_pkg("xgboost")); xm<-model.matrix(form,data=train)[,-1,drop=FALSE]; yt<-if(task=="classification") as.numeric(factor(train[[outcome]]))-1 else train[[outcome]]
      obj<-if(task=="classification") "binary:logistic" else "reg:squarederror"; dt<-xgboost::xgb.DMatrix(data=xm,label=yt); xgboost::xgb.train(params=list(objective=obj,max_depth=6,eta=0.1),data=dt,nrounds=100,verbose=0)},
    "SVM"={stopifnot(has_pkg("e1071")); e1071::svm(form,data=train,probability=TRUE)},
    "KNN"=list(train=train,outcome=outcome,predictors=predictors,k=min(5,nrow(train)-1)),
    "Naive Bayes"={stopifnot(has_pkg("e1071"),task=="classification"); e1071::naiveBayes(form,data=train)},
    "Logistic Reg."={stopifnot(task=="classification"); glm(form,data=train,family=binomial)},
    stop("Unknown method")
  ), error=function(e) list(error=TRUE,message=e$message))
}

ml_predict <- function(fit, test, outcome, predictors, method, task, train) {
  if (task=="classification") test[[outcome]] <- factor(test[[outcome]], levels=levels(factor(train[[outcome]])))
  tryCatch(switch(method,
    "Decision Tree"=if(task=="classification") list(class=as.character(predict(fit,test,type="class")),prob=predict(fit,test,type="prob")) else list(value=predict(fit,test)),
    "Random Forest"={p<-predict(fit,data=test); if(task=="classification") list(class=levels(factor(train[[outcome]]))[apply(p$predictions,1,which.max)],prob=p$predictions) else list(value=p$predictions)},
    "XGBoost"={form<-as.formula(paste(outcome,"~",paste(predictors,collapse="+")));xm<-model.matrix(form,data=test)[,-1,drop=FALSE]; raw<-predict(fit,xgboost::xgb.DMatrix(data=xm)); levs<-levels(factor(train[[outcome]])); if(task=="classification") list(class=levs[(raw>0.5)+1],prob=cbind(1-raw,raw)) else list(value=raw)},
    "SVM"={p<-predict(fit,test,probability=TRUE); if(task=="classification") list(class=as.character(p),prob=attr(p,"probabilities")) else list(value=as.numeric(p))},
    "KNN"={stopifnot(has_pkg("class")); trx<-model.matrix(~.-1,data=fit$train[,fit$predictors,drop=FALSE]); tex<-model.matrix(~.-1,data=test[,fit$predictors,drop=FALSE]); p<-class::knn(trx,tex,fit$train[[fit$outcome]],k=fit$k); list(class=as.character(p))},
    "Naive Bayes"=list(class=as.character(predict(fit,test,type="class")),prob=predict(fit,test,type="raw")),
    "Logistic Reg."={pr<-predict(fit,test,type="response");levs<-levels(factor(train[[outcome]])); list(class=levs[(pr>0.5)+1],prob=cbind(1-pr,pr))},
    list(error=TRUE)
  ), error=function(e) list(error=TRUE,message=e$message))
}

ml_metrics_class <- function(actual, predicted, prob=NULL) {
  actual<-factor(actual); predicted<-factor(predicted,levels=levels(actual)); cm<-table(Predicted=predicted,Actual=actual)
  acc <- sum(diag(cm))/sum(cm); m <- data.frame(metric="Accuracy",value=round(acc,4),stringsAsFactors=FALSE)
  if (nlevels(actual)==2) {
    tp<-cm[2,2];fp<-cm[2,1];fn<-cm[1,2];tn<-cm[1,1]; pr<-ifelse(tp+fp>0,tp/(tp+fp),0); rc<-ifelse(tp+fn>0,tp/(tp+fn),0); f1<-ifelse(pr+rc>0,2*pr*rc/(pr+rc),0)
    m<-rbind(m,data.frame(metric=c("Precision","Recall","F1"),value=round(c(pr,rc,f1),4),stringsAsFactors=FALSE))
    if (!is.null(prob)&&has_pkg("pROC")) { pv<-if(is.matrix(prob)) prob[,ncol(prob)] else prob; auc<-tryCatch(as.numeric(pROC::auc(pROC::roc(actual,pv,quiet=TRUE))),error=function(e)NA); if(!is.na(auc)) m<-rbind(m,data.frame(metric="AUC",value=round(auc,4),stringsAsFactors=FALSE)) }
  }
  list(metrics=m, confusion=cm)
}

ml_metrics_reg <- function(actual, predicted) {
  r<-actual-predicted; data.frame(metric=c("MAE","RMSE","R-squared"),value=round(c(mean(abs(r),na.rm=T),sqrt(mean(r^2,na.rm=T)),1-sum(r^2,na.rm=T)/sum((actual-mean(actual,na.rm=T))^2,na.rm=T)),4),stringsAsFactors=FALSE)
}

ml_importance <- function(fit, method) {
  tryCatch(switch(method,
    "Decision Tree"={imp<-fit$variable.importance; if(is.null(imp)) NULL else data.frame(variable=names(imp),importance=as.numeric(imp),stringsAsFactors=FALSE)},
    "Random Forest"={imp<-ranger::importance(fit); data.frame(variable=names(imp),importance=as.numeric(imp),stringsAsFactors=FALSE)},
    "XGBoost"={imp<-xgboost::xgb.importance(model=fit); data.frame(variable=imp$Feature,importance=imp$Gain,stringsAsFactors=FALSE)},
    NULL
  ), error=function(e) NULL)
}

# ══════════════════════════════════════════════════════════════
# PHASE 10: INDUSTRY TEMPLATES
# ══════════════════════════════════════════════════════════════

control_chart_stats <- function(x, type="individuals", subgroup_size=5) {
  x <- x[!is.na(x)]
  if (type=="individuals") { cl<-mean(x); mr<-mean(abs(diff(x))); ucl<-cl+2.66*mr; lcl<-cl-2.66*mr; return(list(values=x,cl=cl,ucl=ucl,lcl=lcl,index=seq_along(x),type="Individuals (I-MR)",sigma=mr/1.128)) }
  if (type=="xbar") { ng<-floor(length(x)/subgroup_size); if(ng<2) return(NULL); grps<-split(x[1:(ng*subgroup_size)],rep(1:ng,each=subgroup_size)); means<-sapply(grps,mean); ranges<-sapply(grps,function(g)diff(range(g))); xbar<-mean(means); rbar<-mean(ranges)
    d2<-c(0,0,1.128,1.693,2.059,2.326)[min(subgroup_size,6)]; A2<-3/(d2*sqrt(subgroup_size)); return(list(values=means,cl=xbar,ucl=xbar+A2*rbar,lcl=xbar-A2*rbar,index=seq_along(means),type=paste0("X-bar (n=",subgroup_size,")"),sigma=rbar/d2)) }
  if (type=="p") { cl<-mean(x); se<-sqrt(cl*(1-cl)/subgroup_size); return(list(values=x,cl=cl,ucl=cl+3*se,lcl=max(0,cl-3*se),index=seq_along(x),type="P chart",sigma=se)) }
  if (type=="c") { cl<-mean(x); return(list(values=x,cl=cl,ucl=cl+3*sqrt(cl),lcl=max(0,cl-3*sqrt(cl)),index=seq_along(x),type="C chart",sigma=sqrt(cl))) }
  NULL
}

process_capability <- function(x, lsl, usl) {
  x<-x[!is.na(x)]; mu<-mean(x); sig<-sd(x)
  res <- data.frame(metric=c("Mean","Std Dev","n"),value=c(round(mu,4),round(sig,4),length(x)),stringsAsFactors=FALSE)
  if (!is.na(usl) && !is.na(lsl) && usl>lsl && sig>0) {
    cp<-(usl-lsl)/(6*sig); cpk<-min((usl-mu)/(3*sig),(mu-lsl)/(3*sig)); verdict<-ifelse(cpk>=1.33,"CAPABLE",ifelse(cpk>=1,"MARGINAL","NOT CAPABLE"))
    res<-rbind(res,data.frame(metric=c("Cp","Cpk","Verdict"),value=c(round(cp,4),round(cpk,4),verdict),stringsAsFactors=FALSE))
  }
  res
}

compute_rfm <- function(df, cust_col, date_col, amt_col) {
  dates<-df[[date_col]]; if(is.character(dates)) for(fmt in c("%Y-%m-%d","%m/%d/%Y","%d/%m/%Y")){att<-as.Date(dates,format=fmt);if(sum(!is.na(att))/sum(!is.na(dates))>0.7){dates<-att;break}}
  if(!inherits(dates,"Date")) dates<-tryCatch(as.Date(dates),error=function(e)NULL)
  if(is.null(dates)) stop("Cannot parse dates.")
  rfm <- df |> mutate(.dt=dates,.amt=as.numeric(df[[amt_col]])) |> group_by(across(all_of(cust_col))) |>
    summarise(recency=as.numeric(Sys.Date()-max(.dt,na.rm=T)),frequency=n(),monetary=sum(.amt,na.rm=T),.groups="drop")
  rfm$R_score<-as.integer(cut(-rfm$recency,breaks=5,labels=FALSE))
  rfm$F_score<-as.integer(cut(rfm$frequency,breaks=quantile(rfm$frequency,probs=seq(0,1,.2),na.rm=T),labels=FALSE,include.lowest=TRUE))
  rfm$M_score<-as.integer(cut(rfm$monetary,breaks=quantile(rfm$monetary,probs=seq(0,1,.2),na.rm=T),labels=FALSE,include.lowest=TRUE))
  rfm$segment<-ifelse(rfm$R_score>=4&rfm$F_score>=4,"Champions",ifelse(rfm$R_score>=4&rfm$F_score<=2,"New",ifelse(rfm$R_score<=2&rfm$F_score>=4,"At risk",ifelse(rfm$R_score<=2&rfm$F_score<=2,"Lost","Regular"))))
  rfm
}

# ══════════════════════════════════════════════════════════════
# UI DEFINITION
# ══════════════════════════════════════════════════════════════

ui <- fluidPage(
  theme = bs_theme(version = 5, bootswatch = "flatly"),
  tags$head(tags$style(HTML("
    .stat-card {background:#fff;border:1px solid #e9ecef;border-radius:14px;padding:16px 18px;margin-bottom:12px;box-shadow:0 4px 16px rgba(0,0,0,0.05);}
    .stat-card .label {font-size:0.82rem;text-transform:uppercase;color:#6c757d;letter-spacing:0.04em;}
    .stat-card .value {font-size:1.65rem;font-weight:700;color:#212529;}
    .tab-note {background:#f8f9fa;border-left:4px solid #0d6efd;padding:12px 14px;border-radius:6px;margin-bottom:12px;}
  "))),
  titlePanel("Kernel"),
  sidebarLayout(
    sidebarPanel(width = 3,
      h4("Import & Export"),
      fileInput("file", "Choose data file", accept = c(".csv",".tsv",".txt",".xlsx",".xls",".rds",".RData",".sav",".dta",".sas7bdat",".xpt",".feather",".parquet")),
      uiOutput("import_detail_ui"),
      checkboxInput("clean_names", "Clean column names", TRUE),
      checkboxInput("drop_empty", "Drop empty rows/columns", TRUE),
      tags$hr(),
      downloadButton("download_clean_data", "Clean data (.csv)", class = "btn-primary"),
      downloadButton("download_profile", "Column profile (.csv)"),
      downloadButton("download_methods", "Method catalog (.csv)"),
      downloadButton("download_design", "DOE design (.csv)")
    ),
    mainPanel(width = 9,
      tabsetPanel(id = "main_tabs",

        # ── OVERVIEW ──
        tabPanel("Overview", br(),
          fluidRow(column(3,uiOutput("card_rows")),column(3,uiOutput("card_cols")),column(3,uiOutput("card_numeric")),column(3,uiOutput("card_categorical"))),
          div(class="tab-note",uiOutput("insight_notes")),
          fluidRow(column(7,h4("Data preview"),DTOutput("data_preview")),column(5,h4("Package status"),DTOutput("pkg_tbl")))
        ),

        # ── DATA QUALITY ──
        tabPanel("Data Quality", br(),
          fluidRow(column(7,h4("Column profile"),DTOutput("col_prof_tbl")),column(5,h4("Missingness"),plotOutput("miss_plot",height=360))),
          fluidRow(column(12,plotOutput("type_plot",height=280)))
        ),

        # ── VARIABLE DOCTOR ──
        tabPanel("Variable Doctor", icon=icon("stethoscope"), br(),
          div(class="tab-note",style="border-left-color:#198754;",h4(style="margin-top:0;","Variable Doctor"),p("Intelligent diagnostics with one-click fixes.")),
          fluidRow(column(3,uiOutput("doc_card_crit")),column(3,uiOutput("doc_card_warn")),column(3,uiOutput("doc_card_info")),column(3,uiOutput("doc_card_healthy"))),
          fluidRow(column(12,
            actionButton("doc_fix_all","Fix all critical",class="btn-danger",icon=icon("wrench")),
            actionButton("doc_undo","Undo all",class="btn-outline-secondary",icon=icon("rotate-left")),
            downloadButton("doc_report","Report (.csv)",class="btn-outline-primary")
          )),br(),
          fluidRow(column(12,DTOutput("doc_table"))),br(),
          fluidRow(column(6,plotOutput("doc_health",height=300)),column(6,h4("Fix log"),uiOutput("doc_log")))
        ),

        # ── UNIVARIATE ──
        tabPanel("Univariate", br(),
          fluidRow(column(4,selectInput("uni_var","Variable",choices=NULL)),column(4,selectInput("uni_plot","Plot",choices=c("Auto","Histogram","Density","Boxplot","Bar chart"))),column(4,checkboxInput("uni_dropna","Drop NA",TRUE))),
          fluidRow(column(7,plotOutput("uni_plot_out",height=420)),column(5,h4("Summary"),DTOutput("uni_summary")))
        ),

        # ── BIVARIATE ──
        tabPanel("Bivariate", icon=icon("arrows-left-right"), br(),
          div(class="tab-note",style="border-left-color:#6f42c1;",p(strong("Bivariate analysis")," — auto-detects types and suggests tests.")),
          fluidRow(column(3,selectInput("bi_x","X variable",choices=NULL)),column(3,selectInput("bi_y","Y variable",choices=NULL)),
            column(3,selectInput("bi_plot_type","Plot",choices=c("Auto","Scatter","Scatter+Linear","Scatter+Loess","Boxplot","Violin","Grouped bar"))),
            column(3,selectInput("bi_test","Test",choices=c("Auto")))),
          fluidRow(column(7,plotOutput("bi_plot",height=440)),column(5,h4("Test results"),DTOutput("bi_test_tbl"))),
          tags$hr(),h4("Correlation matrix"),
          fluidRow(column(3,selectInput("cor_method","Method",choices=c("pearson","spearman","kendall"))),column(9)),
          fluidRow(column(7,plotOutput("cor_heatmap",height=500)),column(5,DTOutput("cor_table")))
        ),

        # ── CATEGORICAL ASSOCIATION ──
        tabPanel("Categorical", br(),
          fluidRow(column(4,selectInput("assoc_x","Row",choices=NULL)),column(4,selectInput("assoc_y","Column",choices=NULL)),column(4,selectInput("assoc_strata","Strata",choices=c("None"="")))),
          fluidRow(column(6,h4("Table"),DTOutput("assoc_tbl")),column(6,h4("Tests"),DTOutput("assoc_stats"))),
          fluidRow(column(12,plotOutput("assoc_plot",height=420)))
        ),

        # ── HYPOTHESIS TESTS ──
        tabPanel("Hypothesis Tests", icon=icon("flask-vial"), br(),
          div(class="tab-note",style="border-left-color:#d63384;",h4(style="margin-top:0;","Hypothesis Testing Suite")),
          tabsetPanel(id="hyp_sub",type="pills",
            tabPanel("Test Finder",br(),
              div(style="background:#f8f0fc;border:1px solid #d63384;border-radius:10px;padding:20px;max-width:600px;",
                h4(style="color:#d63384;margin-top:0;","Answer 3 questions"),
                fluidRow(column(4,selectInput("finder_g","Groups?",c("1"="1","2"="2","3+"="3"),"2")),column(4,selectInput("finder_p","Paired?",c("Independent"="no","Paired"="yes"),"no")),column(4,selectInput("finder_t","Data?",c("Continuous"="continuous","Categorical"="categorical"),"continuous"))),
                DTOutput("finder_tbl"))),
            tabPanel("Run Test",br(),
              fluidRow(column(3,selectInput("hyp_test","Test",c("One-sample t-test","Wilcoxon signed-rank","Sign test","Binomial test","Chi-square GOF","Welch t-test","Paired t-test","Mann-Whitney U","Wilcoxon paired","One-way ANOVA","Welch ANOVA","Kruskal-Wallis","Tukey HSD"),"Welch t-test")),
                column(3,selectInput("hyp_v1","Variable 1",choices=NULL)),column(3,selectInput("hyp_v2","Variable 2",choices=c("None"=""))),column(3,numericInput("hyp_mu","H0 value",0))),
              fluidRow(column(3,numericInput("hyp_p0","H0 proportion",0.5,0,1,0.05)),column(3,actionButton("run_hyp","Run test",class="btn-primary",icon=icon("play"))),column(6)),
              fluidRow(column(7,DTOutput("hyp_result")),column(5,plotOutput("hyp_plot",height=340)))),
            tabPanel("Normality",br(),
              fluidRow(column(4,selectInput("norm_var","Variable",choices=NULL)),column(4,actionButton("run_norm","Run tests",class="btn-primary")),column(4)),
              fluidRow(column(6,plotOutput("norm_qq",height=380)),column(6,plotOutput("norm_hist",height=380))),
              DTOutput("norm_tbl")),
            tabPanel("Power",br(),
              fluidRow(column(4,selectInput("pwr_test","Test",c("Power: t-test"))),column(4,numericInput("pwr_alpha","Alpha",0.05,0.001,0.1,0.01)),column(4,actionButton("run_pwr","Calculate",class="btn-primary"))),
              fluidRow(column(6,DTOutput("pwr_tbl")),column(6,plotOutput("pwr_curve",height=360))))
          )
        ),

        # ── REGRESSION ──
        tabPanel("Regression", br(),
          div(class="tab-note",textOutput("model_guidance")),
          fluidRow(column(3,selectInput("mod_family","Family",c("Linear (Gaussian)","Binary logistic","Binary probit","Multinomial logistic","Ordinal logistic","Poisson","Negative binomial","Zero-inflated Poisson","Hurdle Poisson","Bias-reduced logistic"))),
            column(3,selectInput("mod_outcome","Outcome",choices=NULL)),column(4,selectizeInput("mod_preds","Predictors",choices=NULL,multiple=TRUE)),column(2,numericInput("mod_poly","Poly degree",1,1,5))),
          fluidRow(column(3,checkboxInput("mod_inter","Interactions",FALSE)),column(3,checkboxInput("mod_ordered","Ordered outcome",FALSE)),column(3,actionButton("fit_mod","Fit model",class="btn-primary")),column(3,textOutput("mod_formula"))),
          fluidRow(column(6,h4("Coefficients"),DTOutput("mod_coef")),column(6,h4("Metrics"),DTOutput("mod_metrics"))),
          fluidRow(column(12,plotOutput("mod_coef_plot",height=420))),
          tags$hr(),h4("Model diagnostics"),
          fluidRow(column(6,plotOutput("diag_resid",height=300)),column(6,plotOutput("diag_qq",height=300))),
          fluidRow(column(6,plotOutput("diag_scale",height=300)),column(6,DTOutput("diag_checks")))
        ),

        # ── MULTIVARIATE ──
        tabPanel("Multivariate", br(),
          fluidRow(column(4,selectInput("multi_method","Method",c("PCA","MCA","FAMD","K-means","PAM (Gower)"))),column(2,numericInput("multi_k","Clusters",3,2,20)),column(2,actionButton("run_multi","Run",class="btn-primary"))),
          fluidRow(column(8,plotOutput("multi_plot",height=440)),column(4,DTOutput("multi_summary"))),
          fluidRow(column(6,plotOutput("multi_scree",height=300)),column(6,plotOutput("multi_dendro",height=300)))
        ),

        # ── TIME SERIES ──
        tabPanel("Time Series", icon=icon("chart-line"), br(),
          div(class="tab-note",style="border-left-color:#0d6efd;",h4(style="margin-top:0;","Time Series Analysis")),
          fluidRow(column(3,selectInput("ts_date","Date column",choices=NULL)),column(3,selectInput("ts_val","Value column",choices=NULL)),
            column(2,selectInput("ts_freq","Frequency",c("Auto"="auto","Daily"="365","Weekly"="52","Monthly"="12","Quarterly"="4","Annual"="1"))),
            column(2,checkboxInput("ts_returns","Log-returns",FALSE)),column(2,checkboxInput("ts_diff","Difference",FALSE))),
          tabsetPanel(id="ts_sub",type="pills",
            tabPanel("Visualization",br(),plotOutput("ts_line",height=360),fluidRow(column(6,plotOutput("ts_acf",height=300)),column(6,plotOutput("ts_pacf",height=300))),plotOutput("ts_decomp",height=400)),
            tabPanel("Stationarity",br(),fluidRow(column(3,actionButton("run_station","Run tests",class="btn-primary")),column(9,div(class="tab-note",p("ADF/PP: H0=unit root. KPSS: H0=stationary.")))),DTOutput("station_tbl"),uiOutput("station_interp")),
            tabPanel("ARIMA",br(),fluidRow(column(3,actionButton("fit_arima","Fit auto.arima",class="btn-primary")),column(3,numericInput("arima_h","Horizon",12,1,365)),column(6)),
              fluidRow(column(6,verbatimTextOutput("arima_summary")),column(6,DTOutput("arima_metrics"))),plotOutput("arima_fc",height=400),
              fluidRow(column(6,plotOutput("arima_resid",height=300)),column(6,DTOutput("arima_ljung")))),
            tabPanel("GARCH",br(),
              div(class="tab-note",style="border-left-color:#dc3545;",p(strong("Volatility modeling.")," GARCH captures volatility clustering. Use log-returns.")),
              fluidRow(column(3,selectInput("garch_type","Model",c("GARCH(1,1)"="sGARCH","GJR-GARCH"="gjrGARCH","EGARCH"="eGARCH"))),
                column(2,numericInput("garch_p","p",1,1,3)),column(2,numericInput("garch_q","q",1,1,3)),column(3,actionButton("fit_garch","Fit GARCH",class="btn-danger",icon=icon("fire"))),column(2,numericInput("garch_h","Forecast h",20,1,100))),
              verbatimTextOutput("garch_summary"),
              fluidRow(column(6,plotOutput("garch_vol",height=350)),column(6,plotOutput("garch_resid",height=350))),
              fluidRow(column(6,plotOutput("garch_nic",height=300)),column(6,plotOutput("garch_fc",height=300))),DTOutput("garch_coef"))
          )
        ),

        # ── SURVIVAL ──
        tabPanel("Survival", icon=icon("heart-pulse"), br(),
          div(class="tab-note",style="border-left-color:#dc3545;",h4(style="margin-top:0;","Survival Analysis")),
          fluidRow(column(3,selectInput("surv_time","Time",choices=NULL)),column(3,selectInput("surv_event","Event (0/1)",choices=NULL)),column(3,selectInput("surv_group","Group",choices=c("None"=""))),column(3,actionButton("run_km","Fit KM",class="btn-danger"))),
          fluidRow(column(8,plotOutput("km_plot",height=440)),column(4,h4("Summary"),DTOutput("km_summary"),br(),DTOutput("km_logrank"))),
          tags$hr(),h4("Cox PH Regression"),
          fluidRow(column(5,selectizeInput("cox_preds","Predictors",choices=NULL,multiple=TRUE)),column(3,actionButton("run_cox","Fit Cox",class="btn-danger")),column(4)),
          fluidRow(column(6,DTOutput("cox_coef")),column(6,plotOutput("cox_forest",height=350))),
          fluidRow(column(6,DTOutput("cox_metrics")),column(6,DTOutput("cox_ph")))
        ),

        # ── ML PIPELINE ──
        tabPanel("Machine Learning", icon=icon("robot"), br(),
          div(class="tab-note",style="border-left-color:#6f42c1;",h4(style="margin-top:0;","ML Pipeline")),
          fluidRow(column(3,selectInput("ml_y","Outcome",choices=NULL)),column(4,selectizeInput("ml_x","Predictors",choices=NULL,multiple=TRUE)),column(2,sliderInput("ml_split","Train%",50,90,80,5)),column(3,uiOutput("ml_task_card"))),
          fluidRow(column(9,checkboxGroupInput("ml_methods","Models:",inline=TRUE,choices=c("Decision Tree","Random Forest","XGBoost","SVM","KNN","Naive Bayes","Logistic Reg."),selected=c("Decision Tree","Random Forest","XGBoost"))),
            column(3,actionButton("ml_train","Train all",class="btn-primary btn-lg",icon=icon("play")))),
          tabsetPanel(id="ml_sub",type="pills",
            tabPanel("Compare",br(),DTOutput("ml_compare"),fluidRow(column(6,plotOutput("ml_bar",height=360)),column(6,plotOutput("ml_roc",height=360)))),
            tabPanel("Importance",br(),fluidRow(column(4,selectInput("ml_imp_sel","Model",choices=NULL)),column(8,plotOutput("ml_imp",height=400)))),
            tabPanel("Predictions",br(),fluidRow(column(4,selectInput("ml_pred_sel","Model",choices=NULL)),column(8,plotOutput("ml_pred_plot",height=400)))),
            tabPanel("Confusion Matrix",br(),fluidRow(column(4,selectInput("ml_cm_sel","Model",choices=NULL)),column(4,plotOutput("ml_cm",height=350)),column(4,DTOutput("ml_cm_tbl")))),
            tabPanel("Tree",br(),plotOutput("ml_tree",height=500))
          )
        ),

        # ── DOE ──
        tabPanel("DOE", br(),
          h4("Generate a design"),
          fluidRow(column(3,selectInput("doe_type","Type",c("Full factorial","Fractional factorial (2-level)","Central composite","Box-Behnken","Latin hypercube","D-optimal"))),
            column(2,numericInput("doe_k","Factors",3,2,12)),column(3,textInput("doe_names","Names","A, B, C")),column(2,numericInput("doe_runs","Runs",12,4,200)),column(2,numericInput("doe_levels","Levels",2,2,5))),
          fluidRow(column(2,numericInput("doe_cp","Center pts",4,0,20)),column(2,checkboxInput("doe_rand","Randomize",TRUE)),column(2,actionButton("gen_doe","Generate",class="btn-primary"))),
          fluidRow(column(7,DTOutput("doe_tbl")),column(5,plotOutput("doe_plot",height=320))),
          tags$hr(),h4("Analyze existing DOE"),
          fluidRow(column(3,selectInput("doe_resp","Response",choices=NULL)),column(5,selectizeInput("doe_factors","Factors",choices=NULL,multiple=TRUE)),column(2,checkboxInput("doe_ia","Interactions",TRUE)),column(2,actionButton("run_doe","Analyze",class="btn-primary"))),
          fluidRow(column(6,DTOutput("doe_anova")),column(6,plotOutput("doe_effects",height=340)))
        ),

        # ── REPORT ──
        tabPanel("Report", icon=icon("file-lines"), br(),
          div(class="tab-note",style="border-left-color:#198754;",h4(style="margin-top:0;","Report Generator")),
          fluidRow(column(3,selectInput("rpt_fmt","Format",c("HTML"="html","Markdown"="md"))),column(3,textInput("rpt_title","Title","Kernel Report")),column(3,textInput("rpt_author","Author","")),column(3,downloadButton("gen_report","Download Report",class="btn-success"))),
          tags$hr(),h4("Preview"),uiOutput("rpt_preview"),
          tags$hr(),h4("R Code Export"),verbatimTextOutput("code_export"),downloadButton("dl_code","Download .R",class="btn-outline-primary")
        ),

        # ── TEMPLATES ──
        tabPanel("Templates", icon=icon("swatchbook"), br(),
          div(class="tab-note",style="border-left-color:#20c997;",h4(style="margin-top:0;","Industry Templates")),
          tabsetPanel(id="tmpl_sub",type="pills",
            tabPanel("Quality/SPC",br(),h4("Control Charts"),
              fluidRow(column(3,selectInput("spc_var","Variable",choices=NULL)),column(2,selectInput("spc_type","Chart",c("Individuals"="individuals","X-bar"="xbar","P chart"="p","C chart"="c"))),column(2,numericInput("spc_n","Subgroup",5,2,25)),column(3,actionButton("run_spc","Generate",class="btn-primary")),column(2,uiOutput("spc_ooc"))),
              plotOutput("spc_chart",height=400),DTOutput("spc_stats"),
              tags$hr(),h4("Process Capability"),
              fluidRow(column(3,selectInput("cap_var","Variable",choices=NULL)),column(2,numericInput("cap_lsl","LSL",NA)),column(2,numericInput("cap_usl","USL",NA)),column(3,actionButton("run_cap","Calculate",class="btn-primary"))),
              fluidRow(column(6,plotOutput("cap_plot",height=350)),column(6,DTOutput("cap_tbl")))),
            tabPanel("Business/RFM",br(),h4("RFM Segmentation"),
              fluidRow(column(3,selectInput("rfm_cust","Customer ID",choices=NULL)),column(3,selectInput("rfm_date","Date",choices=NULL)),column(3,selectInput("rfm_amt","Amount",choices=NULL)),column(3,actionButton("run_rfm","Run RFM",class="btn-primary"))),
              fluidRow(column(7,DTOutput("rfm_tbl")),column(5,plotOutput("rfm_seg",height=350))),
              fluidRow(column(6,plotOutput("rfm_scatter",height=350)),column(6,plotOutput("rfm_heat",height=350))),
              tags$hr(),h4("Pareto Chart"),
              fluidRow(column(4,selectInput("pareto_var","Category",choices=NULL)),column(4,actionButton("run_pareto","Generate",class="btn-primary"))),
              plotOutput("pareto_plot",height=400)),
            tabPanel("Healthcare",br(),h4("Heatmap"),
              fluidRow(column(4,selectizeInput("heat_vars","Variables",choices=NULL,multiple=TRUE)),column(2,selectInput("heat_scale","Scale",c("None"="none","Row"="row","Column"="column"))),column(3,actionButton("run_heat","Generate",class="btn-primary"))),
              plotOutput("tmpl_heatmap",height=550),
              tags$hr(),h4("Volcano Plot"),
              fluidRow(column(3,selectInput("volc_fc","Fold-change",choices=NULL)),column(3,selectInput("volc_p","P-value",choices=NULL)),column(2,numericInput("volc_fc_t","FC thresh",1,0,5,0.5)),column(2,numericInput("volc_p_t","P thresh",0.05,0.001,0.1,0.01)),column(2,actionButton("run_volc","Plot",class="btn-primary"))),
              plotOutput("volc_plot",height=450)),
            tabPanel("Sample Data",br(),
              fluidRow(column(4,selectInput("sample_ds","Dataset",c("mtcars","iris","airquality","ToothGrowth","PlantGrowth","ChickWeight","USArrests","faithful","swiss","sleep","chickwts","InsectSprays","warpbreaks"))),column(3,actionButton("load_sample","Load",class="btn-success",icon=icon("download"))),column(5,uiOutput("sample_info"))))
          )
        ),

        # ── METHODS CATALOG ──
        tabPanel("Methods Catalog", br(),
          div(class="tab-note","Registry of 132 methods mapped to R packages."),DTOutput("methods_tbl"))

      ) # end tabsetPanel
    ) # end mainPanel
  ) # end sidebarLayout
) # end fluidPage

# ══════════════════════════════════════════════════════════════
# SERVER
# ══════════════════════════════════════════════════════════════

server <- function(input, output, session) {

  # ── Data import ──
  raw_data <- reactive({
    req(input$file)
    ext <- tolower(tools::file_ext(input$file$name))
    sheet <- if (ext %in% c("xlsx","xls")) input$excel_sheet %||% NULL else NULL
    obj_nm <- if (ext == "rdata") input$rdata_object %||% NULL else NULL
    tryCatch(import_data(input$file$datapath, input$file$name, sheet=sheet, object_name=obj_nm), error=function(e){showNotification(paste("Import error:",e$message),type="error");NULL})
  })

  analysis_data <- reactive({
    df <- raw_data(); req(df)
    tryCatch(prep_data(df, clean_names=isTRUE(input$clean_names), drop_empty=isTRUE(input$drop_empty)), error=function(e){showNotification(e$message,type="error");NULL})
  })

  output$import_detail_ui <- renderUI({
    req(input$file); ext <- tolower(tools::file_ext(input$file$name))
    if (ext %in% c("xlsx","xls")) { sh <- tryCatch(readxl::excel_sheets(input$file$datapath),error=function(e)character()); if(length(sh)) return(selectInput("excel_sheet","Sheet",sh,sh[1])) }
    if (ext == "rdata") { objs <- tryCatch(list_rdata_frames(input$file$datapath),error=function(e)character()); if(length(objs)) return(selectInput("rdata_object","Object",objs,objs[1])) }
    NULL
  })

  # ── Variable Doctor state ──
  doc <- reactiveValues(original=NULL, current=NULL, fix_log=character(), fix_count=0)
  observeEvent(analysis_data(), { df<-analysis_data(); if(!is.null(df)){doc$original<-df;doc$current<-df;doc$fix_log<-character();doc$fix_count<-0} })
  doctor_data <- reactive(if (!is.null(doc$current)) doc$current else analysis_data())
  doc_findings <- reactive({ df<-doctor_data(); req(df); diagnose_variables(df) })

  # ── Update all inputs when data changes ──
  observeEvent(doctor_data(), {
    df<-doctor_data(); req(df); vars<-names(df); nums<-numeric_vars_of(df); cats<-categorical_vars_of(df)
    updateSelectInput(session,"uni_var",choices=vars,selected=vars[1])
    updateSelectInput(session,"bi_x",choices=vars,selected=vars[1])
    updateSelectInput(session,"bi_y",choices=vars,selected=if(length(vars)>=2) vars[2] else vars[1])
    updateSelectInput(session,"assoc_x",choices=cats,selected=if(length(cats)) cats[1] else NULL)
    updateSelectInput(session,"assoc_y",choices=cats,selected=if(length(cats)>=2) cats[2] else if(length(cats)) cats[1] else NULL)
    updateSelectInput(session,"assoc_strata",choices=c("None"="",cats))
    updateSelectInput(session,"mod_outcome",choices=vars,selected=vars[1])
    updateSelectizeInput(session,"mod_preds",choices=vars,selected=head(vars[-1],2),server=TRUE)
    updateSelectInput(session,"hyp_v1",choices=vars,selected=vars[1])
    updateSelectInput(session,"hyp_v2",choices=c("None"="",vars))
    updateSelectInput(session,"norm_var",choices=nums,selected=if(length(nums)) nums[1] else NULL)
    updateSelectInput(session,"ts_date",choices=vars,selected=detect_date_column(df) %||% vars[1])
    updateSelectInput(session,"ts_val",choices=nums,selected=if(length(nums)) nums[1] else NULL)
    updateSelectInput(session,"doe_resp",choices=nums,selected=if(length(nums)) nums[1] else NULL)
    updateSelectizeInput(session,"doe_factors",choices=vars,selected=head(vars,3),server=TRUE)
    updateSelectInput(session,"surv_time",choices=nums,selected=detect_time_column(df))
    updateSelectInput(session,"surv_event",choices=vars,selected=detect_event_column(df))
    updateSelectInput(session,"surv_group",choices=c("None"="",cats,nums))
    updateSelectizeInput(session,"cox_preds",choices=vars,server=TRUE)
    updateSelectInput(session,"ml_y",choices=vars,selected=vars[1])
    updateSelectizeInput(session,"ml_x",choices=vars,selected=head(vars[-1],4),server=TRUE)
    updateSelectInput(session,"spc_var",choices=nums,selected=if(length(nums)) nums[1] else NULL)
    updateSelectInput(session,"cap_var",choices=nums,selected=if(length(nums)) nums[1] else NULL)
    updateSelectInput(session,"rfm_cust",choices=vars,selected=vars[1])
    updateSelectInput(session,"rfm_date",choices=vars,selected=if(length(vars)>=2) vars[2] else vars[1])
    updateSelectInput(session,"rfm_amt",choices=nums,selected=if(length(nums)) nums[1] else NULL)
    updateSelectInput(session,"pareto_var",choices=c(cats,vars),selected=if(length(cats)) cats[1] else vars[1])
    updateSelectizeInput(session,"heat_vars",choices=nums,selected=head(nums,min(20,length(nums))),server=TRUE)
    updateSelectInput(session,"volc_fc",choices=nums,selected=if(length(nums)) nums[1] else NULL)
    updateSelectInput(session,"volc_p",choices=nums,selected=if(length(nums)>=2) nums[2] else NULL)
  })

  # ── Overview cards ──
  output$card_rows <- renderUI({ df<-doctor_data(); v<-if(is.null(df))"-" else format(nrow(df),big.mark=","); div(class="stat-card",div(class="label","Rows"),div(class="value",v)) })
  output$card_cols <- renderUI({ df<-doctor_data(); v<-if(is.null(df))"-" else ncol(df); div(class="stat-card",div(class="label","Columns"),div(class="value",v)) })
  output$card_numeric <- renderUI({ df<-doctor_data(); v<-if(is.null(df))"-" else length(numeric_vars_of(df)); div(class="stat-card",div(class="label","Numeric"),div(class="value",v)) })
  output$card_categorical <- renderUI({ df<-doctor_data(); v<-if(is.null(df))"-" else length(categorical_vars_of(df)); div(class="stat-card",div(class="label","Categorical"),div(class="value",v)) })
  output$insight_notes <- renderUI({ df<-doctor_data(); if(is.null(df)) return(tags$p("Upload data.")); tags$p(paste0("Rows: ",nrow(df)," | Cols: ",ncol(df)," | Missing: ",sum(is.na(df)))) })
  output$data_preview <- renderDT({ df<-doctor_data();req(df);datatable(df,options=list(pageLength=10,scrollX=TRUE),filter="top") })
  output$pkg_tbl <- renderDT({ datatable(package_status(),options=list(dom="tip",pageLength=15,scrollX=TRUE),rownames=FALSE) })

  # ── Data Quality ──
  output$col_prof_tbl <- renderDT({ df<-doctor_data();req(df);datatable(column_profile(df),options=list(pageLength=10,scrollX=TRUE),rownames=FALSE) })
  output$miss_plot <- renderPlot({ df<-doctor_data();req(df);prof<-column_profile(df); ggplot(prof,aes(x=reorder(variable,missing_pct),y=missing_pct))+geom_col()+coord_flip()+labs(x=NULL,y="Missing %")+scale_y_continuous(labels=label_percent(scale=1))+theme_minimal(base_size=12) })
  output$type_plot <- renderPlot({ df<-doctor_data();req(df);prof<-column_profile(df);tc<-prof|>count(type,sort=TRUE); ggplot(tc,aes(x=reorder(type,n),y=n))+geom_col()+coord_flip()+labs(x=NULL,y="Count")+theme_minimal(base_size=12) })

  # ── Variable Doctor ──
  output$doc_card_crit <- renderUI({ f<-doc_findings();n<-sum(f$severity=="critical");cl<-if(n>0)"#dc3545" else "#198754"; div(class="stat-card",style=paste0("border-left:4px solid ",cl,";"),div(class="label","Critical"),div(class="value",style=paste0("color:",cl,";"),n)) })
  output$doc_card_warn <- renderUI({ f<-doc_findings();n<-sum(f$severity=="warning");cl<-if(n>0)"#fd7e14" else "#198754"; div(class="stat-card",style=paste0("border-left:4px solid ",cl,";"),div(class="label","Warnings"),div(class="value",style=paste0("color:",cl,";"),n)) })
  output$doc_card_info <- renderUI({ f<-doc_findings();n<-sum(f$severity=="info"); div(class="stat-card",style="border-left:4px solid #0d6efd;",div(class="label","Info"),div(class="value",style="color:#0d6efd;",n)) })
  output$doc_card_healthy <- renderUI({ df<-doctor_data();f<-doc_findings();req(df); div(class="stat-card",style="border-left:4px solid #198754;",div(class="label","Healthy"),div(class="value",style="color:#198754;",ncol(df)-length(unique(f$variable)))) })

  output$doc_table <- renderDT({
    f <- doc_findings()
    if (nrow(f)==0) return(datatable(data.frame(Message="All columns healthy!"),options=list(dom="t"),rownames=FALSE))
    dd <- f[,c("variable","severity","issue","detail"),drop=FALSE]
    dd$severity <- ifelse(f$severity=="critical",'<span style="color:#dc3545;font-weight:700;">CRITICAL</span>',ifelse(f$severity=="warning",'<span style="color:#fd7e14;font-weight:700;">WARNING</span>','<span style="color:#0d6efd;">INFO</span>'))
    dd$action <- ifelse(f$fix_action!="none",
      paste0('<button class="btn btn-sm btn-outline-primary" onclick="Shiny.setInputValue(&quot;doc_fix_one&quot;,&quot;',f$fix_id,'&quot;,{priority:&quot;event&quot;})" style="font-size:0.75rem;padding:2px 8px;">',sapply(f$fix_action,icon_for_fix),'</button>'),
      '<span style="color:#999;">Review</span>')
    datatable(dd,escape=FALSE,options=list(pageLength=20,scrollX=TRUE,dom="ftip"),rownames=FALSE,colnames=c("Variable","Severity","Issue","Details","Action"))
  })

  output$doc_health <- renderPlot({
    df<-doctor_data();f<-doc_findings();req(df);av<-data.frame(variable=names(df),stringsAsFactors=FALSE)
    if(nrow(f)>0){worst<-f|>mutate(sr=case_when(severity=="critical"~1,severity=="warning"~2,TRUE~3))|>group_by(variable)|>summarise(ws=severity[which.min(sr)],.groups="drop");av<-av|>left_join(worst,by="variable")|>mutate(status=ifelse(is.na(ws),"healthy",ws))} else av$status<-"healthy"
    av$status<-factor(av$status,levels=c("critical","warning","info","healthy"),labels=c("Critical","Warning","Info","Healthy"))
    ggplot(av,aes(x=reorder(variable,as.numeric(status)),fill=status))+geom_bar(width=0.7)+coord_flip()+scale_fill_manual(values=c("Critical"="#dc3545","Warning"="#fd7e14","Info"="#0d6efd","Healthy"="#198754"),drop=FALSE)+labs(x=NULL,y=NULL,fill="Status")+theme_minimal(base_size=12)+theme(legend.position="bottom",axis.text.x=element_blank(),panel.grid=element_blank())
  })
  output$doc_log <- renderUI({ lg<-doc$fix_log; if(!length(lg)) return(tags$p(class="text-muted","No fixes yet.")); tags$ul(style="font-size:0.85rem;",lapply(rev(lg),tags$li)) })

  observeEvent(input$doc_fix_one, { fid<-input$doc_fix_one;f<-doc_findings();row<-f[f$fix_id==fid,,drop=FALSE]; if(nrow(row)==0)return(); r<-apply_fix(doc$current,row$variable[1],row$fix_action[1]); if(!is.null(r$data)){doc$current<-r$data;doc$fix_count<-doc$fix_count+1;doc$fix_log<-c(doc$fix_log,r$message);showNotification(r$message,type="message",duration=3)} })
  observeEvent(input$doc_fix_all, { f<-doc_findings();cr<-f[f$severity=="critical"&f$fix_action!="none",,drop=FALSE]; if(nrow(cr)==0){showNotification("No critical fixes.",type="warning");return()}; df<-doc$current;fc<-0; for(i in seq_len(nrow(cr))){if(cr$variable[i]%in%names(df)){r<-apply_fix(df,cr$variable[i],cr$fix_action[i]);if(!is.null(r$data)){df<-r$data;doc$fix_log<-c(doc$fix_log,r$message);fc<-fc+1}}}; doc$current<-df;doc$fix_count<-doc$fix_count+fc;showNotification(paste0(fc," fixes applied."),type="message") })
  observeEvent(input$doc_undo, { if(!is.null(doc$original)){doc$current<-doc$original;doc$fix_log<-c(doc$fix_log,paste0("UNDO: Reverted ",doc$fix_count," fixes."));doc$fix_count<-0;showNotification("Reverted.",type="warning")} })
  output$doc_report <- downloadHandler(filename=function()paste0("doctor_",Sys.Date(),".csv"),content=function(file){f<-doc_findings();readr::write_csv(f[,c("variable","severity","issue","detail")],file)})

  # ── Univariate ──
  output$uni_plot_out <- renderPlot({
    df<-doctor_data();req(df,input$uni_var);var<-input$uni_var;x<-df[[var]]; if(isTRUE(input$uni_dropna)){df<-df[!is.na(df[[var]]),,drop=FALSE];x<-df[[var]]}; validate(need(length(x)>0,"No data."))
    chosen<-input$uni_plot; if(chosen=="Auto") chosen<-if(is.numeric(x))"Histogram" else "Bar chart"
    if(chosen=="Histogram"){validate(need(is.numeric(x),"Need numeric.")); ggplot(df,aes(x=.data[[var]]))+geom_histogram(bins=30,color="white")+labs(title=paste("Histogram:",var))+theme_minimal(base_size=12)}
    else if(chosen=="Density"){validate(need(is.numeric(x),"Need numeric.")); ggplot(df,aes(x=.data[[var]]))+geom_density(alpha=0.6)+labs(title=paste("Density:",var))+theme_minimal(base_size=12)}
    else if(chosen=="Boxplot"){validate(need(is.numeric(x),"Need numeric.")); ggplot(df,aes(y=.data[[var]],x=""))+geom_boxplot()+labs(title=paste("Boxplot:",var))+theme_minimal(base_size=12)}
    else { ggplot(data.frame(v=coerce_categorical(x)),aes(x=v))+geom_bar()+coord_flip()+labs(title=paste("Bar:",var))+theme_minimal(base_size=12)}
  })
  output$uni_summary <- renderDT({
    df<-doctor_data();req(df,input$uni_var);x<-df[[input$uni_var]]
    if(is.numeric(x)) out<-data.frame(stat=c("n","missing","mean","sd","min","median","max"),value=c(sum(!is.na(x)),sum(is.na(x)),mean(x,na.rm=T),sd(x,na.rm=T),min(x,na.rm=T),median(x,na.rm=T),max(x,na.rm=T)))
    else {tab<-sort(table(coerce_categorical(x),useNA="ifany"),decreasing=TRUE); out<-data.frame(level=names(tab),count=as.integer(tab),prop=round(as.numeric(tab)/sum(tab),3))}
    datatable(out,options=list(dom="tip",pageLength=10),rownames=FALSE)
  })

  # ── Bivariate ──
  output$bi_plot <- renderPlot({
    df<-doctor_data();req(df,input$bi_x,input$bi_y);xv<-input$bi_x;yv<-input$bi_y;pd<-df[complete.cases(df[[xv]],df[[yv]]),,drop=FALSE];validate(need(nrow(pd)>=3,"Not enough data."))
    xn<-is.numeric(pd[[xv]]);yn<-is.numeric(pd[[yv]]);pt<-input$bi_plot_type
    if(xn&&yn){p<-ggplot(pd,aes(x=.data[[xv]],y=.data[[yv]]))+geom_point(alpha=0.5,color="#6f42c1"); if(pt%in%c("Auto","Scatter+Linear"))p<-p+geom_smooth(method="lm",se=TRUE,color="#dc3545"); if(pt=="Scatter+Loess")p<-p+geom_smooth(method="loess",se=TRUE,color="#0d6efd"); p+labs(title=paste(yv,"vs.",xv))+theme_minimal(base_size=13)}
    else if((!xn&&yn)||(xn&&!yn)){cv<-if(!xn)xv else yv;nv<-if(xn)xv else yv;pd[[cv]]<-factor(pd[[cv]]); if(pt%in%c("Auto","Boxplot"))ggplot(pd,aes(x=.data[[cv]],y=.data[[nv]],fill=.data[[cv]]))+geom_boxplot(alpha=0.7,show.legend=FALSE)+stat_summary(fun=mean,geom="point",shape=18,size=3,color="#dc3545")+labs(title=paste(nv,"by",cv))+theme_minimal(base_size=13)+theme(axis.text.x=element_text(angle=45,hjust=1))
    else ggplot(pd,aes(x=.data[[cv]],y=.data[[nv]],fill=.data[[cv]]))+geom_violin(alpha=0.6,show.legend=FALSE)+geom_boxplot(width=0.15,fill="white")+labs(title=paste(nv,"by",cv))+theme_minimal(base_size=13)}
    else {pd[[xv]]<-factor(pd[[xv]]);pd[[yv]]<-factor(pd[[yv]]); ggplot(pd,aes(x=.data[[xv]],fill=.data[[yv]]))+geom_bar(position="dodge")+labs(title=paste(yv,"by",xv))+theme_minimal(base_size=13)}
  })
  output$bi_test_tbl <- renderDT({
    df<-doctor_data();req(df,input$bi_x,input$bi_y);xv<-input$bi_x;yv<-input$bi_y;xn<-is.numeric(df[[xv]]);yn<-is.numeric(df[[yv]])
    if(xn&&yn){ct<-tryCatch(cor.test(df[[xv]],df[[yv]]),error=function(e)NULL); if(!is.null(ct)) out<-data.frame(stat=c("r","p-value","method"),value=c(round(ct$estimate,4),signif(ct$p.value,4),ct$method),stringsAsFactors=FALSE) else out<-data.frame(stat="Error",value="Cannot compute")}
    else if((!xn&&yn)||(xn&&!yn)){cv<-if(!xn)xv else yv;nv<-if(xn)xv else yv;g<-factor(df[[cv]]);ng<-nlevels(g)
      if(ng==2){tt<-tryCatch(t.test(df[[nv]]~g),error=function(e)NULL); out<-if(!is.null(tt))data.frame(stat=c("t","p-value"),value=c(round(tt$statistic,4),signif(tt$p.value,4)),stringsAsFactors=FALSE) else data.frame(stat="Error",value="Test failed")}
      else{av<-tryCatch(summary(aov(df[[nv]]~g)),error=function(e)NULL); out<-if(!is.null(av))data.frame(stat=c("F","p-value"),value=c(round(av[[1]]$`F value`[1],4),signif(av[[1]]$`Pr(>F)`[1],4)),stringsAsFactors=FALSE) else data.frame(stat="Error",value="Test failed")}}
    else{tbl<-table(factor(df[[xv]]),factor(df[[yv]]));chi<-suppressWarnings(chisq.test(tbl)); out<-data.frame(stat=c("Chi-sq","p-value","Cramer's V"),value=c(round(chi$statistic,4),signif(chi$p.value,4),round(cramers_v_manual(tbl),4)),stringsAsFactors=FALSE)}
    datatable(out,options=list(dom="t"),rownames=FALSE)
  })
  output$cor_heatmap <- renderPlot({
    df<-doctor_data();req(df);cm<-compute_cor_matrix(df,input$cor_method%||%"pearson");validate(need(!is.null(cm),"Need 2+ numeric vars."))
    cl<-reshape_cor(cm$cor,cm$pval); ggplot(cl,aes(x=Var1,y=Var2,fill=value))+geom_tile(color="white",linewidth=0.5)+geom_text(aes(label=label),size=3.2)+scale_fill_gradient2(low="#2166AC",mid="white",high="#B2182B",midpoint=0,limits=c(-1,1))+labs(x=NULL,y=NULL)+theme_minimal(base_size=12)+theme(axis.text.x=element_text(angle=45,hjust=1),panel.grid=element_blank())
  })
  output$cor_table <- renderDT({ df<-doctor_data();req(df);cm<-compute_cor_matrix(df,input$cor_method%||%"pearson");validate(need(!is.null(cm),"Need 2+ numeric.")); out<-as.data.frame(round(cm$cor,3));out$var<-rownames(out);rownames(out)<-NULL;datatable(out,options=list(dom="tip",scrollX=TRUE),rownames=FALSE) })

  # ── Categorical Association ──
  assoc_tbl_r <- reactive({ df<-doctor_data();req(df,input$assoc_x,input$assoc_y); table(coerce_categorical(df[[input$assoc_x]]),coerce_categorical(df[[input$assoc_y]]),useNA="no") })
  output$assoc_tbl <- renderDT({ datatable(as.data.frame.matrix(assoc_tbl_r()),options=list(dom="tip",scrollX=TRUE)) })
  output$assoc_stats <- renderDT({
    df<-doctor_data();req(df,input$assoc_x,input$assoc_y);tbl<-assoc_tbl_r();validate(need(all(dim(tbl)>=2),"Need 2+ levels each."))
    chi<-suppressWarnings(chisq.test(tbl,correct=FALSE));fp<-tryCatch(fisher.test(tbl)$p.value,error=function(e)NA)
    v<-if(has_pkg("DescTools"))tryCatch(DescTools::CramerV(tbl),error=function(e)cramers_v_manual(tbl)) else cramers_v_manual(tbl)
    out<-data.frame(metric=c("Chi-sq","Chi-sq p","Fisher p","Cramer's V"),value=c(unname(chi$statistic),chi$p.value,fp,v),stringsAsFactors=FALSE)
    ef<-odds_and_risk(tbl); if(!is.null(ef)) out<-bind_rows(out,ef)
    datatable(out,options=list(dom="tip",scrollX=TRUE),rownames=FALSE)
  })
  output$assoc_plot <- renderPlot({ tbl<-assoc_tbl_r();validate(need(all(dim(tbl)>=2),"Need 2+ levels.")); if(has_pkg("vcd")) vcd::mosaic(tbl,shade=TRUE,legend=TRUE,main="Mosaic plot") else mosaicplot(tbl,color=TRUE,main="Mosaic plot") })

  # ── Hypothesis Tests ──
  output$finder_tbl <- renderDT({ datatable(find_test(as.integer(input$finder_g),input$finder_p=="yes",input$finder_t),options=list(dom="t",scrollX=TRUE),rownames=FALSE) })
  hyp_r <- eventReactive(input$run_hyp, { df<-doctor_data();req(df,input$hyp_test,input$hyp_v1); run_hypothesis_test(df,input$hyp_test,input$hyp_v1,if(input$hyp_v2!="")input$hyp_v2 else NULL,mu0=input$hyp_mu,p0=input$hyp_p0) })
  output$hyp_result <- renderDT({ datatable(hyp_r(),options=list(dom="t",scrollX=TRUE),rownames=FALSE) })
  output$hyp_plot <- renderPlot({
    df<-doctor_data();req(df,input$hyp_v1);x<-df[[input$hyp_v1]];x<-x[!is.na(x)]
    if(is.numeric(x)&&input$hyp_v2!=""&&input$hyp_v2%in%names(df)){g<-df[[input$hyp_v2]][!is.na(df[[input$hyp_v1]])]; ggplot(data.frame(v=x,g=factor(g)),aes(x=g,y=v,fill=g))+geom_boxplot(alpha=0.7,show.legend=FALSE)+theme_minimal(base_size=13)}
    else if(is.numeric(x)){ggplot(data.frame(v=x),aes(x=v))+geom_histogram(aes(y=after_stat(density)),bins=30,fill="#d63384",alpha=0.5,color="white")+geom_density(linewidth=1,color="#d63384")+geom_vline(xintercept=input$hyp_mu,linetype="dashed",color="#dc3545")+theme_minimal(base_size=13)}
    else{tab<-table(factor(x)); ggplot(data.frame(l=names(tab),c=as.integer(tab)),aes(x=reorder(l,-c),y=c))+geom_col(fill="#d63384",alpha=0.8)+labs(x=NULL,y="Count")+theme_minimal(base_size=13)}
  })
  norm_r <- eventReactive(input$run_norm, { df<-doctor_data();req(df,input$norm_var);x<-df[[input$norm_var]];x<-x[!is.na(x)];validate(need(length(x)>=8,"Need 8+ values.")); list(x=x,var=input$norm_var,tests=lapply(c("Shapiro-Wilk","Anderson-Darling","Kolmogorov-Smirnov"),function(tn)run_hypothesis_test(df,tn,input$norm_var))) })
  output$norm_qq <- renderPlot({ nr<-norm_r();x<-nr$x;n<-length(x); ggplot(data.frame(t=qnorm(ppoints(n)),s=sort(x)),aes(x=t,y=s))+geom_point(alpha=0.5,color="#d63384")+geom_abline(intercept=mean(x),slope=sd(x),linetype="dashed",color="#0d6efd")+labs(title="Q-Q plot",x="Theoretical",y="Sample")+theme_minimal(base_size=13) })
  output$norm_hist <- renderPlot({ nr<-norm_r();x<-nr$x; ggplot(data.frame(x=x),aes(x=x))+geom_histogram(aes(y=after_stat(density)),bins=30,fill="#d63384",alpha=0.4,color="white")+geom_density(linewidth=1.2,color="#d63384")+stat_function(fun=dnorm,args=list(mean=mean(x),sd=sd(x)),linetype="dashed",color="#0d6efd")+labs(title="Distribution",x=nr$var)+theme_minimal(base_size=13) })
  output$norm_tbl <- renderDT({ nr<-norm_r(); out<-do.call(rbind,lapply(seq_along(nr$tests),function(i){r<-nr$tests[[i]];r$test_name<-c("Shapiro-Wilk","Anderson-Darling","KS")[i];r})); datatable(out,options=list(dom="t"),rownames=FALSE) })
  pwr_r <- eventReactive(input$run_pwr, { run_hypothesis_test(doctor_data(),input$pwr_test,input$hyp_v1%||%names(doctor_data())[1],conf_level=1-(input$pwr_alpha%||%0.05)) })
  output$pwr_tbl <- renderDT({ datatable(pwr_r(),options=list(dom="t"),rownames=FALSE) })
  output$pwr_curve <- renderPlot({ alpha<-input$pwr_alpha%||%0.05; n_seq<-seq(5,200,5); pd<-do.call(rbind,lapply(c(0.2,0.5,0.8),function(d){data.frame(n=n_seq,power=sapply(n_seq,function(n)power.t.test(n=n,delta=d,sd=1,sig.level=alpha)$power),effect=paste0("d=",d))})); ggplot(pd,aes(x=n,y=power,color=effect))+geom_line(linewidth=1.2)+geom_hline(yintercept=0.8,linetype="dashed",color="#999")+scale_y_continuous(labels=percent,limits=c(0,1))+labs(title="Power curve",x="n per group",y="Power")+theme_minimal(base_size=13) })

  # ── Regression ──
  output$model_guidance <- renderText({ switch(input$mod_family,"Linear (Gaussian)"="Continuous outcome.","Binary logistic"="Two-level outcome. Log-odds scale.","Poisson"="Non-negative counts.","Negative binomial"="Overdispersed counts.","") })
  output$mod_formula <- renderText({ req(input$mod_outcome); build_formula_text(input$mod_outcome,input$mod_preds%||%character(0),input$mod_inter,input$mod_poly) })

  fitted_model <- eventReactive(input$fit_mod, {
    df<-doctor_data();req(df,input$mod_outcome);validate(need(length(input$mod_preds)>=1||input$mod_family=="Linear (Gaussian)","Select predictors."))
    mdf<-df[,unique(c(input$mod_outcome,input$mod_preds)),drop=FALSE];mdf<-safe_complete(mdf);validate(need(nrow(mdf)>=5,"Not enough rows."))
    ft<-build_formula_text(input$mod_outcome,input$mod_preds%||%character(0),input$mod_inter,input$mod_poly);form<-as.formula(ft);y<-mdf[[input$mod_outcome]]
    switch(input$mod_family,
      "Linear (Gaussian)"={validate(need(is.numeric(y),"Need numeric outcome."));lm(form,data=mdf)},
      "Binary logistic"=glm(form,data=mdf,family=binomial("logit")),
      "Binary probit"=glm(form,data=mdf,family=binomial("probit")),
      "Multinomial logistic"={stopifnot(has_pkg("nnet"));mdf[[input$mod_outcome]]<-factor(y);nnet::multinom(form,data=mdf,trace=FALSE)},
      "Ordinal logistic"={stopifnot(has_pkg("ordinal"));if(isTRUE(input$mod_ordered))mdf[[input$mod_outcome]]<-ordered(y);ordinal::clm(form,data=mdf)},
      "Poisson"=glm(form,data=mdf,family=poisson),
      "Negative binomial"={stopifnot(has_pkg("MASS"));MASS::glm.nb(form,data=mdf)},
      "Zero-inflated Poisson"={stopifnot(has_pkg("pscl"));rhs<-paste(input$mod_preds,collapse="+");pscl::zeroinfl(as.formula(paste(input$mod_outcome,"~",rhs,"|",rhs)),data=mdf)},
      "Hurdle Poisson"={stopifnot(has_pkg("pscl"));rhs<-paste(input$mod_preds,collapse="+");pscl::hurdle(as.formula(paste(input$mod_outcome,"~",rhs,"|",rhs)),data=mdf)},
      "Bias-reduced logistic"={stopifnot(has_pkg("brglm2"));glm(form,data=mdf,family=binomial,method=brglm2::brglmFit)},
      stop("Unknown family."))
  })

  output$mod_coef <- renderDT({ datatable(extract_coefficients(fitted_model()),options=list(pageLength=10,scrollX=TRUE),rownames=FALSE) })
  output$mod_metrics <- renderDT({ datatable(model_metrics(fitted_model()),options=list(dom="tip",scrollX=TRUE),rownames=FALSE) })
  output$mod_coef_plot <- renderPlot({ fit<-fitted_model();ct<-extract_coefficients(fit);ct<-ct[!grepl("Intercept|\\|",ct$term),];validate(need(nrow(ct)>0,"No coefficients.")); ggplot(ct,aes(x=reorder(term,estimate),y=estimate,ymin=conf_low,ymax=conf_high))+geom_pointrange()+coord_flip()+labs(x=NULL,y="Estimate")+theme_minimal(base_size=12) })

  # ── Model diagnostics ──
  output$diag_resid <- renderPlot({ fit<-fitted_model();req(inherits(fit,"lm")); ggplot(data.frame(f=fitted(fit),r=residuals(fit)),aes(x=f,y=r))+geom_point(alpha=0.5,color="#6f42c1")+geom_hline(yintercept=0,linetype="dashed",color="#dc3545")+geom_smooth(method="loess",se=FALSE,color="#0d6efd")+labs(title="Residuals vs Fitted")+theme_minimal(base_size=12) })
  output$diag_qq <- renderPlot({ fit<-fitted_model();req(inherits(fit,"lm"));r<-residuals(fit); ggplot(data.frame(t=qnorm(ppoints(length(r))),s=sort(r)),aes(x=t,y=s))+geom_point(alpha=0.5,color="#6f42c1")+geom_abline(slope=sd(r),intercept=mean(r),linetype="dashed",color="#dc3545")+labs(title="Q-Q plot")+theme_minimal(base_size=12) })
  output$diag_scale <- renderPlot({ fit<-fitted_model();req(inherits(fit,"lm")); ggplot(data.frame(f=fitted(fit),s=sqrt(abs(rstandard(fit)))),aes(x=f,y=s))+geom_point(alpha=0.5,color="#6f42c1")+geom_smooth(method="loess",se=FALSE,color="#0d6efd")+labs(title="Scale-Location")+theme_minimal(base_size=12) })
  output$diag_checks <- renderDT({ fit<-fitted_model();req(inherits(fit,"lm"));ch<-check_assumptions_lm(fit);rows<-list(); for(nm in c("normality","homoscedasticity","autocorrelation"))if(!is.null(ch[[nm]]))rows[[length(rows)+1]]<-ch[[nm]]; if(!length(rows)) return(datatable(data.frame(msg="N/A"),options=list(dom="t"),rownames=FALSE)); datatable(do.call(rbind,rows),options=list(dom="t",scrollX=TRUE),rownames=FALSE) })

  # ── Multivariate ──
  multi_result <- eventReactive(input$run_multi, {
    df<-doctor_data();req(df)
    if(input$multi_method=="PCA"){num<-df[,numeric_vars_of(df),drop=FALSE];num<-num[,vapply(num,function(x)sd(x,na.rm=T)>0,logical(1)),drop=FALSE];num<-safe_complete(num);validate(need(ncol(num)>=2&&nrow(num)>=3,"Need 2+ numeric vars, 3+ rows."));list(method="PCA",fit=prcomp(num,center=TRUE,scale.=TRUE),data=num)}
    else if(input$multi_method=="MCA"){validate(need(has_pkg("FactoMineR"),"Install FactoMineR."));cdf<-df[,categorical_vars_of(df),drop=FALSE]|>mutate(across(everything(),coerce_categorical))|>safe_complete();validate(need(ncol(cdf)>=2,"Need 2+ categorical."));list(method="MCA",fit=FactoMineR::MCA(cdf,graph=FALSE),data=cdf)}
    else if(input$multi_method=="FAMD"){validate(need(has_pkg("FactoMineR"),"Install FactoMineR."));keep<-df[,vapply(df,function(x)is.numeric(x)||is.factor(x)||is.character(x),logical(1)),drop=FALSE]|>mutate(across(where(function(x)is.character(x)||is.logical(x)),coerce_categorical))|>safe_complete();list(method="FAMD",fit=FactoMineR::FAMD(keep,graph=FALSE),data=keep)}
    else if(input$multi_method=="K-means"){num<-df[,numeric_vars_of(df),drop=FALSE];num<-num[,vapply(num,function(x)sd(x,na.rm=T)>0,logical(1)),drop=FALSE];num<-safe_complete(num);validate(need(ncol(num)>=2,"Need 2+ vars."));sc<-scale(num);list(method="K-means",fit=kmeans(sc,centers=input$multi_k,nstart=25),data=as.data.frame(sc))}
    else{validate(need(has_pkg("cluster"),"Install cluster."));keep<-df[,vapply(df,function(x)is.numeric(x)||is.factor(x)||is.character(x),logical(1)),drop=FALSE]|>mutate(across(where(function(x)is.character(x)||is.logical(x)),coerce_categorical))|>safe_complete();d<-cluster::daisy(keep,metric="gower");list(method="PAM",fit=cluster::pam(d,k=input$multi_k,diss=TRUE),data=keep,diss=d)}
  })

  output$multi_plot <- renderPlot({
    res<-multi_result()
    if(res$method=="PCA"){sc<-as.data.frame(res$fit$x[,1:2]);ggplot(sc,aes(PC1,PC2))+geom_point(alpha=0.7)+labs(title="PCA scores")+theme_minimal(base_size=12)}
    else if(res$method%in%c("MCA","FAMD")){co<-as.data.frame(res$fit$ind$coord[,1:2]);names(co)<-c("D1","D2");ggplot(co,aes(D1,D2))+geom_point(alpha=0.7)+labs(title=paste(res$method,"map"))+theme_minimal(base_size=12)}
    else if(res$method=="K-means"){pc<-prcomp(res$data,center=T,scale.=T);sc<-as.data.frame(pc$x[,1:2]);sc$cl<-factor(res$fit$cluster);ggplot(sc,aes(PC1,PC2,color=cl))+geom_point(alpha=0.8)+labs(title="K-means clusters")+theme_minimal(base_size=12)}
    else{co<-as.data.frame(cmdscale(res$diss,k=2));names(co)<-c("D1","D2");co$cl<-factor(res$fit$clustering);ggplot(co,aes(D1,D2,color=cl))+geom_point(alpha=0.8)+labs(title="PAM/Gower")+theme_minimal(base_size=12)}
  })
  output$multi_summary <- renderDT({
    res<-multi_result()
    if(res$method=="PCA"){imp<-summary(res$fit)$importance;out<-data.frame(comp=colnames(imp),var_pct=round(as.numeric(imp[2,])*100,1),cum=round(as.numeric(imp[3,])*100,1))}
    else if(res$method%in%c("MCA","FAMD")){eig<-as.data.frame(res$fit$eig);eig$dim<-paste0("Dim",seq_len(nrow(eig)));out<-eig}
    else{tab<-table(if(res$method=="K-means")res$fit$cluster else res$fit$clustering);out<-data.frame(cluster=names(tab),size=as.integer(tab))}
    datatable(out,options=list(dom="tip",pageLength=10),rownames=FALSE)
  })
  output$multi_scree <- renderPlot({
    res<-multi_result()
    if(res$method=="PCA"){ve<-summary(res$fit)$importance[2,]*100;pd<-data.frame(c=seq_along(ve),v=ve);ggplot(pd,aes(c,v))+geom_line(color="#6f42c1",linewidth=1)+geom_point(color="#6f42c1",size=3)+labs(title="Scree plot",x="Component",y="% Variance")+theme_minimal(base_size=12)}
    else if(res$method=="K-means"){mk<-min(10,nrow(res$data)-1);if(mk<2)return(NULL);wss<-sapply(2:mk,function(k)kmeans(res$data,k,nstart=10)$tot.withinss);ggplot(data.frame(k=2:mk,w=wss),aes(k,w))+geom_line(color="#6f42c1",linewidth=1)+geom_point(color="#6f42c1",size=3)+labs(title="Elbow plot",x="k",y="Within SS")+theme_minimal(base_size=12)}
    else{plot.new();text(0.5,0.5,"N/A for this method.",cex=1.2)}
  })
  output$multi_dendro <- renderPlot({
    df<-doctor_data();req(df);num<-df[,vapply(df,is.numeric,logical(1)),drop=FALSE];num<-num[,vapply(num,function(x)sd(x,na.rm=T)>0,logical(1)),drop=FALSE];num<-safe_complete(num);validate(need(ncol(num)>=2,"Need 2+ numeric."))
    nu<-min(nrow(num),200);if(nrow(num)>200)num<-num[sample(nrow(num),200),];hc<-hclust(dist(scale(num)),method="ward.D2");plot(hc,labels=FALSE,main="Dendrogram",sub="",hang=-1);if(!is.null(input$multi_k))rect.hclust(hc,k=input$multi_k,border=c("#dc3545","#0d6efd","#198754","#fd7e14","#6f42c1"))
  })

  # ── Time Series ──
  ts_data <- reactive({
    df<-doctor_data();req(df,input$ts_date,input$ts_val)
    dates<-df[[input$ts_date]]; if(is.character(dates))for(fmt in c("%Y-%m-%d","%m/%d/%Y","%d/%m/%Y","%Y/%m/%d","%m-%d-%Y")){att<-as.Date(dates,format=fmt);if(sum(!is.na(att))/sum(!is.na(dates))>0.7){dates<-att;break}}
    if(!inherits(dates,"Date"))dates<-tryCatch(as.Date(dates),error=function(e)NULL);validate(need(!is.null(dates),"Cannot parse dates."))
    vals<-as.numeric(df[[input$ts_val]]);cc<-!is.na(dates)&!is.na(vals);dates<-dates[cc];vals<-vals[cc];ord<-order(dates);dates<-dates[ord];vals<-vals[ord]
    freq<-if(input$ts_freq=="auto")detect_frequency(dates) else as.integer(input$ts_freq)
    if(isTRUE(input$ts_returns)&&length(vals)>1){vals<-c(0,diff(log(vals)))*100}
    if(isTRUE(input$ts_diff)&&length(vals)>1){vals<-c(0,diff(vals))}
    ts_obj<-tryCatch(make_ts_object(vals,dates,freq),error=function(e)ts(vals,frequency=freq))
    list(dates=dates,values=vals,ts=ts_obj,freq=freq,name=input$ts_val)
  })

  output$ts_line <- renderPlot({ td<-ts_data();ggplot(data.frame(d=td$dates,v=td$values),aes(d,v))+geom_line(color="#0d6efd",linewidth=0.6)+geom_smooth(method="loess",se=FALSE,color="#dc3545",linewidth=0.8,linetype="dashed")+labs(title=paste("Time series:",td$name),subtitle=paste0("Freq: ",freq_label(td$freq)," | n=",length(td$values)),x="Date",y=td$name)+theme_minimal(base_size=13) })
  output$ts_acf <- renderPlot({ td<-ts_data();a<-acf(td$values,lag.max=min(40,length(td$values)/3),plot=FALSE);ci<-1.96/sqrt(length(td$values));ad<-data.frame(lag=a$lag[-1],acf=a$acf[-1]); ggplot(ad,aes(lag,acf))+geom_hline(yintercept=0)+geom_hline(yintercept=c(-ci,ci),linetype="dashed",color="#dc3545")+geom_segment(aes(xend=lag,yend=0),color="#0d6efd",linewidth=0.8)+labs(title="ACF")+theme_minimal(base_size=12) })
  output$ts_pacf <- renderPlot({ td<-ts_data();p<-pacf(td$values,lag.max=min(40,length(td$values)/3),plot=FALSE);ci<-1.96/sqrt(length(td$values));pd<-data.frame(lag=p$lag,acf=p$acf); ggplot(pd,aes(lag,acf))+geom_hline(yintercept=0)+geom_hline(yintercept=c(-ci,ci),linetype="dashed",color="#dc3545")+geom_segment(aes(xend=lag,yend=0),color="#6f42c1",linewidth=0.8)+labs(title="PACF")+theme_minimal(base_size=12) })
  output$ts_decomp <- renderPlot({ td<-ts_data();validate(need(td$freq>1&&length(td$values)>=2*td$freq,"Need freq>1 and 2+ cycles.")); dc<-tryCatch(stl(td$ts,s.window="periodic"),error=function(e)tryCatch(decompose(td$ts),error=function(e2)NULL));validate(need(!is.null(dc),"Cannot decompose.")); if(inherits(dc,"stl"))plot(dc) else plot(dc) })

  station_r <- eventReactive(input$run_station, { td<-ts_data();validate(need(length(td$values)>=20,"Need 20+ obs."));run_stationarity_tests(td$values) })
  output$station_tbl <- renderDT({ datatable(station_r(),options=list(dom="t",scrollX=TRUE),rownames=FALSE) })
  output$station_interp <- renderUI({ res<-station_r();adf_p<-res$p_value[res$test=="ADF"];kpss_p<-res$p_value[res$test=="KPSS"]; msg<-if(length(adf_p)&&length(kpss_p)){if(adf_p<0.05&&kpss_p>=0.05)"Series appears STATIONARY." else if(adf_p>=0.05&&kpss_p<0.05)"Series appears NON-STATIONARY. Try differencing." else "Tests disagree."} else "Install tseries."; div(class="tab-note",style="border-left-color:#0d6efd;margin-top:12px;",p(strong("Interpretation: "),msg)) })

  arima_r <- eventReactive(input$fit_arima, { td<-ts_data();validate(need(has_pkg("forecast"),"Install forecast."),need(length(td$values)>=20,"Need 20+ obs.")); fit<-forecast::auto.arima(td$ts,stepwise=TRUE,approximation=TRUE);fc<-forecast::forecast(fit,h=input$arima_h%||%12);list(fit=fit,fc=fc,data=td) })
  output$arima_summary <- renderPrint({ summary(arima_r()$fit) })
  output$arima_metrics <- renderDT({ af<-arima_r();acc<-tryCatch(forecast::accuracy(af$fit),error=function(e)NULL); if(!is.null(acc))datatable(data.frame(metric=colnames(acc),value=round(as.numeric(acc[1,]),4)),options=list(dom="t"),rownames=FALSE) else datatable(data.frame(metric=c("AIC","BIC"),value=c(round(AIC(af$fit),2),round(BIC(af$fit),2))),options=list(dom="t"),rownames=FALSE) })
  output$arima_fc <- renderPlot({ af<-arima_r();plot(af$fc,main=paste("ARIMA Forecast:",af$data$name),xlab="Time",ylab=af$data$name,col="#0d6efd",fcol="#dc3545",lwd=2) })
  output$arima_resid <- renderPlot({ af<-arima_r();r<-residuals(af$fit); ggplot(data.frame(x=r),aes(x=x))+geom_histogram(aes(y=after_stat(density)),bins=30,fill="#0d6efd",alpha=0.5,color="white")+geom_density(linewidth=1,color="#0d6efd")+stat_function(fun=dnorm,args=list(mean=mean(r),sd=sd(r)),linetype="dashed",color="#dc3545")+labs(title="ARIMA residuals")+theme_minimal(base_size=12) })
  output$arima_ljung <- renderDT({ af<-arima_r();r<-residuals(af$fit); out<-do.call(rbind,lapply(c(10,15,20),function(lag){bt<-Box.test(r,lag=lag,type="Ljung-Box");data.frame(lag=lag,stat=round(bt$statistic,4),p=signif(bt$p.value,4),verdict=ifelse(bt$p.value>0.05,"PASS","FAIL"),stringsAsFactors=FALSE)})); datatable(out,options=list(dom="t"),rownames=FALSE) })

  # ── GARCH ──
  garch_r <- eventReactive(input$fit_garch, { td<-ts_data();validate(need(has_pkg("rugarch"),"Install rugarch."),need(length(td$values)>=50,"Need 50+ obs.")); vals<-td$values[!is.na(td$values)]; vm<-switch(input$garch_type,"sGARCH"="sGARCH","gjrGARCH"="gjrGARCH","eGARCH"="eGARCH","sGARCH")
    spec<-rugarch::ugarchspec(variance.model=list(model=vm,garchOrder=c(input$garch_p,input$garch_q)),mean.model=list(armaOrder=c(0,0),include.mean=TRUE),distribution.model="std"); fit<-rugarch::ugarchfit(spec,data=vals,solver="hybrid"); fc<-rugarch::ugarchforecast(fit,n.ahead=input$garch_h%||%20); list(fit=fit,fc=fc,data=td,vals=vals) })
  output$garch_summary <- renderPrint({ show(garch_r()$fit) })
  output$garch_coef <- renderDT({ gf<-garch_r();cf<-rugarch::coef(gf$fit);se<-sqrt(diag(rugarch::vcov(gf$fit))); datatable(data.frame(param=names(cf),est=round(cf,6),se=round(se,6),t=round(cf/se,4),p=signif(2*pnorm(-abs(cf/se)),4)),options=list(dom="t",scrollX=TRUE),rownames=FALSE) })
  output$garch_vol <- renderPlot({ gf<-garch_r();sig<-as.numeric(rugarch::sigma(gf$fit)); ggplot(data.frame(t=seq_along(sig),s=sig),aes(t,s))+geom_line(color="#dc3545",linewidth=0.7)+labs(title=paste(input$garch_type,"— Conditional volatility"),x="Time",y=expression(sigma[t]))+theme_minimal(base_size=13) })
  output$garch_resid <- renderPlot({ gf<-garch_r();sr<-as.numeric(rugarch::residuals(gf$fit,standardize=TRUE)); ggplot(data.frame(t=seq_along(sr),r=sr),aes(t,r))+geom_point(alpha=0.3,color="#6f42c1",size=1)+geom_hline(yintercept=c(-2,2),linetype="dashed",color="#dc3545")+labs(title="Std. residuals")+theme_minimal(base_size=12) })
  output$garch_nic <- renderPlot({ gf<-garch_r();ni<-tryCatch(rugarch::newsimpact(gf$fit),error=function(e)NULL); if(is.null(ni)){plot.new();text(0.5,0.5,"N/A",cex=1.2);return()}; ggplot(data.frame(z=ni$zx,h=ni$zy),aes(z,h))+geom_line(color="#dc3545",linewidth=1.2)+geom_vline(xintercept=0,linetype="dashed",color="#999")+labs(title="News impact curve")+theme_minimal(base_size=13) })
  output$garch_fc <- renderPlot({ gf<-garch_r();fs<-as.numeric(rugarch::sigma(gf$fc)); ggplot(data.frame(h=seq_along(fs),s=fs),aes(h,s))+geom_line(color="#dc3545",linewidth=1.2)+geom_point(color="#dc3545",size=2)+labs(title="Volatility forecast",x="Horizon",y=expression(sigma[t+h]))+theme_minimal(base_size=13) })

  # ── Survival ──
  km_r <- eventReactive(input$run_km, {
    df<-doctor_data();req(df,input$surv_time,input$surv_event);validate(need(has_pkg("survival"),"Install survival."))
    sdf<-df[,unique(c(input$surv_time,input$surv_event,if(input$surv_group!="")input$surv_group)),drop=FALSE];sdf<-safe_complete(sdf);validate(need(nrow(sdf)>=5,"Need 5+ rows."),need(all(sdf[[input$surv_event]]%in%c(0,1)),"Event must be 0/1."))
    so<-survival::Surv(sdf[[input$surv_time]],sdf[[input$surv_event]])
    if(input$surv_group!=""&&input$surv_group%in%names(sdf)){fit<-survival::survfit(so~factor(sdf[[input$surv_group]]));lr<-survival::survdiff(so~factor(sdf[[input$surv_group]]))} else{fit<-survival::survfit(so~1);lr<-NULL}
    list(fit=fit,lr=lr,grp=input$surv_group)
  })
  output$km_plot <- renderPlot({ km<-km_r(); if(has_pkg("survminer"))print(survminer::ggsurvplot(km$fit,pval=!is.null(km$lr),conf.int=TRUE,ggtheme=theme_minimal(base_size=13))) else{plot(km$fit,col=c("#0d6efd","#dc3545","#198754","#fd7e14"),lwd=2,main="KM curve")} })
  output$km_summary <- renderDT({ km<-km_r();sm<-summary(km$fit)$table; if(is.matrix(sm)){out<-as.data.frame(sm);out$grp<-rownames(out)} else out<-data.frame(metric=names(sm),value=round(as.numeric(sm),3)); datatable(out,options=list(dom="t",scrollX=TRUE),rownames=FALSE) })
  output$km_logrank <- renderDT({ km<-km_r(); if(is.null(km$lr))return(datatable(data.frame(msg="Add grouping variable."),options=list(dom="t"),rownames=FALSE)); pv<-1-pchisq(km$lr$chisq,length(km$lr$n)-1); datatable(data.frame(stat=c("Chi-sq","df","p-value"),value=c(round(km$lr$chisq,4),length(km$lr$n)-1,signif(pv,4))),options=list(dom="t"),rownames=FALSE) })

  cox_r <- eventReactive(input$run_cox, {
    df<-doctor_data();req(df,input$surv_time,input$surv_event,input$cox_preds);validate(need(has_pkg("survival"),"Install survival."),need(length(input$cox_preds)>=1,"Select predictors."))
    cdf<-df[,unique(c(input$surv_time,input$surv_event,input$cox_preds)),drop=FALSE];cdf<-safe_complete(cdf);validate(need(nrow(cdf)>=10,"Need 10+ rows."))
    so<-survival::Surv(cdf[[input$surv_time]],cdf[[input$surv_event]]);form<-as.formula(paste("so~",paste(input$cox_preds,collapse="+")));list(fit=survival::coxph(form,data=cdf),data=cdf)
  })
  output$cox_coef <- renderDT({ cx<-cox_r();sm<-summary(cx$fit);cf<-as.data.frame(sm$coefficients);cf$term<-rownames(cf);cf$HR<-round(exp(cf$coef),4);cf$HR_lo<-round(exp(cf$coef-1.96*cf$`se(coef)`),4);cf$HR_hi<-round(exp(cf$coef+1.96*cf$`se(coef)`),4); datatable(cf[,c("term","coef","se(coef)","HR","HR_lo","HR_hi","Pr(>|z|)")],options=list(dom="t",scrollX=TRUE),rownames=FALSE) })
  output$cox_forest <- renderPlot({ cx<-cox_r();sm<-summary(cx$fit);cf<-as.data.frame(sm$coefficients);cf$term<-rownames(cf);cf$HR<-exp(cf$coef);cf$lo<-exp(cf$coef-1.96*cf$`se(coef)`);cf$hi<-exp(cf$coef+1.96*cf$`se(coef)`); ggplot(cf,aes(x=HR,y=reorder(term,HR),xmin=lo,xmax=hi))+geom_pointrange(color="#dc3545",linewidth=0.8)+geom_vline(xintercept=1,linetype="dashed")+scale_x_log10()+labs(title="Hazard ratio forest plot",x="HR (log scale)",y=NULL)+theme_minimal(base_size=13) })
  output$cox_metrics <- renderDT({ cx<-cox_r();sm<-summary(cx$fit); datatable(data.frame(metric=c("n","Events","C-index","LR p","Wald p"),value=c(sm$n,sm$nevent,round(sm$concordance[1],4),signif(sm$logtest[3],4),signif(sm$waldtest[3],4))),options=list(dom="t"),rownames=FALSE) })
  output$cox_ph <- renderDT({ cx<-cox_r();ph<-tryCatch({zt<-survival::cox.zph(cx$fit);out<-as.data.frame(zt$table);out$term<-rownames(out);out$verdict<-ifelse(out[,3]<0.05,"VIOLATION","OK");out},error=function(e)data.frame(msg=e$message)); datatable(ph,options=list(dom="t",scrollX=TRUE),rownames=FALSE) })

  # ── ML Pipeline ──
  output$ml_task_card <- renderUI({ df<-doctor_data();req(df,input$ml_y);task<-ml_detect_task(df[[input$ml_y]]);cl<-if(task=="classification")"#6f42c1" else "#0d6efd"; div(class="stat-card",style=paste0("border-left:4px solid ",cl,";"),div(class="label","Task"),div(class="value",style=paste0("color:",cl,";font-size:1.2rem;"),toupper(task))) })

  ml_r <- eventReactive(input$ml_train, {
    df<-doctor_data();req(df,input$ml_y,input$ml_x,input$ml_methods);validate(need(length(input$ml_x)>=1,"Select predictors."))
    task<-ml_detect_task(df[[input$ml_y]]);sp<-ml_split_data(df,input$ml_y,input$ml_x,input$ml_split/100);train<-sp$train;test<-sp$test
    if(task=="classification"){train[[input$ml_y]]<-factor(train[[input$ml_y]]);test[[input$ml_y]]<-factor(test[[input$ml_y]],levels=levels(train[[input$ml_y]]))}
    results<-list()
    withProgress(message="Training...",value=0,{
      for(i in seq_along(input$ml_methods)){m<-input$ml_methods[i];incProgress(1/length(input$ml_methods),detail=m)
        if(m=="Logistic Reg."&&task=="regression")next; if(m%in%c("Naive Bayes","KNN")&&task=="regression")next
        fit<-ml_fit(train,input$ml_y,input$ml_x,m,task); if(is.list(fit)&&isTRUE(fit$error)){results[[m]]<-list(error=TRUE,message=fit$message);next}
        pred<-ml_predict(fit,test,input$ml_y,input$ml_x,m,task,train); if(is.list(pred)&&isTRUE(pred$error)){results[[m]]<-list(error=TRUE,message=pred$message%||%"Failed");next}
        if(task=="classification"){met<-ml_metrics_class(test[[input$ml_y]],pred$class,pred$prob);results[[m]]<-list(fit=fit,pred=pred,metrics=met$metrics,confusion=met$confusion,task=task,error=FALSE)}
        else{met<-ml_metrics_reg(test[[input$ml_y]],pred$value);results[[m]]<-list(fit=fit,pred=pred,metrics=met,task=task,error=FALSE)}
      }
    })
    list(results=results,task=task,train=train,test=test,outcome=input$ml_y,predictors=input$ml_x)
  })

  observe({ res<-tryCatch(ml_r(),error=function(e)NULL); if(!is.null(res)){v<-names(res$results)[!sapply(res$results,function(r)isTRUE(r$error))]; updateSelectInput(session,"ml_imp_sel",choices=v,selected=v[1]); updateSelectInput(session,"ml_pred_sel",choices=v,selected=v[1]); updateSelectInput(session,"ml_cm_sel",choices=v,selected=v[1])} })

  output$ml_compare <- renderDT({ res<-ml_r();v<-res$results[!sapply(res$results,function(r)isTRUE(r$error))]; if(!length(v))return(datatable(data.frame(msg="No models trained."),options=list(dom="t"),rownames=FALSE)); all_m<-do.call(rbind,lapply(names(v),function(m){d<-v[[m]]$metrics;d$model<-m;d})); datatable(pivot_wider(all_m,names_from=metric,values_from=value),options=list(dom="t",scrollX=TRUE),rownames=FALSE) })
  output$ml_bar <- renderPlot({ res<-ml_r();v<-res$results[!sapply(res$results,function(r)isTRUE(r$error))];pm<-if(res$task=="classification")"Accuracy" else "R-squared"; vals<-sapply(v,function(r){vv<-r$metrics$value[r$metrics$metric==pm];if(length(vv))as.numeric(vv) else NA});pd<-data.frame(m=names(vals),v=vals);pd<-pd[!is.na(pd$v),]; ggplot(pd,aes(reorder(m,v),v,fill=m))+geom_col(show.legend=FALSE,alpha=0.85,width=0.6)+geom_text(aes(label=round(v,3)),hjust=-0.1,size=4)+coord_flip()+labs(title=pm,x=NULL,y=pm)+theme_minimal(base_size=13) })
  output$ml_roc <- renderPlot({ res<-ml_r();req(res$task=="classification"); if(!has_pkg("pROC")){plot.new();text(0.5,0.5,"Install pROC");return()}; v<-res$results[!sapply(res$results,function(r)isTRUE(r$error))];actual<-res$test[[res$outcome]]; cols<-c("#6f42c1","#0d6efd","#dc3545","#198754","#fd7e14","#d63384","#20c997"); plot(NULL,xlim=c(1,0),ylim=c(0,1),xlab="Specificity",ylab="Sensitivity",main="ROC curves");abline(a=1,b=-1,lty=2,col="#999");ll<-c();lc<-c(); for(i in seq_along(v)){m<-names(v)[i];r<-v[[m]];pr<-r$pred$prob; if(is.null(pr))next;pv<-if(is.matrix(pr))pr[,ncol(pr)] else pr; ro<-tryCatch(pROC::roc(actual,pv,quiet=TRUE),error=function(e)NULL); if(!is.null(ro)){lines(ro,col=cols[i],lwd=2);ll<-c(ll,paste0(m," (AUC=",round(pROC::auc(ro),3),")"));lc<-c(lc,cols[i])}}; if(length(ll))legend("bottomright",legend=ll,col=lc,lwd=2,cex=0.8) })
  output$ml_imp <- renderPlot({ res<-ml_r();req(input$ml_imp_sel);r<-res$results[[input$ml_imp_sel]]; if(is.null(r)||isTRUE(r$error))return(NULL); imp<-ml_importance(r$fit,input$ml_imp_sel); if(is.null(imp)){plot.new();text(0.5,0.5,"N/A",cex=1.2);return()}; imp<-head(imp[order(imp$importance,decreasing=TRUE),],20); ggplot(imp,aes(reorder(variable,importance),importance))+geom_col(fill="#6f42c1",alpha=0.8)+coord_flip()+labs(title=paste("Importance:",input$ml_imp_sel),x=NULL)+theme_minimal(base_size=13) })
  output$ml_pred_plot <- renderPlot({ res<-ml_r();req(input$ml_pred_sel);r<-res$results[[input$ml_pred_sel]]; if(is.null(r)||isTRUE(r$error))return(NULL); if(res$task=="regression"){ggplot(data.frame(a=res$test[[res$outcome]],p=r$pred$value),aes(a,p))+geom_point(alpha=0.5,color="#6f42c1")+geom_abline(slope=1,intercept=0,linetype="dashed",color="#dc3545")+labs(title="Actual vs Predicted",x="Actual",y="Predicted")+theme_minimal(base_size=13)} else{actual<-factor(res$test[[res$outcome]]);predicted<-factor(r$pred$class,levels=levels(actual)); ggplot(data.frame(a=actual,c=actual==predicted),aes(a,fill=c))+geom_bar(position="fill")+scale_fill_manual(values=c("TRUE"="#198754","FALSE"="#dc3545"),labels=c("Wrong","Correct"))+labs(title="Classification",x="Actual",y="Proportion")+theme_minimal(base_size=13)} })
  output$ml_cm <- renderPlot({ res<-ml_r();req(input$ml_cm_sel,res$task=="classification");r<-res$results[[input$ml_cm_sel]]; if(is.null(r)||isTRUE(r$error)||is.null(r$confusion))return(NULL);cm<-as.data.frame(r$confusion); ggplot(cm,aes(x=Actual,y=Predicted,fill=Freq))+geom_tile(color="white")+geom_text(aes(label=Freq),size=6,fontface="bold")+scale_fill_gradient(low="white",high="#6f42c1")+labs(title="Confusion matrix")+theme_minimal(base_size=13)+theme(panel.grid=element_blank()) })
  output$ml_cm_tbl <- renderDT({ res<-ml_r();req(input$ml_cm_sel);r<-res$results[[input$ml_cm_sel]]; if(is.null(r)||isTRUE(r$error))return(NULL); datatable(r$metrics,options=list(dom="t"),rownames=FALSE) })
  output$ml_tree <- renderPlot({ res<-ml_r();dt<-res$results[["Decision Tree"]]; if(is.null(dt)||isTRUE(dt$error)){plot.new();text(0.5,0.5,"Train a Decision Tree first.",cex=1.2);return()}; if(has_pkg("rpart.plot"))rpart.plot::rpart.plot(dt$fit,main="Decision Tree",roundint=FALSE) else{plot(dt$fit);text(dt$fit,use.n=TRUE,cex=0.8)} })

  # ── DOE ──
  parsed_doe_names <- reactive({ nm<-trimws(strsplit(input$doe_names%||%"",",")[[1]]); nm<-nm[nm!=""]; n<-input$doe_k%||%3; if(length(nm)<n)nm<-c(nm,paste0("X",seq(length(nm)+1,n))); nm[seq_len(n)] })
  gen_doe_r <- eventReactive(input$gen_doe, { tryCatch(make_design(input$doe_type,parsed_doe_names(),input$doe_runs,input$doe_levels,input$doe_cp,isTRUE(input$doe_rand)),error=function(e){showNotification(e$message,type="error");NULL}) })
  output$doe_tbl <- renderDT({ des<-gen_doe_r();req(des);datatable(des,options=list(pageLength=10,scrollX=TRUE),rownames=FALSE) })
  output$doe_plot <- renderPlot({ des<-gen_doe_r();req(des);validate(need(ncol(des)>=2,"Need 2+ cols.")); pd<-des;pd$.run<-seq_len(nrow(pd)); ggplot(pd,aes(.data[[names(pd)[1]]],.data[[names(pd)[2]]],label=.run))+geom_point(size=3)+geom_text(vjust=-0.7,size=3)+labs(title="Design map")+theme_minimal(base_size=12) })
  doe_r <- eventReactive(input$run_doe, { df<-doctor_data();req(df,input$doe_resp,input$doe_factors);ddf<-df[,unique(c(input$doe_resp,input$doe_factors)),drop=FALSE];ddf<-safe_complete(ddf);validate(need(nrow(ddf)>=5,"Need 5+ rows.")); rhs<-paste(input$doe_factors,collapse="+"); if(isTRUE(input$doe_ia)&&length(input$doe_factors)>1)rhs<-paste0("(",rhs,")^2"); list(fit=lm(as.formula(paste(input$doe_resp,"~",rhs)),data=ddf),data=ddf) })
  output$doe_anova <- renderDT({ res<-doe_r();out<-as.data.frame(anova(res$fit));out$term<-rownames(out);rownames(out)<-NULL; datatable(out[,c("term",setdiff(names(out),"term"))],options=list(dom="tip",scrollX=TRUE),rownames=FALSE) })
  output$doe_effects <- renderPlot({ res<-doe_r();df<-res$data;resp<-input$doe_resp;fcts<-input$doe_factors; edf<-purrr::map_dfr(fcts,function(f){df|>mutate(.lev=as.factor(.data[[f]]))|>group_by(.lev)|>summarise(mean_r=mean(.data[[resp]],na.rm=TRUE),.groups="drop")|>mutate(factor=f)}); ggplot(edf,aes(.lev,mean_r,group=1))+geom_line()+geom_point(size=2.5)+facet_wrap(~factor,scales="free_x")+labs(title="Main effects",x="Level",y=paste("Mean",resp))+theme_minimal(base_size=12) })

  # ── Report ──
  output$rpt_preview <- renderUI({ df<-doctor_data(); if(is.null(df))return(div(class="tab-note",p("Upload data first."))); f<-doc_findings();fl<-doc$fix_log; div(style="background:#f8f9fa;border:1px solid #dee2e6;border-radius:10px;padding:16px;", tags$h5("Data"),tags$ul(tags$li(paste0("Rows: ",nrow(df)," | Cols: ",ncol(df))),tags$li(paste0("Missing: ",sum(is.na(df))))), if(nrow(f)>0)tags$h5(paste0("Doctor: ",sum(f$severity=="critical")," critical, ",sum(f$severity=="warning")," warnings")), if(length(fl)>0)tagList(tags$h5(paste0("Fixes (",length(fl),")")),tags$ul(lapply(fl,tags$li)))) })

  output$gen_report <- downloadHandler(
    filename=function()paste0("kernel_report_",Sys.Date(),".",input$rpt_fmt%||%"html"),
    content=function(file){
      df<-doctor_data();req(df);f<-doc_findings();fl<-doc$fix_log
      md<-c(paste0("# ",input$rpt_title%||%"Report"),if(nzchar(input$rpt_author%||%""))paste0("**Author:** ",input$rpt_author),paste0("**Date:** ",Sys.time()),"","---","",
        "## Data Overview","",paste0("- Rows: ",nrow(df)),paste0("- Columns: ",ncol(df)),paste0("- Numeric: ",length(numeric_vars_of(df))),paste0("- Missing: ",sum(is.na(df))),"")
      if(nrow(f)>0) md<-c(md,"## Variable Doctor","",paste0("- Critical: ",sum(f$severity=="critical")),paste0("- Warnings: ",sum(f$severity=="warning")),"")
      if(length(fl)>0) md<-c(md,"## Fixes Applied","",paste0("1. ",fl),"")
      prof<-column_profile(df); md<-c(md,"## Column Profile","",paste0("| ",paste(names(prof),collapse=" | ")," |"),paste0("| ",paste(rep("---",ncol(prof)),collapse=" | ")," |"),apply(prof,1,function(r)paste0("| ",paste(r,collapse=" | ")," |")),"")
      md<-c(md,"","---",paste0("*Generated by Kernel — ",Sys.time(),"*"))
      mt<-paste(md,collapse="\n")
      if((input$rpt_fmt%||%"html")=="md") writeLines(mt,file)
      else{html<-paste0("<html><head><meta charset='utf-8'><title>",input$rpt_title%||%"Report","</title><style>body{font-family:system-ui;max-width:800px;margin:40px auto;padding:0 20px;line-height:1.6;}table{border-collapse:collapse;width:100%;}th,td{border:1px solid #ddd;padding:6px 10px;text-align:left;}th{background:#f5f5f5;}</style></head><body>",gsub("\n","<br>",mt),"</body></html>");writeLines(html,file)}
    }
  )
  output$code_export <- renderText({ df<-doctor_data(); if(is.null(df))return("# Upload data first."); paste(c("# Kernel — R Code","# Generated:",as.character(Sys.time()),"","library(readr)","library(dplyr)","library(ggplot2)","",paste0("# Data: ",nrow(df)," x ",ncol(df)),"# df <- read_csv('your_data.csv')","","# EDA","# ggplot(df, aes(x = var)) + geom_histogram(bins = 30)","","# Regression","# fit <- lm(y ~ x1 + x2, data = df)","# summary(fit)"),collapse="\n") })
  output$dl_code <- downloadHandler(filename=function()paste0("kernel_code_",Sys.Date(),".R"),content=function(file)writeLines(output$code_export(),file))

  # ── Templates ──
  spc_r <- eventReactive(input$run_spc, { df<-doctor_data();req(df,input$spc_var);x<-df[[input$spc_var]];x<-x[!is.na(x)];validate(need(length(x)>=10,"Need 10+ obs.")); control_chart_stats(x,input$spc_type,input$spc_n) })
  output$spc_chart <- renderPlot({ cc<-spc_r();ooc<-cc$values>cc$ucl|cc$values<cc$lcl; ggplot(data.frame(i=cc$index,v=cc$values,o=ooc),aes(i,v))+geom_line(color="#0d6efd",linewidth=0.7)+geom_point(aes(color=o),size=2,show.legend=FALSE)+scale_color_manual(values=c("FALSE"="#0d6efd","TRUE"="#dc3545"))+geom_hline(yintercept=cc$cl,color="#198754",linewidth=1)+geom_hline(yintercept=c(cc$ucl,cc$lcl),linetype="dashed",color="#dc3545")+labs(title=paste("Control Chart:",cc$type),x="Observation",y=input$spc_var)+theme_minimal(base_size=13) })
  output$spc_ooc <- renderUI({ cc<-spc_r();n<-sum(cc$values>cc$ucl|cc$values<cc$lcl);cl<-if(n>0)"#dc3545" else "#198754"; div(class="stat-card",style=paste0("border-left:4px solid ",cl,";"),div(class="label","OOC"),div(class="value",style=paste0("color:",cl,";"),n)) })
  output$spc_stats <- renderDT({ cc<-spc_r();n_ooc<-sum(cc$values>cc$ucl|cc$values<cc$lcl); datatable(data.frame(metric=c("CL","UCL","LCL","Sigma","n","OOC","% OOC"),value=c(round(cc$cl,4),round(cc$ucl,4),round(cc$lcl,4),round(cc$sigma,4),length(cc$values),n_ooc,round(n_ooc/length(cc$values)*100,1))),options=list(dom="t"),rownames=FALSE) })

  cap_r <- eventReactive(input$run_cap, { df<-doctor_data();req(df,input$cap_var);validate(need(!is.na(input$cap_lsl)&&!is.na(input$cap_usl),"Enter LSL and USL."),need(input$cap_usl>input$cap_lsl,"USL > LSL.")); x<-df[[input$cap_var]];x<-x[!is.na(x)];validate(need(length(x)>=10,"Need 10+ obs.")); list(stats=process_capability(x,input$cap_lsl,input$cap_usl),x=x,lsl=input$cap_lsl,usl=input$cap_usl) })
  output$cap_plot <- renderPlot({ cr<-cap_r(); ggplot(data.frame(x=cr$x),aes(x=x))+geom_histogram(aes(y=after_stat(density)),bins=30,fill="#0d6efd",alpha=0.5,color="white")+geom_density(linewidth=1.2,color="#0d6efd")+geom_vline(xintercept=c(cr$lsl,cr$usl),linetype="dashed",color="#dc3545",linewidth=1)+geom_vline(xintercept=mean(cr$x),color="#198754",linewidth=1)+labs(title="Process capability")+theme_minimal(base_size=13) })
  output$cap_tbl <- renderDT({ datatable(cap_r()$stats,options=list(dom="t"),rownames=FALSE) })

  rfm_r <- eventReactive(input$run_rfm, { df<-doctor_data();req(df,input$rfm_cust,input$rfm_date,input$rfm_amt); compute_rfm(df,input$rfm_cust,input$rfm_date,input$rfm_amt) })
  output$rfm_tbl <- renderDT({ datatable(rfm_r(),options=list(pageLength=10,scrollX=TRUE,dom="ftip"),rownames=FALSE) })
  output$rfm_seg <- renderPlot({ rfm<-rfm_r();sc<-rfm|>count(segment,sort=TRUE); ggplot(sc,aes(reorder(segment,n),n,fill=segment))+geom_col(show.legend=FALSE,alpha=0.85)+coord_flip()+scale_fill_manual(values=c("Champions"="#198754","Regular"="#0d6efd","New"="#20c997","At risk"="#fd7e14","Lost"="#dc3545"))+labs(title="Segments",x=NULL,y="Count")+theme_minimal(base_size=13) })
  output$rfm_scatter <- renderPlot({ rfm<-rfm_r(); ggplot(rfm,aes(recency,monetary,size=frequency,color=segment))+geom_point(alpha=0.6)+scale_size_continuous(range=c(2,10))+scale_color_manual(values=c("Champions"="#198754","Regular"="#0d6efd","New"="#20c997","At risk"="#fd7e14","Lost"="#dc3545"))+labs(title="RFM scatter")+theme_minimal(base_size=12) })
  output$rfm_heat <- renderPlot({ rfm<-rfm_r();hd<-rfm|>group_by(R_score,F_score)|>summarise(m=mean(monetary,na.rm=TRUE),.groups="drop"); ggplot(hd,aes(factor(F_score),factor(R_score),fill=m))+geom_tile(color="white")+geom_text(aes(label=round(m,0)),size=3.5)+scale_fill_gradient(low="#EBF1F9",high="#0d6efd")+labs(title="RFM heatmap",x="Frequency",y="Recency")+theme_minimal(base_size=12)+theme(panel.grid=element_blank()) })

  pareto_r <- eventReactive(input$run_pareto, { df<-doctor_data();req(df,input$pareto_var);tab<-sort(table(df[[input$pareto_var]]),decreasing=TRUE); d<-data.frame(cat=factor(names(tab),levels=names(tab)),count=as.integer(tab)); d$pct<-d$count/sum(d$count)*100; d$cum<-cumsum(d$pct); d })
  output$pareto_plot <- renderPlot({ d<-pareto_r();co<-max(d$count)/100; ggplot(d,aes(x=cat))+geom_col(aes(y=count),fill="#0d6efd",alpha=0.8,width=0.7)+geom_line(aes(y=cum*co,group=1),color="#dc3545",linewidth=1.2)+geom_point(aes(y=cum*co),color="#dc3545",size=3)+geom_hline(yintercept=80*co,linetype="dashed",color="#999")+scale_y_continuous(name="Count",sec.axis=sec_axis(~./co,name="Cumulative %"))+labs(title="Pareto chart",x=NULL)+theme_minimal(base_size=13)+theme(axis.text.x=element_text(angle=45,hjust=1)) })

  heat_r <- eventReactive(input$run_heat, { df<-doctor_data();req(df,input$heat_vars);validate(need(length(input$heat_vars)>=2,"Need 2+ vars."));nums<-df[,input$heat_vars,drop=FALSE];nums<-safe_complete(nums);validate(need(nrow(nums)>=3,"Need 3+ rows.")); list(data=nums,scale=input$heat_scale) })
  output$tmpl_heatmap <- renderPlot({ hr<-heat_r();mat<-as.matrix(hr$data); if(hr$scale=="row")mat<-t(scale(t(mat))) else if(hr$scale=="column")mat<-scale(mat); heatmap(mat,scale="none",col=colorRampPalette(c("#2166AC","white","#B2182B"))(50),margins=c(8,8),main="Heatmap") })

  volc_r <- eventReactive(input$run_volc, { df<-doctor_data();req(df,input$volc_fc,input$volc_p); data.frame(fc=df[[input$volc_fc]],pval=df[[input$volc_p]]) })
  output$volc_plot <- renderPlot({ vr<-volc_r();vr<-vr[!is.na(vr$fc)&!is.na(vr$pval)&vr$pval>0,];vr$lp<--log10(vr$pval);ft<-input$volc_fc_t;pt<-input$volc_p_t; vr$sig<-ifelse(abs(vr$fc)>=ft&vr$pval<pt,ifelse(vr$fc>0,"Up","Down"),"NS"); ggplot(vr,aes(fc,lp,color=sig))+geom_point(alpha=0.6,size=1.5)+scale_color_manual(values=c("Up"="#dc3545","Down"="#0d6efd","NS"="#ccc"))+geom_vline(xintercept=c(-ft,ft),linetype="dashed",color="#999")+geom_hline(yintercept=-log10(pt),linetype="dashed",color="#999")+labs(title="Volcano plot",x="Log2 FC",y="-Log10 p")+theme_minimal(base_size=13) })

  # ── Sample datasets ──
  observeEvent(input$load_sample, { ds<-input$sample_ds;df<-tryCatch(as.data.frame(get(ds)),error=function(e)NULL); if(!is.null(df)){doc$original<-df;doc$current<-df;doc$fix_log<-c(doc$fix_log,paste0("Loaded: ",ds));doc$fix_count<-0;showNotification(paste0("Loaded '",ds,"' (",nrow(df),"x",ncol(df),")"),type="message")} })
  output$sample_info <- renderUI({ div(class="tab-note",p(switch(input$sample_ds,"mtcars"="32 cars, 11 vars.","iris"="150 flowers, 3 species.","airquality"="NYC air quality. Has NAs.","ToothGrowth"="Vitamin C experiment.","PlantGrowth"="3 conditions.","ChickWeight"="Growth curves by diet.","USArrests"="50 states, crime data.","faithful"="Old Faithful geyser.","swiss"="47 provinces.","sleep"="Paired drug trial.","chickwts"="6 feed types.","InsectSprays"="6 sprays.","warpbreaks"="Wool/tension breaks.","Select a dataset."))) })

  # ── Downloads ──
  output$download_clean_data <- downloadHandler(filename=function()paste0("kernel_clean_",Sys.Date(),".csv"),content=function(file){readr::write_csv(doctor_data(),file)})
  output$download_profile <- downloadHandler(filename=function()paste0("kernel_profile_",Sys.Date(),".csv"),content=function(file){readr::write_csv(column_profile(doctor_data()),file)})
  output$download_methods <- downloadHandler(filename=function()paste0("kernel_methods_",Sys.Date(),".csv"),content=function(file){readr::write_csv(methods_catalog(),file)})
  output$download_design <- downloadHandler(filename=function()paste0("kernel_design_",Sys.Date(),".csv"),content=function(file){des<-gen_doe_r();req(des);readr::write_csv(des,file)})
  output$methods_tbl <- renderDT({ datatable(methods_catalog(),options=list(pageLength=15,scrollX=TRUE),filter="top",rownames=FALSE) })

} # end server

shinyApp(ui = ui, server = server)
