# Data Dictionary — Gold Layer Tables

All tables reside in the `portfolio_finance.gold` schema.

---

## fact_loans

One row per loan application — the central fact table. Sourced from `silver.loans_clean`.

| Column | Type | Description |
|---|---|---|
| person_age | INT | Borrower age in years |
| person_income | DOUBLE | Annual income (USD) |
| person_home_ownership | STRING | RENT / OWN / MORTGAGE / OTHER |
| emp_length_years | DOUBLE | Employment length (years, capped at 50) |
| loan_intent | STRING | Education / Medical / Personal / Venture / Homeimprovement / Debtconsolidation |
| loan_grade | STRING | A / B / C / D / E / F / G (lender-assigned credit grade) |
| loan_amount | DOUBLE | Loan amount (USD) |
| interest_rate | DOUBLE | Annual interest rate (%) |
| default_flag | BOOLEAN | True = loan defaulted (loan_status = 1) |
| loan_pct_income | DOUBLE | Loan amount as fraction of annual income (debt-to-income ratio) |
| prior_default_flag | BOOLEAN | True = prior default on credit bureau file |
| credit_hist_years | INT | Length of credit history (years) |
| age_band | STRING | 18-25 / 26-35 / 36-45 / 46-55 / 56+ |
| emp_band | STRING | <1yr / 1-3yr / 3-5yr / 5-10yr / 10+yr |
| rate_band | STRING | Low (<8%) / Mid (8-12%) / High (12-16%) / Very High (16%+) |
| income_band | STRING | Low / Mid / High / Top |
| income_percentile | DOUBLE | Income rank across all borrowers (0–100) |
| loan_amount_band | STRING | <$5K / $5-10K / $10-20K / $20K+ |
| grade_risk_order | INT | Numeric grade rank: A=1 (lowest risk) to G=7 (highest) |

---

## credit_default_risk_scores

Per-loan credit default risk score (0–100) derived from 7 weighted factors.

| Column | Type | Description |
|---|---|---|
| loan_grade | STRING | Lender-assigned grade |
| credit_risk_score | DOUBLE | Normalized score 0–100 (higher = more default risk) |
| raw_score | INT | Raw sum of factor points (max 120) |
| risk_band | STRING | Low / Medium / High / Critical |
| default_flag | BOOLEAN | Actual outcome — used for model validation |
| score_grade | INT | 0–35 pts based on grade (A=0, G=35) |
| score_prior_default | INT | 25 pts if prior default on file |
| score_dti | INT | 20 pts if DTI > 40%; 10 pts if 25–40% |
| score_rate | INT | 15 pts if rate ≥ 16%; 7 pts if 12–16% |
| score_income | INT | 10 pts if Low income; 5 pts if Mid |
| score_cred_hist | INT | 10 pts if credit history < 2 yrs; 5 pts if 2–4 yrs |
| score_intent_risk | INT | 5 pts if Debt Consolidation/Medical loan > $15K |

**Model validation (actual default rates by band):**
| Band | Score Range | Default Rate |
|------|-------------|--------------|
| Low | 0–24 | 11.7% |
| Medium | 25–49 | 40.8% |
| High | 50–74 | 68.6% |
| Critical | 75–100 | 92.8% |

---

## agg_kpi_summary

Single-row KPI summary for HTML report header cards.

| Column | Type | Description |
|---|---|---|
| total_loans | LONG | 32,581 |
| total_loan_value | DOUBLE | $312.4M |
| avg_loan_amount | DOUBLE | $9,589 |
| default_rate_pct | DOUBLE | 21.8% |
| total_defaults | LONG | 7,108 |
| avg_interest_rate | DOUBLE | 11.01% |
| avg_dti_pct | DOUBLE | 17.0% |
| borrowers_prior_default | LONG | 5,745 |
| loan_grades | LONG | 7 |
| loan_intents | LONG | 6 |

---

## agg_default_by_grade

Default rate and loan stats by grade A–G.

| Column | Type | Description |
|---|---|---|
| loan_grade | STRING | A–G |
| grade_risk_order | INT | 1–7 |
| total_loans | LONG | Loans in grade |
| defaults | LONG | Defaulted loans |
| default_rate_pct | DOUBLE | % defaulted |
| avg_interest_rate | DOUBLE | Avg rate |
| avg_loan_amount | DOUBLE | Avg loan amount |
| avg_dti_pct | DOUBLE | Avg debt-to-income % |

---

## agg_default_by_intent

Default rate by loan purpose.

| Column | Type | Description |
|---|---|---|
| loan_intent | STRING | Loan purpose |
| total_loans | LONG | Total loans |
| defaults | LONG | Defaults |
| default_rate_pct | DOUBLE | Default rate |
| avg_loan_amount | DOUBLE | Avg loan |
| avg_interest_rate | DOUBLE | Avg rate |
| avg_dti_pct | DOUBLE | Avg DTI |

---

## agg_default_by_income

Default rate by borrower income band.

| Column | Type | Description |
|---|---|---|
| income_band | STRING | Low / Mid / High / Top |
| sort_order | INT | 1–4 |
| total_loans | LONG | Total loans |
| defaults | LONG | Defaults |
| default_rate_pct | DOUBLE | Default rate |
| avg_income | DOUBLE | Avg annual income |
| avg_loan_amount | DOUBLE | Avg loan |
| avg_rate | DOUBLE | Avg interest rate |

---

## agg_default_by_rate_band

Default rate by interest rate band.

| Column | Type | Description |
|---|---|---|
| rate_band | STRING | Low / Mid / High / Very High |
| sort_order | INT | 1–4 |
| total_loans | LONG | Total loans |
| defaults | LONG | Defaults |
| default_rate_pct | DOUBLE | Default rate |
| avg_rate | DOUBLE | Avg rate in band |
| avg_loan_amount | DOUBLE | Avg loan |

---

## agg_default_by_age

Default rate by borrower age band.

| Column | Type | Description |
|---|---|---|
| age_band | STRING | 18-25 / 26-35 / 36-45 / 46-55 / 56+ |
| sort_order | INT | 1–5 |
| total_loans | LONG | Total loans |
| defaults | LONG | Defaults |
| default_rate_pct | DOUBLE | Default rate |
| avg_income | DOUBLE | Avg income |
| avg_loan_amount | DOUBLE | Avg loan |

---

## agg_loan_benchmarks

Loan amount and rate statistics per grade.

| Column | Type | Description |
|---|---|---|
| loan_grade | STRING | A–G |
| grade_risk_order | INT | 1–7 |
| total_loans | LONG | Loans in grade |
| min_loan | DOUBLE | Minimum loan amount |
| avg_loan | DOUBLE | Average loan amount |
| median_loan | DOUBLE | Median loan amount |
| max_loan | DOUBLE | Maximum loan amount |
| min_rate | DOUBLE | Minimum interest rate |
| avg_rate | DOUBLE | Average interest rate |
| max_rate | DOUBLE | Maximum interest rate |

---

## agg_risk_by_grade

Risk band distribution per loan grade.

| Column | Type | Description |
|---|---|---|
| loan_grade | STRING | A–G |
| grade_risk_order | INT | 1–7 |
| risk_band | STRING | Low / Medium / High / Critical |
| loans | LONG | Loans in that band |
| avg_score | DOUBLE | Avg credit risk score |
| actual_default_rate | DOUBLE | Actual default rate |

---

## xtab_grade_risk

Cross-tab: loan grade × risk band (for HTML report).

| Column | Type | Description |
|---|---|---|
| loan_grade | STRING | A–G |
| grade_risk_order | INT | 1–7 |
| risk_band | STRING | Risk band |
| loans | LONG | Loan count |
| avg_score | DOUBLE | Avg score |
| default_rate | DOUBLE | Actual default rate |

---

## xtab_intent_grade

Cross-tab: loan intent × grade.

| Column | Type | Description |
|---|---|---|
| loan_intent | STRING | Loan purpose |
| loan_grade | STRING | A–G |
| grade_risk_order | INT | 1–7 |
| loans | LONG | Loan count |
| default_rate | DOUBLE | Default rate |
| avg_loan | DOUBLE | Avg loan amount |

---

## xtab_income_grade

Cross-tab: income band × grade.

| Column | Type | Description |
|---|---|---|
| income_band | STRING | Low / Mid / High / Top |
| loan_grade | STRING | A–G |
| grade_risk_order | INT | 1–7 |
| loans | LONG | Loan count |
| default_rate | DOUBLE | Default rate |
| avg_rate | DOUBLE | Avg interest rate |
