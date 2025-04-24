# [Last Modified: 20245-02-07]
# 2025-02-07: Find variables for the entire country

library(dplyr)
library(readr)
library(parallel)
library(microbenchmark)

setwd("~/RAWorkingDirectory")

gdp_deflator <- read_csv("GDP_deflator/USgdp_deflator_FRED.csv")
exports_imports_sales_va <- read_csv("Export_Import_Sales_VA2/Export_Import_Sales_VA_EntireCountry (2).csv")
exports_imports_sales_va2 <- left_join(exports_imports_sales_va, gdp_deflator, by = c("Year" = "year"))
exports_imports_sales_va2$VA_con <- exports_imports_sales_va2$VA_cur / (exports_imports_sales_va2$gdp_def / 100)
exports_imports_sales_va3 <- exports_imports_sales_va2 %>% select(-gdp_def)
write_csv(exports_imports_sales_va3, "Export_Import_Sales_VA2/Export_Import_Sales_VA_EntireCountry (2).csv")
