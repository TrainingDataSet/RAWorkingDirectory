# [Last Modified: 2024-11-02]
# 2024-11-02: Merge Exports_importance and Imports_importance
# 2025-01-20: Merge Tradeness and Exports_importance_intermedaite

library(readr)
library(dplyr)

setwd("~/RAWorkingDirectory")

exports <- read_csv("Tradeness/Exports_importance/Exports_importance.csv")
imports <- read_csv("Tradeness/Imports_importance/Imports_importance.csv")

tradeness <- inner_join(exports, imports, by = c("Year", "Country_i", "Country_j", "Industry_s"))

write_csv(tradeness, "Tradeness/Tradeness.csv")

# 2025-01-20
tradeness <- read_csv("Tradeness/Tradeness.csv")
exports_intermediate <- read_csv("Tradeness/Exports_importance_intermediate/Exports_importance_intermediate.csv")
tradeness2 <- inner_join(tradeness, exports_intermediate, by = c("Year", "Country_i", "Country_j", "Industry_s"))
write_csv(tradeness2, "Tradeness/Tradeness.csv")


