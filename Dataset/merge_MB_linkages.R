# [Last Modified: 2025-01-20]
# 2025-01-20: Add Exports_importance_intermediate

library(dplyr)
library(readr)

setwd("~/RAWorkingDirectory")

dataset <- read_csv("Dataset/data_eoracountry.csv")
df1 <- read_csv("MB_linkages/MB_linkages_mean_tw.csv")
df2 <- read_csv("MB_linkages/MB_linkages_first_tw.csv")

df1 <- df1 %>%
  rename(Forward_MB_mean = Forward_MB, Backward_MB_mean = Backward_MB)

df2 <- df2 %>%
  rename(Forward_MB_first = Forward_MB, Backward_MB_first = Backward_MB)

dataset1 <- left_join(dataset, df1, by = c("t", "Country_i", "Country_j"))
dataset2 <- left_join(dataset1, df2, by = c("t", "Country_i", "Country_j"))

write_csv(dataset2, "Dataset/data_eoracountry.csv")


# 2025-01-20
dataset <- read_csv("Dataset/data_eoracountry2.csv")
df1 <- read_csv("MB_linkages/MB_linkages_mean_tw2.csv")
df2 <- read_csv("MB_linkages/MB_linkages_first_tw2.csv")

df1 <- df1 %>%
  rename(Forward_MB_intermediate_mean = Forward_MB_intermediate)

df2 <- df2 %>%
  rename(Forward_MB_intermediate_first = Forward_MB_intermediate)

df1 <- df1[, c("t", "Country_i", "Country_j", "Forward_MB_intermediate_mean")]
df2 <- df2[, c("t", "Country_i", "Country_j", "Forward_MB_intermediate_first")]


dataset1 <- left_join(dataset, df1, by = c("t", "Country_i", "Country_j"))
dataset2 <- left_join(dataset1, df2, by = c("t", "Country_i", "Country_j"))

write_csv(dataset2, "Dataset/data_eoracountry3.csv")

