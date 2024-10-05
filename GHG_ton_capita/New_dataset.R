# [Last Modified: 2024-09-09]

library(dplyr)
library(readr)

setwd("~/RAWorkingDirectory")

dataset <- read_csv("GHG_ton_capita/data_eoracountry.csv")
first_tw <- read_csv("GHG_ton_capita/GHG_ton_capita_first_tw.csv")
mean_tw <- read_csv("GHG_ton_capita/GHG_ton_capita_mean_tw.csv")

merged_df <- full_join(dataset, first_tw, by = c("t", "Country_i" = "Country"), relationship = "many-to-one")
merged_df <- merged_df %>%
  rename(Ighg_ton_capita_first = ghg_ton_capita)

merged_df <- full_join(merged_df, first_tw, by = c("t", "Country_j" = "Country"), relationship = "many-to-one")
merged_df <- merged_df %>%
  rename(Jghg_ton_capita_first = ghg_ton_capita)

merged_df <- full_join(merged_df, mean_tw, by = c("t", "Country_i" = "Country"), relationship = "many-to-one")
merged_df <- merged_df %>%
  rename(Ighg_ton_capita_mean = ghg_ton_capita)

merged_df <- full_join(merged_df, mean_tw, by = c("t", "Country_j" = "Country"), relationship = "many-to-one")
merged_df <- merged_df %>%
  rename(Jghg_ton_capita_mean = ghg_ton_capita)

write_csv(merged_df, "dataset.csv")