# [Last Modified: 2024-11-17]

library(dplyr)
library(readr)

setwd("~/RAWorkingDirectory")

dataset <- read_csv("Dataset/data_eoracountry.csv")
df1 <- read_csv("Intermediate_inputs-to-sales_ratio/Country_level_IO_ratio_mean_tw.csv")
df2 <- read_csv("Intermediate_inputs-to-sales_ratio/Country_level_IO_ratio_first_tw.csv")

df1_i <- df1 %>%
  rename(Inter_inputs_to_Sales_i_mean = Inter_inputs_to_Sales)

df1_j <- df1 %>%
  rename(Inter_inputs_to_Sales_j_mean = Inter_inputs_to_Sales)

df2_i <- df2 %>%
  rename(Inter_inputs_to_Sales_i_first = Inter_inputs_to_Sales)

df2_j <- df2 %>%
  rename(Inter_inputs_to_Sales_j_first = Inter_inputs_to_Sales)


dataset1 <- full_join(dataset, df1_i, by = c("t", "Country_i" = "Country"))
dataset2 <- full_join(dataset1, df1_j, by = c("t", "Country_j" = "Country"))
dataset3 <- full_join(dataset2, df2_i, by = c("t", "Country_i" = "Country"))
dataset4 <- full_join(dataset3, df2_j, by = c("t", "Country_j" = "Country"))

write_csv(dataset4, "Dataset/data_eoracountry.csv")
