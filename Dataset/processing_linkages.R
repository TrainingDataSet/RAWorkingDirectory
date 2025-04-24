# [Last Modified: 2025-01-20]
# 2025-01-20: Add Exports_importance_intermediate

library(dplyr)
library(readr)

setwd("~/RAWorkingDirectory")

dataset <- read_csv("Dataset/data_eoracountry.csv")

dataset2 <- dataset[, c("t","Country_i", "Country_j", "Forward_to_trade_mean", 
                        "Backward_to_trade_mean", "Forward_to_trade_first",
                        "Backward_to_trade_first", "Forward_MB_mean",
                        "Backward_MB_mean", "Forward_MB_first", "Backward_MB_first")]

dataset3 <- full_join(dataset2, dataset2, by = c("t", "Country_i" = "Country_j", "Country_j" = "Country_i"))

dataset4 <- dataset3 %>%
  rename(Forward_to_trade_i_mean = Forward_to_trade_mean.x,
         Backward_to_trade_i_mean = Backward_to_trade_mean.x,
         Forward_to_trade_i_first = Forward_to_trade_first.x,
         Backward_to_trade_i_first = Backward_to_trade_first.x,
         Forward_to_trade_j_mean = Forward_to_trade_mean.y,
         Backward_to_trade_j_mean = Backward_to_trade_mean.y,
         Forward_to_trade_j_first = Forward_to_trade_first.y,
         Backward_to_trade_j_first = Backward_to_trade_first.y,
         
         Forward_to_trade_MB_i_mean = Forward_MB_mean.x,
         Backward_to_trade_MB_i_mean = Backward_MB_mean.x,
         Forward_to_trade_MB_i_first = Forward_MB_first.x,
         Backward_to_trade_MB_i_first = Backward_MB_first.x,
         Forward_to_trade_MB_j_mean = Forward_MB_mean.y,
         Backward_to_trade_MB_j_mean = Backward_MB_mean.y,
         Forward_to_trade_MB_j_first = Forward_MB_first.y,
         Backward_to_trade_MB_j_first = Backward_MB_first.y)

dataset5 <- full_join(dataset, dataset4, by = c("t", "Country_i", "Country_j"))

dataset6 <- dataset5 %>%
  select(-c(Forward_to_trade_mean, 
            Backward_to_trade_mean, Forward_to_trade_first,
            Backward_to_trade_first, Forward_MB_mean,
            Backward_MB_mean, Forward_MB_first, Backward_MB_first))


dataset7 <- dataset6 %>%
  select(t, Country_i, Country_j, Corr_GDP, Corr_CO2, Corr_GHG,
         Within_country_corr_CO2_i, Within_country_corr_GHG_i, Within_country_corr_CO2_j,
         Within_country_corr_GHG_j, Corr_prod_CO2, Corr_prod_GHG, Corr_diff_CO2, Corr_diff_GHG,
         
         Forward_to_trade_i_mean, Forward_to_trade_j_mean,
         Backward_to_trade_i_mean, Backward_to_trade_j_mean,
         Forward_to_trade_MB_i_mean, Forward_to_trade_MB_j_mean,
         Backward_to_trade_MB_i_mean, Backward_to_trade_MB_j_mean,
         
         Forward_to_trade_i_first, Forward_to_trade_j_first,
         Backward_to_trade_i_first, Backward_to_trade_j_first,
         Forward_to_trade_MB_i_first, Forward_to_trade_MB_j_first,
         Backward_to_trade_MB_i_first, Backward_to_trade_MB_j_first,
         
         everything())

write_csv(dataset7, "Dataset/data_eoracountry.csv")


# 2025-01-20
dataset <- read_csv("Dataset/data_eoracountry3.csv")

dataset2 <- dataset[, c("t","Country_i", "Country_j", 
                        "Forward_to_trade_intermediate_mean", "Forward_to_trade_intermediate_first",
                        "Forward_MB_intermediate_mean", "Forward_MB_intermediate_first")]

dataset3 <- full_join(dataset2, dataset2, by = c("t", "Country_i" = "Country_j", "Country_j" = "Country_i"))

dataset4 <- dataset3 %>%
  rename(Forward_to_trade_intermediate_i_mean = Forward_to_trade_intermediate_mean.x,
         Forward_to_trade_intermediate_i_first = Forward_to_trade_intermediate_first.x,
         Forward_to_trade_intermediate_j_mean = Forward_to_trade_intermediate_mean.y,
         Forward_to_trade_intermediate_j_first = Forward_to_trade_intermediate_first.y,
         
         Forward_to_trade_MB_intermediate_i_mean = Forward_MB_intermediate_mean.x,
         Forward_to_trade_MB_intermediate_i_first = Forward_MB_intermediate_first.x,
         Forward_to_trade_MB_intermediate_j_mean = Forward_MB_intermediate_mean.y,
         Forward_to_trade_MB_intermediate_j_first = Forward_MB_intermediate_first.y)

dataset5 <- full_join(dataset, dataset4, by = c("t", "Country_i", "Country_j"))

dataset6 <- dataset5 %>%
  select(-c(Forward_to_trade_intermediate_mean, Forward_to_trade_intermediate_first,
            Forward_MB_intermediate_mean, Forward_MB_intermediate_first))

dataset7 <- dataset6 %>%
  select(t, Country_i, Country_j, Corr_GDP, Corr_CO2, Corr_GHG,
         Within_country_corr_CO2_i, Within_country_corr_GHG_i, Within_country_corr_CO2_j,
         Within_country_corr_GHG_j, Corr_prod_CO2, Corr_prod_GHG, Corr_diff_CO2, Corr_diff_GHG,
         
         Forward_to_trade_i_mean, Forward_to_trade_j_mean,
         Forward_to_trade_intermediate_i_mean, Forward_to_trade_intermediate_j_mean,
         Backward_to_trade_i_mean, Backward_to_trade_j_mean,
         Forward_to_trade_MB_i_mean, Forward_to_trade_MB_j_mean,
         Forward_to_trade_MB_intermediate_i_mean, Forward_to_trade_MB_intermediate_j_mean,
         Backward_to_trade_MB_i_mean, Backward_to_trade_MB_j_mean,
         
         Forward_to_trade_i_first, Forward_to_trade_j_first,
         Forward_to_trade_intermediate_i_first, Forward_to_trade_intermediate_j_first,
         Backward_to_trade_i_first, Backward_to_trade_j_first,
         Forward_to_trade_MB_i_first, Forward_to_trade_MB_j_first,
         Forward_to_trade_MB_intermediate_i_first, Forward_to_trade_MB_intermediate_j_first,
         Backward_to_trade_MB_i_first, Backward_to_trade_MB_j_first,
         
         everything())

write_csv(dataset7, "Dataset/data_eoracountry.csv")



