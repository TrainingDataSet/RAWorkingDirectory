# [Last Modified: 2024-11-17]

library(dplyr)
library(readr)

setwd("~/RAWorkingDirectory")

dataset <- read_csv("Dataset/data_eoracountry.csv")

dataset1 <- dataset %>%
  rename(Within_country_corr_CO2_i = Corr_GDP_CO2_i, Within_country_corr_GHG_i = Corr_GDP_GHG_i,
         Within_country_corr_CO2_j = Corr_GDP_CO2_j, Within_country_corr_GHG_j = Corr_GDP_GHG_j)


dataset1$Corr_prod_CO2 <- dataset1$Within_country_corr_CO2_i * dataset1$Within_country_corr_CO2_j
dataset1$Corr_prod_GHG <- dataset1$Within_country_corr_GHG_i * dataset1$Within_country_corr_GHG_j

dataset2 <- dataset1 %>%
  relocate(Corr_prod_CO2, Corr_prod_GHG, .after = Within_country_corr_GHG_j)


write_csv(dataset2, "Dataset/data_eoracountry.csv")
