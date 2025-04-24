# [Last Modified: 2025-03-10]

library(dplyr)
library(readr)
library(readxl)

setwd("~/RAWorkingDirectory")

dataset <- read_xlsx("OWID/globaltrade_country.xlsx")
owid_corr <- read_csv("OWID/OWID_corr.csv")

owid_corr$Country_i <- recode(owid_corr$Country_i, 
                              "CHE" = "SWI",
                              "CHN" = "PRC",
                              "DEU" = "GER",
                              "DNK" = "DEN",
                              "ESP" = "SPA",
                              "GBR" = "UKG",
                              "IDN" = "INO",
                              "IRL" = "IRE",
                              "NLD" = "NET",
                              "PRT" = "POR",
                              "ROU" = "ROM",
                              "TWN" = "TAP",
                              "BGD" = "BAN",
                              "MYS" = "MAL",
                              "PHL" = "PHI",
                              "VNM" = "VIE",
                              "MNG" = "MON",
                              "LKA" = "SRI",
                              "FJI" = "FIJ",
                              "BRN" = "BRU",
                              "BTN" = "BHU",
                              "KHM" = "CAM",
                              "MDV" = "MLD",
                              "NPL" = "NEP",
                              "SGP" = "SIN")

owid_corr$Country_j <- recode(owid_corr$Country_j, 
                              "CHE" = "SWI",
                              "CHN" = "PRC",
                              "DEU" = "GER",
                              "DNK" = "DEN",
                              "ESP" = "SPA",
                              "GBR" = "UKG",
                              "IDN" = "INO",
                              "IRL" = "IRE",
                              "NLD" = "NET",
                              "PRT" = "POR",
                              "ROU" = "ROM",
                              "TWN" = "TAP",
                              "BGD" = "BAN",
                              "MYS" = "MAL",
                              "PHL" = "PHI",
                              "VNM" = "VIE",
                              "MNG" = "MON",
                              "LKA" = "SRI",
                              "FJI" = "FIJ",
                              "BRN" = "BRU",
                              "BTN" = "BHU",
                              "KHM" = "CAM",
                              "MDV" = "MLD",
                              "NPL" = "NEP",
                              "SGP" = "SIN")


dataset2 <- inner_join(dataset, owid_corr, by = c("index_bc" = "t", "country_s" = "Country_i", "country_r" = "Country_j"))
dataset3 <- dataset2 %>%
  select(index_bc, country_s, country_r, corr_rgdp, corr_rgdp_co2, corr_co2, corr_rgdp_capita, corr_rgdp_co2_capita, corr_co2_capita,
         Within_country_corr_CO2_i, Within_country_corr_CO2_j, Corr_prod_CO2, Corr_diff_CO2, everything())


write_csv(dataset3, "OWID/globaltrade_country (2).csv")
