# [Last Modified: 2025-03-14]

library(dplyr)
library(readr)

setwd("~/RAWorkingDirectory")

df <- read_csv("OWID/owid-co2-data_corrected.csv")

# Calculate average for each period
df2 <- df %>% group_by(iso_code) %>%
  summarise(
    `real GDP` = mean(rgdp),
    `real GDP per capita` = mean(rgdp_capita),
    `CO2` = mean(co2),
    `CO2 per capita` = mean(co2_per_capita)
  )

# Convert country code from owid to adb
df2$iso_code <- recode(df2$iso_code, 
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

# Merge df2 with dataset
dataset <- read_csv("ADB/Dataset/globaltrade_country_entirePeriod.csv")

dataset2 <- inner_join(dataset, df2, by = c("country_s" = "iso_code")) %>%
  rename(`real GDP_s` = `real GDP`,
         `real GDP per capita_s` = `real GDP per capita`,
         `CO2_s` = `CO2`,
         `CO2 per capita_s` = `CO2 per capita`)

dataset3 <- inner_join(dataset2, df2, by = c("country_r" = "iso_code")) %>%
  rename(`real GDP_r` = `real GDP`,
         `real GDP per capita_r` = `real GDP per capita`,
         `CO2_r` = `CO2`,
         `CO2 per capita_r` = `CO2 per capita`)

dataset4 <- dataset3 %>%
  select(country_s, country_r, `real GDP_s`, `real GDP per capita_s`,
         `CO2_s`, `CO2 per capita_s`, `real GDP_r`, `real GDP per capita_r`,
         `CO2_r`, `CO2 per capita_r`, everything())

# Save it as a csv file
write_csv(dataset4, "ADB/Dataset/globaltrade_country_entirePeriod.csv (2).csv")


