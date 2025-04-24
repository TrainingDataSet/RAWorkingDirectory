# [Last Modified: 2025-01-20]
# 2025-01-20: Use Exports_importance_intermediate instead of Exports_importance

library(dplyr)
library(readr)

setwd("~/RAWorkingDirectory")

# Time Series (Country_i, S)
linkages <- read_parquet("Linkages/Linkages_to_tradedSector/Linkages_to_tradedSector_iSj2.parquet")
linkages2 <- linkages[, c("Year", "Country_i",  "S", "Country_j", "Forward_weighted", "Forward_intermediate_weighted")]
linkages3 <- linkages2 %>%
  group_by(Year, Country_i, S) %>%
  summarize(Forward_to_trade = mean(Forward_weighted), 
            Forward_to_trade_intermediate = mean(Forward_intermediate_weighted), .groups = "drop") %>%
  arrange(Country_i, S, Year)
  
MB_linkages <- read_parquet("MB_linkages/MB_linkages_i_S_j2.parquet")
MB_linkages2 <- MB_linkages[, c("Year", "Country_i", "S", "Country_j", "Forward_MB_weighted", "Forward_MB_intermediate_weighted")]
MB_linkages3 <- MB_linkages2 %>%
  group_by(Year, Country_i, S) %>%
  summarize(Forward_to_trade_MB = mean(Forward_MB_weighted),
            Forward_to_trade_MB_intermediate = mean(Forward_MB_intermediate_weighted), .groups = "drop") %>%
  arrange(Country_i, S, Year)

linkages_iS <- full_join(linkages3, MB_linkages3, by = c("Year", "Country_i", "S"))
write_csv(linkages_iS, "TimeSeries_linkages_country_industry.csv")


# Time Series (Country_i)
linkages <- read_parquet("Linkages/Linkages_to_tradedSector/Linkages_to_tradedSector_ij2.parquet")
linkages2 <- linkages[, c("Year", "Country_i", "Country_j", "Forward_to_trade", "Forward_to_trade_intermediate")]
linkages3 <- linkages2 %>%
  group_by(Year, Country_i) %>%
  summarize(Forward_to_trade = mean(Forward_to_trade),
            Forward_to_trade_intermediate = mean(Forward_to_trade_intermediate), .groups = "drop") %>%
  arrange(Country_i, Year)

MB_linkages <- read_parquet("MB_linkages/MB_linkages_i_j2.parquet")
MB_linkages2 <- MB_linkages[, c("Year", "Country_i", "Country_j", "Forward_MB", "Forward_MB_intermediate")]
MB_linkages3 <- MB_linkages2 %>%
  group_by(Year, Country_i) %>%
  summarize(Forward_to_trade_MB = mean(Forward_MB),
            Forward_to_trade_MB_intermediate = mean(Forward_MB_intermediate), .groups = "drop") %>%
  arrange(Country_i, Year)

linkages_i <- full_join(linkages3, MB_linkages3, by = c("Year", "Country_i"))
write_csv(linkages_i, "TimeSeries_linkages_country.csv")


# Time Series (Country_i, S, Country_j)
linkages <- read_parquet("Linkages/Linkages_to_tradedSector/Linkages_to_tradedSector_iSj2.parquet")
linkages2 <- linkages[, c("Year", "Country_i",  "S", "Country_j", "Forward_weighted", "Forward_intermediate_weighted")]
linkages3 <- linkages2 %>%
  arrange(Country_i, S, Country_j, Year) %>%
  rename(Forward_to_trade = Forward_weighted, Forward_to_trade_intermediate = Forward_intermediate_weighted)

MB_linkages <- read_parquet("MB_linkages/MB_linkages_i_S_j2.parquet")
MB_linkages2 <- MB_linkages[, c("Year", "Country_i", "S", "Country_j", "Forward_MB_weighted", "Forward_MB_intermediate_weighted")]
MB_linkages3 <- MB_linkages2 %>%
  arrange(Country_i, S, Country_j, Year) %>%
  rename(Forward_to_trade_MB = Forward_MB_weighted, Forward_to_trade_MB_intermediate = Forward_MB_intermediate_weighted)

linkages_iSj <- full_join(linkages3, MB_linkages3, by = c("Year", "Country_i", "S", "Country_j"))
write_csv(linkages_iSj, "TimeSeries_linkages_countryPair_industry.csv")


# Time Series (Country_i, Country_j)
linkages <- read_parquet("Linkages/Linkages_to_tradedSector/Linkages_to_tradedSector_ij2.parquet")
linkages2 <- linkages[, c("Year", "Country_i", "Country_j", "Forward_to_trade", "Forward_to_trade_intermediate")]
linkages3 <- linkages2 %>%
  arrange(Country_i, Country_j, Year)

MB_linkages <- read_parquet("MB_linkages/MB_linkages_i_j2.parquet")
MB_linkages2 <- MB_linkages[, c("Year", "Country_i", "Country_j", "Forward_MB", "Forward_MB_intermediate")]
MB_linkages3 <- MB_linkages2 %>%
  arrange(Country_i, Country_j, Year) %>%
  rename(Forward_to_trade_MB = Forward_MB, Forward_to_trade_MB_intermediate = Forward_MB_intermediate)

linkages_ij <- full_join(linkages3, MB_linkages3, by = c("Year", "Country_i", "Country_j"))
write_csv(linkages_ij, "TimeSeries_linkages_countryPair.csv")




