# [Last Modified: 2024-08-08]
# 2024-08-08: Processing GDP(constant 2015 US$) data from World Bank (https://data.worldbank.org/indicator/NY.GDP.MKTP.KD)

library(tidyr)
library(dplyr)
library(readr)

df <- read_csv("GDP_raw.csv")

df_melted <- df %>%
  pivot_longer(cols = 3:ncol(df), names_to = "Year", values_to = "GDP") %>%
  rename(Country = "Country Code") %>%
  arrange(Year, Country) %>%
  select(Year, Country, GDP, -"Country Name")


country_lst <- c("DZA", "AGO", "ARG", "AUS", "AUT", "AZE", "BHR", "BGD", "BLR", "BEL", "BOL", "BIH", "BRA", "BGR", "KHM", "CAN", "CHL", "CHN", "COL", "COG", "CRI",
                 "HRV", "CUB", "CYP", "CZE", "DNK", "DOM", "ECU", "EGY", "SLV", "EST", "ETH", "FIN", "FRA", "GEO", "DEU", "GHA", "GRC", "GTM", "HND", "HKG", "HUN",
                 "ISL", "IND", "IDN", "IRN", "IRQ", "IRL", "ISR", "ITA", "JPN", "KAZ", "KEN", "KWT", "LVA", "LBN", "LTU", "LUX", "MYS", "MLT", "MEX", "MAR", "NLD",
                 "NZL", "NOR", "OMN", "PAK", "PRY", "PER", "PHL", "POL", "PRT", "QAT", "KOR", "ROU", "RUS", "SAU", "SRB", "SGP", "SVK", "SVN", "ZAF", "ESP", "LKA",
                 "SWE", "CHE", "THA", "TUN", "TUR", "UKR", "ARE", "GBR", "TZA", "USA", "URY", "UZB", "VEN", "VNM")
  
df_selected <- df_melted[(df_melted$Year %in% c(1990:2022) &
                         df_melted$Country %in% country_lst), ]

write_csv(df_selected, "GDP/GDP.csv")

# nan_rows <- df_selected[!complete.cases(df_selected), ]
