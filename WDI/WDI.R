# [Last Modified: 2025-02-09]
# 2025-02-09: Processing World Development Indicators from World Bank

library(tidyr)
library(dplyr)
library(readr)
library(readxl)

# WDI Code extraction
variable_lst <- read_excel("Variable_list_WorldDevelopmentIndicator.xlsx", sheet = 1, col_names = c("Variable", "Description"))

code_lst <- unlist(regmatches(variable_lst[[2]], gregexpr("\\[(.*?)\\]", variable_lst[[2]]))) # Extract text inside square brackets
code_lst <- gsub("\\[|\\]", "", code_lst) # Remove square brackets
variable_lst$Code <- code_lst  # Add Code column
col_mapping <- setNames(variable_lst$Variable, variable_lst$Code)  # Create mapping table of code and variable


# Country List
country_lst <- c("AFG", "ALB", "DZA", "AND", "AGO", "ATG", "ARG", "ARM", "ABW", "AUS", "AUT", "AZE", "BHS", "BHR", "BGD", "BRB", "BLR", "BEL", "BLZ", "BEN", "BMU",
    "BTN", "BOL", "BIH", "BWA", "BRA", "VGB", "BRN", "BGR", "BFA", "BDI", "KHM", "CMR", "CAN", "CPV", "CYM", "CAF", "TCD", "CHL", "CHN", "COL", "COG", "CRI", "HRV",
    "CUB", "CYP", "CZE", "CIV", "PRK", "COD", "DNK", "DJI", "DOM", "ECU", "EGY", "SLV", "ERI", "EST", "ETH", "FJI", "FIN", "FRA", "PYF", "GAB", "GMB", "GEO", "DEU",
    "GHA", "GRC", "GRL", "GTM", "GIN", "GUY", "HTI", "HND", "HKG", "HUN", "ISL", "IND", "IDN", "IRN", "IRQ", "IRL", "ISR", "ITA", "JAM", "JPN", "JOR", "KAZ", "KEN",
    "KWT", "KGZ", "LAO", "LVA", "LBN", "LSO", "LBR", "LBY", "LIE", "LTU", "LUX", "MAC", "MDG", "MWI", "MYS", "MDV", "MLI", "MLT", "MRT", "MUS", "MEX", "MCO", "MNG",
    "MNE", "MAR", "MOZ", "MMR", "NAM", "NPL", "NLD", "ANT", "NCL", "NZL", "NIC", "NER", "NGA", "NOR", "PSE", "OMN", "PAK", "PAN", "PNG", "PRY", "PER", "PHL", "POL",
    "PRT", "QAT", "KOR", "MDA", "ROU", "RUS", "RWA", "WSM", "SMR", "STP", "SAU", "SEN", "SRB", "SYC", "SLE", "SGP", "SVK", "SVN", "SOM", "ZAF", "SDS", "ESP", "LKA",
    "SUD", "SUR", "SWZ", "SWE", "CHE", "SYR", "TWN", "TJK", "THA", "MKD", "TGO", "TTO", "TUN", "TUR", "TKM", "USR", "UGA", "UKR", "ARE", "GBR", "TZA", "USA", "URY",
    "UZB", "VUT", "VEN", "VNM", "YEM", "ZMB", "ZWE")

sample_country_lst <- c("DZA", "AGO", "ARG", "AUS", "AUT", "AZE", "BHR", "BGD", "BLR", "BEL", "BOL", "BIH", "BRA", "BGR", "KHM", "CAN", "CHL", "CHN", "COL", "COG", "CRI",
                 "HRV", "CUB", "CYP", "CZE", "DNK", "DOM", "ECU", "EGY", "SLV", "EST", "ETH", "FIN", "FRA", "GEO", "DEU", "GHA", "GRC", "GTM", "HND", "HKG", "HUN",
                 "ISL", "IND", "IDN", "IRN", "IRQ", "IRL", "ISR", "ITA", "JPN", "KAZ", "KEN", "KWT", "LVA", "LBN", "LTU", "LUX", "MYS", "MLT", "MEX", "MAR", "NLD",
                 "NZL", "NOR", "OMN", "PAK", "PRY", "PER", "PHL", "POL", "PRT", "QAT", "KOR", "ROU", "RUS", "SAU", "SRB", "SGP", "SVK", "SVN", "ZAF", "ESP", "LKA",
                 "SWE", "CHE", "THA", "TUN", "TUR", "UKR", "ARE", "GBR", "TZA", "USA", "URY", "UZB", "VEN", "VNM")


# Reconstruction of WDI Dataset
df <- read_csv("WorldDevelopmentIndicator/WDICSV.csv")

df_melted <- df %>%
  filter(`Country Code` %in% country_lst & `Indicator Code` %in% code_lst) %>%  # Filtering Countries and Indicators
  pivot_longer(cols = 5:ncol(df), names_to = "Year", values_to = "WDI") %>%  # Unpivoting by Year
  filter(Year >= 1990 & Year <= 2022) %>%  # Filtering Years
  select(-c(`Indicator Name`, `Country Name`)) %>%  # Delete Name Columns
  pivot_wider(names_from = `Indicator Code`, values_from = WDI) %>%  # Pivoting by Indicator Code
  rename(Country = `Country Code`) %>%  # Rename "Country Code" to "Country"
  mutate(Sample = as.numeric(Country %in% sample_country_lst)) %>%  # Create Sample Countries Dummy Variable
  select(Country, Year, Sample, everything())  # Reorder Columns

# Mapping Column names from Code to Variable
colnames(df_melted)[4:length(df_melted)] <- col_mapping[colnames(df_melted)[4:length(df_melted)]]

# Save as a csv file
write_csv(df_melted, "WDI/WDI_processed.csv")

