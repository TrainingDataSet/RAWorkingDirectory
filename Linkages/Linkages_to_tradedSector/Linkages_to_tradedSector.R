# [Last Modified: 2025-01-20]
# 2024-11-02: Forward/Backward linkage of s to traded sector
# 2025-01-20: Use Exports_importance_intermediate instead of Exports_importance

library(dplyr)
library(readr)
library(parallel)
library(microbenchmark)
library(data.table)
library(arrow)

setwd("~/RAWorkingDirectory")

# ------------------------------
## (1) Linkages(Forward, Backward) * Tradeness(Exports_importance, Imports_importance)

# Read csv
linkages <- as.data.table(read_csv("Linkages/Linkages_to_stilde/Linkages_to_stilde.csv"))
tradeness <- as.data.table(read_csv("Tradeness/Tradeness.csv"))

# Rename columns
linkages <- linkages %>% rename(Country_i = Country)
tradeness <- tradeness %>% rename(S_tilde = Industry_s)

# Merge by c("Year", "Country_i", "S_tilde")
cartesian_df <- inner_join(linkages, tradeness, by=c("Year", "Country_i", "S_tilde"), relationship="many-to-many")

# Calculate Forward/Backward linkage of s to traded sector
cartesian_df$Forward_to_trade <- cartesian_df$Forward * cartesian_df$Exports_importance
cartesian_df$Forward_to_trade_intermediate <- cartesian_df$Forward * cartesian_df$Exports_importance_intermediate
cartesian_df$Backward_to_trade <- cartesian_df$Backward * cartesian_df$Imports_importance

# Sort column order
cartesian_df$Year <- factor(cartesian_df$Year, levels = c(1990:2022))
cartesian_df$Country_i <- factor(cartesian_df$Country_i, 
                          level = c("DZA", "AGO", "ARG", "AUS", "AUT", "AZE", "BHR", "BGD", "BLR", "BEL", "BOL", "BIH", "BRA", "BGR", "KHM", "CAN", "CHL", "CHN", "COL", "COG", "CRI",
                                    "HRV", "CUB", "CYP", "CZE", "DNK", "DOM", "ECU", "EGY", "SLV", "EST", "ETH", "FIN", "FRA", "GEO", "DEU", "GHA", "GRC", "GTM", "HND", "HKG", "HUN",
                                    "ISL", "IND", "IDN", "IRN", "IRQ", "IRL", "ISR", "ITA", "JPN", "KAZ", "KEN", "KWT", "LVA", "LBN", "LTU", "LUX", "MYS", "MLT", "MEX", "MAR", "NLD",
                                    "NZL", "NOR", "OMN", "PAK", "PRY", "PER", "PHL", "POL", "PRT", "QAT", "KOR", "ROU", "RUS", "SAU", "SRB", "SGP", "SVK", "SVN", "ZAF", "ESP", "LKA",
                                    "SWE", "CHE", "THA", "TUN", "TUR", "UKR", "ARE", "GBR", "TZA", "USA", "URY", "UZB", "VEN", "VNM"))
cartesian_df$S <- factor(cartesian_df$S, level = c(paste0("c", 1:26)))
cartesian_df$S_tilde <- factor(cartesian_df$S_tilde, level = c(paste0("c", 1:26)))
cartesian_df$Country_j <- factor(cartesian_df$Country_j, 
                          level = c("DZA", "AGO", "ARG", "AUS", "AUT", "AZE", "BHR", "BGD", "BLR", "BEL", "BOL", "BIH", "BRA", "BGR", "KHM", "CAN", "CHL", "CHN", "COL", "COG", "CRI",
                                    "HRV", "CUB", "CYP", "CZE", "DNK", "DOM", "ECU", "EGY", "SLV", "EST", "ETH", "FIN", "FRA", "GEO", "DEU", "GHA", "GRC", "GTM", "HND", "HKG", "HUN",
                                    "ISL", "IND", "IDN", "IRN", "IRQ", "IRL", "ISR", "ITA", "JPN", "KAZ", "KEN", "KWT", "LVA", "LBN", "LTU", "LUX", "MYS", "MLT", "MEX", "MAR", "NLD",
                                    "NZL", "NOR", "OMN", "PAK", "PRY", "PER", "PHL", "POL", "PRT", "QAT", "KOR", "ROU", "RUS", "SAU", "SRB", "SGP", "SVK", "SVN", "ZAF", "ESP", "LKA",
                                    "SWE", "CHE", "THA", "TUN", "TUR", "UKR", "ARE", "GBR", "TZA", "USA", "URY", "UZB", "VEN", "VNM"))

cartesian_df <- cartesian_df %>%
  arrange(Year, Country_i, S, S_tilde, Country_j) %>%
  select(Year, Country_i, S, S_tilde, Country_j, everything())

# Write in parquet format
write_parquet(cartesian_df, "Linkages/Linkages_to_tradedSector/Linkages_to_tradedSector2.parquet")


# ------------------------------
## (2) Summed over s_tilde

# Read in parquet format
i_S_S_tilde_j <- read_parquet("Linkages/Linkages_to_tradedSector/Linkages_to_tradedSector2.parquet")

# Summed over S_tilde
i_S_j <- i_S_S_tilde_j %>%
  group_by(Year, Country_i, S, Country_j) %>%
  summarize(Forward_to_trade = sum(Forward_to_trade), Backward_to_trade = sum(Backward_to_trade), 
            Forward_to_trade_intermediate = sum(Forward_to_trade_intermediate))

# Sort column order
i_S_j$Year <- factor(i_S_j$Year, levels = c(1990:2022))
i_S_j$Country_i <- factor(i_S_j$Country_i, 
                          level = c("DZA", "AGO", "ARG", "AUS", "AUT", "AZE", "BHR", "BGD", "BLR", "BEL", "BOL", "BIH", "BRA", "BGR", "KHM", "CAN", "CHL", "CHN", "COL", "COG", "CRI",
                                    "HRV", "CUB", "CYP", "CZE", "DNK", "DOM", "ECU", "EGY", "SLV", "EST", "ETH", "FIN", "FRA", "GEO", "DEU", "GHA", "GRC", "GTM", "HND", "HKG", "HUN",
                                    "ISL", "IND", "IDN", "IRN", "IRQ", "IRL", "ISR", "ITA", "JPN", "KAZ", "KEN", "KWT", "LVA", "LBN", "LTU", "LUX", "MYS", "MLT", "MEX", "MAR", "NLD",
                                    "NZL", "NOR", "OMN", "PAK", "PRY", "PER", "PHL", "POL", "PRT", "QAT", "KOR", "ROU", "RUS", "SAU", "SRB", "SGP", "SVK", "SVN", "ZAF", "ESP", "LKA",
                                    "SWE", "CHE", "THA", "TUN", "TUR", "UKR", "ARE", "GBR", "TZA", "USA", "URY", "UZB", "VEN", "VNM"))
i_S_j$S <- factor(i_S_j$S, level = c(paste0("c", 1:26)))
i_S_j$Country_j <- factor(i_S_j$Country_j, 
                          level = c("DZA", "AGO", "ARG", "AUS", "AUT", "AZE", "BHR", "BGD", "BLR", "BEL", "BOL", "BIH", "BRA", "BGR", "KHM", "CAN", "CHL", "CHN", "COL", "COG", "CRI",
                                    "HRV", "CUB", "CYP", "CZE", "DNK", "DOM", "ECU", "EGY", "SLV", "EST", "ETH", "FIN", "FRA", "GEO", "DEU", "GHA", "GRC", "GTM", "HND", "HKG", "HUN",
                                    "ISL", "IND", "IDN", "IRN", "IRQ", "IRL", "ISR", "ITA", "JPN", "KAZ", "KEN", "KWT", "LVA", "LBN", "LTU", "LUX", "MYS", "MLT", "MEX", "MAR", "NLD",
                                    "NZL", "NOR", "OMN", "PAK", "PRY", "PER", "PHL", "POL", "PRT", "QAT", "KOR", "ROU", "RUS", "SAU", "SRB", "SGP", "SVK", "SVN", "ZAF", "ESP", "LKA",
                                    "SWE", "CHE", "THA", "TUN", "TUR", "UKR", "ARE", "GBR", "TZA", "USA", "URY", "UZB", "VEN", "VNM"))

i_S_j <- i_S_j %>%
  arrange(Year, Country_i, S, Country_j) %>%
  select(Year, Country_i, S, Country_j, everything())

# Write in parquet format
write_parquet(i_S_j, "Linkages/Linkages_to_tradedSector/Linkages_to_tradedSector_iSj2.parquet")


# ------------------------------
## (3) Weighted mean for non-traded sections 

# Read parquet
i_S_j <- read_parquet("Linkages/Linkages_to_tradedSector/Linkages_to_tradedSector_iSj2.parquet")
weights <- read_csv("Gross_sales_weight/Gross_sales_weight.csv")

# Convert to factor type
weights$Year <- factor(weights$Year, levels = c(1990:2022))
weights$Country <- factor(weights$Country,
                          level = c("DZA", "AGO", "ARG", "AUS", "AUT", "AZE", "BHR", "BGD", "BLR", "BEL", "BOL", "BIH", "BRA", "BGR", "KHM", "CAN", "CHL", "CHN", "COL", "COG", "CRI",
                                    "HRV", "CUB", "CYP", "CZE", "DNK", "DOM", "ECU", "EGY", "SLV", "EST", "ETH", "FIN", "FRA", "GEO", "DEU", "GHA", "GRC", "GTM", "HND", "HKG", "HUN",
                                    "ISL", "IND", "IDN", "IRN", "IRQ", "IRL", "ISR", "ITA", "JPN", "KAZ", "KEN", "KWT", "LVA", "LBN", "LTU", "LUX", "MYS", "MLT", "MEX", "MAR", "NLD",
                                    "NZL", "NOR", "OMN", "PAK", "PRY", "PER", "PHL", "POL", "PRT", "QAT", "KOR", "ROU", "RUS", "SAU", "SRB", "SGP", "SVK", "SVN", "ZAF", "ESP", "LKA",
                                    "SWE", "CHE", "THA", "TUN", "TUR", "UKR", "ARE", "GBR", "TZA", "USA", "URY", "UZB", "VEN", "VNM"))
weights$Industry <- factor(weights$Industry, level = c(paste0("c", 1:26)))


# Merge by c("Year", "Country_i", "S")
i_S_j <- inner_join(i_S_j, weights, by=c("Year", "Country_i" = "Country", "S" = "Industry"), relationship="many-to-one")

# Multiply the weights
i_S_j$Forward_weighted <- i_S_j$Forward_to_trade * i_S_j$gross_sales_weight
i_S_j$Backward_weighted <- i_S_j$Backward_to_trade * i_S_j$gross_sales_weight
i_S_j$Forward_intermediate_weighted <- i_S_j$Forward_to_trade_intermediate * i_S_j$gross_sales_weight

# Write in parquet format
write_parquet(i_S_j, "Linkages/Linkages_to_tradedSector/Linkages_to_tradedSector_iSj2.parquet")

# Summed over S
i_j <- i_S_j %>%
  group_by(Year, Country_i, Country_j) %>%
  summarize(Forward_to_trade = sum(Forward_weighted), Backward_to_trade = sum(Backward_weighted),
            Forward_to_trade_intermediate = sum(Forward_intermediate_weighted), .groups = "drop")

# Write in parquet format
write_parquet(i_j, "Linkages/Linkages_to_tradedSector/Linkages_to_tradedSector_ij2.parquet")


