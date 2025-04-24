# [Last Modified: 2025-01-20]
# 2024-11-27: Model-based Forward/Backward linkage of s to s_tilde
# 2025-01-20: Use Exports_importance_intermediate instead of Exports_importance

library(dplyr)
library(readr)
library(parallel)
library(microbenchmark)

setwd("~/RAWorkingDirectory")

# ------------------------------
## (1) Forward/Backward MB(s,k,s_tilde)

# Read file
Linkages <- read_csv("Linkages/Linkages_to_stilde/Linkages_to_stilde.csv")

# Create linkages of S_to_k & linkages of k_to_S_tilde
S_to_k <- Linkages %>%
  select(-Backward) %>%
  rename(k = S_tilde)

k_to_S_tilde <- Linkages %>%
  select(-Backward) %>%
  rename(k = S)

# Merge S_to_k and k_to_S_tilde by third-party k
Forward_MB <- inner_join(S_to_k, k_to_S_tilde, by = c("Year", "Country", "k"), 
                          relationship = "many-to-many") %>%
  select(Year, Country, S, k, S_tilde, Forward.x, Forward.y) %>%
  filter(S != S_tilde) # S exclude S_tilde

# Convert S and S_tilde and merge by third party k to create backward MB
Linkages_MB <- inner_join(Forward_MB, Forward_MB, by = c("Year", "Country", "k", "S" = "S_tilde", "S_tilde" = "S")) %>%
  rename(S_to_k = Forward.x.x, k_to_S_tilde = Forward.y.x, S_tilde_to_k = Forward.x.y, k_to_S = Forward.y.y)

# Calcuate Forward/Backward linkages_MB
Linkages_MB$Forward_MB <- Linkages_MB$S_to_k * Linkages_MB$k_to_S_tilde
Linkages_MB$Backward_MB <- Linkages_MB$S_tilde_to_k * Linkages_MB$k_to_S

# Write in parquet format
write_parquet(Linkages_MB, "MB_linkages/MB_linkages_S_k_Stilde.parquet")


# ------------------------------
## (2) Forward/Backward MB(s,s_tilde)

# Read file
Linkages_MB_S_k_Stilde <- read_parquet("MB_linkages/MB_linkages_S_k_Stilde.parquet")

# Summed over k
Linkages_MB_S_Stilde <- Linkages_MB_S_k_Stilde %>%
  group_by(Year, Country, S, S_tilde) %>%
  summarize(Forward_MB = sum(Forward_MB), Backward_MB = sum(Backward_MB), .groups = "drop")

# Sort column order
Linkages_MB_S_Stilde$Year <- factor(Linkages_MB_S_Stilde$Year, levels = c(1990:2022))
Linkages_MB_S_Stilde$Country <- factor(Linkages_MB_S_Stilde$Country, 
                          level = c("DZA", "AGO", "ARG", "AUS", "AUT", "AZE", "BHR", "BGD", "BLR", "BEL", "BOL", "BIH", "BRA", "BGR", "KHM", "CAN", "CHL", "CHN", "COL", "COG", "CRI",
                                    "HRV", "CUB", "CYP", "CZE", "DNK", "DOM", "ECU", "EGY", "SLV", "EST", "ETH", "FIN", "FRA", "GEO", "DEU", "GHA", "GRC", "GTM", "HND", "HKG", "HUN",
                                    "ISL", "IND", "IDN", "IRN", "IRQ", "IRL", "ISR", "ITA", "JPN", "KAZ", "KEN", "KWT", "LVA", "LBN", "LTU", "LUX", "MYS", "MLT", "MEX", "MAR", "NLD",
                                    "NZL", "NOR", "OMN", "PAK", "PRY", "PER", "PHL", "POL", "PRT", "QAT", "KOR", "ROU", "RUS", "SAU", "SRB", "SGP", "SVK", "SVN", "ZAF", "ESP", "LKA",
                                    "SWE", "CHE", "THA", "TUN", "TUR", "UKR", "ARE", "GBR", "TZA", "USA", "URY", "UZB", "VEN", "VNM"))
Linkages_MB_S_Stilde$S <- factor(Linkages_MB_S_Stilde$S, level = c(paste0("c", 1:26)))
Linkages_MB_S_Stilde$S_tilde <- factor(Linkages_MB_S_Stilde$S_tilde, level = c(paste0("c", 1:26)))

Linkages_MB_S_Stilde <- Linkages_MB_S_Stilde %>%
  arrange(Year, Country, S, S_tilde)

# Write in parquet format
write_parquet(Linkages_MB_S_Stilde, "MB_linkages/MB_linkages_S_Stilde.parquet")


# ------------------------------
## (3) Forward/Backward MB(i,s,s_tilde,j)

# Read files
Linkages_MB_S_Stilde <- read_parquet("MB_linkages/MB_linkages_S_Stilde.parquet")
tradeness <- read_csv("Tradeness/Tradeness.csv")

# Rename columns
Linkages_MB_S_Stilde <- Linkages_MB_S_Stilde %>% rename(Country_i = Country)
tradeness <- tradeness %>% rename(S_tilde = Industry_s)

# Convert index to factor in tradeness
tradeness$Year <- factor(tradeness$Year, levels = c(1990:2022))
tradeness$Country_i <- factor(tradeness$Country_i, 
                                 level = c("DZA", "AGO", "ARG", "AUS", "AUT", "AZE", "BHR", "BGD", "BLR", "BEL", "BOL", "BIH", "BRA", "BGR", "KHM", "CAN", "CHL", "CHN", "COL", "COG", "CRI",
                                           "HRV", "CUB", "CYP", "CZE", "DNK", "DOM", "ECU", "EGY", "SLV", "EST", "ETH", "FIN", "FRA", "GEO", "DEU", "GHA", "GRC", "GTM", "HND", "HKG", "HUN",
                                           "ISL", "IND", "IDN", "IRN", "IRQ", "IRL", "ISR", "ITA", "JPN", "KAZ", "KEN", "KWT", "LVA", "LBN", "LTU", "LUX", "MYS", "MLT", "MEX", "MAR", "NLD",
                                           "NZL", "NOR", "OMN", "PAK", "PRY", "PER", "PHL", "POL", "PRT", "QAT", "KOR", "ROU", "RUS", "SAU", "SRB", "SGP", "SVK", "SVN", "ZAF", "ESP", "LKA",
                                           "SWE", "CHE", "THA", "TUN", "TUR", "UKR", "ARE", "GBR", "TZA", "USA", "URY", "UZB", "VEN", "VNM"))
tradeness$Country_j <- factor(tradeness$Country_j, 
                                 level = c("DZA", "AGO", "ARG", "AUS", "AUT", "AZE", "BHR", "BGD", "BLR", "BEL", "BOL", "BIH", "BRA", "BGR", "KHM", "CAN", "CHL", "CHN", "COL", "COG", "CRI",
                                           "HRV", "CUB", "CYP", "CZE", "DNK", "DOM", "ECU", "EGY", "SLV", "EST", "ETH", "FIN", "FRA", "GEO", "DEU", "GHA", "GRC", "GTM", "HND", "HKG", "HUN",
                                           "ISL", "IND", "IDN", "IRN", "IRQ", "IRL", "ISR", "ITA", "JPN", "KAZ", "KEN", "KWT", "LVA", "LBN", "LTU", "LUX", "MYS", "MLT", "MEX", "MAR", "NLD",
                                           "NZL", "NOR", "OMN", "PAK", "PRY", "PER", "PHL", "POL", "PRT", "QAT", "KOR", "ROU", "RUS", "SAU", "SRB", "SGP", "SVK", "SVN", "ZAF", "ESP", "LKA",
                                           "SWE", "CHE", "THA", "TUN", "TUR", "UKR", "ARE", "GBR", "TZA", "USA", "URY", "UZB", "VEN", "VNM"))
tradeness$S_tilde <- factor(tradeness$S_tilde, level = c(paste0("c", 1:26)))

# Merge by c("Year", "Country_i", "S_tilde")
Linkages_MB_i_S_Stilde_j <- inner_join(Linkages_MB_S_Stilde, tradeness, by=c("Year", "Country_i", "S_tilde"), relationship="many-to-many")

Linkages_MB_i_S_Stilde_j <- Linkages_MB_i_S_Stilde_j %>%
  select(Year, Country_i, S, S_tilde, Country_j, everything())

# Calculate Forward/Backward linkage of s to traded sector
Linkages_MB_i_S_Stilde_j$MB_forward_to_trade <- Linkages_MB_i_S_Stilde_j$Forward_MB * Linkages_MB_i_S_Stilde_j$Exports_importance
Linkages_MB_i_S_Stilde_j$MB_backward_to_trade <- Linkages_MB_i_S_Stilde_j$Backward_MB * Linkages_MB_i_S_Stilde_j$Imports_importance
Linkages_MB_i_S_Stilde_j$MB_forward_to_trade_intermediate <- Linkages_MB_i_S_Stilde_j$Forward_MB * Linkages_MB_i_S_Stilde_j$Exports_importance_intermediate

# Write in parquet format
write_parquet(Linkages_MB_i_S_Stilde_j, "MB_linkages/MB_linkages_i_S_Stilde_j2.parquet")


# ------------------------------
## (4) Forward/Backward MB(i,s,j)

# Read files
Linkages_MB_i_S_Stilde_j <- read_parquet("MB_linkages/MB_linkages_i_S_Stilde_j2.parquet")
weights <- read_csv("Gross_sales_weight/Gross_sales_weight.csv")

# Summed over S_tilde
Linkages_MB_i_S_j <- Linkages_MB_i_S_Stilde_j %>%
  group_by(Year, Country_i, S, Country_j) %>%
  summarize(Forward_MB = sum(MB_forward_to_trade), Backward_MB = sum(MB_backward_to_trade),
            Forward_MB_intermediate = sum(MB_forward_to_trade_intermediate), .groups="drop")

# Convert index to factor in weights
weights$Year <- factor(weights$Year, levels = c(1990:2022))
weights$Country <- factor(weights$Country,
                          level = c("DZA", "AGO", "ARG", "AUS", "AUT", "AZE", "BHR", "BGD", "BLR", "BEL", "BOL", "BIH", "BRA", "BGR", "KHM", "CAN", "CHL", "CHN", "COL", "COG", "CRI",
                                    "HRV", "CUB", "CYP", "CZE", "DNK", "DOM", "ECU", "EGY", "SLV", "EST", "ETH", "FIN", "FRA", "GEO", "DEU", "GHA", "GRC", "GTM", "HND", "HKG", "HUN",
                                    "ISL", "IND", "IDN", "IRN", "IRQ", "IRL", "ISR", "ITA", "JPN", "KAZ", "KEN", "KWT", "LVA", "LBN", "LTU", "LUX", "MYS", "MLT", "MEX", "MAR", "NLD",
                                    "NZL", "NOR", "OMN", "PAK", "PRY", "PER", "PHL", "POL", "PRT", "QAT", "KOR", "ROU", "RUS", "SAU", "SRB", "SGP", "SVK", "SVN", "ZAF", "ESP", "LKA",
                                    "SWE", "CHE", "THA", "TUN", "TUR", "UKR", "ARE", "GBR", "TZA", "USA", "URY", "UZB", "VEN", "VNM"))
weights$Industry <- factor(weights$Industry, level = c(paste0("c", 1:26)))


# Merge by c("Year", "Country_i", "S")
Linkages_MB_i_S_j <- inner_join(Linkages_MB_i_S_j, weights, by=c("Year", "Country_i" = "Country", "S" = "Industry"), relationship="many-to-one")

# Multiply the weights
Linkages_MB_i_S_j$Forward_MB_weighted <- Linkages_MB_i_S_j$Forward_MB * Linkages_MB_i_S_j$gross_sales_weight
Linkages_MB_i_S_j$Backward_MB_weighted <- Linkages_MB_i_S_j$Backward_MB * Linkages_MB_i_S_j$gross_sales_weight
Linkages_MB_i_S_j$Forward_MB_intermediate_weighted <- Linkages_MB_i_S_j$Forward_MB_intermediate * Linkages_MB_i_S_j$gross_sales_weight

# Write in parquet format
write_parquet(Linkages_MB_i_S_j, "MB_linkages/MB_linkages_i_S_j2.parquet")


# ------------------------------
## (5) Forward/Backward MB(i,j)

# Read file
Linkages_MB_i_S_j <- read_parquet("MB_linkages/MB_linkages_i_S_j2.parquet")

# Summed over S
Linkages_MB_i_j <- Linkages_MB_i_S_j %>%
  group_by(Year, Country_i, Country_j) %>%
  summarize(Forward_MB = sum(Forward_MB_weighted), Backward_MB = sum(Backward_MB_weighted),
            Forward_MB_intermediate = sum(Forward_MB_intermediate_weighted), .groups = "drop")

# Write in parquet format
write_parquet(Linkages_MB_i_j, "MB_linkages/MB_linkages_i_j2.parquet")


