# [Last Modified: 2025-01-05]

library(dplyr)
library(readr)

setwd("~/RAWorkingDirectory")

dataset <- read_csv("Dataset/data_eoracountry.csv")
dataset2 <- dataset[, c("t", "Country_i", "Country_j", "Forward_to_trade_i_mean",         
                        "Forward_to_trade_j_mean", "Backward_to_trade_i_mean", "Backward_to_trade_j_mean",     
                        "Forward_to_trade_MB_i_mean", "Forward_to_trade_MB_j_mean", "Backward_to_trade_MB_i_mean",     
                        "Backward_to_trade_MB_j_mean", "Forward_to_trade_i_first", "Forward_to_trade_j_first",        
                        "Backward_to_trade_i_first", "Backward_to_trade_j_first", "Forward_to_trade_MB_i_first",     
                        "Forward_to_trade_MB_j_first", "Backward_to_trade_MB_i_first", "Backward_to_trade_MB_j_first")]


dataset3 <- subset(dataset2, Country_i == "AGO" & Country_j == "ARG")
dataset4 <- dataset3[, c("Forward_to_trade_MB_i_mean", "Forward_to_trade_MB_i_first")]

dataset5 <- read_parquet("MB_linkages/MB_linkages_i_j.parquet")
dataset6 <- subset(dataset5, Country_i == "AGO" & Country_j == "ARG")

dataset7 <- read_parquet("Linkages/Linkages_to_tradedSector/Linkages_to_tradedSector_ij.parquet")
dataset8 <- subset(dataset7, Country_i == "AGO" & Country_j == "ARG")

dataset7 <- subset(dataset6, as.numeric(as.character(Year)) < 2000)
dataset8 <- subset(dataset6, as.numeric(as.character(Year)) >= 2000 & as.numeric(as.character(Year)) < 2010)
dataset9 <- subset(dataset6, as.numeric(as.character(Year)) >= 2010 & as.numeric(as.character(Year)) < 2020)

print(mean(dataset7$Forward_MB))  # 0.0865345
print(mean(dataset8$Forward_MB))  # 0.07490044
print(mean(dataset9$Forward_MB))  # 0.04424397
print(first(dataset7$Forward_MB))  # 0.05573895
print(first(dataset8$Forward_MB))  # 0.1090343
print(first(dataset9$Forward_MB))  # 0.05775072
# --> 1. Verification: It is the same as dataset4


dataset10 <- read_parquet("Linkages/Linkages_to_tradedSector/Linkages_to_tradedSector_iSj.parquet")
dataset11 <- subset(dataset10, Country_i == "AGO" & Country_j == "ARG")

# Summed over S
dataset12 <- dataset11 %>%
  group_by(Year, Country_i, Country_j) %>%
  summarize(Forward_to_trade = sum(Forward_weighted), Backward_to_trade = sum(Backward_weighted), .groups = "drop")
print(dataset12)
# --> 2. Verification: It is the same as dataset6


dataset13 <- dataset11$Forward_to_trade * dataset11$gross_sales_weight
dataset14 <- dataset11$Backward_to_trade * dataset11$gross_sales_weight
print(dataset13)
print(dataset14)
# --> 3. Verification: It is the same as dataset11


dataset15 <- read_parquet("Linkages/Linkages_to_tradedSector/Linkages_to_tradedSector.parquet")
dataset16 <- subset(dataset15, Country_i == "AGO" & Country_j == "ARG")

# Summed over S_tilde
dataset17 <- dataset16 %>%
  group_by(Year, Country_i, S, Country_j) %>%
  summarize(Forward_to_trade = sum(Forward_to_trade), Backward_to_trade = sum(Backward_to_trade), .groups = "drop")
# --> 4. Verification: It is the same as dataset11


dataset18 <- as.data.table(read_csv("Linkages/Linkages_to_stilde/Linkages_to_stilde.csv"))
dataset19 <- as.data.table(read_csv("Tradeness/Tradeness.csv"))
dataset20 <- subset(dataset18, Country == "AGO")
dataset21 <- subset(dataset19, Country_i == "AGO" & Country_j == "ARG")

dataset22 <- dataset20 %>% rename(Country_i = Country)
dataset23 <- dataset21 %>% rename(S_tilde = Industry_s)
dataset24 <- inner_join(dataset22, dataset23, by=c("Year", "Country_i", "S_tilde"), relationship="many-to-many")

dataset24$Forward_to_trade <- dataset24$Forward * dataset24$Exports_importance
dataset24$Backward_to_trade <- dataset24$Backward * dataset24$Imports_importance

dataset24$Year <- factor(dataset24$Year, levels = c(1990:2022))
dataset24$Country_i <- factor(dataset24$Country_i, 
                              level = c("DZA", "AGO", "ARG", "AUS", "AUT", "AZE", "BHR", "BGD", "BLR", "BEL", "BOL", "BIH", "BRA", "BGR", "KHM", "CAN", "CHL", "CHN", "COL", "COG", "CRI",
                                        "HRV", "CUB", "CYP", "CZE", "DNK", "DOM", "ECU", "EGY", "SLV", "EST", "ETH", "FIN", "FRA", "GEO", "DEU", "GHA", "GRC", "GTM", "HND", "HKG", "HUN",
                                        "ISL", "IND", "IDN", "IRN", "IRQ", "IRL", "ISR", "ITA", "JPN", "KAZ", "KEN", "KWT", "LVA", "LBN", "LTU", "LUX", "MYS", "MLT", "MEX", "MAR", "NLD",
                                        "NZL", "NOR", "OMN", "PAK", "PRY", "PER", "PHL", "POL", "PRT", "QAT", "KOR", "ROU", "RUS", "SAU", "SRB", "SGP", "SVK", "SVN", "ZAF", "ESP", "LKA",
                                        "SWE", "CHE", "THA", "TUN", "TUR", "UKR", "ARE", "GBR", "TZA", "USA", "URY", "UZB", "VEN", "VNM"))
dataset24$S <- factor(dataset24$S, level = c(paste0("c", 1:26)))
dataset24$S_tilde <- factor(dataset24$S_tilde, level = c(paste0("c", 1:26)))
dataset24$Country_j <- factor(dataset24$Country_j, 
                              level = c("DZA", "AGO", "ARG", "AUS", "AUT", "AZE", "BHR", "BGD", "BLR", "BEL", "BOL", "BIH", "BRA", "BGR", "KHM", "CAN", "CHL", "CHN", "COL", "COG", "CRI",
                                        "HRV", "CUB", "CYP", "CZE", "DNK", "DOM", "ECU", "EGY", "SLV", "EST", "ETH", "FIN", "FRA", "GEO", "DEU", "GHA", "GRC", "GTM", "HND", "HKG", "HUN",
                                        "ISL", "IND", "IDN", "IRN", "IRQ", "IRL", "ISR", "ITA", "JPN", "KAZ", "KEN", "KWT", "LVA", "LBN", "LTU", "LUX", "MYS", "MLT", "MEX", "MAR", "NLD",
                                        "NZL", "NOR", "OMN", "PAK", "PRY", "PER", "PHL", "POL", "PRT", "QAT", "KOR", "ROU", "RUS", "SAU", "SRB", "SGP", "SVK", "SVN", "ZAF", "ESP", "LKA",
                                        "SWE", "CHE", "THA", "TUN", "TUR", "UKR", "ARE", "GBR", "TZA", "USA", "URY", "UZB", "VEN", "VNM"))

dataset24 <- dataset24 %>%
  arrange(Year, Country_i, S, S_tilde, Country_j) %>%
  select(Year, Country_i, S, S_tilde, Country_j, everything())
# --> 5. Verification: It is the same as dataset16

