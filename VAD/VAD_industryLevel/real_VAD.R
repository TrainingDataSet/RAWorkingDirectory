# [Last Modified: 2025-04-08]
# 2024-08-02: Apply HP filter to VAD
# 2025-04-08: Use real VAD, not nominal VAD

library(dplyr)
library(readr)
library(microbenchmark)

setwd("~/RAWorkingDirectory")

cal_real_vad <- function(df, country, sector) {
  vad <- df[(df$Country_i == country) &
            (df$Industry_s %in% sector[[1]]), ]
  
  vad1 <- vad %>%
    group_by(Year, Country_i) %>%
    summarise(VA_con = sum(VA_con), .groups = "drop")
  
  return (vad1$VA_con)
}


#---------------- Test Code ----------------#

# # Industry list (goods and services)
# goods_lst <- list("goods" = paste0("c", 1:12))
# services_lst <- list("services" = paste0("c", 13:25))
# industry_lst <- list("goods" = goods_lst, "services" = services_lst)
# 
# df <- read_csv("Export_Import_Sales_VA2/Export_Import_Sales_VA_EntireCountry (2).csv")
# country <- "USA"
# sector <- services_lst
# 
# print(cal_real_vad(df, country, sector))
# 
# # Running Time of Function
# microbenchmark(
#   test = cal_real_vad(df, country, sector),
#   times = 1000
# ) # --> 0.01 sec


#------------------------- Run Entire -------------------------# 

df <- read_csv("Export_Import_Sales_VA2/Export_Import_Sales_VA_EntireCountry (2).csv")

# Country list (Entire country)
# country_lst <- c("AFG", "ALB", "DZA", "AND", "AGO", "ATG", "ARG", "ARM", "ABW", "AUS", "AUT", "AZE", "BHS", "BHR", "BGD", "BRB", "BLR", "BEL", "BLZ", "BEN", "BMU",
# "BTN", "BOL", "BIH", "BWA", "BRA", "VGB", "BRN", "BGR", "BFA", "BDI", "KHM", "CMR", "CAN", "CPV", "CYM", "CAF", "TCD", "CHL", "CHN", "COL", "COG", "CRI", "HRV",
# "CUB", "CYP", "CZE", "CIV", "PRK", "COD", "DNK", "DJI", "DOM", "ECU", "EGY", "SLV", "ERI", "EST", "ETH", "FJI", "FIN", "FRA", "PYF", "GAB", "GMB", "GEO", "DEU",
# "GHA", "GRC", "GRL", "GTM", "GIN", "GUY", "HTI", "HND", "HKG", "HUN", "ISL", "IND", "IDN", "IRN", "IRQ", "IRL", "ISR", "ITA", "JAM", "JPN", "JOR", "KAZ", "KEN",
# "KWT", "KGZ", "LAO", "LVA", "LBN", "LSO", "LBR", "LBY", "LIE", "LTU", "LUX", "MAC", "MDG", "MWI", "MYS", "MDV", "MLI", "MLT", "MRT", "MUS", "MEX", "MCO", "MNG",
# "MNE", "MAR", "MOZ", "MMR", "NAM", "NPL", "NLD", "ANT", "NCL", "NZL", "NIC", "NER", "NGA", "NOR", "PSE", "OMN", "PAK", "PAN", "PNG", "PRY", "PER", "PHL", "POL",
# "PRT", "QAT", "KOR", "MDA", "ROU", "RUS", "RWA", "WSM", "SMR", "STP", "SAU", "SEN", "SRB", "SYC", "SLE", "SGP", "SVK", "SVN", "SOM", "ZAF", "SDS", "ESP", "LKA",
# "SUD", "SUR", "SWZ", "SWE", "CHE", "SYR", "TWN", "TJK", "THA", "MKD", "TGO", "TTO", "TUN", "TUR", "TKM", "USR", "UGA", "UKR", "ARE", "GBR", "TZA", "USA", "URY",
# "UZB", "VUT", "VEN", "VNM", "YEM", "ZMB", "ZWE")
country_lst <- c("DZA", "AGO", "ARG", "AUS", "AUT", "AZE", "BHR", "BGD", "BLR", "BEL", "BOL", "BIH", "BRA", "BGR", "KHM", "CAN", "CHL", "CHN", "COL", "COG", "CRI",
                 "HRV", "CUB", "CYP", "CZE", "DNK", "DOM", "ECU", "EGY", "SLV", "EST", "ETH", "FIN", "FRA", "GEO", "DEU", "GHA", "GRC", "GTM", "HND", "HKG", "HUN",
                 "ISL", "IND", "IDN", "IRN", "IRQ", "IRL", "ISR", "ITA", "JPN", "KAZ", "KEN", "KWT", "LVA", "LBN", "LTU", "LUX", "MYS", "MLT", "MEX", "MAR", "NLD",
                 "NZL", "NOR", "OMN", "PAK", "PRY", "PER", "PHL", "POL", "PRT", "QAT", "KOR", "ROU", "RUS", "SAU", "SRB", "SGP", "SVK", "SVN", "ZAF", "ESP", "LKA",
                 "SWE", "CHE", "THA", "TUN", "TUR", "UKR", "ARE", "GBR", "TZA", "USA", "URY", "UZB", "VEN", "VNM")

# Sector list (goods and services)
goods_lst <- list("goods" = paste0("c", 1:12))
services_lst <- list("services" = paste0("c", 13:25))
sector_lst <- list("goods" = goods_lst, "services" = services_lst)

lst_length <- numeric(length(country_lst) * length(sector_lst) * 33)

VAD_lst <- list("Year" = lst_length,
                "Country" = lst_length,
                "Sector" = lst_length,
                "VAD" = lst_length
)

index <- 1
for (country in country_lst) {
  for (sector in sector_lst) {
    vad_filtered <- cal_real_vad(df, country, sector)
    VAD_lst$Year[index:(index+32)] <- c(1990:2022)
    VAD_lst$Country[index:(index+32)] <- rep(country, 33) 
    VAD_lst$Sector[index:(index+32)] <- rep(names(sector), 33)
    VAD_lst$VAD[index:(index+32)] <- vad_filtered
    
    index <- index + 33
  }
}

VAD_df <- as.data.frame(VAD_lst)
write_csv(VAD_df, "VAD/real_VAD.csv")

