# [Last Modified: 2024-08-02]
# 2024-08-02: Apply HP filter to VAD

library(dplyr)
library(readr)
library(hpfilter)
library(microbenchmark)

setwd("~/RAWorkingDirectory")

hp_filtered <- function(df, country, sector) {
  vad <- df[(df$Country == country) &
            (df$Sector == sector), "VAD"]
  
  vad_ts <- ts(vad, start = 1990, frequency = 1)
  vad_hp <- hp1(vad_ts, lambda = 100)
  
  return (vad_hp)
}


#---------------- Test Code ----------------#

# df <- read_csv("VAD/Eora_VAD.csv")
# country <- "KOR"
# sector <- "goods"
# 
# print(hp_filtered(df, country, sector))
# 
# # Running Time of Function
# microbenchmark(
#   test = hp_filtered(df, country, sector),
#   times = 100
# ) # --> 0.003 sec


#------------------------- Run Entire -------------------------# 

df <- read_csv("VAD/Eora_VAD.csv")

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
sector_lst <- c("goods", "services")

lst_length <- numeric(length(country_lst) * length(sector_lst) * 33)

VAD_lst <- list("Year" = lst_length,
                "Country" = lst_length,
                "Sector" = lst_length,
                "VAD" = lst_length
                )

index <- 1
for (country in country_lst) {
  for (sector in sector_lst) {
    vad_filtered <- hp_filtered(df, country, sector)
    VAD_lst$Year[index:(index+32)] <- c(1990:2022)
    VAD_lst$Country[index:(index+32)] <- rep(country, 33) 
    VAD_lst$Sector[index:(index+32)] <- rep(sector, 33)
    VAD_lst$VAD[index:(index+32)] <- as.vector(vad_filtered)[[1]]
    
    index <- index + 33
  }
}

VAD_df <- as.data.frame(VAD_lst)
write_csv(VAD_df, "VAD/VAD_HP_filtered.csv")

