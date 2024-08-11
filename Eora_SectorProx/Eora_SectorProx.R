# [Last Modified: 2024-06-24]
# 2024-02-03: Excluding domestic consumption when calculating exports
# 2024-06-23: MRIO -> Eora, Considering sector(goods/services)

library(dplyr)
library(readr)

setwd("~/RAWorkingDirectory")

#---------------------------------
# Calculate exports in specific industry of country
#
# Args:
#   df: Eora_exports_EachIndustry.csv
#   year: Year
#   country_i: Code of country_i
#   country_j: Code of country_j
#   sector: Vector of industry (goods/services)
#
# Returns:
#   sector_prox1: Using exports considering neither pollutants nor sectors
#   sector_prox2: Using exports considering sectors but not pollutants
#   sector_prox3: Using exports considering CO2 but not sectors
#   sector_prox4: Using exports considering GHG but not sectors
#   sector_prox5: Using exports considering both CO2 and sectors
#   sector_prox6: Using exports considering both GHG and sectors
#---------------------------------

cal_sector_prox <- function(df, year, country_i, country_j, sector) {
  
  #----------------- Calculate all exports variables -----------------#
  
  exports_i <- df[(df$Year == year) & 
                  (df$Country == country_i), ]
  
  exports_j <- df[(df$Year == year) & 
                  (df$Country == country_j), ]
  
  exports_is <- exports_i[(exports_i$Industry %in% sector), ]
  
  exports_js <- exports_j[(exports_j$Industry %in% sector), ]
  
  # pollutants_i <- pollutants_df[(pollutants_df$Year == year) &
  #                        (pollutants_df$Country == country_i), ]
  # 
  # pollutants_j <- pollutants_df[(pollutants_df$Year == year) &
  #                        (pollutants_df$Country == country_j), ]
  # 
  # CO2_is <- pollutants_i[(pollutants_i$Industry %in% sector), "CO2_rate"]
  # 
  # CO2_js <- pollutants_j[(pollutants_j$Industry %in% sector), "CO2_rate"]
  # 
  # GHG_is <- pollutants_i[(pollutants_i$Industry %in% sector), "GHG_rate"]
  # 
  # GHG_js <- pollutants_j[(pollutants_j$Industry %in% sector), "GHG_rate"]
  # 
  # exports_i_CO2 <- exports_i$Exports * pollutants_i$CO2_rate
  # 
  # exports_i_GHG <- exports_i$Exports * pollutants_i$GHG_rate
  # 
  # exports_j_CO2 <- exports_j$Exports * pollutants_j$CO2_rate
  #  
  # exports_j_GHG <- exports_j$Exports * pollutants_j$GHG_rate
  
  
  #--------------------- Calculate sector_prox ---------------------#
  
  # Considering neither pollutants nor sectors
  sum_exports_i <- sum(exports_i$Exports)
  
  EX_i <- exports_i$Exports / sum_exports_i
  
  sum_exports_j <- sum(exports_j$Exports)
  
  EX_j <- exports_j$Exports / sum_exports_j
  
  sector_prox1 <- 1 - (sum(abs(EX_i - EX_j)) / 2)
  
  # Considering sectors but not pollutants
  sum_exports_is <- sum(exports_is$Exports)
  
  EX_is <- exports_is$Exports / sum_exports_is
  
  sum_exports_js <- sum(exports_js$Exports)
  
  EX_js <- exports_js$Exports / sum_exports_js
  
  sector_prox2 <- 1 - (sum(abs(EX_is - EX_js)) / 2)
  
  
  return (c(sector_prox1, sector_prox2))
  
}


#---------------- Test Code ----------------#

# df <- read_csv("Eora_exports_EachIndustry/Eora_exports_EachIndustry.csv")
# year <- 1990
# country_i <- "DZA"
# country_j <- "AGO"
# 
# # Industry list (goods and services)
# goods_lst <- paste0("c", 1:12)
# services_lst <- paste0("c", 13:25)
# sector_lst <- list("goods" = goods_lst, "services" = services_lst)
# sector <- sector_lst$services
# 
# print(cal_sector_prox(df, year, country_i, country_j, sector))
# 
# # Running Time of Function
# time_vec <- vector("numeric", 100)
# index <- 1
# for (i in 1:100) {
#   time <- Sys.time()
#   test <- cal_sector_prox(df, year, country_i, country_j, sector)
#   time_vec[index] <- Sys.time() - time
#   index <- index + 1
# }
# print(mean(time_vec))  # --> 0.002 sec


#------------------------- Run Entire -------------------------# 

df <- read_csv("Eora_exports_EachIndustry/Eora_exports_EachIndustry.csv")

year_lst <- c(1990:2022)

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
country_lst2 <- country_lst

# Industry list (goods and services)
goods_lst <- paste0("c", 1:12)
services_lst <- paste0("c", 13:25)
sector_lst <- list("goods" = goods_lst, "services" = services_lst)

lst_length <- numeric(length(year_lst) * sum(1:length(country_lst)-1) * length(sector_lst))

Sector_prox_lst <- list("Year" = lst_length,
                        "Country_i" = lst_length,
                        "Country_j" = lst_length,
                        "Sector" = lst_length,
                        "Sector_prox1" = lst_length,
                        "Sector_prox2" = lst_length)

index <- 1
for (year in year_lst) {
  num <- 2
  
  for (i in country_lst[1:length(country_lst)-1]) {
    for (j in country_lst2[num:length(country_lst2)]) {
      for (s in c("goods", "services")) {
        Sector_prox <- cal_sector_prox(df, year, i, j, sector_lst[[s]])
        Sector_prox1 <- Sector_prox[1]
        Sector_prox2 <- Sector_prox[2]
        
        Sector_prox_lst$Year[index] <- year
        Sector_prox_lst$Country_i[index] <- i
        Sector_prox_lst$Country_j[index] <- j
        Sector_prox_lst$Sector[index] <- s
        Sector_prox_lst$Sector_prox1[index] <- Sector_prox1
        Sector_prox_lst$Sector_prox2[index] <- Sector_prox2
        
        index <- index + 1
      }
    }
    num <- num + 1
  }
  
  #--------- Copy Duplicate Tables & Save ---------#
  
  # list -> DataFrame
  Sector_prox_EachYear <- as.data.frame(Sector_prox_lst)
  
  # Replicate table which is interchanged country & Industry pair and remove duplicated rows
  Sector_prox_EachYear1 <- Sector_prox_EachYear
  Sector_prox_EachYear1[c("Country_i", "Country_j")] <- Sector_prox_EachYear[c("Country_j", "Country_i")]
  Sector_prox_EachYear2 <- rbind(Sector_prox_EachYear, Sector_prox_EachYear1)
  Sector_prox_EachYear3 <- unique(Sector_prox_EachYear2)

  Sector_prox_EachYear3$Country_i <- factor(Sector_prox_EachYear3$Country_i, levels = country_lst)
  Sector_prox_EachYear3$Country_j <- factor(Sector_prox_EachYear3$Country_j, levels = country_lst)

  Sector_prox_EachYear4 <- Sector_prox_EachYear3 %>%
    arrange(Year, Country_i, Country_j)
  
  # Save it as a csv file
  write_csv(Sector_prox_EachYear4, paste0("Eora_SectorProx/Eora_SectorProx.csv"))
}



