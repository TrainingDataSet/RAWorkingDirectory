# [Last Modified: 2024-08-10]
# 2024-08-10: Calculate sector prox considering nothing [Sector(x), Pollutants(x)]

library(dplyr)
library(readr)
library(microbenchmark)

setwd("~/RAWorkingDirectory")

#---------------------------------
# Calculate sector prox considering nothing [Sector(x), Pollutants(x)]
#
# Args:
#   df: Eora_exports_EachIndustry.csv
#   year: Year
#   country_i: Code of country_i
#   country_j: Code of country_j
#
# Returns:
#   sector_prox: Sector prox variable considering nothing [Sector(x), Pollutants(x)]
#---------------------------------

cal_sector_prox <- function(df, year, country_i, country_j) {
  
  #----------------- Calculate all exports variables -----------------#
  
  exports_i <- df[(df$Year == year) & 
                  (df$Country == country_i), ]
  
  exports_j <- df[(df$Year == year) & 
                  (df$Country == country_j), ]
  
  
  #--------------------- Calculate sector_prox ---------------------#
  
  sum_exports_i <- sum(exports_i$Exports)
  
  EX_i <- exports_i$Exports / sum_exports_i
  
  sum_exports_j <- sum(exports_j$Exports)
  
  EX_j <- exports_j$Exports / sum_exports_j
  
  sector_prox <- 1 - (sum(abs(EX_i - EX_j)) / 2)
  
  
  return (sector_prox)
  
}


#---------------- Test Code ----------------#

# df <- read_csv("Eora_exports_EachIndustry/Eora_exports_EachIndustry.csv")
# year <- 2022
# country_i <- "AUT"
# country_j <- "AUS"
# 
# print(cal_sector_prox(df, year, country_i, country_j))
# 
# # Running Time of Function
# microbenchmark(
#   test = cal_sector_prox(df, year, country_i, country_j),
#   times = 100
# )  # --> 0.002 sec


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

lst_length <- numeric(length(year_lst) * sum(1:length(country_lst)-1))

Sector_prox_lst <- list("Year" = lst_length,
                        "Country_i" = lst_length,
                        "Country_j" = lst_length,
                        "Sector_prox" = lst_length)

index <- 1
for (year in year_lst) {
  num <- 2
  
  for (i in country_lst[1:length(country_lst)-1]) {
    for (j in country_lst2[num:length(country_lst2)]) {
      sector_prox <- cal_sector_prox(df, year, i, j)
      
      Sector_prox_lst$Year[index] <- year
      Sector_prox_lst$Country_i[index] <- i
      Sector_prox_lst$Country_j[index] <- j
      Sector_prox_lst$Sector_prox[index] <- sector_prox
      
      index <- index + 1
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
  
  Sector_prox_EachYear2$Country_i <- factor(Sector_prox_EachYear2$Country_i, levels = country_lst)
  Sector_prox_EachYear2$Country_j <- factor(Sector_prox_EachYear2$Country_j, levels = country_lst)
  
  Sector_prox_EachYear3 <- Sector_prox_EachYear2 %>%
    arrange(Year, Country_i, Country_j)
  
  # Save it as a csv file
  write_csv(Sector_prox_EachYear3, paste0("consideringNothing/SectorProx/SectorProx.csv"))
}  # --> 5.35 mins



