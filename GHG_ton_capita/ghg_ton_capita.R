# [Last Modified: 2024-09-08]

library(dplyr)
library(readr)
library(parallel)
library(microbenchmark)

setwd("~/RAWorkingDirectory")


#---------------------------------
# Calculate GHG per capita
#
# Args:
#   pollutants_df: Sum_pollutants/Sum_pollutants.csv
#   capita_df: ghg_ton_capita/pop(capita).csv
#   year: Year
#   country: Code of country
#   
# Returns:
#   GHG_ton_capita: GHG(ton) per capita
#---------------------------------

cal_ghg_ton_capita <- function(pollutants_df, capita_df, year, country) {
  
  #-------------- GHG of specific country & year --------------#
  
  ghg <- sum(pollutants_df[(pollutants_df$Year == year) &
                           (pollutants_df$Country == country), "GHG"]) * 1000
  
  
  #------------- Capita of specific country & year -------------#
  
  capita <- as.numeric(capita_df[(capita_df$year == year) &
                                 (capita_df$ctrycode == country), "pop(capita)"])
  
  GHG_ton_capita <- ghg / capita
  
  return (GHG_ton_capita)
  
}


#---------------- Test Code ----------------#

# pollutants_df <- read_csv("Sum_pollutants/Sum_pollutants.csv")
# capita_df <- read_csv("ghg_ton_capita/pop(capita).csv")
# year <- 2022
# country <- "VEN"
# 
# print(cal_ghg_ton_capita(pollutants_df, capita_df, year, country))
# 
# # Running Time of Function
# microbenchmark(
#   test = cal_ghg_ton_capita(pollutants_df, capita_df, year, country),
#   times = 1000
# )  # --> 0.0006 sec


#------------------------- Run Entire -------------------------# 

pollutants_df <- read_csv("Sum_pollutants/Sum_pollutants.csv")
capita_df <- read_csv("ghg_ton_capita/pop(capita).csv")

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

  
#------------------- Create List -------------------#
lst_length <- numeric(length(year_lst) * length(country_lst))

GHG_ton_capita_lst <- list("Year" = lst_length,
                           "Country" = lst_length,
                           "ghg_ton_capita" = lst_length)

#------------------- Run Entire -------------------#
index <- 1
for (year in year_lst) {
  for (country in country_lst) {
    ghg_ton_capita <- cal_ghg_ton_capita(pollutants_df, capita_df, year, country)
    
    GHG_ton_capita_lst$Year[index] <- year
    GHG_ton_capita_lst$Country[index] <- country
    GHG_ton_capita_lst$ghg_ton_capita[index] <- ghg_ton_capita
    
    index <- index + 1
  }
  
  # list -> DataFrame
  GHG_ton_capita_df <- as.data.frame(GHG_ton_capita_lst)
  
  # Save it as a csv file
  write_csv(GHG_ton_capita_df, paste0("GHG_ton_capita/data_eoracountry (2).csv"))
}


