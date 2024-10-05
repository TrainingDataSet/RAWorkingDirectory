# [Last Modified: 2024-08-28]
# 2024-08-28: Calculate correlation between GDP and pollutants

library(dplyr)
library(readr)
library(microbenchmark)

setwd("~/RAWorkingDirectory")


#---------------------------------
# Calculate correlation between GDP and pollutants

# Args:
#   gdp: GDP/GDP_HP_filtered.csv
#   pollutants: Sum_pollutants/Sum_pollutants_HP_filtered_noSector.csv
#   t: Time Window
#   country_i: Code of country_s
#   country_j: Code of country_r
#
# Returns:
#   Corr_diff: Corr(GDP, pollutants) of Country_i - Corr(GDP, pollutants) of Country_j
#---------------------------------

cal_corr_gdp_pollutants <- function(gdp, pollutants, t, country_i, country_j) {
  
  #-------------- Extract Vector of GDP & Pollutants in Time Window --------------#
  
  gdp_i <- gdp[(gdp$Year %in% t) &
               (gdp$Country == country_i), ]
  
  pollutants_i <- pollutants[(pollutants$Year %in% t) &
                             (pollutants$Country == country_i), ]
  
  gdp_j <- gdp[(gdp$Year %in% t) &
               (gdp$Country == country_j), ]
  
  pollutants_j <- pollutants[(pollutants$Year %in% t) &
                             (pollutants$Country == country_j), ]
  
  
  #------------------- Calculate Difference of Correlation -------------------#
  
  Corr_GDP_CO2_i <- cor(gdp_i$GDP, pollutants_i$CO2)
  Corr_GDP_GHG_i <- cor(gdp_i$GDP, pollutants_i$GHG)
  Corr_GDP_CO2_j <- cor(gdp_j$GDP, pollutants_j$CO2)
  Corr_GDP_GHG_j <- cor(gdp_j$GDP, pollutants_j$GHG)
  
  
  return (c(Corr_GDP_CO2_i, Corr_GDP_GHG_i, Corr_GDP_CO2_j, Corr_GDP_GHG_j))
  
}


#---------------- Test Code ----------------#

# gdp <- read_csv("GDP/GDP_HP_filtered.csv")
# pollutants <- read_csv("Sum_pollutants/Sum_pollutants_HP_filtered_noSector.csv")
# 
# t <- c(2010:2022)
# country_i <- "ZAF"
# country_j <- "VNM"
# 
# print(cal_corr_gdp_pollutants(gdp, pollutants, t, country_i, country_j))
# 
# # Running Time of Function
# microbenchmark(
#   test = cal_corr_gdp_pollutants(gdp, pollutants, t, country_i, country_j),
#   times = 1000
# ) # --> 0.001 sec



#------------------------- Run Entire -------------------------# 

gdp <- read_csv("GDP/GDP_HP_filtered.csv")
pollutants <- read_csv("Sum_pollutants/Sum_pollutants_HP_filtered_noSector.csv")

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

# Time window list
tw_lst <- list('1' = c(1990:1999),
               '2' = c(2000:2009),
               '3' = c(2010:2019),
               '4' = c(2010:2022))

# Create List
lst_length <- numeric(length(tw_lst) * length(country_lst) * (length(country_lst2)-1))

Corr_lst <- list("t" = lst_length,
                 "Country_i" = lst_length,
                 "Country_j" = lst_length,
                 "Corr_GDP_CO2_i" = lst_length,
                 "Corr_GDP_GHG_i" = lst_length,
                 "Corr_GDP_CO2_j" = lst_length,
                 "Corr_GDP_GHG_j" = lst_length)

# Run Entire
time <- Sys.time()

index <- 1
for (t in c("1", "2", "3", "4")) {
  for (i in country_lst) {
    for (j in country_lst2) {
      if (i == j) next
      Corr_GDP_pollutants <- cal_corr_gdp_pollutants(gdp, pollutants, tw_lst[[t]], i, j)
      Corr_GDP_CO2_i <- Corr_GDP_pollutants[1]
      Corr_GDP_GHG_i <- Corr_GDP_pollutants[2]
      Corr_GDP_CO2_j <- Corr_GDP_pollutants[3]
      Corr_GDP_GHG_j <- Corr_GDP_pollutants[4]
      
      Corr_lst$t[index] <- t
      Corr_lst$Country_i[index] <- i
      Corr_lst$Country_j[index] <- j
      Corr_lst$Corr_GDP_CO2_i[index] <- Corr_GDP_CO2_i
      Corr_lst$Corr_GDP_GHG_i[index] <- Corr_GDP_GHG_i
      Corr_lst$Corr_GDP_CO2_j[index] <- Corr_GDP_CO2_j
      Corr_lst$Corr_GDP_GHG_j[index] <- Corr_GDP_GHG_j
      
      index <- index + 1
    }
  }
  
  #--------- Copy Duplicate Tables & Save ---------#
  
  # list -> DataFrame
  Corr_df <- as.data.frame(Corr_lst)
  
  # Save it as a csv file
  write_csv(Corr_df, paste0("consideringPollutants/Time_window/Corr_GDP_pollutants.csv"))
}

print(Sys.time()-time)  # --> running time 35.27 sec

