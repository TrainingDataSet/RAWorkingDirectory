# [Last Modified: 2024-07-24]
# 2024-08-18: Use GDP and pollutants data not considering sector

library(dplyr)
library(readr)
library(microbenchmark)

setwd("~/RAWorkingDirectory")


#---------------------------------
# Calculate difference of correlation between GDP and pollutants 

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

cal_corr_diff <- function(gdp, pollutants, t, country_i, country_j) {
  
  #-------------- Extract Vector of GDP & Pollutants in Time Window --------------#
  
  gdp_is <- gdp[(gdp$Year %in% t) &
               (gdp$Country == country_i), ]
  
  pollutants_is <- pollutants[(pollutants$Year %in% t) &
                              (pollutants$Country == country_i), ]
  
  gdp_ju <- gdp[(gdp$Year %in% t) &
                (gdp$Country == country_j), ]
  
  pollutants_ju <- pollutants[(pollutants$Year %in% t) &
                              (pollutants$Country == country_j), ]
  
  
  #------------------- Calculate Difference of Correlation -------------------#
  
  Corr_GDP_CO2_is <- cor(gdp_is$GDP, pollutants_is$CO2)
  Corr_GDP_GHG_is <- cor(gdp_is$GDP, pollutants_is$GHG)
  Corr_GDP_CO2_ju <- cor(gdp_ju$GDP, pollutants_ju$CO2)
  Corr_GDP_GHG_ju <- cor(gdp_ju$GDP, pollutants_ju$GHG)
  
  Corr_diff_CO2 <- Corr_GDP_CO2_is - Corr_GDP_CO2_ju
  Corr_diff_GHG <- Corr_GDP_GHG_is - Corr_GDP_GHG_ju
  
  
  return (c(Corr_diff_CO2, Corr_diff_GHG))
  
}


#---------------- Test Code ----------------#

# gdp <- read_csv("GDP/GDP_HP_filtered.csv")
# pollutants <- read_csv("Sum_pollutants/Sum_pollutants_HP_filtered_noSector.csv")
# 
# t <- c(2010:2022)
# country_i <- "VNM"
# country_j <- "UZB"
# 
# print(cal_corr_diff(gdp, pollutants, t, country_i, country_j))
# 
# # Running Time of Function
# microbenchmark(
#   test = cal_corr_diff(gdp, pollutants, t, country_i, country_j),
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

Corr_diff_lst <- list("t" = lst_length,
                 "Country_i" = lst_length,
                 "Country_j" = lst_length,
                 "Corr_diff_CO2" = lst_length,
                 "Corr_diff_GHG" = lst_length)

# Run Entire
time <- Sys.time()

index <- 1
for (t in c("1", "2", "3", "4")) {
  for (i in country_lst) {
    for (j in country_lst2) {
      if (i == j) next
      Corr_diff <- cal_corr_diff(gdp, pollutants, tw_lst[[t]], i, j)
      Corr_diff_CO2 <- Corr_diff[1]
      Corr_diff_GHG <- Corr_diff[2]
      
      Corr_diff_lst$t[index] <- t
      Corr_diff_lst$Country_i[index] <- i
      Corr_diff_lst$Country_j[index] <- j
      Corr_diff_lst$Corr_diff_CO2[index] <- Corr_diff_CO2
      Corr_diff_lst$Corr_diff_GHG[index] <- Corr_diff_GHG
      
      index <- index + 1
    }
  }
  
  #--------- Copy Duplicate Tables & Save ---------#
  
  # list -> DataFrame
  Corr_diff_df <- as.data.frame(Corr_diff_lst)
  
  # Save it as a csv file
  write_csv(Corr_diff_df, paste0("consideringPollutants/Time_window/Corr_diff.csv"))
}

print(Sys.time()-time)  # --> running time 35.23 sec

