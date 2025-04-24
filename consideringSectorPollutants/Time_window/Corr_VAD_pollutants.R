# [Last Modified: 2025-03-11]
# 2025-03-11: Add Within_country_corr_pollutants and Corr_prod_pollutants

library(dplyr)
library(readr)
library(microbenchmark)

setwd("~/RAWorkingDirectory")


#---------------------------------
# Calculate correlation between value added and pollutants 

# Args:
#   vad: VAD/VAD_HP_filtered.csv
#   pollutants: Sum_pollutants/Sum_pollutants_HP_filtered.csv
#   t: Time Window
#   country_i: Code of country_s
#   country_j: Code of country_r
#   sector_s: Sector of country_s (goods/services) 
#   sector_u: Sector of country_r (goods/services) 
#
# Returns:
#   Within_country_corr_CO2_i: Correlation between Value Added and CO2 in the country_i
#   Within_country_corr_GHG_i: Correlation between Value Added and GHG in the country_i
#   Within_country_corr_CO2_j: Correlation between Value Added and CO2 in the country_j
#   Within_country_corr_GHG_j: Correlation between Value Added and GHG in the country_j
#   Corr_prod_CO2: Within_country_corr_CO2_i * Within_country_corr_CO2_j
#   Corr_prod_GHG: Within_country_corr_GHG_i * Within_country_corr_GHG_j
#   Corr_diff_CO2: Within_country_corr_CO2_i - Within_country_corr_CO2_j
#   Corr_diff_GHG: Within_country_corr_GHG_i - Within_country_corr_GHG_j
#---------------------------------

cal_corr_diff <- function(vad, pollutants, t, country_i, country_j, sector_s, sector_u) {
  
  #-------------- Extract Vector of VAD & Pollutants in Time Window --------------#
  
  vad_is <- vad[(vad$Year %in% t) &
                (vad$Country == country_i) &
                (vad$Sector == sector_s), ]
  
  pollutants_is <- pollutants[(pollutants$Year %in% t) &
                              (pollutants$Country == country_i) &
                              (pollutants$Sector == sector_s), ]
  
  vad_ju <- vad[(vad$Year %in% t) &
                (vad$Country == country_j) &
                (vad$Sector == sector_u), ]
  
  pollutants_ju <- pollutants[(pollutants$Year %in% t) &
                              (pollutants$Country == country_j) &
                              (pollutants$Sector == sector_u), ]
  
  
  #------------------- Calculate Correlation -------------------#
  
  Within_country_corr_CO2_i <- cor(vad_is$VAD, pollutants_is$CO2)
  Within_country_corr_GHG_i <- cor(vad_is$VAD, pollutants_is$GHG)
  Within_country_corr_CO2_j <- cor(vad_ju$VAD, pollutants_ju$CO2)
  Within_country_corr_GHG_j <- cor(vad_ju$VAD, pollutants_ju$GHG)
  
  Corr_prod_CO2 <- Within_country_corr_CO2_i * Within_country_corr_CO2_j
  Corr_prod_GHG <- Within_country_corr_GHG_i * Within_country_corr_GHG_j
  
  Corr_diff_CO2 <- Within_country_corr_CO2_i - Within_country_corr_CO2_j
  Corr_diff_GHG <- Within_country_corr_GHG_i - Within_country_corr_GHG_j
  
  
  return (c(Within_country_corr_CO2_i, Within_country_corr_GHG_i, 
            Within_country_corr_CO2_j, Within_country_corr_GHG_j,
            Corr_prod_CO2, Corr_prod_GHG,
            Corr_diff_CO2, Corr_diff_GHG))
  
}


#---------------- Test Code ----------------#

# vad <- read_csv("VAD/VAD_HP_filtered.csv")
# pollutants <- read_csv("Sum_pollutants/Sum_pollutants_HP_filtered.csv")
# 
# country_i <- "VNM"
# country_j <- "VEN"
# sector_s <- "services"
# sector_u <- "services"
# t <- c(2010:2022)
# 
# print(cal_corr_diff(vad, pollutants, t, country_i, country_j, sector_s, sector_u))
# 
# # Running Time of Function
# microbenchmark(
#   test = cal_corr_diff(vad, pollutants, t, country_i, country_j, sector_s, sector_u),
#   times = 1000
# ) # --> 0.002 sec



#------------------------- Run Entire -------------------------# 

vad <- read_csv("VAD/VAD_HP_filtered.csv")
pollutants <- read_csv("Sum_pollutants/Sum_pollutants_HP_filtered.csv")

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

# Sector list (goods and services)
sector_lst <- c("goods", "services")
sector_lst2 <- sector_lst

# Time window list
tw_lst <- list('1' = c(1990:1999),
               '2' = c(2000:2009),
               '3' = c(2010:2019),
               '4' = c(2010:2022))

# Create List
lst_length <- numeric(length(tw_lst) * length(country_lst) * (length(country_lst2)-1) * length(sector_lst) * length(sector_lst2))

Corr_diff_lst <- list("t" = lst_length,
                      "Country_i" = lst_length,
                      "Country_j" = lst_length,
                      "Sector_s" = lst_length,
                      "Sector_u" = lst_length,
                      "Within_country_corr_CO2_i" = lst_length,
                      "Within_country_corr_GHG_i" = lst_length,
                      "Within_country_corr_CO2_j" = lst_length,
                      "Within_country_corr_GHG_j" = lst_length,
                      "Corr_prod_CO2" = lst_length,
                      "Corr_prod_GHG" = lst_length,
                      "Corr_diff_CO2" = lst_length,
                      "Corr_diff_GHG" = lst_length)

# Run Entire
time <- Sys.time()

index <- 1
for (t in c("1", "2", "3", "4")) {
  num <- 2
  
  for (i in country_lst) {
    for (j in country_lst2) {
      if (i == j) next
      for (s in sector_lst) {
        for (u in sector_lst2) {
          Corr_diff <- cal_corr_diff(vad, pollutants, tw_lst[[t]], i, j, s, u)
          Within_country_corr_CO2_i <- Corr_diff[1]
          Within_country_corr_GHG_i <- Corr_diff[2]
          Within_country_corr_CO2_j <- Corr_diff[3]
          Within_country_corr_GHG_j <- Corr_diff[4]
          Corr_prod_CO2 <- Corr_diff[5]
          Corr_prod_GHG <- Corr_diff[6]
          Corr_diff_CO2 <- Corr_diff[7]
          Corr_diff_GHG <- Corr_diff[8]
          
          Corr_diff_lst$t[index] <- t
          Corr_diff_lst$Country_i[index] <- i
          Corr_diff_lst$Country_j[index] <- j
          Corr_diff_lst$Sector_s[index] <- s
          Corr_diff_lst$Sector_u[index] <- u
          Corr_diff_lst$Within_country_corr_CO2_i[index] <- Within_country_corr_CO2_i
          Corr_diff_lst$Within_country_corr_GHG_i[index] <- Within_country_corr_GHG_i
          Corr_diff_lst$Within_country_corr_CO2_j[index] <- Within_country_corr_CO2_j
          Corr_diff_lst$Within_country_corr_GHG_j[index] <- Within_country_corr_GHG_j
          
          Corr_diff_lst$Corr_prod_CO2[index] <- Corr_prod_CO2
          Corr_diff_lst$Corr_prod_GHG[index] <- Corr_prod_GHG
          Corr_diff_lst$Corr_diff_CO2[index] <- Corr_diff_CO2
          Corr_diff_lst$Corr_diff_GHG[index] <- Corr_diff_GHG
          
          
          index <- index + 1
        }
      }
    }
    num <- num + 1
  }
  
  #--------- Copy Duplicate Tables & Save ---------#
  
  # list -> DataFrame
  Corr_diff_df <- as.data.frame(Corr_diff_lst)
  
  # Save it as a csv file
  write_csv(Corr_diff_df, paste0("consideringSectorPollutants/Time_window/Corr_VAD_pollutants.csv"))
}

print(Sys.time()-time)  # --> running time 3.83 mins

