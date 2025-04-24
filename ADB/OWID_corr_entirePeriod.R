# [Last Modified: 2025-03-10]
# 2024-08-28: Calculate correlation between GDP and pollutants
# 2025-03-10: Use OWID dataset

library(dplyr)
library(readr)
library(microbenchmark)

setwd("~/RAWorkingDirectory")


#---------------------------------
# Calculate correlation between GDP and pollutants

# Args:
#   owid: OWID/OWID_hp_filtered.csv
#   t: Time Window
#   country_i: Code of country_i
#   country_j: Code of country_j
#
# Returns:
#   Within_country_corr_CO2_i: Correlation between Real GDP and CO2 in the country_i
#   Within_country_corr_CO2_j: Correlation between Real GDP and CO2 in the country_j
#---------------------------------

cal_corr_gdp_pollutants <- function(owid, country_i, country_j) {
  
  #-------------- Extract Vector of GDP & Pollutants in Time Window --------------#
  
  rgdp_i <- owid[(owid$Year %in% c(2007:2020)) &
                   (owid$Country == country_i), "rgdp"]
  
  co2_i <- owid[(owid$Year %in% c(2007:2020)) &
                  (owid$Country == country_i), "co2"]
  
  rgdp_j <- owid[(owid$Year %in% c(2007:2020)) &
                   (owid$Country == country_j), "rgdp"]
  
  co2_j <- owid[(owid$Year %in% c(2007:2020)) &
                  (owid$Country == country_j), "co2"]
  
  
  #------------------- Calculate Correlation -------------------#
  
  Within_country_corr_CO2_i <- cor(rgdp_i, co2_i)
  Within_country_corr_CO2_j <- cor(rgdp_j, co2_j)
  
  Corr_prod_CO2 = Within_country_corr_CO2_i * Within_country_corr_CO2_j
  Corr_diff_CO2 = Within_country_corr_CO2_i - Within_country_corr_CO2_j
  
  
  return (c(Within_country_corr_CO2_i, Within_country_corr_CO2_j, Corr_prod_CO2, Corr_diff_CO2))
  
}


#---------------- Test Code ----------------#

# owid <- read_csv("OWID/OWID_hp_filtered.csv")
# 
# t <- c(2014:2020)
# country_i <- "FIN"
# country_j <- "ITA"
# 
# print(cal_corr_gdp_pollutants(owid, t, country_i, country_j))
# 
# # Running Time of Function
# microbenchmark(
#   test = cal_corr_gdp_pollutants(owid, t, country_i, country_j),
#   times = 1000
# ) # --> 0.001 sec



#------------------------- Run Entire -------------------------# 

owid <- read_csv("OWID/OWID_hp_filtered.csv")

# Country list (Entire country)
country_lst = c("AUS", "AUT", "BEL", "BGR", "BRA", "CAN", "CHE", "CHN",
                "CYP", "CZE", "DEU", "DNK", "ESP", "EST", "FIN", "FRA",
                "GBR", "GRC", "HRV", "HUN", "IDN", "IND", "IRL", "ITA",
                "JPN", "KOR", "LTU", "LUX", "LVA", "MEX", "MLT", "NLD",
                "NOR", "POL", "PRT", "ROU", "RUS", "SVK", "SVN", "SWE",
                "TUR", "TWN", "USA", "BGD", "MYS", "PHL", "THA", "VNM",
                "KAZ", "MNG", "LKA", "PAK", "FJI", "LAO", "BRN", "BTN",
                "KGZ", "KHM", "MDV", "NPL", "SGP", "HKG")
country_lst2 <- country_lst

# # Time window list
# tw_lst <- c(2007:2020)

# Create List
lst_length <- numeric(length(country_lst) * length(country_lst))

Corr_lst <- list("Country_i" = lst_length,
                 "Country_j" = lst_length,
                 "Within_country_corr_CO2_i" = lst_length,
                 "Within_country_corr_CO2_j" = lst_length,
                 "Corr_prod_CO2" = lst_length,
                 "Corr_diff_CO2" = lst_length)

# Run Entire
time <- Sys.time()

index <- 1
for (i in country_lst) {
  for (j in country_lst2) {
    Corr_GDP_pollutants <- cal_corr_gdp_pollutants(owid, i, j)
    Within_country_corr_CO2_i <- Corr_GDP_pollutants[1]
    Within_country_corr_CO2_j <- Corr_GDP_pollutants[2]
    Corr_prod_CO2 <- Corr_GDP_pollutants[3]
    Corr_diff_CO2 <- Corr_GDP_pollutants[4]
    
    Corr_lst$Country_i[index] <- i
    Corr_lst$Country_j[index] <- j
    Corr_lst$Within_country_corr_CO2_i[index] <- Within_country_corr_CO2_i
    Corr_lst$Within_country_corr_CO2_j[index] <- Within_country_corr_CO2_j
    Corr_lst$Corr_prod_CO2[index] <- Corr_prod_CO2
    Corr_lst$Corr_diff_CO2[index] <- Corr_diff_CO2
    
    index <- index + 1
  }
}

#--------- Copy Duplicate Tables & Save ---------#

# list -> DataFrame
Corr_df <- as.data.frame(Corr_lst)

# Save it as a csv file
write_csv(Corr_df, paste0("ADB/OWID_corr_entirePeroid.csv"))

print(Sys.time()-time)  # --> running time 6.8 sec

