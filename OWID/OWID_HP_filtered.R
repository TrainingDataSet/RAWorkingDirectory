# [Last Modified: 2025-03-10]
# 2024-08-10: HP filter for rgdp and co2 data in OWID file

library(dplyr)
library(readr)
library(hpfilter)
library(microbenchmark)

setwd("~/RAWorkingDirectory")

hp_filtered <- function(df, country) {
  df2 <- df[(df$iso_code == country), c("year", "iso_code", "rgdp", "co2")]
  
  rgdp_ts <- ts(df2[, "rgdp"], start = 2007, frequency = 1)
  co2_ts <- ts(df2[, "co2"], start = 2007, frequency = 1)
  
  rgdp_hp <- hp1(rgdp_ts, lambda = 100)
  co2_hp <- hp1(co2_ts, lambda = 100)
  
  return (c(rgdp_hp, co2_hp))
}


#---------------- Test Code ----------------#

# df <- read_csv("OWID/owid-co2-data_corrected.csv")
# country <- "HKG"
# 
# print(hp_filtered(df, country))
#
# Running Time of Function
# microbenchmark(
#   test = hp_filtered(df, country),
#   times = 1000
# ) # --> 0.002 sec


#------------------------- Run Entire -------------------------# 

df <- read_csv("OWID/owid-co2-data_corrected.csv")

# Country list (Entire country)
country_lst = c("AUS", "AUT", "BEL", "BGR", "BRA", "CAN", "CHE", "CHN",
                "CYP", "CZE", "DEU", "DNK", "ESP", "EST", "FIN", "FRA",
                "GBR", "GRC", "HRV", "HUN", "IDN", "IND", "IRL", "ITA",
                "JPN", "KOR", "LTU", "LUX", "LVA", "MEX", "MLT", "NLD",
                "NOR", "POL", "PRT", "ROU", "RUS", "SVK", "SVN", "SWE",
                "TUR", "TWN", "USA", "BGD", "MYS", "PHL", "THA", "VNM",
                "KAZ", "MNG", "LKA", "PAK", "FJI", "LAO", "BRN", "BTN",
                "KGZ", "KHM", "MDV", "NPL", "SGP", "HKG")

lst_length <- numeric(length(country_lst) * 14)

Pollutants_lst <- list("Year" = lst_length,
                       "Country" = lst_length,
                       "rgdp" = lst_length,
                       "co2" = lst_length
)

index <- 1
for (country in country_lst) {
  pollutants_filtered <- hp_filtered(df, country)
  Pollutants_lst$Year[index:(index+13)] <- c(2007:2020)
  Pollutants_lst$Country[index:(index+13)] <- rep(country, 14) 
  Pollutants_lst$rgdp[index:(index+13)] <- pollutants_filtered$rgdp
  Pollutants_lst$co2[index:(index+13)] <- pollutants_filtered$co2
  
  index <- index + 14
}

Pollutants_df <- as.data.frame(Pollutants_lst)
write_csv(Pollutants_df, "OWID/OWID_HP_filtered.csv")




