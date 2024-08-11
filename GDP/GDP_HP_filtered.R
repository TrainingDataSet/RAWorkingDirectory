# [Last Modified: 2024-08-09]
# 2024-08-09: Apply HP filter to GDP

library(dplyr)
library(readr)
library(hpfilter)
library(microbenchmark)

setwd("~/RAWorkingDirectory")

hp_filtered <- function(df, country) {
  gdp <- df[(df$Country == country), ]
  gdp1 <- gdp[complete.cases(gdp), ]
  
  # If the data is empty, return NA
  if (nrow(gdp1) == 0) {
    NA_df <- as.data.frame(rep(NA, 33))
    colnames(NA_df) <- "GDP"
    return (NA_df)
  }
  
  NA_data <- as.data.frame(rep(NA, 33 - nrow(gdp1)))
  colnames(NA_data) <- "GDP"
  gdp_ts <- ts(gdp1["GDP"], start = min(gdp1$Year), frequency = 1)
  gdp_hp <- hp1(gdp_ts, lambda = 100)
  
  gdp_hp1 <- rbind(NA_data, gdp_hp)
  
  return (gdp_hp1)
}


#---------------- Test Code ----------------#

# df <- read_csv("GDP/GDP.csv")
# country <- "KOR"
# 
# print(hp_filtered(df, country))
# 
# # Running Time of Function
# microbenchmark(
#   test = hp_filtered(df, country),
#   times = 100
# ) # --> 0.002 sec


#------------------------- Run Entire -------------------------# 

df <- read_csv("GDP/GDP.csv")

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

lst_length <- numeric(length(country_lst) * 33)

GDP_lst <- list("Year" = lst_length,
                "Country" = lst_length,
                "GDP" = lst_length
)

index <- 1
for (country in country_lst) {
  gdp_filtered <- hp_filtered(df, country)
  GDP_lst$Year[index:(index+32)] <- c(1990:2022)
  GDP_lst$Country[index:(index+32)] <- rep(country, 33) 
  GDP_lst$GDP[index:(index+32)] <- as.vector(gdp_filtered)[[1]]
  
  index <- index + 33
}

GDP_df <- as.data.frame(GDP_lst)
write_csv(GDP_df, "GDP/GDP_HP_filtered.csv")

