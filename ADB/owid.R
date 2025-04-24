# [Last Modified: 2023-12-31]

library(dplyr)
library(hpfilter)
library(readxl)
library(readr)

setwd("~/RAWorkingDirectory")



# Calculate correlation of Country_s's variable1 and Country_r's variable2

#---------------------------------
# Calculate correlation of Country_s's variable1 and Country_r's variable2
#
# Args:
#   df: "owid-co2-data" dataframe
#   country_s: Code of country_s 
#   country_r: Code of country_r
#   var1: Variable of country_s
#   var2: Variable of country_r
#
# Returns:
#   t1_corr: correlation of Country_s's variable1 and Country_r's variable2 in 2007~2013
#   t2_corr: correlation of Country_s's variable1 and Country_r's variable2 in 2014~2020
#---------------------------------

cal_corr <- function(df, country_s, country_r, var1, var2) {
  # Vector of Country_s's variable1
  s_var1 <- df[df$iso_code == country_s, var1]
  # Vector of Country_r's variable2
  r_var2 <- df[df$iso_code == country_r, var2]
  
  # Rename each variable's column
  colnames(s_var1) <- "s_var1"
  colnames(r_var2) <- "r_var2"
  
  # Bind year data to previous vectors
  year <- list(year = c(2007:2020))
  target_var <- bind_cols(year, s_var1, r_var2)
  target_var <- na.omit(target_var)  # remove NA
  
  # Return NA if data frame is empty after remove NA
  if (nrow(target_var) == 0) {return (NA)}
  
  # Convert to time series and apply HP filter
  target_ts <- ts(target_var[, 2:3], start = min(target_var$year), frequency = 1)
  target_hp <- hp1(target_ts, lambda = 100)
  
  target_hp_df <- bind_cols(target_var[, 1], target_hp)
  
  # Calculate correlation by each time window
  t1_corr <- cor(target_hp_df[2], target_hp_df[3])
  
  return (t1_corr)
}

owid <- read_csv("OWID/owid-co2-data_corrected.csv")

# Country list (Entire country)
country_lst = c("AUS", "AUT", "BEL", "BGR", "BRA", "CAN", "CHE", "CHN",
               "CYP", "CZE", "DEU", "DNK", "ESP", "EST", "FIN", "FRA",
               "GBR", "GRC", "HRV", "HUN", "IDN", "IND", "IRL", "ITA",
               "JPN", "KOR", "LTU", "LUX", "LVA", "MEX", "MLT", "NLD",
               "NOR", "POL", "PRT", "ROU", "RUS", "SVK", "SVN", "SWE",
               "TUR", "TWN", "USA", "BGD", "MYS", "PHL", "THA", "VNM",
               "KAZ", "MNG", "LKA", "PAK", "FJI", "LAO", "BRN", "BTN",
               "KGZ", "KHM", "MDV", "NPL", "SGP", "HKG")
# country_lst <- c("AUS", "AUT", "BEL", "BGR", "BRA")

# copy
country_lst2 <- country_lst

# Target variable
var1 <- "rgdp"  # Country_s's variable 
var2 <- "rgdp"  # Country_r's variable
colname <- "corr_rgdp"

# Create list
lst_length = numeric(length(country_lst) * length(country_lst2))
corr_lst <- list("Country_s" = lst_length, "Country_r" = lst_length)

# Bilateral BT_d for the entire period and country
index <- 1
for (country_s in country_lst) {
  for (country_r in country_lst2) {
    corr_lst[["Country_s"]][index] <- country_s
    corr_lst[["Country_r"]][index] <- country_r
    
    # Calculate correlation by each time window
    corr_lst[[colname]][index] <- cal_corr(owid, country_s, country_r, var1, var2)
    
    index <- index + 1
  }
}

# list -> DataFrame
corr_df <- data.frame(corr_lst)

corr_df$Country_s <- recode(corr_df$Country_s, 
                              "CHE" = "SWI",
                              "CHN" = "PRC",
                              "DEU" = "GER",
                              "DNK" = "DEN",
                              "ESP" = "SPA",
                              "GBR" = "UKG",
                              "IDN" = "INO",
                              "IRL" = "IRE",
                              "NLD" = "NET",
                              "PRT" = "POR",
                              "ROU" = "ROM",
                              "TWN" = "TAP",
                              "BGD" = "BAN",
                              "MYS" = "MAL",
                              "PHL" = "PHI",
                              "VNM" = "VIE",
                              "MNG" = "MON",
                              "LKA" = "SRI",
                              "FJI" = "FIJ",
                              "BRN" = "BRU",
                              "BTN" = "BHU",
                              "KHM" = "CAM",
                              "MDV" = "MLD",
                              "NPL" = "NEP",
                              "SGP" = "SIN")

corr_df$Country_r <- recode(corr_df$Country_r, 
                              "CHE" = "SWI",
                              "CHN" = "PRC",
                              "DEU" = "GER",
                              "DNK" = "DEN",
                              "ESP" = "SPA",
                              "GBR" = "UKG",
                              "IDN" = "INO",
                              "IRL" = "IRE",
                              "NLD" = "NET",
                              "PRT" = "POR",
                              "ROU" = "ROM",
                              "TWN" = "TAP",
                              "BGD" = "BAN",
                              "MYS" = "MAL",
                              "PHL" = "PHI",
                              "VNM" = "VIE",
                              "MNG" = "MON",
                              "LKA" = "SRI",
                              "FJI" = "FIJ",
                              "BRN" = "BRU",
                              "BTN" = "BHU",
                              "KHM" = "CAM",
                              "MDV" = "MLD",
                              "NPL" = "NEP",
                              "SGP" = "SIN")

# Save it as a csv file
write_csv(corr_df, "ADB/Dataset/owid_corr_co2_capita.csv")

