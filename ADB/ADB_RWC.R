# [Last Modified: 2024-05-10]

library(readr)
library(dplyr)
library(parallel)
library(microbenchmark)

setwd("~/RAWorkingDirectory")

#---------------------------------
# Calculate RWC considering nothing [Sector(x), Pollutants(x)]
#
# Args:
#   df: ADB/TradeFlows.csv
#   gdp_df: ADB/GDP.csv
#   year: Year
#   country_i: Code of country_i
#   country_j: Code of country_j
#
# Returns:
#   RWC: RWC variable considering nothing [Sector(x), Pollutants(x)]
#---------------------------------

cal_RWC <- function(df, gdp_df, year, country_i, country_j) {
  
  #----------------- Calculate RWC  -----------------#
  
  except_j <- df[(df$Year == year) & 
                   (df$Country_s == country_i) & 
                   (df$Country_r != country_j) &
                   (df$Country_r != country_i), ]
  
  sum_exj <- sum(as.data.frame(lapply(except_j$TradeFlows_d_inter, as.numeric)))
  
  except_j$T_rate <- as.vector(except_j$TradeFlows_d_inter / sum_exj)
  
  RWC <- as.numeric(except_j$T_rate %*% except_j$GDP_r)
  
  return (RWC)
}



#---------------- Test Code ----------------#

# df <- read_csv("ADB/TradeFlows.csv")
# gdp_df <- read_csv("ADB/GDP.csv")
# year <- 2007
# country_i <- "AUS"
# country_j <- "AUT"
# 
# print(cal_RWC(df, gdp_df, year, country_i, country_j))
# 
# # Running Time of Function
# microbenchmark(
#   test = cal_RWC(df, gdp_df, year, country_i, country_j),
#   times = 1000
# )  # --> 0.02 sec



#------------------------- Run Entire -------------------------# 

df <- read_csv("ADB/TradeFlows.csv")
gdp_df <- read_csv("ADB/GDP.csv")

cl <- makeCluster(11)

year_lst <- c(2007:2020)

# Country list (Entire country)
country_lst <- c("AUS", "AUT", "BEL", "BGR", "BRA", "CAN", "SWI", "PRC",
                 "CYP", "CZE", "GER", "DEN", "SPA", "EST", "FIN", "FRA",
                 "UKG", "GRC", "HRV", "HUN", "INO", "IND", "IRE", "ITA",
                 "JPN", "KOR", "LTU", "LUX", "LVA", "MEX", "MLT", "NET",
                 "NOR", "POL", "POR", "ROM", "RUS", "SVK", "SVN", "SWE",
                 "TUR", "TAP", "USA", "BAN", "MAL", "PHI", "THA", "VIE",
                 "KAZ", "MON", "SRI", "PAK", "FIJ", "LAO", "BRU", "BHU",
                 "KGZ", "CAM", "MLD", "NEP", "SIN", "HKG")
country_lst2 <- country_lst


#------------------- Create List -------------------#
lst_length <- numeric(length(year_lst) * sum(1:length(country_lst)-1))

RWC_lst <- list("Year" = lst_length,
                "Country_i" = lst_length,
                "Country_j" = lst_length,
                "RWC" = lst_length)

#------------------- Run Entire -------------------#
index <- 1
for (year in year_lst) {
  for (i in country_lst) {
    for (j in country_lst2) {
      RWC <- cal_RWC(df, gdp_df, year, i, j)
      
      RWC_lst$Year[index] <- year
      RWC_lst$Country_i[index] <- i
      RWC_lst$Country_j[index] <- j
      RWC_lst$RWC[index] <- RWC
      
      index <- index + 1
    }
  }
  
  #--------- Save ---------#
  
  # list -> DataFrame
  RWC_EachYear <- as.data.frame(RWC_lst)
  
  # Save it as a csv file
  write_csv(RWC_EachYear, "ADB/ADB_RWC.csv")
}

time <- Sys.time()
print(Sys.time()-time)  # --> 4.63 mins

