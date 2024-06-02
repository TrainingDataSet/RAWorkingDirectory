# [Last Modified: 2024-06-02]

library(dplyr)
library(readr)
library(parallel)

setwd("~/RAWorkingDirectory")


#---------------------------------
# Calculate pollutants weight of specific country and industry
#
# Args:
#   df_Q: Preprocessed pollutants dataframe
#   country: Code of country
#   sector: Code of industry
#
# Returns:
#   CO2_sum: Sum of CO2 in specific country & sector
#   GHG_sum: Sum of GHG in specific country & sector
#---------------------------------


cal_pollutants <- function(df_Q, country, sector) {
  
  #-------------- CO2 and GHG of specific country & sector --------------#
  
  CO2 <- as.numeric(df_Q[2720, which((df_Q[2, ] == country) &
                                     (df_Q[3, ] %in% sector))])
  
  GHG <- as.numeric(df_Q[2712, which((df_Q[2, ] == country) &
                                     (df_Q[3, ] %in% sector))])
  
  
  #------------------------- Sum of CO2 and GHG -------------------------#
  
  CO2_sum <- sum(CO2)
  GHG_sum <- sum(GHG)
  
  
  return (c(CO2_sum, GHG_sum))
  
  }
                                 
                                 
#---------------- Test Code ----------------#

# year <- 1990
# df_Q <- read_csv(sprintf("Eora_csv_files/Eora_Q%d.csv", year))
# 
# country <- "KOR"
# sector <- goods_lst
# 
# print(cal_pollutants(df_Q, country, sector))
# 
# # Running Time of Function
# time_vec <- vector("numeric", 100)
# index <- 1
# for (i in 1:100) {
#   time <- Sys.time()
#   test <- cal_pollutants(df_Q, country, sector)
#   time_vec[index] <- Sys.time() - time
#   index <- index + 1
# }
# print(mean(time_vec)) # --> 0.23 sec


#------------------------- Run Entire -------------------------# 

cl <- makeCluster(11)

year_lst <- list(c(1990:1992), c(1993:1995), c(1996:1998), c(1999:2001), c(2002:2004), c(2005:2007), c(2008:2010),
                 c(2011:2013), c(2014:2016), c(2017:2019), c(2020:2022))

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

# Sector list (goods and services)
goods_lst <- paste0("c", 1:12)
services_lst <- paste0("c", 13:25)
sector_lst <- list("goods" = goods_lst, "services" = services_lst)


task <- function(year_lst) {
  
  #------------------- Create List -------------------#
  lst_length <- numeric(length(year_lst) * length(country_lst) * length(sector_lst))
  
  Pollutants_lst <- list("Year" = lst_length,
                 "Country" = lst_length,
                 "Sector" = lst_length,
                 "CO2" = lst_length,
                 "GHG" = lst_length)
  
  #------------------- Run Entire -------------------#
  index <- 1
  for (year in year_lst) {
    df_Q <- read_csv(sprintf("Eora_csv_files/Eora_Q%d.csv", year))
    
    for (country in country_lst) {
      for (sector in c("goods", "services")) {
        pollutants <- cal_pollutants(df_Q, country, sector_lst[[sector]])
        CO2 <- pollutants[1]
        GHG <- pollutants[2]
        
        Pollutants_lst$Year[index] <- year
        Pollutants_lst$Country[index] <- country
        Pollutants_lst$Sector[index] <- sector
        Pollutants_lst$CO2[index] <- CO2
        Pollutants_lst$GHG[index] <- GHG
        
        index <- index + 1
      }
    }
    
    # list -> DataFrame
    Pollutants_df <- as.data.frame(Pollutants_lst)
    
    # Save it as a csv file
    write_csv(Pollutants_df, paste0("Sum_Pollutants/Sum_Pollutants_", 
                                   year_lst[1], "-", year_lst[3], ".csv"))
  }
}

clusterExport(cl, varlist = c("task", "year_lst", "country_lst",
                              "sector_lst", "cal_pollutants",
                              "read_csv", "write_csv"))


tasks <- list(
  function() task(year_lst[[1]]),
  function() task(year_lst[[2]]),
  function() task(year_lst[[3]]),
  function() task(year_lst[[4]]),
  function() task(year_lst[[5]]),
  function() task(year_lst[[6]]),
  function() task(year_lst[[7]]),
  function() task(year_lst[[8]]),
  function() task(year_lst[[9]]),
  function() task(year_lst[[10]]),
  function() task(year_lst[[11]])
)

time <- Sys.time()
parLapply(cl, tasks, function(f) f())
stopCluster(cl)
print(Sys.time()-time)  # --> running time 3.94 mins





