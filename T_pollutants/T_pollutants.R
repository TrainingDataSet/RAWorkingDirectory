# [Last Modified: 2024-05-03]
# 2023-09-22: Add natural log by calculating time window variable, Remove hp filter
# 2023-09-25: read.csv -> read_csv (improve speed)
# 2023-10-11: write_csv(BT_d_df, "BT_d/TradeFlows.csv") Save trade flows for each year as a csv file
# 2023-12-26: Code optimization [running time improvement 2101.48sec -> 1648.17sec per year (21.57%)] 
# 2024-01-09: Add industry operation with goods and services
# 2024-02-29: Read Eora file instead of MRIO file and add intermediate save
# 2024-03-13: Replicate table which interchanged country & Industry pair and remove duplicated rows
# 2024-05-03: Add pollutants factor to calculate variable

library(dplyr)
library(readr)
library(readxl)
library(parallel)

setwd("~/RAWorkingDirectory")

### Calculate Total Trade Flows ###

#---------------------------------
# Calculate total trade flows considering pollutants weight
#
# Args:
#   exports_df: Eora_exports/Eora_exports.csv
#   pollutants_df: Pollutants_weight/Pollutants_weight.csv
#   year: Year of variable
#   country_i: Code of country_s
#   country_j: Code of country_r
#   sector_s: Sector of country_s (goods/services) 
#   sector_u: Sector of country_r (goods/services) 
#
# Returns:
#   T_CO2: total trade flows considering CO2 weight
#   T_GHG: total trade flows considering GHG weight
#---------------------------------

cal_total_trade_flows <- function(exports_df, pollutants_df, year, country_i, country_j, sector_s, sector_u) {
  
  
  #----------------- Vector of Pollutants Weight -----------------#
  
  CO2_is <- as.vector(pollutants_df[(pollutants_df$Year == year) &
                                    (pollutants_df$Country == country_i) &
                                    (pollutants_df$Industry %in% unlist(sector_s)), "CO2_rate"])[[1]]
  
  CO2_ju <- as.vector(pollutants_df[(pollutants_df$Year == year) &
                                    (pollutants_df$Country == country_j) &
                                    (pollutants_df$Industry %in% unlist(sector_u)), "CO2_rate"])[[1]]
  
  GHG_is <- as.vector(pollutants_df[(pollutants_df$Year == year) &
                                    (pollutants_df$Country == country_i) &
                                    (pollutants_df$Industry %in% unlist(sector_s)), "GHG_rate"])[[1]]
  
  GHG_ju <- as.vector(pollutants_df[(pollutants_df$Year == year) &
                                    (pollutants_df$Country == country_j) &
                                    (pollutants_df$Industry %in% unlist(sector_u)), "GHG_rate"])[[1]]
  
  
  #---------------------- Vector of Exports ----------------------#
  
  EX_ij <- as.vector(exports_df[(exports_df$Year == year) &
                                (exports_df$Country_i == country_i) &
                                (exports_df$Country_j == country_j) &
                                (exports_df$From_industry %in% unlist(sector_s)) &
                                (exports_df$To_sector == names(sector_u)), "Exports"])[[1]]
  
  EX_ji <- as.vector(exports_df[(exports_df$Year == year) &
                                (exports_df$Country_i == country_j) &
                                (exports_df$Country_j == country_i) &
                                (exports_df$From_industry %in% unlist(sector_u)) &
                                (exports_df$To_sector == names(sector_s)), "Exports"])[[1]]
  
  
  #---------- Dot Product of Pollutants Weight and Exports ----------#
  
  T_CO2_ij <- as.numeric(CO2_is %*% EX_ij)
  T_CO2_ji <- as.numeric(CO2_ju %*% EX_ji)
  T_GHG_ij <- as.numeric(GHG_is %*% EX_ij)
  T_GHG_ji <- as.numeric(GHG_ju %*% EX_ji)
  
  
  #---------- Sum of Exports of Bilateral Countries ----------#
  
  T_CO2 <- T_CO2_ij + T_CO2_ji
  T_GHG <- T_GHG_ij + T_GHG_ji
  
  
  return (c(T_CO2, T_GHG))
  
}  


#---------------- Test Code ----------------#

# goods_lst <- list("goods" = paste0("c", 1:12))
# services_lst <- list("services" = paste0("c", 13:25))
# industry_lst <- list("goods" = goods_lst, "services" = services_lst)
# 
# exports_df <- read_csv("Eora_exports/Eora_exports.csv")
# pollutants_df <- read_csv("Pollutants_weight/Pollutants_weight.csv")
# year <- 2021
# country_i <- "CYP"
# country_j <- "MEX"
# sector_s <- services_lst
# sector_u <- goods_lst
# 
# print(cal_total_trade_flows(exports_df, pollutants_df, year, country_i, country_j, sector_s, sector_u))
# 
# # Running Time of Function
# time_vec <- vector("numeric", 100)
# index <- 1
# for (i in 1:100) {
#   time <- Sys.time()
#   test <- cal_total_trade_flows(exports_df, pollutants_df, year, country_i, country_j, sector_s, sector_u)
#   time_vec[index] <- Sys.time() - time
#   index <- index + 1
# }
# print(mean(time_vec))  # --> 1.35 sec




#------------------------- Run Entire -------------------------# 

exports_df <- read_csv("Eora_exports/Eora_exports.csv")
pollutants_df <- read_csv("Pollutants_weight/Pollutants_weight.csv")

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

# Industry list (goods and services)
goods_lst <- list("goods" = paste0("c", 1:12))
services_lst <- list("services" = paste0("c", 13:25))
industry_lst <- list("goods" = goods_lst, "services" = services_lst)

# copy
country_lst2 <- country_lst
industry_lst2 <- industry_lst

task <- function(year_lst) {

  # Create ordered pair of country_s, country_r
  lst_length <- numeric(length(year_lst) * sum(1:length(country_lst)-1) * length(industry_lst) * length(industry_lst2))
  
  T_lst <- list("Year" = lst_length,
                "Country_i" = lst_length,
                "Country_j" = lst_length,
                "Sector_s" = lst_length,
                "Sector_u" = lst_length,
                "T_CO2" = lst_length,
                "T_GHG" = lst_length)
  
  index <- 1
  for (year in year_lst) {
    num <- 2
    
    for (i in country_lst[1:length(country_lst)-1]) {
      for (j in country_lst2[num:length(country_lst2)]) {
        for (s in names(industry_lst)) {
          for (u in names(industry_lst2)) {
            T_var <- cal_total_trade_flows(exports_df, pollutants_df, year, i, j, industry_lst[[s]], industry_lst[[u]])
            T_CO2 <- T_var[1]
            T_GHG <- T_var[2]
            
            T_lst$Year[index] <- year
            T_lst$Country_i[index] <- i
            T_lst$Country_j[index] <- j
            T_lst$Sector_s[index] <- s
            T_lst$Sector_u[index] <- u
            T_lst$T_CO2[index] <- T_CO2
            T_lst$T_GHG[index] <- T_GHG
            
            index <- index + 1
          }
        }
      }
      num <- num + 1
    }
    # list -> DataFrame
    T_EachYear <- as.data.frame(T_lst)
    
    # Replicate table which is interchanged country & Industry pair and remove duplicated rows
    T_EachYear1 <- T_EachYear
    T_EachYear1[c("Country_i", "Country_j")] <- T_EachYear[c("Country_j", "Country_i")]
    T_EachYear1[c("Sector_s", "Sector_u")] <- T_EachYear[c("Sector_u", "Sector_s")]
    T_EachYear2 <- rbind(T_EachYear, T_EachYear1)
    T_EachYear3 <- unique(T_EachYear2)
    
    T_EachYear3$Country_i <- factor(T_EachYear3$Country_i, levels = country_lst)
    T_EachYear3$Country_j <- factor(T_EachYear3$Country_j, levels = country_lst)
    T_EachYear3$Sector_s <- factor(T_EachYear3$Sector_s, levels = names(industry_lst))
    T_EachYear3$Sector_u <- factor(T_EachYear3$Sector_u, levels = names(industry_lst))
    
    T_EachYear4 <- T_EachYear3 %>%
      arrange(Year, Country_i, Country_j, Sector_s, Sector_u)
    
    # Save it as a csv file
    write_csv(T_EachYear4, paste0("T_pollutants/T_pollutants_", 
                                  year_lst[1], "-", year_lst[3], ".csv"))
  }
}


clusterExport(cl, varlist = c("task", "year_lst", "country_lst", "country_lst2",
                              "industry_lst", "industry_lst2", "cal_total_trade_flows",
                              "read_csv", "%>%", "bind_rows", "arrange", "write_csv",
                              "pollutants_df", "exports_df"))


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
print(Sys.time()-time)
