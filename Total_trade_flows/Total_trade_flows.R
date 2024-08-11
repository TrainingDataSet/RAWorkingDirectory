# [Last Modified: 2024-08-05]
# 2024-08-05: Calculate total trade flows considering nothing [Sector(x), Pollutants(x)]

library(dplyr)
library(readr)
library(readxl)
library(parallel)

setwd("~/RAWorkingDirectory")

### Calculate Total Trade Flows ###

#---------------------------------
# Calculate total trade flows considering nothing [Sector(x), Pollutants(x)]
#
# Args:
#   df: Eora_exports_EachSector/Eora_exports_EachSector.csv
#   year: Year of variable
#   country_i: Code of country_i
#   country_j: Code of country_j
#
# Returns:
#   Total_trade_flows: Total trade flows considering nothing
#---------------------------------

cal_total_trade_flows <- function(df, year, country_i, country_j) {
  
  #------------ Sum of Exports of Bilateral Countries ------------#
  
  EX_ij <- sum(df[(df$Year == year) &
                  (df$Country_i == country_i) &
                  (df$Country_j == country_j), "Exports"])
  
  EX_ji <- sum(df[(df$Year == year) &
                  (df$Country_i == country_j) &
                  (df$Country_j == country_i), "Exports"])
  
  Total_trade_flows <- EX_ij + EX_ji
  
  return (Total_trade_flows)
  
}  


#---------------- Test Code ----------------#

# df <- read_csv("Eora_exports_EachSector/Eora_exports_EachSector.csv")
# year <- 2022
# country_i <- "AUT"
# country_j <- "AUS"
# 
# print(cal_total_trade_flows(df, year, country_i, country_j))
# 
# # Running Time of Function
# microbenchmark(
#   test = cal_total_trade_flows(df, year, country_i, country_j),
#   times = 100
# ) # --> 0.04 sec


#------------------------- Run Entire -------------------------# 

df <- read_csv("Eora_exports_EachSector/Eora_exports_EachSector.csv")

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

# copy
country_lst2 <- country_lst

task <- function(year_lst) {
  
  # Create ordered pair of country_s, country_r
  lst_length <- numeric(length(year_lst) * sum(1:length(country_lst)-1))
  
  T_lst <- list("Year" = lst_length,
                "Country_i" = lst_length,
                "Country_j" = lst_length,
                "Trade_flows" = lst_length)
  
  index <- 1
  for (year in year_lst) {
    num <- 2
    
    for (i in country_lst[1:length(country_lst)-1]) {
      for (j in country_lst2[num:length(country_lst2)]) {
        Total_trade_flows <- cal_total_trade_flows(df, year, i, j)
        
        T_lst$Year[index] <- year
        T_lst$Country_i[index] <- i
        T_lst$Country_j[index] <- j
        T_lst$Trade_flows[index] <- Total_trade_flows
        
        index <- index + 1
      }
      num <- num + 1
    }
    # list -> DataFrame
    T_EachYear <- as.data.frame(T_lst)
    
    # Replicate table which is interchanged country pair
    T_EachYear1 <- T_EachYear
    T_EachYear1[c("Country_i", "Country_j")] <- T_EachYear[c("Country_j", "Country_i")]
    T_EachYear2 <- rbind(T_EachYear, T_EachYear1)
    
    T_EachYear2$Country_i <- factor(T_EachYear2$Country_i, levels = country_lst)
    T_EachYear2$Country_j <- factor(T_EachYear2$Country_j, levels = country_lst)
    
    T_EachYear3 <- T_EachYear2 %>%
      arrange(Year, Country_i, Country_j)
    
    # Save it as a csv file
    write_csv(T_EachYear3, paste0("Total_trade_flows/Total_trade_flows_", 
                                  year_lst[1], "-", year_lst[3], ".csv"))
  }
}


clusterExport(cl, varlist = c("task", "year_lst", "country_lst", "country_lst2", 
                              "cal_total_trade_flows", "read_csv", "%>%", "bind_rows", 
                              "arrange", "write_csv", "df"))


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
print(Sys.time()-time) # --> 15.82 mins
