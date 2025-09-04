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
#   df: Total_trade_flows/Total_trade_flows.csv
#   gdp_df: GDP/GDP.csv
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
                   (df$Country_i == country_i) & 
                   (df$Country_j != country_j), ]
  
  except_j1 <- na.omit(left_join(except_j, gdp_df, by = c("Year", "Country_j" = "Country")))
  
  
  sum_exj <- sum(as.data.frame(lapply(except_j1$Trade_flows, as.numeric)))
  
  except_j1$T_rate <- as.vector(except_j1$Trade_flows / sum_exj)
  
  RWC <- as.numeric(except_j1$T_rate %*% except_j1$GDP)
  
  return (RWC)
}



#---------------- Test Code ----------------#

# df <- read_csv("Total_trade_flows/Total_trade_flows.csv")
# gdp_df <- read_csv("GDP/GDP.csv")
# year <- 2022
# country_i <- "VNM"
# country_j <- "VEN"
# 
# print(cal_RWC(df, gdp_df, year, country_i, country_j))
# 
# # Running Time of Function
# microbenchmark(
#   test = cal_RWC(df, gdp_df, year, country_i, country_j),
#   times = 1000
# )  # --> 0.02 sec



#------------------------- Run Entire -------------------------# 

df <- read_csv("Total_trade_flows/Total_trade_flows.csv")
gdp_df <- read_csv("GDP/GDP.csv")

cl <- makeCluster(11)

year_lst <- list(c(1990:1992), c(1993:1995), c(1996:1998), c(1999:2001), c(2002:2004), c(2005:2007), c(2008:2010),
                 c(2011:2013), c(2014:2016), c(2017:2019), c(2020:2022))

# Country list (Entire country)
# country_lst <- c("AFG", "ALB", "DZA", "AND", "AGO", "ATG", "ARG", "ARM", "ABW", "AUS", "AUT", "AZE", "BHS", "BHR", "BGD", "BRB", "BLR", "BEL", "BLZ", "BEN", "BMU",
# "BTN", "BOL", "BIH", "BWA", "BRA", "VGB", "BRN", "BGR", "BFA", "BDI", "KHM", "CMR", "CAN", "CPV", "CYM", "CAF", "TCD", "CHL", "CHN", "COL", "COG", "CRI", "HRV",
# "CUB", "CYP", "CZE", "CIV", "PRK", "COD", "DNK", "DJI", "DOM", "ECU", "EGY", "SLV", "ERI", "EST", "ETH", "FJI", "FIN", "FRA", "PYF", "GAB", "GMB", "GEO", "DEU",
# "GHA", "GRC", "GRL", "GTM", "GIN", "GUY", "HTI", "HND", "HKG", "HUN", "ISL", IND", "IDN", "IRN", "IRQ", "IRL", "ISR", "ITA", "JAM", "JPN", "JOR", "KAZ", "KEN",
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


task <- function(year_lst) {
  
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
        if (i == j) {next}
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
    write_csv(RWC_EachYear, paste0("consideringNothing/RWC/RWC_", 
                                   year_lst[1], "-", year_lst[3], ".csv"))
  }
}

clusterExport(cl, varlist = c("task", "year_lst", "country_lst", "country_lst2",
                              "cal_RWC", "read_csv", "%>%", "arrange", "write_csv", 
                              "df", "gdp_df", "left_join"))


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
print(Sys.time()-time)  # --> 4.63 mins

