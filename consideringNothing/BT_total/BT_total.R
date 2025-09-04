# [Last Modified: 2025-05-05]
# 2024-08-09: Calculate BT considering nothing [Sector(x), Pollutants(x)]

library(dplyr)
library(readr)
library(parallel)
library(microbenchmark)

setwd("~/RAWorkingDirectory")


#---------------------------------
# Calculate BT considering nothing [Sector(x), Pollutants(x)]
#
# Args:
#   t_df: Total_trade_flows/Total_trade_flows.csv
#   gdp_df: GDP/GDP.csv
#   year: Year
#   country_i: Code of country_s
#   country_j: Code of country_r
#
# Returns:
#   BT: BT variable considering nothing [Sector(x), Pollutants(x)]
#---------------------------------

cal_BT <- function(t_df, gdp_df, year, country_i, country_j) {
  
  #----------------- Total GDP between Two Countries -----------------#
  
  gdp_i <- as.numeric(gdp_df[(gdp_df$Year == year) &
                               (gdp_df$Country == country_i), "GDP"])
  
  gdp_j <- as.numeric(gdp_df[(gdp_df$Year == year) &
                               (gdp_df$Country == country_j), "GDP"])
  
  gdp <- gdp_i + gdp_j
  
  
  #---------------- Trade flows between Two Countries ----------------#
  
  EX_ij <- sum(t_df[(t_df$Year == year) &
                      (t_df$Country_i == country_i) &
                      (t_df$Country_j == country_j), "Exports"])
  
  EX_ji <- sum(t_df[(t_df$Year == year) &
                      (t_df$Country_i == country_j) &
                      (t_df$Country_j == country_i), "Exports"])
  
  
  #-------------------------- Calculate BT --------------------------#
  
  BT <- (EX_ij + EX_ji) / gdp
  
  return (BT)
  
}


#---------------- Test Code ----------------#

# t_df <- read_csv("Eora_exports_pair_FromIndustryToCountry/Total_exports/Total_exports.csv")
# gdp_df <- read_csv("GDP/GDP.csv")
# 
# year <- 2022
# country_i <- "VNM"
# country_j <- "UZB"
# 
# print(cal_BT(t_df, gdp_df, year, country_i, country_j))
# 
# # Running Time of Function
# microbenchmark(
#   test = cal_BT(t_df, gdp_df, year, country_i, country_j),
#   times = 1000
# ) # --> 0.255 sec



#------------------------- Run Entire -------------------------# 

t_df <- read_csv("Eora_exports_pair_FromIndustryToCountry/Total_exports/Total_exports.csv")
gdp_df <- read_csv("GDP/GDP.csv")

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
country_lst2 <- country_lst


task <- function(year_lst) {
  
  #------------------- Create List -------------------#
  lst_length <- numeric(length(year_lst) * sum(1:length(country_lst)-1))
  
  BT_lst <- list("Year" = lst_length,
                 "Country_i" = lst_length,
                 "Country_j" = lst_length,
                 "BT_total" = lst_length)
  
  #------------------- Run Entire -------------------#
  index <- 1
  for (year in year_lst) {
    num <- 2
    
    for (i in country_lst[1:length(country_lst)-1]) {
      for (j in country_lst2[num:length(country_lst2)]) {
        BT <- cal_BT(t_df, gdp_df, year, i, j)
        
        BT_lst$Year[index] <- year
        BT_lst$Country_i[index] <- i
        BT_lst$Country_j[index] <- j
        BT_lst$BT_total[index] <- BT
        
        index <- index + 1
      }
      num <- num + 1
    }
    
    #--------- Copy Duplicate Tables & Save ---------#
    
    # list -> DataFrame
    BT_EachYear <- as.data.frame(BT_lst)
    
    # Replicate table which is interchanged country & Industry pair and remove duplicated rows
    BT_EachYear1 <- BT_EachYear
    BT_EachYear1[c("Country_i", "Country_j")] <- BT_EachYear[c("Country_j", "Country_i")]
    BT_EachYear2 <- rbind(BT_EachYear, BT_EachYear1)
    
    BT_EachYear2$Country_i <- factor(BT_EachYear2$Country_i, levels = country_lst)
    BT_EachYear2$Country_j <- factor(BT_EachYear2$Country_j, levels = country_lst)
    
    BT_EachYear3 <- BT_EachYear2 %>%
      arrange(Year, Country_i, Country_j)
    
    # Save it as a csv file
    write_csv(BT_EachYear3, paste0("consideringNothing/BT_total/BT_", 
                                   year_lst[1], "-", year_lst[3], ".csv"))
  }
}

clusterExport(cl, varlist = c("task", "year_lst", "country_lst", "country_lst2",
                              "cal_BT", "read_csv", "%>%", "arrange", "write_csv",
                              "t_df", "gdp_df"))


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
print(Sys.time()-time)  # --> 2.27 mins

