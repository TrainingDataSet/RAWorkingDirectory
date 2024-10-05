# [Last Modified: 2024-08-18]
# 2024-08-18: Calculate Network2nd considering pollutants

library(readr)
library(dplyr)
library(parallel)
library(microbenchmark)

setwd("~/RAWorkingDirectory")

#---------------------------------
# Calculate Network2nd
#
# Args:
#   df: T_pollutants/T_pollutants.csv
#   MB_df: consideringPollutants/MB/MB.csv
#   LB_df: consideringPollutants/LB/LB.csv
#   year: Year
#   country_i: Code of country_i
#   country_j: Code of country_j
#
# Returns:
#   Network2nd_MB: Network2nd(MB) variable considering pollutants
#   Network2nd_LB: Network2nd(LB) variable considering pollutants
#---------------------------------

cal_network2nd <- function(df, MB_df, LB_df, year, country_i, country_j) {
  
  #------------- Calculate vector of third country of country_i -------------#
  country_i_thirdCountry <- df[(df$Year == year) &
                                 (df$Country_i == country_i) &
                                 (df$Country_j != country_j), ]
  
  country_i_thirdCountry <- country_i_thirdCountry %>%
    group_by(Country_i, Country_j) %>%
    summarise(T_CO2 = sum(T_CO2), T_GHG = sum(T_GHG), .groups = "drop")
  
  country_i_thirdCountry[, c("T_CO2", "T_GHG")] <- sapply(country_i_thirdCountry[, c("T_CO2", "T_GHG")], function(x) (x / sum(x)))
  country_i_thirdCountry <- country_i_thirdCountry %>% rename(Country_z = Country_j)
  
  
  #------------- Calculate vector of third country of country_j -------------#
  country_j_thirdCountry <- df[(df$Year == year) &
                                 (df$Country_i == country_j) &
                                 (df$Country_j != country_i), ]
  
  country_j_thirdCountry <- country_j_thirdCountry %>%
    group_by(Country_i, Country_j) %>%
    summarise(T_CO2 = sum(T_CO2), T_GHG = sum(T_GHG), .groups = "drop")
  
  country_j_thirdCountry[, c("T_CO2", "T_GHG")] <- sapply(country_j_thirdCountry[, c("T_CO2", "T_GHG")], function(x) (x / sum(x)))
  country_j_thirdCountry <- country_j_thirdCountry %>% rename(Country_j = Country_i, Country_y = Country_j)
  
  
  #------------- Create cartesian product vector & Merging vector into MB, LB -------------#
  cartesian_vec <- merge(country_i_thirdCountry[, c("Country_z", "T_CO2", "T_GHG")], country_j_thirdCountry[, c("Country_y", "T_CO2", "T_GHG")], by = NULL)
  cartesian_vec <- subset(cartesian_vec, Country_z != Country_y)
  
  MB_df2 <- subset(MB_df, Year == year)
  LB_df2 <- subset(LB_df, Year == year)
  
  merging1 <- merge(cartesian_vec, MB_df2 %>% select(-Year), by.x = c("Country_z", "Country_y"), by.y = c("Country_i", "Country_j"), all = FALSE)
  merging2 <- merge(merging1, LB_df2 %>% select(-Year), by.x = c("Country_z", "Country_y"), by.y = c("Country_i", "Country_j"), all = FALSE)
  
  network2nd_MB <- colSums(merging2[, c("T_CO2.x", "T_GHG.x")] * merging2[, c("T_CO2.y", "T_GHG.y")] * merging2[, c("MB_CO2", "MB_GHG")])
  network2nd_LB <- colSums(merging2[, c("T_CO2.x", "T_GHG.x")] * merging2[, c("T_CO2.y", "T_GHG.y")] * merging2[, c("LB_CO2", "LB_GHG")])
  
  return (c(network2nd_MB[[1]], network2nd_MB[[2]], network2nd_LB[[1]], network2nd_LB[[2]]))
}


#---------------- Test Code ----------------#

# df <- read_csv("T_pollutants/T_pollutants.csv")
# MB_df <- read_csv("consideringPollutants/MB/MB.csv")
# LB_df <- read_csv("consideringPollutants/LB/LB.csv")
# year <- 2022
# country_i <- "AUT"
# country_j <- "AUS"
# 
# print(cal_network2nd(df, MB_df, LB_df, year, country_i, country_j))
# 
# # Running Time of Function
# microbenchmark(
#   test = cal_network2nd(df, MB_df, LB_df, year, country_i, country_j),
#   times = 1000
# ) # --> 0.131 sec


#------------------------- Run Entire -------------------------# 

df <- read_csv("T_pollutants/T_pollutants.csv")
MB_df <- read_csv("consideringPollutants/MB/MB.csv")
LB_df <- read_csv("consideringPollutants/LB/LB.csv")

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
  
  Network2nd_lst <- list("Year" = lst_length,
                         "Country_i" = lst_length,
                         "Country_j" = lst_length,
                         "MB_network2nd_CO2" = lst_length,
                         "MB_network2nd_GHG" = lst_length,
                         "LB_network2nd_CO2" = lst_length,
                         "LB_network2nd_GHG" = lst_length)
  
  #------------------- Run Entire -------------------#
  index <- 1
  for (year in year_lst) {
    num <- 2
    
    for (i in country_lst[1:length(country_lst)-1]) {
      for (j in country_lst2[num:length(country_lst2)]) {
        Network2nd <- cal_network2nd(df, MB_df, LB_df, year, i, j)
        MB_CO2 <- Network2nd[1]
        MB_GHG <- Network2nd[2]
        LB_CO2 <- Network2nd[3]
        LB_GHG <- Network2nd[4]
        
        Network2nd_lst$Year[index] <- year
        Network2nd_lst$Country_i[index] <- i
        Network2nd_lst$Country_j[index] <- j
        Network2nd_lst$MB_network2nd_CO2[index] <- MB_CO2
        Network2nd_lst$MB_network2nd_GHG[index] <- MB_GHG
        Network2nd_lst$LB_network2nd_CO2[index] <- LB_CO2
        Network2nd_lst$LB_network2nd_GHG[index] <- LB_GHG
        
        index <- index + 1
      }
      num <- num + 1
    }
    
    #--------- Copy Duplicate Tables & Save ---------#
    
    # list -> DataFrame
    Network2nd_EachYear <- as.data.frame(Network2nd_lst)
    
    # Replicate table which is interchanged country & Industry pair and remove duplicated rows
    Network2nd_EachYear1 <- Network2nd_EachYear
    Network2nd_EachYear1[c("Country_i", "Country_j")] <- Network2nd_EachYear[c("Country_j", "Country_i")]
    Network2nd_EachYear2 <- rbind(Network2nd_EachYear, Network2nd_EachYear1)
    
    Network2nd_EachYear2$Country_i <- factor(Network2nd_EachYear2$Country_i, levels = country_lst)
    Network2nd_EachYear2$Country_j <- factor(Network2nd_EachYear2$Country_j, levels = country_lst)
    
    Network2nd_EachYear3 <- Network2nd_EachYear2 %>%
      arrange(Year, Country_i, Country_j)
    
    # Save it as a csv file
    write_csv(Network2nd_EachYear3, paste0("consideringPollutants/Network2nd/Network2nd_", 
                                           year_lst[1], "-", year_lst[3], ".csv"))
  }
}

clusterExport(cl, varlist = c("task", "year_lst", "country_lst", "country_lst2",
                              "cal_network2nd", "rename", "MB_df", "LB_df", "select", 
                              "read_csv", "%>%", "arrange", "write_csv", "df",
                              "group_by", "summarise"))


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

