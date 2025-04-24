# [Last Modified: 2024-08-05]
# 2024-08-05: Calculate LB considering pollutants

library(readr)
library(dplyr)
library(parallel)
library(microbenchmark)

setwd("~/RAWorkingDirectory")

#---------------------------------
# Calculate MB considering pollutants
#
# Args:
#   df: T_pollutants/T_pollutants.csv
#   year: Year
#   country_i: Code of country_i
#   country_j: Code of country_j
#
# Returns:
#   LB: LB variable considering pollutants
#---------------------------------

cal_LB <- function(df, year, country_i, country_j, oecd) {
  
  #----------------- Left-Hand Side of MB_network_1st  -----------------#
  
  except_j <- df[(df$Year == year) &
                   (df$Country_i == country_i) &
                   (df$Country_j != country_j), ]
  
  except_j <- except_j %>%
    group_by(Year, Country_i, Country_j) %>%
    summarise(T_CO2 = sum(T_CO2), T_GHG = sum(T_GHG), .groups = "drop")
  
  sum_except_j <- as.numeric(colSums(except_j[c("T_CO2", "T_GHG")]))
  
  # Considering only OECD
  except_j <- except_j[except_j$Country_j %in% oecd, ]
  
  lhs <- sweep(except_j[c("T_CO2", "T_GHG")], 2, sum_except_j, "/")
  
  
  #----------------- Right-Hand Side of MB_network_1st  -----------------#
  
  except_i <- df[(df$Year == year) &
                   (df$Country_i == country_j) &
                   (df$Country_j != country_i), ]
  
  except_i <- except_i %>%
    group_by(Year, Country_i, Country_j) %>%
    summarise(T_CO2 = sum(T_CO2), T_GHG = sum(T_GHG), .groups = "drop")
  
  sum_except_i <- as.numeric(colSums(except_i[c("T_CO2", "T_GHG")]))
  
  # Considering only OECD
  except_i <- except_i[except_i$Country_j %in% oecd, ]
  
  rhs <- sweep(except_i[c("T_CO2", "T_GHG")], 2, sum_except_i, "/")
  
  
  #----------------- Final Result of LB_network_1st  -----------------#
  
  LB <- 1 - (colSums(abs(lhs - rhs)) / 2)
  LB_CO2 <- as.numeric(LB[1])
  LB_GHG <- as.numeric(LB[2])
  
  
  return (c(LB_CO2, LB_GHG))
  
}


#---------------- Test Code ----------------#

# df <- read_csv("T_pollutants/T_pollutants.csv")
# year <- 2022
# country_i <- "VNM"
# country_j <- "VEN"
# oecd <- c("AUT", "BEL", "CAN", "DNK", "FRA", "DEU", "GRC", "ISL", "IRL", "ITA",
#           "LUX", "NLD", "NOR", "PRT", "ESP", "SWE", "CHE", "TUR", "GBR", "USA",
#           "JPN", "FIN", "AUS", "NZL", "MEX", "CZE", "HUN", "POL", "KOR", "SVK",
#           "CHL", "SVN", "EST", "ISR", "LVA", "LTU", "COL", "CRI")
#
# print(cal_LB(df, year, country_i, country_j, oecd))
# 
# # Running Time of Function
# microbenchmark(
#   test = cal_LB(df, year, country_i, country_j, oecd),
#   times = 1000
# )  # --> 0.054 sec


#------------------------- Run Entire -------------------------# 

df <- read_csv("T_pollutants/T_pollutants.csv")

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
oecd <- c("AUT", "BEL", "CAN", "DNK", "FRA", "DEU", "GRC", "ISL", "IRL", "ITA",
          "LUX", "NLD", "NOR", "PRT", "ESP", "SWE", "CHE", "TUR", "GBR", "USA",
          "JPN", "FIN", "AUS", "NZL", "MEX", "CZE", "HUN", "POL", "KOR", "SVK",
          "CHL", "SVN", "EST", "ISR", "LVA", "LTU", "COL", "CRI")


task <- function(year_lst) {
  
  #------------------- Create List -------------------#
  lst_length <- numeric(length(year_lst) * sum(1:length(country_lst)-1))
  
  LB_lst <- list("Year" = lst_length,
                 "Country_i" = lst_length,
                 "Country_j" = lst_length,
                 "LB_CO2_oecd" = lst_length,
                 "LB_GHG_oecd" = lst_length
  )
  
  #------------------- Run Entire -------------------#
  index <- 1
  for (year in year_lst) {
    num <- 2
    
    for (i in country_lst[1:length(country_lst)-1]) {
      for (j in country_lst2[num:length(country_lst2)]) {
        LB <- cal_LB(df, year, i, j, oecd)
        LB_CO2 <- LB[1]
        LB_GHG <- LB[2]
        
        LB_lst$Year[index] <- year
        LB_lst$Country_i[index] <- i
        LB_lst$Country_j[index] <- j
        LB_lst$LB_CO2_oecd[index] <- LB_CO2
        LB_lst$LB_GHG_oecd[index] <- LB_GHG
        
        index <- index + 1
      }
      num <- num + 1
    }
    
    #--------- Copy Duplicate Tables & Save ---------#
    
    # list -> DataFrame
    LB_EachYear <- as.data.frame(LB_lst)
    
    # Replicate table which is interchanged country pair
    LB_EachYear1 <- LB_EachYear
    LB_EachYear1[c("Country_i", "Country_j")] <- LB_EachYear[c("Country_j", "Country_i")]
    LB_EachYear2 <- rbind(LB_EachYear, LB_EachYear1)
    
    LB_EachYear2$Country_i <- factor(LB_EachYear2$Country_i, levels = country_lst)
    LB_EachYear2$Country_j <- factor(LB_EachYear2$Country_j, levels = country_lst)
    
    LB_EachYear3 <- LB_EachYear2 %>%
      arrange(Year, Country_i, Country_j)
    
    # Save it as a csv file
    write_csv(LB_EachYear3, paste0("consideringPollutantsOECD/LB/LB_pollutants_oecd", 
                                   year_lst[1], "-", year_lst[3], ".csv"))
  }
}

clusterExport(cl, varlist = c("task", "year_lst", "country_lst", "country_lst2",
                              "cal_LB", "read_csv", "%>%", "arrange", "write_csv", 
                              "select", "summarise", "group_by", "df", "oecd"))


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
print(Sys.time()-time)  # --> 19.33 mins
