# [Last Modified: 2024-09-01]
# 2024-09-01: Calculate Network2nd considering sector

library(readr)
library(dplyr)
library(parallel)
library(microbenchmark)

setwd("~/RAWorkingDirectory")

#---------------------------------
# Calculate Network2nd
#
# Args:
#   df: T_sector/T_sector.csv
#   MB: MB_pollutants/MB_pollutants.csv
#   LB: LB_pollutants/LB_pollutants.csv
#   year: Year
#   country_i: Code of country_s
#   country_j: Code of country_r
#   sector_s: Sector of country_s (goods/services) 
#   sector_u: Sector of country_r (goods/services) 
#
# Returns:
#   Network2nd_MB: Network2nd(MB) considering sector
#   Network2nd_LB: Network2nd(LB) considering sector
#---------------------------------

cal_network2nd <- function(df, MB, LB, year, country_i, country_j, sector_s, sector_u) {
  
  #------------- Calculate vector of third country of country_is -------------#
  country_is_thirdCountry <- df[(df$Year == year) &
                                (df$Country_i == country_i) &
                                (df$Sector_s == sector_s) &
                                (df$Country_j != country_j), ]
  
  country_is_thirdCountry[, "Trade_flows"] <- sapply(country_is_thirdCountry[, "Trade_flows"], function(x) (x / sum(x)))
  country_is_thirdCountry <- country_is_thirdCountry %>% rename(Country_z = Country_j, Sector_z = Sector_u)
  
  
  #------------- Calculate vector of third country of country_ju -------------#
  country_ju_thirdCountry <- df[(df$Year == year) &
                                  (df$Country_i == country_j) &
                                  (df$Sector_s == sector_u) &
                                  (df$Country_j != country_i), ]
  
  country_ju_thirdCountry[, "Trade_flows"] <- sapply(country_ju_thirdCountry[, "Trade_flows"], function(x) (x / sum(x)))
  country_ju_thirdCountry <- country_ju_thirdCountry %>% rename(Country_j = Country_i, Sector_u = Sector_s, Country_y = Country_j, Sector_y = Sector_u)
  
  
  #------------- Create cartesian product vector & Merging vector into MB, LB -------------#
  cartesian_vec <- merge(country_is_thirdCountry[, c("Country_z", "Sector_z", "Trade_flows")], country_ju_thirdCountry[, c("Country_y", "Sector_y", "Trade_flows")], by = NULL)
  cartesian_vec <- subset(cartesian_vec, Country_z != Country_y)
  
  MB_df <- subset(MB, Year == year)
  LB_df <- subset(LB, Year == year)
  
  merging1 <- merge(cartesian_vec, MB_df %>% select(-Year), by.x = c("Country_z", "Country_y", "Sector_z", "Sector_y"), by.y = c("Country_i", "Country_j", "Sector_s", "Sector_u"), all = FALSE)
  merging2 <- merge(merging1, LB_df %>% select(-Year), by.x = c("Country_z", "Country_y", "Sector_z", "Sector_y"), by.y = c("Country_i", "Country_j", "Sector_s", "Sector_u"), all = FALSE)
  
  network2nd_MB <- sum(merging2[, "Trade_flows.x"] * merging2[, "Trade_flows.y"] * merging2[, "MB"])
  network2nd_LB <- sum(merging2[, "Trade_flows.x"] * merging2[, "Trade_flows.y"] * merging2[, "LB"])
  
  return (c(network2nd_MB, network2nd_LB))
}


#---------------- Test Code ----------------#

# df <- read_csv("T_sector/T_sector.csv")
# MB <- read_csv("consideringSector/MB/MB.csv")
# LB <- read_csv("consideringSector/LB/LB.csv")
# year <- 2022
# country_i <- "ARG"
# country_j <- "AGO"
# sector_s <- "services"
# sector_u <- "goods"
# 
# print(cal_network2nd(df, MB, LB, year, country_i, country_j, sector_s, sector_u))
# 
# # Running Time of Function
# microbenchmark(
#   test = cal_network2nd(df, MB, LB, year, country_i, country_j, sector_s, sector_u),
#   times = 1000
# ) # --> 0.44 sec


#------------------------- Run Entire -------------------------# 

df <- read_csv("T_sector/T_sector.csv")
MB <- read_csv("consideringSector/MB/MB.csv")
LB <- read_csv("consideringSector/LB/LB.csv")

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

# Sector list (goods and services)
sector_lst <- c("goods", "services")
sector_lst2 <- sector_lst


task <- function(year_lst) {
  
  #------------------- Create List -------------------#
  lst_length <- numeric(length(year_lst) * sum(1:length(country_lst)-1) * length(sector_lst) * length(sector_lst2))
  
  Network2nd_lst <- list("Year" = lst_length,
                         "Country_i" = lst_length,
                         "Country_j" = lst_length,
                         "Sector_s" = lst_length,
                         "Sector_u" = lst_length,
                         "MB_network2nd" = lst_length,
                         "LB_network2nd" = lst_length)
  
  #------------------- Run Entire -------------------#
  index <- 1
  for (year in year_lst) {
    num <- 2
    
    for (i in country_lst[1:length(country_lst)-1]) {
      for (j in country_lst2[num:length(country_lst2)]) {
        for (s in sector_lst) {
          for (u in sector_lst2) {
            Network2nd <- cal_network2nd(df, MB, LB, year, i, j, s, u)
            MB_network2nd <- Network2nd[1]
            LB_network2nd <- Network2nd[2]
            
            Network2nd_lst$Year[index] <- year
            Network2nd_lst$Country_i[index] <- i
            Network2nd_lst$Country_j[index] <- j
            Network2nd_lst$Sector_s[index] <- s
            Network2nd_lst$Sector_u[index] <- u
            Network2nd_lst$MB_network2nd[index] <- MB_network2nd
            Network2nd_lst$LB_network2nd[index] <- LB_network2nd
            
            index <- index + 1
          }
        }
      }
      num <- num + 1
    }
    
    #--------- Copy Duplicate Tables & Save ---------#
    
    # list -> DataFrame
    Network2nd_EachYear <- as.data.frame(Network2nd_lst)
    
    # Replicate table which is interchanged country & Industry pair and remove duplicated rows
    Network2nd_EachYear1 <- Network2nd_EachYear
    Network2nd_EachYear1[c("Country_i", "Country_j")] <- Network2nd_EachYear[c("Country_j", "Country_i")]
    Network2nd_EachYear1[c("Sector_s", "Sector_u")] <- Network2nd_EachYear[c("Sector_u", "Sector_s")]
    Network2nd_EachYear2 <- rbind(Network2nd_EachYear, Network2nd_EachYear1)
    Network2nd_EachYear3 <- unique(Network2nd_EachYear2)
    
    Network2nd_EachYear3$Country_i <- factor(Network2nd_EachYear3$Country_i, levels = country_lst)
    Network2nd_EachYear3$Country_j <- factor(Network2nd_EachYear3$Country_j, levels = country_lst)
    Network2nd_EachYear3$Sector_s <- factor(Network2nd_EachYear3$Sector_s, levels = sector_lst)
    Network2nd_EachYear3$Sector_u <- factor(Network2nd_EachYear3$Sector_u, levels = sector_lst)
    
    Network2nd_EachYear4 <- Network2nd_EachYear3 %>%
      arrange(Year, Country_i, Country_j, Sector_s, Sector_u)
    
    # Save it as a csv file
    write_csv(Network2nd_EachYear4, paste0("consideringSector/Network2nd/Network2nd_", 
                                           year_lst[1], "-", year_lst[3], ".csv"))
  }
}

clusterExport(cl, varlist = c("task", "year_lst", "country_lst", "country_lst2",
                              "sector_lst", "sector_lst2", "cal_network2nd", "rename",
                              "MB", "LB", "select", "read_csv", "%>%", "arrange", "write_csv", "df"))


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

