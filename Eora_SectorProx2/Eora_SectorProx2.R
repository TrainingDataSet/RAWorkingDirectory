# [Last Modified: 2024-07-02]
# 2024-02-03: Excluding domestic consumption when calculating exports
# 2024-06-23: MRIO -> Eora, Considering sector(goods/services)
# 2024-07-02: Considering pair of sector(Sector_s/Sector_u)

library(dplyr)
library(readr)
library(parallel)

setwd("~/RAWorkingDirectory")

#---------------------------------
# Calculate exports in specific industry of country
#
# Args:
#   df1: Eora_SectorProx.csv
#   df2: Eora_exports_EachSector.csv
#   year: Year
#   country_i: Code of country_i
#   country_j: Code of country_j
#   sector_s: "goods" or "services"
#   sector_u: "goods" or "services"
#
# Returns:
#   sector_prox: Using exports considering pair of sectors but not pollutants
#---------------------------------

cal_sector_prox2 <- function(df1, df2, year, country_i, country_j, sector_s, sector_u) {
  
  #--------- Enter the same value if sector_s and sector_u are the same ---------#
  
  if (sector_s == sector_u) {
      sector_prox <- df1[(df1$Year == year) &
                         (df1$Country_i == country_i) &
                         (df1$Country_j == country_j) &
                         (df1$Sector == sector_s), "Sector_prox2"]
      
      return (as.numeric(sector_prox))
  } 
  
  #--------------------- Value of exports sector to sector ---------------------#
  
  i_goods_j_services <- df2[(df2$Year == year) &
                            (df2$Country_i == country_i) &
                            (df2$Country_j == country_j) &
                            (df2$Sector_s == "goods") &
                            (df2$Sector_u == "services"), "Exports"]
  
  j_services_i_goods <- df2[(df2$Year == year) &
                            (df2$Country_i == country_j) &
                            (df2$Country_j == country_i) &
                            (df2$Sector_s == "services") &
                            (df2$Sector_u == "goods"), "Exports"]
  
  i_services_j_goods <- df2[(df2$Year == year) &
                            (df2$Country_i == country_i) &
                            (df2$Country_j == country_j) &
                            (df2$Sector_s == "services") &
                            (df2$Sector_u == "goods"), "Exports"]
  
  j_goods_i_services <- df2[(df2$Year == year) &
                            (df2$Country_i == country_j) &
                            (df2$Country_j == country_i) &
                            (df2$Sector_s == "goods") &
                            (df2$Sector_u == "services"), "Exports"]
  
  
  #-------------- Sector prox for the same sector(goods/services) --------------#
  
  gg_prox <- df1[(df1$Year == year) &
                 (df1$Country_i == country_i) &
                 (df1$Country_j == country_j) &
                 (df1$Sector == "goods"), "Sector_prox2"]
  
  ss_prox <- df1[(df1$Year == year) &
                 (df1$Country_i == country_i) &
                 (df1$Country_j == country_j) &
                 (df1$Sector == "services"), "Sector_prox2"]
  
  
  #------- Calculate sector_prox for the different sector(goods/services) -------#
  
  if (sector_s == "goods") {
    sector_prox = (i_goods_j_services / (i_goods_j_services + j_services_i_goods)) * gg_prox +
                  (j_services_i_goods / (i_goods_j_services + j_services_i_goods)) * ss_prox
  } else {
    sector_prox = (i_services_j_goods / (i_services_j_goods + j_goods_i_services)) * ss_prox +
                  (j_goods_i_services / (i_services_j_goods + j_goods_i_services)) * gg_prox
  }
  
  return (as.numeric(sector_prox))
  
}


#---------------- Test Code ----------------#

# df1 <- read_csv("Eora_SectorProx/Eora_SectorProx.csv")
# df2 <- read_csv("Eora_exports_EachSector/Eora_exports_EachSector.csv")
# year <- 1990
# country_i <- "KOR"
# country_j <- "JPN"
# 
# sector_s = "goods"
# sector_u = "services"
# 
# print(cal_sector_prox2(df1, df2, year, country_i, country_j, sector_s, sector_u))
# 
# # Running Time of Function
# time_vec <- vector("numeric", 100)
# index <- 1
# for (i in 1:100) {
#   time <- Sys.time()
#   test <- cal_sector_prox2(df1, df2, year, country_i, country_j, sector_s, sector_u)
#   time_vec[index] <- Sys.time() - time
#   index <- index + 1
# }
# print(mean(time_vec))  # --> 0.16 sec


#------------------------- Run Entire -------------------------# 

df1 <- read_csv("Eora_SectorProx/Eora_SectorProx.csv")
df2 <- read_csv("Eora_exports_EachSector/Eora_exports_EachSector.csv")

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

# Industry list (goods and services)
sector_lst <- c("goods", "services")
sector_lst2 <- sector_lst

task <- function(year_lst) {

  lst_length <- numeric(length(year_lst) * length(country_lst) * (length(country_lst2)-1) * length(sector_lst) * length(sector_lst2))
  
  Sector_prox_lst <- list("Year" = lst_length,
                          "Country_i" = lst_length,
                          "Country_j" = lst_length,
                          "Sector_s" = lst_length,
                          "Sector_u" = lst_length,
                          "Sector_prox2" = lst_length)
  
  index <- 1
  for (year in year_lst) {
    for (i in country_lst) {
      for (j in country_lst2) {
        if (i == j) {next}
        for (s in sector_lst) {
          for (u in sector_lst2) {
            sector_prox2 <- cal_sector_prox2(df1, df2, year, i, j, s, u)
            
            Sector_prox_lst$Year[index] <- year
            Sector_prox_lst$Country_i[index] <- i
            Sector_prox_lst$Country_j[index] <- j
            Sector_prox_lst$Sector_s[index] <- s
            Sector_prox_lst$Sector_u[index] <- u
            Sector_prox_lst$Sector_prox2[index] <- sector_prox2
            
            index <- index + 1
          }
        }
      }
    }
    
    #--------- Save it as a csv file ---------#
    
    # list -> DataFrame
    SectorProx_EachYear <- as.data.frame(Sector_prox_lst)
    
    # Save it as a csv file
    write_csv(SectorProx_EachYear, paste0("Eora_SectorProx2/Eora_SectorProx2_",
                                  year_lst[1], "-", year_lst[3], ".csv"))
  
  }
  
}

clusterExport(cl, varlist = c("task", "year_lst", "country_lst", "country_lst2",
                              "sector_lst", "sector_lst2", "cal_sector_prox2",
                              "read_csv", "write_csv", "df1", "df2"))


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
