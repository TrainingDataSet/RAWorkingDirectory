# [Last Modified: 2024-06-30]

library(dplyr)
library(readr)
library(parallel)

setwd("~/RAWorkingDirectory")

#---------------------------------
# Calculate exports in sector to sector
#
# Args:
#   df: Eora_exports/Eora_exports.csv
#   sector_lst: A list containing the industries of each sector (goods/services)
#   year: Year
#   country_i: Code of country_i
#   country_j: Code of country_j
#   sector_s: "goods" or "services"
#   sector_u: "goods" or "services"
#
# Returns:
#   exports: Exports in sector to sector
#---------------------------------

cal_exports_EachSector <- function(df, sector_lst, year, country_i, country_j, sector_s, sector_u) {
  
  #----------------- Calculate exports in sector to sector -----------------#
  
  exports <- sum(df[(df$Year == year) &
                    (df$Country_i == country_i) &
                    (df$Country_j == country_j) &
                    (df$From_industry %in% sector_lst[[sector_s]]) &
                    (df$To_sector == sector_u), "Exports"])

  return (exports)
  
}


#---------------- Test Code ----------------#

# df <- read_csv("Eora_exports/Eora_exports.csv")
# year <- 2002
# country_i <- "KOR"
# country_j <- "CHN"
# 
# # Industry list (goods and services)
# goods_lst <- paste0("c", 1:12)
# services_lst <- paste0("c", 13:25)
# sector_lst <- list("goods" = goods_lst, "services" = services_lst)
# sector_s <- "services"
# sector_u <- "goods"
# 
# cal_exports_EachSector(df, sector_lst, year, country_i, country_j, sector_s, sector_u)
# 
# # Running Time of Function
# time_vec <- vector("numeric", 100)
# index <- 1
# for (i in 1:100) {
#   time <- Sys.time()
#   test <- cal_exports_EachSector(df, sector_lst, year, country_i, country_j, sector_s, sector_u)
#   time_vec[index] <- Sys.time() - time
#   index <- index + 1
# }
# print(mean(time_vec))  # --> 0.68 sec


#------------------------- Run Entire -------------------------# 

df <- read_csv("Eora_exports/Eora_exports.csv")

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
goods_lst <- paste0("c", 1:12)
services_lst <- paste0("c", 13:25)
sector_lst <- list("goods" = goods_lst, "services" = services_lst)

task <- function(year_lst) {
  
  #------------------- Create List -------------------#
  lst_length <- numeric(length(year_lst) * length(country_lst) * (length(country_lst2)-1) * length(c("goods", "services")) * length(c("goods", "services")))
  
  Exports_lst <- list("Year" = lst_length,
                      "Country_i" = lst_length,
                      "Country_j" = lst_length,
                      "Sector_s" = lst_length,
                      "Sector_u" = lst_length,
                      "Exports" = lst_length)
  
  #------------------- Run Entire -------------------#
  index <- 1
  for (year in year_lst) {
    for (i in country_lst) {
      for (j in country_lst2) {
        if (i == j) {next}
        for (s in c("goods", "services")) {
          for (u in c("goods", "services")) {
            Exports <- cal_exports_EachSector(df, sector_lst, year, i, j, s, u)
            
            Exports_lst$Year[index] <- year
            Exports_lst$Country_i[index] <- i
            Exports_lst$Country_j[index] <- j
            Exports_lst$Sector_s[index] <- s
            Exports_lst$Sector_u[index] <- u
            Exports_lst$Exports[index] <- Exports
            
            index <- index + 1
          }
        }
      }
    }
    
    #--------- Save it as a csv file ---------#
    
    # list -> DataFrame
    Ex_EachYear <- as.data.frame(Exports_lst)
  
    # Save it as a csv file
    write_csv(Ex_EachYear, paste0("Eora_exports_EachSector/Eora_exports_EachSector_", 
                                   year_lst[1], "-", year_lst[3], ".csv"))
  
  }
}

clusterExport(cl, varlist = c("task", "year_lst", "country_lst", "country_lst2",
                              "sector_lst", "cal_exports_EachSector",
                              "read_csv", "write_csv", "df"))


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
