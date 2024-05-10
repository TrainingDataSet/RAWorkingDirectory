# [Last Modified: 2024-04-10]

library(dplyr)
library(readr)
library(parallel)

setwd("~/RAWorkingDirectory")

### Total bilateral trade flows for the entire period and country ###

# Year list (Entire year)
year_lst <- list(c(1990:1992), c(1993:1995), c(1996:1998), c(1999:2001), c(2002:2004), c(2005:2007), c(2008:2010),
                 c(2011:2013), c(2014:2016), c(2017:2019), c(2020:2022))
# year_lst <- c(2007:2020)

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
goods_lst <- paste0("c", 1:12)
services_lst <- paste0("c", 13:25)

from_industry_lst <- c(goods_lst, services_lst)
to_sector_lst <- list("goods" = goods_lst, "services" = services_lst)


### Trade flows from sub-industry in country_i to industry sector(goods/services) in country_j ###

#---------------------------------
# Calculate Exports
#
# Args:
#   df: Preprocessed Eora Data frame
#   country_i: Code of country_i 
#   country_j: Code of country_j
#   from_industry: Code of industry
#   to_sector: vector of industry sector(goods/services)
#
# Returns:
#   Trade_flows: Trade flows from sub-industry in country_i to industry sector(goods/services) in country_j
#---------------------------------

cal_exports <- function(df, country_i, country_j, from_industry, to_sector) {
  
  export <- df[(df$X2 == country_i) & (df$X3 == from_industry), which((df[3, ] == country_j) & (df[4, ] %in% to_sector))]
  export_sum <- sum(as.numeric(na.omit(export)))
  
  return (export_sum)
}  

#---------------- Test Code ----------------#

df <- read_csv("Eora_csv_files/Eora1992.csv")
test <- cal_exports(df, "KOR", "GHA", "c1", goods_lst)
print(test)


# time_vec <- vector("numeric", 10)
# 
# index <- 1
# for (i in 1:100) {
#   time <- Sys.time()
#   test2 <- cal_exports(df, "DZA", "AGO", "c1", goods_lst)
#   time_vec[index] <- Sys.time() - time
#   index <- index + 1
# }
# 
# print(mean(time_vec))  # --> 0.13 sec


# Make multi-thread
cl <- makeCluster(11)

task <- function(year_lst) {
  # Create list
  lst_length <- numeric(length(year_lst) * sum(1:length(country_lst)) * length(from_industry_lst) * length(to_sector_lst))
  trade_flows_lst <- list("Year" = lst_length, "Country_i" = lst_length, "Country_j" = lst_length, 
                          "From_industry" = lst_length, "To_sector" = lst_length, "Exports" = lst_length)

  # Read csv file with entire year
  index <- 1
  for (year in year_lst) {
    # Read csv file
    df <- read_csv(sprintf("Eora_csv_files/Eora%d.csv", year))
    cat(paste("#### [Collecting TradeFlows] Year: ", year, " ####\n", sep = ""))
    
    # Calculate trade flows with the function and append to list
    num <- 1
    for (i in country_lst) {
      for (j in country_lst[-num]) {
        for (k in from_industry_lst) {
          for (n in names(to_sector_lst)) {
            exports <- cal_exports(df, i, j, k, to_sector_lst[[n]])
            trade_flows_lst$Year[index] <- year
            trade_flows_lst$Country_i[index] <- i
            trade_flows_lst$Country_j[index] <- j
            trade_flows_lst$From_industry[index] <- k
            trade_flows_lst$To_sector[index] <- n
            trade_flows_lst$Exports[index] <- exports
            index <- index + 1
          }
        }
      }
      num <- num + 1
    }
    # list -> DataFrame
    trade_flows_df = data.frame(trade_flows_lst)
    
    # save as a csv file
    write_csv(trade_flows_df, paste0("Eora_exports/Eora_exports_", 
                                       year_lst[1], "-", year_lst[3], ".csv"))
  }

}


clusterExport(cl, varlist = c("task", "year_lst", "country_lst", "from_industry_lst", 
                              "to_sector_lst", "read_csv", "write_csv",
                              "cal_exports"))


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

# test code
time <- Sys.time()
parLapply(cl, tasks, function(f) f())
stopCluster(cl)
print(Sys.time()-time)

