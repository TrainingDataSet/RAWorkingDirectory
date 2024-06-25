# [Last Modified: 2024-06-25]

library(dplyr)
library(readr)
library(parallel)

setwd("~/RAWorkingDirectory")

#---------------------------------
# Calculate exports in specific industry of country
#
# Args:
#   df: Eora_exports.csv
#   year: Year
#   country: Code of country_i
#   industry: Vector of industry (goods/services)
#
# Returns:
#   exports: exports in specific industry of country
#---------------------------------

cal_exports <- function(df, year, country, industry) {
  
  #----------------- Calculate exports in specific industry of country  -----------------#
  
  exports <- sum(df[(df$Year == year) &
                    (df$Country_i == country) &
                    (df$From_industry == industry), "Exports"])
  
  return (exports)
}



#---------------- Test Code ----------------#

# df <- read_csv("Eora_exports/Eora_exports.csv")
# country = "KOR"
# industry = "c1"
# test <- cal_exports(df, year, country, industry)
# print(test)
# 
# Running Time of Function
# time_vec <- vector("numeric", 100)
# index <- 1
# for (i in 1:100) {
#   time <- Sys.time()
#   test <- cal_exports(df, year, country, industry)
#   time_vec[index] <- Sys.time() - time
#   index <- index + 1
# }
# 
# print(mean(time_vec))  # --> 0.32 sec



#------------------------- Run Entire -------------------------# 

df <- read_csv("Eora_exports/Eora_exports.csv")

cl <- makeCluster(11)

year_lst <- list(c(1990:1992), c(1993:1995), c(1996:1998), c(1999:2001), c(2002:2004), c(2005:2007), c(2008:2010),
                 c(2011:2013), c(2014:2016), c(2017:2019), c(2020:2022))

country_lst <- c("DZA", "AGO", "ARG", "AUS", "AUT", "AZE", "BHR", "BGD", "BLR", "BEL", "BOL", "BIH", "BRA", "BGR", "KHM", "CAN", "CHL", "CHN", "COL", "COG", "CRI",
                 "HRV", "CUB", "CYP", "CZE", "DNK", "DOM", "ECU", "EGY", "SLV", "EST", "ETH", "FIN", "FRA", "GEO", "DEU", "GHA", "GRC", "GTM", "HND", "HKG", "HUN",
                 "ISL", "IND", "IDN", "IRN", "IRQ", "IRL", "ISR", "ITA", "JPN", "KAZ", "KEN", "KWT", "LVA", "LBN", "LTU", "LUX", "MYS", "MLT", "MEX", "MAR", "NLD",
                 "NZL", "NOR", "OMN", "PAK", "PRY", "PER", "PHL", "POL", "PRT", "QAT", "KOR", "ROU", "RUS", "SAU", "SRB", "SGP", "SVK", "SVN", "ZAF", "ESP", "LKA",
                 "SWE", "CHE", "THA", "TUN", "TUR", "UKR", "ARE", "GBR", "TZA", "USA", "URY", "UZB", "VEN", "VNM")
industry_lst <- c(paste0("c", 1:25))


lst_length <- numeric(length(year_lst) * length(country_lst) * length(industry_lst))

task <- function(year_lst) {
  
  #------------------- Create List -------------------#
  lst_length <- numeric(length(year_lst) * length(country_lst) * length(industry_lst))
  
  EX_lst <- list("Year" = lst_length,
                 "Country" = lst_length,
                 "Industry" = lst_length,
                 "Exports" = lst_length)
  
  #------------------- Run Entire -------------------#
  index <- 1
  for (year in year_lst) {
    for (country in country_lst) {
      for (industry in industry_lst) {
        EX <- cal_exports(df, year, country, industry)
        EX_lst$Year[index] <- year
        EX_lst$Country[index] <- country
        EX_lst$Industry[index] <- industry
        EX_lst$Exports[index] <- EX
        
        index <- index + 1
      }
    }
    
    #--------- Save as a csv file ---------#
    
    # list -> DataFrame
    EX_EachYear <- as.data.frame(EX_lst)
    
    # Save it as a csv file
    write_csv(EX_EachYear, paste0("Eora_exports_EachIndustry/Eora_exports_EachIndustry_", 
                                   year_lst[1], "-", year_lst[3], ".csv"))
  }
}

clusterExport(cl, varlist = c("task", "year_lst", "country_lst", "industry_lst",
                              "cal_exports", "read_csv", "write_csv", "df"))


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



