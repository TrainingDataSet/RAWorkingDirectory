# [Last Modified: 2024-10-12]
# 2024-10-12: Exports of industry s of country i to foreign country j

library(dplyr)
library(readr)
library(parallel)
library(microbenchmark)

setwd("~/RAWorkingDirectory")


### Exports of industry s of country i to foreign country j ###

#---------------------------------
# Calculate Exports of industry s of country i to foreign country j
#
# Args:
#   df: Preprocessed Eora Data frame
#   country_i: Code of country_i 
#   country_j: Code of country_j
#
# Returns:
#   Tradeness: Exports of industry s of country i to foreign country j (ratio)
#---------------------------------

cal_tradeness <- function(df, country_i, country_j) {
  
  export <- df[which(df$X2 == country_i), which(df[3, ] == country_j)]
  export_sum <- rowSums(as.data.frame(lapply(export, as.numeric)))
  tradeness <- export_sum / sum(export_sum)
  
  return (tradeness)
}  

#---------------- Test Code ----------------#

df <- read_csv("Eora_csv_files/Eora2022.csv")
country_i <- "AFG"
country_j <- "ALB"

print(cal_tradeness(df, country_i, country_j))
# 
# # Running Time of Function
# microbenchmark(
#   test = cal_tradeness(df, country_i, country_j),
#   times = 1000
# ) # --> 0.09 sec


#------------------------- Run Entire -------------------------# 

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
country_lst <- c("DZA", "AGO", "ARG", "AUS")
country_lst2 <- country_lst

industry_lst <- paste0("c", c(1:26))


task <- function(year_lst) {
  # Create list
  lst_length <- numeric(length(year_lst) * sum(1:length(country_lst)-1) * length(industry_lst))
  Tradeness_lst <- list("Year" = lst_length, "Country_i" = lst_length, "Country_j" = lst_length,
                        "Industry_s" = lst_length, "Tradeness_index" = lst_length)
  
  # Read csv file with entire year
  index <- 1
  for (year in year_lst) {
    # Read csv file
    df <- read_csv(sprintf("Eora_csv_files/Eora%d.csv", year))
    
    # Calculate Tradeness with the function and append to list
    for (country_i in country_lst) {
      for (country_j in country_lst2) {
        if (country_i == country_j) 
          next
        tradeness <- cal_tradeness(df, country_i, country_j)
        Tradeness_lst$Year[index:(index+25)] <- rep(year, 26)
        Tradeness_lst$Country_i[index:(index+25)] <- rep(country_i, 26)
        Tradeness_lst$Country_j[index:(index+25)] <- rep(country_j, 26)
        Tradeness_lst$Industry_s[index:(index+25)] <- industry_lst
        Tradeness_lst$Tradeness_index[index:(index+25)] <- tradeness
        index <- index + 26
      }
    }
    
    # list -> DataFrame
    Tradeness_df = data.frame(Tradeness_lst)
    
    # save as a csv file
    write_csv(Tradeness_df, paste0("Tradeness/Tradeness_",
    year_lst[1], "-", year_lst[3], ".csv"))
  }
  
}


clusterExport(cl, varlist = c("task", "year_lst", "country_lst", "country_lst2", 
                              "industry_lst", "read_csv", "write_csv", "cal_tradeness"))


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

