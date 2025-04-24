# [Last Modified: 2024-10-06]
# 2024-10-06: Gross output of pair of (country, sector)

library(dplyr)
library(readr)
library(parallel)
library(microbenchmark)

setwd("~/RAWorkingDirectory")


#---------------------------------
# Calculate gross output
#
# Args:
#   df: Eora_csv_files/
#   year: Year
#   country: Code of country
#   sector: Vector of industry sector(goods/services)
#
# Returns:
#   Gross_output_sum: Gross output
#---------------------------------

cal_gross_output <- function(df, country, sector) {
  
  #------------------------- Calculate Gross Output -------------------------#
  
  gross_output <- df[4926, which(df[3, ] == country & df[4, ] %in% sector)]
  gross_output_sum <- sum(as.numeric(gross_output))

  
  return (gross_output_sum)
  
}


#---------------- Test Code ----------------#

# df <- read_csv("Eora_csv_files/Eora2010.csv")
# country <- "TZA"
# 
# # Industry list (goods and services)
# goods_lst <- paste0("c", 1:12)
# services_lst <- paste0("c", 13:25)
# sector <- goods_lst
# 
# print(cal_gross_output(df, country, sector))
# 
# # Running Time of Function
# microbenchmark(
#   test = cal_gross_output(df, country, sector),
#   times = 1000
# ) # --> 0.097 sec


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

# Industry list (goods and services)
goods_lst <- paste0("c", 1:12)
services_lst <- paste0("c", 13:25)
sector_lst <- list("goods" = goods_lst, "services" = services_lst)

task <- function(year_lst) {
  # Create list
  lst_length <- numeric(length(year_lst) * length(country_lst) * length(sector_lst))
  gross_output_lst <- list("Year" = lst_length, "Country" = lst_length, "Sector" = lst_length,
                           "Gross_output" = lst_length)
  
  # Read csv file with entire year
  index <- 1
  for (year in year_lst) {
    # Read csv file
    df <- read_csv(sprintf("Eora_csv_files/Eora%d.csv", year))
    
    # Calculate trade flows with the function and append to list
    for (country in country_lst) {
      for (sector in c("goods", "services")) {
        gross_output <- cal_gross_output(df, country, sector_lst[[sector]])
        gross_output_lst$Year[index] <- year
        gross_output_lst$Country[index] <- country
        gross_output_lst$Sector[index] <- sector
        gross_output_lst$Gross_output[index] <- gross_output
        index <- index + 1
      }
    }
    
    # list -> DataFrame
    gross_output_df = data.frame(gross_output_lst)
    
    # save as a csv file
    write_csv(gross_output_df, paste0("Gross_output/Gross_output_", 
                                      year_lst[1], "-", year_lst[3], ".csv"))
  }
  
}


clusterExport(cl, varlist = c("task", "year_lst", "country_lst", "sector_lst", 
                              "read_csv", "write_csv", "cal_gross_output"))


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



