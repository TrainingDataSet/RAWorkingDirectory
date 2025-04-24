# [Last Modified: 2024-10-05]
# 2024-10-05: Intermediate inputs of pair of (country, sector)

library(dplyr)
library(readr)
library(parallel)
library(microbenchmark)

setwd("~/RAWorkingDirectory")


#---------------------------------
# Calculate intermediate inputs
#
# Args:
#   df: Eora_csv_files/
#   year: Year
#   country: Code of country
#   sector: Vector of industry sector(goods/services)
#
# Returns:
#   inter_input_sum: Intermediate inputs
#---------------------------------

cal_intermediate_inputs <- function(df, country, sector) {
  
  #--------------------- Calculate Intermediate Inputs ---------------------#
  
  inter_input <- df[5:4919, which(df[3, ] == country & df[4, ] %in% sector)]
  inter_input_sum <- sum(data.frame(lapply(inter_input, as.numeric)))
  
  
  return (inter_input_sum)
  
}


#---------------- Test Code ----------------#

# df <- read_csv("Eora_csv_files/Eora1990.csv")
# country <- "AFG"
# 
# # Industry list (goods and services)
# goods_lst <- paste0("c", 1:12)
# services_lst <- paste0("c", 13:25)
# sector <- services_lst
# 
# print(cal_intermediate_inputs(df, country, sector))
# 
# # Running Time of Function
# microbenchmark(
#   test = cal_intermediate_inputs(df, country, sector),
#   times = 1000
# ) # --> 0.123 sec


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
  inter_inputs_lst <- list("Year" = lst_length, "Country" = lst_length, "Sector" = lst_length,
                          "Inter_inputs" = lst_length)
  
  # Read csv file with entire year
  index <- 1
  for (year in year_lst) {
    # Read csv file
    df <- read_csv(sprintf("Eora_csv_files/Eora%d.csv", year))
    
    # Calculate trade flows with the function and append to list
    for (country in country_lst) {
          for (sector in c("goods", "services")) {
            inter_inputs <- cal_intermediate_inputs(df, country, sector_lst[[sector]])
            inter_inputs_lst$Year[index] <- year
            inter_inputs_lst$Country[index] <- country
            inter_inputs_lst$Sector[index] <- sector
            inter_inputs_lst$Inter_inputs[index] <- inter_inputs
            index <- index + 1
          }
        }

    # list -> DataFrame
    inter_inputs_df = data.frame(inter_inputs_lst)
    
    # save as a csv file
    write_csv(inter_inputs_df, paste0("Inter_inputs/Inter_inputs_", 
                                     year_lst[1], "-", year_lst[3], ".csv"))
  }
  
}


clusterExport(cl, varlist = c("task", "year_lst", "country_lst", "sector_lst", 
                              "read_csv", "write_csv", "cal_intermediate_inputs"))


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


