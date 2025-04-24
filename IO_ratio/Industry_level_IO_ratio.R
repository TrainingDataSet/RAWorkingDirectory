# [Last Modified: 2024-10-08]
# 2024-10-08: Intermediate inputs & Gross output of pair of (country, industry)

library(dplyr)
library(readr)
library(parallel)
library(microbenchmark)

setwd("~/RAWorkingDirectory")


#---------------------------------
# Calculate Intermediate inputs & Gross Output
#
# Args:
#   df: Eora_csv_files/
#   year: Year
#   country: Code of country
#   industry: Industries(c1, c2, ..., c26)
#
# Returns:
#   inter_input_sum: Intermediate inputs
#---------------------------------

cal_IO <- function(df, country) {
  
  c_index <- which(df[3, ] == country & grepl("^c", df[4, ]))
  
  #-------------------- Inter Inputs / Gross Output --------------------#
  
  inter_input <- as.numeric(df[4928, c_index])
  gross_output <- as.numeric(df[4926, c_index])
  
  IO_ratio <- inter_input / gross_output
  
  
  #------------ Sector-level VAD / Sum of Sector-level VAD ------------#
  
  vad <- as.numeric(df[4927, c_index])
  vad_sum <- sum(vad)
  
  VAD_ratio <- vad / vad_sum
  
  
  #------------ IO ratio * VAD_ratio ------------#
  
  IO_weighted_VAD <- IO_ratio * VAD_ratio
  
  
  return (IO_weighted_VAD)
  
}


#---------------- Test Code ----------------#

df <- read_csv("Eora_csv_files/Eora1990.csv")
Gross_output <- as.numeric(df[4926, 4:4918])
Value_added <- as.numeric(df[4927, 4:4918])

inter_inputs <- Gross_output - Value_added
df[4928, 1:4918] <- as.list(c("Intermediate inputs", NA, NA, as.character(inter_inputs)))

country <- "AZE"

print(cal_IO(df, country))
# 
# # Running Time of Function
# microbenchmark(
#   test = cal_IO(df, country),
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
industry_lst <- paste0("c", c(1:26))


task <- function(year_lst) {
  # Create list
  lst_length <- numeric(length(year_lst) * length(country_lst) * length(industry_lst))
  IO_lst <- list("Year" = lst_length, "Country" = lst_length, "Industry" = lst_length,
                 "IO_ratio" = lst_length)
  
  # Read csv file with entire year
  index <- 1
  for (year in year_lst) {
    # Read csv file
    df <- read_csv(sprintf("Eora_csv_files/Eora%d.csv", year))
    
    # Add Intermediate inputs row
    Gross_output <- as.numeric(df[4926, 4:4918])
    Value_added <- as.numeric(df[4927, 4:4918])
    inter_inputs <- Gross_output - Value_added
    df[4928, 1:4918] <- as.list(c("Intermediate inputs", NA, NA, as.character(inter_inputs)))
    
    # Calculate IO ratio with the function and append to list
    for (country in country_lst) {
      IO_ratio <- cal_IO(df, country)
      IO_lst$Year[index:(index+25)] <- rep(year, 26)
      IO_lst$Country[index:(index+25)] <- rep(country, 26)
      IO_lst$Industry[index:(index+25)] <- industry_lst
      IO_lst$IO_ratio[index:(index+25)] <- IO_ratio
      index <- index + 26
    }
    
    # list -> DataFrame
    IO_df = data.frame(IO_lst)
    
    # save as a csv file
    write_csv(IO_df, paste0("IO_ratio/IO_ratio_", 
                                      year_lst[1], "-", year_lst[3], ".csv"))
  }
  
}


clusterExport(cl, varlist = c("task", "year_lst", "country_lst", "industry_lst", 
                              "read_csv", "write_csv", "cal_IO"))


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


