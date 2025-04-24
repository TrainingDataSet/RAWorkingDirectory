# [Last Modified: 2025-02-18]
# 2025-02-07: Find variables for the entire country
# 2025-02-18: Collect Gross Output data in the right column

library(dplyr)
library(readr)
library(parallel)
library(microbenchmark)

setwd("~/RAWorkingDirectory")


cal_export_import_sales_va <- function(df, country) {
  
  exports <- df[df$X2 == country, which(df[3, ] != country & grepl("^c", df[4, ]))]
  exports_sum <- rowSums(as.data.frame(lapply(na.omit(exports), as.numeric)))
  
  imports <- df[df$X2 != country & grepl("^c", df$X3), which(df[3, ] == country & grepl("^c", df[4, ]))]
  imports_sum <- as.vector(colSums(as.data.frame(lapply(imports, as.numeric))))
  
  sales_vad <- df[4926:4927, which(df[3, ] == country & grepl("^c", df[4, ]))]
  
  import_sales <- as.numeric(sales_vad[1, ])
  vad_cur <- as.numeric(sales_vad[2, ])
  
  export_sales <- as.vector(sapply(na.omit(df[df$X2 == country, 6059]), as.numeric))
  
  return (list("exports_sum" = exports_sum, "imports_sum" = imports_sum, 
               "export_sales" = export_sales, "import_sales" = import_sales, "vad_cur" = vad_cur))
}



#---------------- Test Code ----------------#

# df <- read_csv("Eora_csv_files/Eora1990.csv")
# country = "AFG"
# test <- cal_export_import_sales_va(df, country)
# print(test)
#
# # Running Time of Function
# microbenchmark(
#   test = cal_export_import_sales_va(df, country),
#   times = 1000
# ) # --> 0.97 sec



#------------------------- Run Entire -------------------------# 

cl <- makeCluster(11)

year_lst <- list(c(1990:1992), c(1993:1995), c(1996:1998), c(1999:2001), c(2002:2004), c(2005:2007), c(2008:2010),
                 c(2011:2013), c(2014:2016), c(2017:2019), c(2020:2022))

# Country list (Entire country)
country_lst <- c("AFG", "ALB", "DZA", "AND", "AGO", "ATG", "ARG", "ARM", "ABW", "AUS", "AUT", "AZE", "BHS", "BHR", "BGD", "BRB", "BLR", "BEL", "BLZ", "BEN", "BMU",
                 "BTN", "BOL", "BIH", "BWA", "BRA", "VGB", "BRN", "BGR", "BFA", "BDI", "KHM", "CMR", "CAN", "CPV", "CYM", "CAF", "TCD", "CHL", "CHN", "COL", "COG", "CRI", "HRV",
                 "CUB", "CYP", "CZE", "CIV", "PRK", "COD", "DNK", "DJI", "DOM", "ECU", "EGY", "SLV", "ERI", "EST", "ETH", "FJI", "FIN", "FRA", "PYF", "GAB", "GMB", "GEO", "DEU",
                 "GHA", "GRC", "GRL", "GTM", "GIN", "GUY", "HTI", "HND", "HKG", "HUN", "ISL", "IND", "IDN", "IRN", "IRQ", "IRL", "ISR", "ITA", "JAM", "JPN", "JOR", "KAZ", "KEN",
                 "KWT", "KGZ", "LAO", "LVA", "LBN", "LSO", "LBR", "LBY", "LIE", "LTU", "LUX", "MAC", "MDG", "MWI", "MYS", "MDV", "MLI", "MLT", "MRT", "MUS", "MEX", "MCO", "MNG",
                 "MNE", "MAR", "MOZ", "MMR", "NAM", "NPL", "NLD", "ANT", "NCL", "NZL", "NIC", "NER", "NGA", "NOR", "PSE", "OMN", "PAK", "PAN", "PNG", "PRY", "PER", "PHL", "POL",
                 "PRT", "QAT", "KOR", "MDA", "ROU", "RUS", "RWA", "WSM", "SMR", "STP", "SAU", "SEN", "SRB", "SYC", "SLE", "SGP", "SVK", "SVN", "SOM", "ZAF", "SDS", "ESP", "LKA",
                 "SUD", "SUR", "SWZ", "SWE", "CHE", "SYR", "TWN", "TJK", "THA", "MKD", "TGO", "TTO", "TUN", "TUR", "TKM", "USR", "UGA", "UKR", "ARE", "GBR", "TZA", "USA", "URY",
                 "UZB", "VUT", "VEN", "VNM", "YEM", "ZMB", "ZWE")
# country_lst <- c("DZA", "AGO", "ARG", "AUS", "AUT", "AZE", "BHR", "BGD", "BLR", "BEL", "BOL", "BIH", "BRA", "BGR", "KHM", "CAN", "CHL", "CHN", "COL", "COG", "CRI",
#                  "HRV", "CUB", "CYP", "CZE", "DNK", "DOM", "ECU", "EGY", "SLV", "EST", "ETH", "FIN", "FRA", "GEO", "DEU", "GHA", "GRC", "GTM", "HND", "HKG", "HUN",
#                  "ISL", "IND", "IDN", "IRN", "IRQ", "IRL", "ISR", "ITA", "JPN", "KAZ", "KEN", "KWT", "LVA", "LBN", "LTU", "LUX", "MYS", "MLT", "MEX", "MAR", "NLD",
#                  "NZL", "NOR", "OMN", "PAK", "PRY", "PER", "PHL", "POL", "PRT", "QAT", "KOR", "ROU", "RUS", "SAU", "SRB", "SGP", "SVK", "SVN", "ZAF", "ESP", "LKA",
#                  "SWE", "CHE", "THA", "TUN", "TUR", "UKR", "ARE", "GBR", "TZA", "USA", "URY", "UZB", "VEN", "VNM")

industry_lst <- paste0("c", c(1:26))


task <- function(year_lst) {
  # Create list
  lst_length <- numeric(length(year_lst) * length(country_lst) * length(industry_lst))
  lst <- list("Year" = lst_length, "Country_i" = lst_length, "Industry_s" = lst_length, 
              "Export" = lst_length, "Import" = lst_length,
              "Export_Sales" = lst_length, "Import_Sales" = lst_length, "VA_cur" = lst_length)
  
  # Read csv file with entire year
  index <- 1
  for (year in year_lst) {
    # Read csv file
    df <- read_csv(sprintf("Eora_csv_files/Eora%d.csv", year))
    
    for (country_i in country_lst) {
      val <- cal_export_import_sales_va(df, country_i)
      
      lst$Year[index:(index+25)] <- rep(year, 26)
      lst$Country_i[index:(index+25)] <- rep(country_i, 26)
      lst$Industry_s[index:(index+25)] <- industry_lst
      lst$Export[index:(index+25)] <- val$exports_sum
      lst$Import[index:(index+25)] <- val$imports_sum
      lst$Export_Sales[index:(index+25)] <- val$export_sales
      lst$Import_Sales[index:(index+25)] <- val$import_sales
      lst$VA_cur[index:(index+25)] <- val$vad_cur
      
      index <- index + 26
    }
    
    # list -> DataFrame
    df = data.frame(lst)
    
    # save as a csv file
    write_csv(df, paste0("Export_Import_Sales_VA2/Export_Import_Sales_VA_EntireCountry", 
                         year_lst[1], "-", year_lst[length(year_lst)], ".csv"))
  }
  
}


clusterExport(cl, varlist = c("task", "year_lst", "country_lst", "industry_lst", 
                              "read_csv", "write_csv", "cal_export_import_sales_va"))


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


