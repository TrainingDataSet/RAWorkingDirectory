# [Last Modified: 2024-10-28]
# 2024-10-28: Forward/Backward linkage of s to s_tilde

library(dplyr)
library(readr)
library(parallel)
library(microbenchmark)

setwd("~/RAWorkingDirectory")


#---------------------------------
# Calculate Forward/Backward linkage of s to s_tilde
#
# Args:
#   df: Eora_csv_files/
#   country: Code of country
#
# Returns:
#   Forward: Forward linkage of s to s_tilde
#   Backward: Backward linkage of s to s_tilde
#---------------------------------

cal_linkages <- function(df, country) {
  
  r_index <- which(df[, 2] == country)
  c_index <- which(df[3, ] == country & grepl("^c", df[4, ]))
  
  #-------------------- Create a square matrix --------------------#
  
  # Extract square matrix
  square_matrix <- sapply(df[r_index, c_index], as.numeric)
  
  # Convert diagonal elements to NaN
  square_matrix[row(square_matrix) == col(square_matrix)] <- NaN
  
  
  #-------------------- Create Forward Linkages --------------------#
  
  # Make sum of the rows
  s_tilde_sum1 <- rowSums(square_matrix, na.rm=TRUE)
  
  # Divide each element by the sum of the rows
  forward_matrix <- sweep(square_matrix, 1, s_tilde_sum1, FUN = "/")
  
  # Make a two-dimensional matrix a one-dimensional vector
  forward <- as.vector(t(forward_matrix))
  forward <- forward[!is.nan(forward)]
  
  
  #-------------------- Create Backward Linkages --------------------#
  
  # Make sum of the columns
  s_tilde_sum2 <- colSums(square_matrix, na.rm=TRUE)
  
  # Divide each element by the sum of the columns
  backward_matrix <- sweep(square_matrix, 2, s_tilde_sum2, FUN = "/")
  
  # Make a two-dimensional matrix a one-dimensional vector that hasn't NaN
  backward <- as.vector(backward_matrix)
  backward <- backward[!is.nan(backward)]
  
  return (list("forward"=forward, "backward"=backward))
  
}


#---------------- Test Code ----------------#

# df <- read_csv("Eora_csv_files/Eora2022.csv")
# country <- "SVN"
# 
# print(cal_linkages(df, country))
# 
# # Running Time of Function
# microbenchmark(
#   test = cal_linkages(df, country),
#   times = 1000
# ) # --> 0.136 sec


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
  lst_length <- numeric(length(year_lst) * length(country_lst) * length(industry_lst) * (length(industry_lst)-1))
  linkages_lst <- list("Year" = lst_length, "Country" = lst_length, 
                       "S" = lst_length, "S_tilde" = lst_length, 
                       "Forward" = lst_length, "Backward" = lst_length)
  
  index1 <- 1
  index2 <- 1
  # Read csv file with entire year
  for (year in year_lst) {
    
    # Read csv file
    df <- read_csv(sprintf("Eora_csv_files/Eora%d.csv", year))
  
    
    # Calculate IO ratio with the function and append to list

    for (country in country_lst) {
      
      # Create index
      for (i in industry_lst) {
        for (j in industry_lst) {
          if (i == j) next
            
          linkages_lst$Year[index1] <- year
          linkages_lst$Country[index1] <- country
          linkages_lst$S[index1] <- i
          linkages_lst$S_tilde[index1] <- j
          index1 <- index1 + 1
        } 
      }
      
      Linkages <- cal_linkages(df, country)
      Forward <- Linkages$forward
      Backward <- Linkages$backward
      
      linkages_lst$Forward[index2:(index2+649)] <- Forward
      linkages_lst$Backward[index2:(index2+649)] <- Backward
      index2 <- index2 + 650
    }
    
    # list -> DataFrame
    linkages_df = data.frame(linkages_lst)
    
    # save as a csv file
    write_csv(linkages_df, paste0("Linkages/Linkages_", 
                            year_lst[1], "-", year_lst[3], ".csv"))
  }
  
}


clusterExport(cl, varlist = c("task", "year_lst", "country_lst", "industry_lst", 
                              "read_csv", "write_csv", "cal_linkages"))


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


