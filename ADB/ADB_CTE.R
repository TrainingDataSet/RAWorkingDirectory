# [Last Modified: 2025-05-10]

library(dplyr)
library(readr)
library(microbenchmark)

setwd("~/RAWorkingDirectory")


#---------------------------------
# Calculate CTE

# Args:
#   df: consideringNothing/RWC/RWC.csv
#   t: Time Window
#   country_i: Code of country_s
#   country_j: Code of country_r
#
# Returns:
#   CTE: Correlation of RWC(ij) and RWC(ji)
#---------------------------------

cal_cte <- function(df, country_i, country_j) {
  
  #-------------- Calculate CTE --------------#
  
  ij <- df[(df$Country_i == country_i) &
           (df$Country_j == country_j), ]
  
  ji <- df[(df$Country_i == country_j) &
           (df$Country_j == country_i), ]
  
  CTE <- cor(ij$RWC, ji$RWC)
  
  
  return (CTE)
  
}



#---------------- Test Code ----------------#

# df <- read_csv("ADB/ADB_RWC.csv")
# 
# country_i <- "AUS"
# country_j <- "AUT"
# 
# print(cal_cte(df, country_i, country_j))
# 
# # Running Time of Function
# microbenchmark(
#   test = cal_cte(df, t, country_i, country_j),
#   times = 1000
# )  # --> 0.02 sec


#------------------------- Run Entire -------------------------# 

df <- read_csv("ADB/ADB_RWC.csv")

# Country list (Entire country)
country_lst <- c("AUS", "AUT", "BEL", "BGR", "BRA", "CAN", "SWI", "PRC",
                 "CYP", "CZE", "GER", "DEN", "SPA", "EST", "FIN", "FRA",
                 "UKG", "GRC", "HRV", "HUN", "INO", "IND", "IRE", "ITA",
                 "JPN", "KOR", "LTU", "LUX", "LVA", "MEX", "MLT", "NET",
                 "NOR", "POL", "POR", "ROM", "RUS", "SVK", "SVN", "SWE",
                 "TUR", "TAP", "USA", "BAN", "MAL", "PHI", "THA", "VIE",
                 "KAZ", "MON", "SRI", "PAK", "FIJ", "LAO", "BRU", "BHU",
                 "KGZ", "CAM", "MLD", "NEP", "SIN", "HKG")
country_lst2 <- country_lst


# Create List
lst_length <- numeric(sum(1:length(country_lst)-1))

Corr_lst <- list("Country_i" = lst_length,
                 "Country_j" = lst_length,
                 "CTE" = lst_length)

# Run Entire
time <- Sys.time()

index <- 1
num <- 2

for (i in country_lst) {
  for (j in country_lst2) {
    cte <- cal_cte(df, i, j)
    
    Corr_lst$Country_i[index] <- i
    Corr_lst$Country_j[index] <- j
    Corr_lst$CTE[index] <- cte
    
    index <- index + 1
  }
  num <- num + 1
}

#--------- Copy Duplicate Tables & Save ---------#

# list -> DataFrame
Corr_df <- as.data.frame(Corr_lst)

# Save it as a csv file
write_csv(Corr_df, paste0("ADB/Dataset/CTE.csv"))



