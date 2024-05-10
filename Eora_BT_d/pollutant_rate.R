# [Last Modified: 2024-02-29]

library(dplyr)
library(readr)
library(readxl)

setwd("C:/Users/user/RAWorkingDirectory")


# The share of specific pollutant mass in total pollutant mass
cal_polutant_rate <- function(df, industry, pollutant) {
  
  # Type of pollutant 
  pollutant_row <- which(df$X1 == pollutant)
  
  #--------Total industrial/final demand pollutant mass--------#
  
  if (startsWith(industry, "c")) {
    all_ind_columns <- which(grepl("^c", df[3, ]))
  } else {
    all_ind_columns <- which(grepl("^F", df[3, ]))
  }
  
  ind_total_vec <- as.numeric(df[pollutant_row, all_ind_columns])
  ind_total_sum <- sum(ind_total_vec)
  
  #-------Specific industrial/final demand pollutant mass-------#
  
  ind_column <- which(df[3, ] == industry)
  ind_vec <- as.numeric(df[pollutant_row, ind_column])
  ind_sum <- sum(ind_vec)
  
  #------------Industrial/final demand pollutant rate------------#
  
  pollutant_rate <- ind_sum / ind_total_sum
  
  return(pollutant_rate)
  
}

# Example Variable
df <- read_csv("Eora_csv_files/Eora_Q1990.csv")
industry <- "c1" # It's possible to insert "F1"~"F6". But if sum of the pollutant in final demand is 0, function will return NA.
pollutant <- "Total emissions of CO2 excluding LULUCF (Gg) using PRIMAPv2.1"

# Example Result
print(cal_polutant_rate(df, industry, pollutant))

sum <- 0
for (industry in paste0("c", 1:26)) {
  rate <- cal_polutant_rate(df, industry, pollutant)
  sum = sum + rate
}

print(sum)

sum <- 0
for (industry in paste0("F", 1:6)) {
  rate <- cal_polutant_rate(df, industry, "TOTAL")
  sum = sum + rate
}

print(sum)

