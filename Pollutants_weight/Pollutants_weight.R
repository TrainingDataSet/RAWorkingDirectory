# [Last Modified: 2024-04-06]

library(dplyr)
library(readr)
library(readxl)

setwd("~/RAWorkingDirectory")


# Year list (Entire year)
year_lst <- c(1990:2022)
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
industry_lst <- paste0("c", 1:25)
goods_lst <- paste0("c", 1:12)
services_lst <- paste0("c", 13:25)


#---------------------------------
# Calculate pollutants weight of specific country and industry
#
# Args:
#   df_Q: Preprocessed pollutants dataframe
#   country: Code of country
#   industry: Code of industry
#
# Returns:
#   CO2_rate: CO2 / Sum of CO2 in Goods or Services
#   GHG_rate: GHG / Sum of GHG in Goods or Services
#---------------------------------

cal_weight <- function(df_Q, country) {
  
  #---------------- CO2 and GHG of specific industry ----------------#
  
  CO2_GHG <- df_Q[c(2720, 2712), ]
  
  CO2_GHG1 <- bind_rows(df_Q[1:3, ], CO2_GHG)
  
  CO2_goods <- as.numeric(CO2_GHG1[4, which((CO2_GHG1[2, ] == country) &
                                            (CO2_GHG1[3, ] %in% goods_lst))])
  
  CO2_services <- as.numeric(CO2_GHG1[4, which((CO2_GHG1[2, ] == country) &
                                               (CO2_GHG1[3, ] %in% services_lst))])
  
  GHG_goods <- as.numeric(CO2_GHG1[5, which((CO2_GHG1[2, ] == country) &
                                            (CO2_GHG1[3, ] %in% goods_lst))])
  
  GHG_services <- as.numeric(CO2_GHG1[5, which((CO2_GHG1[2, ] == country) &
                                               (CO2_GHG1[3, ] %in% services_lst))])
  
  
  #------------ Sum of CO2 and GHG in Services or Goods ------------#
  
  CO2_goods_sum <- sum(CO2_goods)
  CO2_services_sum <- sum(CO2_services)
  GHG_goods_sum <- sum(GHG_goods)
  GHG_services_sum <- sum(GHG_services)
  
  
  #------------ Rate of CO2 and GHG ------------#
  
  CO2_goods_rate <- CO2_goods / CO2_goods_sum
  CO2_services_rate <- CO2_services / CO2_services_sum
  GHG_goods_rate <- GHG_goods / GHG_goods_sum
  GHG_services_rate <- GHG_services / GHG_services_sum
  
  
  return (c(CO2_goods_rate, CO2_services_rate, GHG_goods_rate, GHG_services_rate))
  
}


#---------------- Test Code ----------------#
# time_vec <- vector("numeric", 10)
# 
# index <- 1
# for (i in 1:100) {
#   time <- Sys.time()
#   test2 <- cal_weight(df_Q, "AUS")
#   time_vec[index] <- Sys.time() - time
#   index <- index + 1
# }
# 
# print(mean(time_vec))


### Weights for the entire period and country ###

# Create index list

lst_length <- numeric(length(year_lst) * length(country_lst) * length(industry_lst))
index_lst <- list("Year" = lst_length, "Country" = lst_length, "Industry" = lst_length)

# Create index
index <- 1
for (year in year_lst) {
  for (country in country_lst) {
    for (industry in industry_lst) {
      index_lst[["Year"]][index] <- year
      index_lst[["Country"]][index] <- country
      index_lst[["Industry"]][index] <- industry
      index <- index + 1
    }
  }
}

index_df <- as.data.frame(index_lst)



# Read csv file with entire year
weight_lst <- list("CO2_rate" = lst_length, "GHG_rate" = lst_length)

index <- 1
for (year in year_lst) {
  
  # Read csv file
  df_Q <- read_csv(sprintf("Eora_csv_files/Eora_Q%d.csv", year))
  cat(paste0("#### [Collecting Weight] Year: ", year, " ####\n"))
  
  # Calculate Pollutants weight with the function and append to list
  for (country in country_lst) {
    rate <- cal_weight(df_Q, country)
    weight_lst[["CO2_rate"]][index:(index+24)] <- rate[1:25]
    weight_lst[["GHG_rate"]][index:(index+24)] <- rate[26:50]
    index <- index + 25
  }
  
  # list -> DataFrame
  weight_df <- data.frame(weight_lst)

  # bind columns
  weight_df <- bind_cols(index_df, weight_df)

  # save as a csv file
  write_csv(weight_df, "Pollutants_weight.csv")
}

               