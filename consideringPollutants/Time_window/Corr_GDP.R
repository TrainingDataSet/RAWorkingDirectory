# [Last Modified: 2024-08-10]
# 2024-08-10: Calculate correlation of GDP

library(dplyr)
library(readr)
library(microbenchmark)

setwd("~/RAWorkingDirectory")


#---------------------------------
# Calculate correlation of GDP

# Args:
#   df: GDP/GDP_HP_filtered.csv
#   t: Time Window
#   country_i: Code of country_s
#   country_j: Code of country_r
#
# Returns:
#   Corr_GDP: Correlation of GDP
#---------------------------------

cal_corr_vad <- function(df, t, country_i, country_j) {
  
  #-------------- Extract Vector of GDP in Time Window --------------#
  
  is <- df[(df$Year %in% t) &
           (df$Country == country_i), ]
  
  ju <- df[(df$Year %in% t) &
           (df$Country == country_j), ]
  
  GDP_is <- is$GDP
  GDP_ju <- ju$GDP
  
  
  #------------------- Calculate Correlation of GDP -------------------#
  
  Corr_GDP <- cor(GDP_is, GDP_ju)
  
  
  return (Corr_GDP)
  
}



#---------------- Test Code ----------------#

# df <- read_csv("GDP/GDP_HP_filtered.csv")

# country_i <- "AUT"
# country_j <- "AUS"
# t <- c(2010:2022)
# 
# print(cal_corr_vad(df, t, country_i, country_j))

# Running Time of Function
# microbenchmark(
#   test = cal_corr_vad(df, t, country_i, country_j),
#   times = 1000
# )  # --> 0.0003 sec


#------------------------- Run Entire -------------------------# 

df <- read_csv("GDP/GDP_HP_filtered.csv")

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
country_lst2 <- country_lst


# Time window list
tw_lst <- list('1' = c(1990:1999),
               '2' = c(2000:2009),
               '3' = c(2010:2019),
               '4' = c(2010:2022))


# Create List
lst_length <- numeric(length(tw_lst) * sum(1:length(country_lst)-1))

Corr_lst <- list("t" = lst_length,
                 "Country_i" = lst_length,
                 "Country_j" = lst_length,
                 "Corr_GDP" = lst_length)

# Run Entire
time <- Sys.time()

index <- 1
for (t in c("1", "2", "3", "4")) {
  num <- 2
  
  for (i in country_lst[1:length(country_lst)-1]) {
    for (j in country_lst2[num:length(country_lst2)]) {
      gdp <- cal_corr_vad(df, tw_lst[[t]], i, j)
      
      Corr_lst$t[index] <- t
      Corr_lst$Country_i[index] <- i
      Corr_lst$Country_j[index] <- j
      Corr_lst$Corr_GDP[index] <- gdp
      
      index <- index + 1
    }
    num <- num + 1
  }
  
  #--------- Copy Duplicate Tables & Save ---------#
  
  # list -> DataFrame
  Corr_df <- as.data.frame(Corr_lst)
  
  # Replicate table which is interchanged country & Industry pair and remove duplicated rows
  Corr_df1 <- Corr_df
  Corr_df1[c("Country_i", "Country_j")] <- Corr_df[c("Country_j", "Country_i")]
  Corr_df2 <- rbind(Corr_df, Corr_df1)
  
  Corr_df2$Country_i <- factor(Corr_df2$Country_i, levels = country_lst)
  Corr_df2$Country_j <- factor(Corr_df2$Country_j, levels = country_lst)
  
  Corr_df3 <- Corr_df2 %>%
    arrange(t, Country_i, Country_j)
  
  # Save it as a csv file
  write_csv(Corr_df3, paste0("consideringNothing/Time_window/Corr_GDP.csv"))
}

print(Sys.time()-time)  # --> 10.31 sec



