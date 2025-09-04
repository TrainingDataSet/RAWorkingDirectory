# [Last Modified: 2025-05-27]
# 2025-05-27: Correlation of real VAD growth

library(dplyr)
library(readr)
library(microbenchmark)

setwd("~/RAWorkingDirectory")


#---------------------------------
# Calculate correlation of value added

# Args:
#   df: VAD/VAD_HP_filtered.csv
#   t: Time Window
#   country_i: Code of country_s
#   country_j: Code of country_r
#   sector_s: Sector of country_s (goods/services) 
#   sector_u: Sector of country_r (goods/services) 
#
# Returns:
#   Corr_VAD: Correlation of VAD in (country_i, sector_s) and (country_j, sector_u)
#---------------------------------

cal_corr_vad <- function(df, t, country_i, country_j, sector_s, sector_u) {
  
  #-------------- Extract Vector of VAD in Time Window --------------#
  
  is <- df[(df$Year %in% t) &
             (df$Country == country_i) &
             (df$Sector == sector_s), ]
  
  ju <- df[(df$Year %in% t) &
             (df$Country == country_j) &
             (df$Sector == sector_u), ]
  
  VAD_is <- is$VAD_growth
  VAD_ju <- ju$VAD_growth
  
  
  #------------------- Calculate Correlation of VAD -------------------#
  
  Corr_VAD <- cor(VAD_is, VAD_ju)
  
  
  return (Corr_VAD)
  
}



#---------------- Test Code ----------------#

# df <- read_csv("VAD/real_VAD_growth.csv")
# 
# country_i <- "VNM"
# country_j <- "VEN"
# sector_s <- "services"
# sector_u <- "services"
# t <- c(2010:2022)
# 
# print(cal_corr_vad(df, t, country_i, country_j, sector_s, sector_u))
# 
# # Running Time of Function
# microbenchmark(
#   test = cal_corr_vad(df, t, country_i, country_j, sector_s, sector_u),
#   times = 1000
# )  # --> 0.0007 sec



#------------------------- Run Entire -------------------------# 

df <- read_csv("VAD/real_VAD_growth.csv")

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

# Sector list (goods and services)
sector_lst <- c("goods", "services")
sector_lst2 <- sector_lst

# Time window list
tw_lst <- list('1' = c(1991:1999),
               '2' = c(2000:2009),
               '3' = c(2010:2019),
               '4' = c(2010:2022))


# Create List
lst_length <- numeric(length(tw_lst) * sum(1:length(country_lst)-1) * length(sector_lst) * length(sector_lst2))

Corr_lst <- list("t" = lst_length,
                 "Country_i" = lst_length,
                 "Country_j" = lst_length,
                 "Sector_s" = lst_length,
                 "Sector_u" = lst_length,
                 "Corr_VAD_growth" = lst_length)

# Run Entire
time <- Sys.time()

index <- 1
for (t in c("1", "2", "3", "4")) {
  num <- 2
  
  for (i in country_lst[1:length(country_lst)-1]) {
    for (j in country_lst2[num:length(country_lst2)]) {
      for (s in sector_lst) {
        for (u in sector_lst2) {
          vad <- cal_corr_vad(df, tw_lst[[t]], i, j, s, u)
          
          Corr_lst$t[index] <- t
          Corr_lst$Country_i[index] <- i
          Corr_lst$Country_j[index] <- j
          Corr_lst$Sector_s[index] <- s
          Corr_lst$Sector_u[index] <- u
          Corr_lst$Corr_VAD_growth[index] <- vad
          
          index <- index + 1
        }
      }
    }
    num <- num + 1
  }
  
  #--------- Copy Duplicate Tables & Save ---------#
  
  # list -> DataFrame
  Corr_df <- as.data.frame(Corr_lst)
  
  # Replicate table which is interchanged country & Industry pair and remove duplicated rows
  Corr_df1 <- Corr_df
  Corr_df1[c("Country_i", "Country_j")] <- Corr_df[c("Country_j", "Country_i")]
  Corr_df1[c("Sector_s", "Sector_u")] <- Corr_df[c("Sector_u", "Sector_s")]
  Corr_df2 <- rbind(Corr_df, Corr_df1)
  Corr_df3 <- unique(Corr_df2)
  
  Corr_df3$Country_i <- factor(Corr_df3$Country_i, levels = country_lst)
  Corr_df3$Country_j <- factor(Corr_df3$Country_j, levels = country_lst)
  Corr_df3$Sector_s <- factor(Corr_df3$Sector_s, levels = sector_lst)
  Corr_df3$Sector_u <- factor(Corr_df3$Sector_u, levels = sector_lst)
  
  Corr_df4 <- Corr_df3 %>%
    arrange(t, Country_i, Country_j, Sector_s, Sector_u)
  
  # Save it as a csv file
  write_csv(Corr_df4, paste0("temp/Time_window/Corr_VAD_growth.csv"))
}

print(Sys.time()-time)  # --> running time 1.05 mins
