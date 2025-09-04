# [Last Modified: 2024-05-25]
# 2024-05-09: Add pollutants factor to calculate variable
# 2024-06-17: Exclude trading partner from third country k

library(readr)
library(dplyr)
library(parallel)

setwd("~/RAWorkingDirectory")

#---------------------------------
# Calculate LB_pollutants
#
# Args:
#   df: Total trade flows considering pollutants weight
#   year: Year
#   country_i: Code of country_s
#   country_j: Code of country_r
#   sector_s: Sector of country_s (goods/services) 
#   sector_u: Sector of country_r (goods/services) 
#
# Returns:
#   LB_CO2: LB variable considering CO2 weight
#   LB_GHG: LB variable considering GHG weight
#---------------------------------

cal_LB <- function(df, year, country_i, country_j, sector_s, sector_u) {
  
  #----------------- Left-Hand Side of LB_network_1st  -----------------#
  
  except_j_CO2 <- df[(df$Year == year) & 
                       (df$Country_i == country_i) & 
                       (df$Sector_s == sector_s) & 
                       (df$Country_j != country_j), "T_CO2"]
  
  except_j_GHG <- df[(df$Year == year) & 
                       (df$Country_i == country_i) & 
                       (df$Sector_s == sector_s) & 
                       (df$Country_j != country_j), "T_GHG"]
  
  sum_exj_CO2 <- sum(as.data.frame(lapply(except_j_CO2, as.numeric)))
  
  sum_exj_GHG <- sum(as.data.frame(lapply(except_j_GHG, as.numeric)))
  
  
  lhs_CO2 <- as.vector(except_j_CO2 / sum_exj_CO2)[[1]]
  
  lhs_GHG <- as.vector(except_j_GHG / sum_exj_GHG)[[1]]
  
  
  #----------------- Right-Hand Side of LB_network_1st  -----------------#
  
  except_i_CO2 <- df[(df$Year == year) & 
                       (df$Country_i == country_j) & 
                       (df$Sector_s == sector_u) & 
                       (df$Country_j != country_i), "T_CO2"]
  
  except_i_GHG <- df[(df$Year == year) & 
                       (df$Country_i == country_j) & 
                       (df$Sector_s == sector_u) & 
                       (df$Country_j != country_i), "T_GHG"]
  
  sum_exi_CO2 <- sum(as.data.frame(lapply(except_i_CO2, as.numeric)))
  
  sum_exi_GHG <- sum(as.data.frame(lapply(except_i_GHG, as.numeric)))
  
  
  rhs_CO2 <- as.vector(except_i_CO2 / sum_exi_CO2)[[1]]
  
  rhs_GHG <- as.vector(except_i_GHG / sum_exi_GHG)[[1]]
  
  
  #----------------- Final Result of LB_network_1st  -----------------#
  
  LB_CO2 <- 1 - (sum(abs(lhs_CO2 - rhs_CO2)) / 2)
  LB_GHG <- 1 - (sum(abs(lhs_GHG - rhs_GHG)) / 2)
  
  
  return (c(LB_CO2, LB_GHG))
  
}



#---------------- Test Code ----------------#

df <- read_csv("T_pollutants/T_pollutants.csv")
year <- 2020
country_i <- "JPN"
country_j <- "KOR"
sector_s <- "goods"
sector_u <- "services"

print(cal_LB(df, year, country_i, country_j, sector_s, sector_u))

# Running Time of Function
time_vec <- vector("numeric", 100)
index <- 1
for (i in 1:100) {
  time <- Sys.time()
  test <- cal_LB(df, year, country_i, country_j, sector_s, sector_u)
  time_vec[index] <- Sys.time() - time
  index <- index + 1
}
print(mean(time_vec))  # --> 0.14 sec



#------------------------- Run Entire -------------------------# 

df <- read_csv("T_pollutants/T_pollutants.csv")

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
country_lst2 <- country_lst

# Sector list (goods and services)
sector_lst <- c("goods", "services")
sector_lst2 <- sector_lst


task <- function(year_lst) {
  
  #------------------- Create List -------------------#
  lst_length <- numeric(length(year_lst) * sum(1:length(country_lst)-1) * length(sector_lst) * length(sector_lst2))
  
  LB_lst <- list("Year" = lst_length,
                 "Country_i" = lst_length,
                 "Country_j" = lst_length,
                 "Sector_s" = lst_length,
                 "Sector_u" = lst_length,
                 "LB_CO2" = lst_length,
                 "LB_GHG" = lst_length)
  
  #------------------- Run Entire -------------------#
  index <- 1
  for (year in year_lst) {
    num <- 2
    
    for (i in country_lst[1:length(country_lst)-1]) {
      for (j in country_lst2[num:length(country_lst2)]) {
        for (s in sector_lst) {
          for (u in sector_lst2) {
            LB <- cal_LB(df, year, i, j, s, u)
            LB_CO2 <- LB[1]
            LB_GHG <- LB[2]
            
            LB_lst$Year[index] <- year
            LB_lst$Country_i[index] <- i
            LB_lst$Country_j[index] <- j
            LB_lst$Sector_s[index] <- s
            LB_lst$Sector_u[index] <- u
            LB_lst$LB_CO2[index] <- LB_CO2
            LB_lst$LB_GHG[index] <- LB_GHG
            
            index <- index + 1
          }
        }
      }
      num <- num + 1
    }
    
    #--------- Copy Duplicate Tables & Save ---------#
    
    # list -> DataFrame
    LB_EachYear <- as.data.frame(LB_lst)
    
    # Replicate table which is interchanged country & Industry pair and remove duplicated rows
    LB_EachYear1 <- LB_EachYear
    LB_EachYear1[c("Country_i", "Country_j")] <- LB_EachYear[c("Country_j", "Country_i")]
    LB_EachYear1[c("Sector_s", "Sector_u")] <- LB_EachYear[c("Sector_u", "Sector_s")]
    LB_EachYear2 <- rbind(LB_EachYear, LB_EachYear1)
    LB_EachYear3 <- unique(LB_EachYear2)
    
    LB_EachYear3$Country_i <- factor(LB_EachYear3$Country_i, levels = country_lst)
    LB_EachYear3$Country_j <- factor(LB_EachYear3$Country_j, levels = country_lst)
    LB_EachYear3$Sector_s <- factor(LB_EachYear3$Sector_s, levels = sector_lst)
    LB_EachYear3$Sector_u <- factor(LB_EachYear3$Sector_u, levels = sector_lst)
    
    LB_EachYear4 <- LB_EachYear3 %>%
      arrange(Year, Country_i, Country_j, Sector_s, Sector_u)
    
    # Save it as a csv file
    write_csv(LB_EachYear4, paste0("LB_pollutants/LB_pollutants_", 
                                   year_lst[1], "-", year_lst[3], ".csv"))
  }
}

clusterExport(cl, varlist = c("task", "year_lst", "country_lst", "country_lst2",
                              "sector_lst", "sector_lst2", "cal_LB",
                              "read_csv", "%>%", "arrange", "write_csv", "df"))


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

time <- Sys.time()
parLapply(cl, tasks, function(f) f())
stopCluster(cl)
print(Sys.time()-time)  