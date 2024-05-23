# [Last Modified: 2023-10-07]
# 2023-09-22: Add natural log by calculating time window variable
#             Remove TradeFlows between the same country by calculating MB network 1st
# 2023-10-07: Modify formula of function, Add same country by calculating MB network 1st
# 2024-03-13: Read Eora file instead of MRIO file and Add industry factor
# 2024-03-15: Add intermediate save & Replicate table which interchanged country & Industry pair and remove duplicated rows
# 2024-05-23: Add pollutants factor to calculate variable

library(readr)
library(dplyr)

setwd("~/RAWorkingDirectory")

df <- read_csv("T_pollutants/T_pollutants.csv")


#---------------------------------
# Calculate MB_pollutants
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
#   MB_CO2: MB variable considering CO2 weight
#   MB_GHG: MB variable considering GHG weight
#---------------------------------

cal_MB <- function(df, year, country_i, country_j, sector_s, sector_u) {
  
  #----------------- Left-Hand Side of MB_network_1st  -----------------#

  T_i_CO2 <- df[(df$Year == year) & 
                (df$Country_i == country_i) & 
                (df$Sector_s == sector_s), "T_CO2"]
  
  T_i_GHG <- df[(df$Year == year) & 
                (df$Country_i == country_i) & 
                (df$Sector_s == sector_s), "T_GHG"]
  
  
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
  
  
  lhs_CO2 <- as.vector(T_i_CO2 / sum_exj_CO2)[[1]]
  
  lhs_GHG <- as.vector(T_i_GHG / sum_exj_GHG)[[1]]
  
  
  #----------------- Right-Hand Side of MB_network_1st  -----------------#
  
  T_j_CO2 <- df[(df$Year == year) & 
                (df$Country_i == country_j) & 
                (df$Sector_s == sector_u), "T_CO2"]
  
  T_j_GHG <- df[(df$Year == year) & 
                (df$Country_i == country_j) & 
                (df$Sector_s == sector_u), "T_GHG"]
  
  
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
  
  
  rhs_CO2 <- as.vector(T_j_CO2 / sum_exi_CO2)[[1]]
  
  rhs_GHG <- as.vector(T_j_GHG / sum_exi_GHG)[[1]]
  
  
  #----------------- Final Result of MB_network_1st  -----------------#
  
  MB_CO2 <- log(lhs_CO2 %*% rhs_CO2)
  MB_GHG <- log(lhs_GHG %*% rhs_GHG)
  
  
  return (c(MB_CO2, MB_GHG))
  
}



#---------------- Test Code ----------------#

df <- read_csv("T_pollutants/T_pollutants.csv")
year <- 1990
country_i <- "AGO"
country_j <- "DZA"
sector_s <- "services"
sector_u <- "goods"

print(cal_MB(df, year, country_i, country_j, sector_s, sector_u))

# Running Time of Function
time_vec <- vector("numeric", 100)
index <- 1
for (i in 1:100) {
  time <- Sys.time()
  test <- cal_MB(df, year, country_i, country_j, sector_s, sector_u)
  time_vec[index] <- Sys.time() - time
  index <- index + 1
}
print(mean(time_vec))  # --> 0.2 sec
  
  

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
  
  MB_lst <- list("Year" = lst_length,
                 "Country_i" = lst_length,
                 "Country_j" = lst_length,
                 "Sector_s" = lst_length,
                 "Sector_u" = lst_length,
                 "MB_CO2" = lst_length,
                 "MB_GHG" = lst_length)
  
  #------------------- Run Entire -------------------#
  index <- 1
  for (year in year_lst) {
    num <- 2
    
    for (i in country_lst[1:length(country_lst)-1]) {
      for (j in country_lst2[num:length(country_lst2)]) {
        for (s in sector_lst) {
          for (u in sector_lst2) {
            MB <- cal_MB(df, year, i, j, s, u)
            MB_CO2 <- MB[1]
            MB_GHG <- MB[2]
            
            MB_lst$Year[index] <- year
            MB_lst$Country_i[index] <- i
            MB_lst$Country_j[index] <- j
            MB_lst$Sector_s[index] <- s
            MB_lst$Sector_u[index] <- u
            MB_lst$MB_CO2[index] <- MB_CO2
            MB_lst$MB_GHG[index] <- MB_GHG
            
            index <- index + 1
          }
        }
      }
      num <- num + 1
    }
    
    #--------- Copy Duplicate Tables & Save ---------#
    
    # list -> DataFrame
    MB_EachYear <- as.data.frame(MB_lst)
    
    # Replicate table which is interchanged country & Industry pair and remove duplicated rows
    MB_EachYear1 <- MB_EachYear
    MB_EachYear1[c("Country_i", "Country_j")] <- MB_EachYear[c("Country_j", "Country_i")]
    MB_EachYear1[c("Sector_s", "Sector_u")] <- MB_EachYear[c("Sector_u", "Sector_s")]
    MB_EachYear2 <- rbind(MB_EachYear, MB_EachYear1)
    MB_EachYear3 <- unique(MB_EachYear2)
    
    MB_EachYear3$Country_i <- factor(MB_EachYear3$Country_i, levels = country_lst)
    MB_EachYear3$Country_j <- factor(MB_EachYear3$Country_j, levels = country_lst)
    MB_EachYear3$Sector_s <- factor(MB_EachYear3$Sector_s, levels = sector_lst)
    MB_EachYear3$Sector_u <- factor(MB_EachYear3$Sector_u, levels = sector_lst)
    
    MB_EachYear4 <- MB_EachYear3 %>%
      arrange(Year, Country_i, Country_j, Sector_s, Sector_u)
    
    # Save it as a csv file
    write_csv(MB_EachYear4, paste0("MB_pollutants/MB_pollutants_", 
                                   year_lst[1], "-", year_lst[3], ".csv"))
  }
}

clusterExport(cl, varlist = c("task", "year_lst", "country_lst", "country_lst2",
                              "sector_lst", "sector_lst2", "cal_MB",
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
print(Sys.time()-time)  # --> running time 1:48:44 
  