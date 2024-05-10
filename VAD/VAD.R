## 국가 i,j 섹터 s, u

### VAD of country_s, industry_i and country_r, industry_j ###

#---------------------------------
# Calculate value added of industry_i in country_s and industry_j in country_r
#
# Args:
#   df: Preprocessed MRIO Data frame
#   country_s: Code of country_s
#   industry_i: Code of industry_i
#   country_r: Code of country_r
#   industry_j: Code of industry_j
#
# Returns:
#   VAD_s_i: Value added of industry_i in country_s
#   VAD_r_j: Value added of industry_j in country_r
#---------------------------------

cal_VAD <- function(df, country, sector) {
  
  df2 <- df[, 5:ncol(df)-1]
  
  VAD <- df2[4927, (as.vector(df2[3, ]) == country) &
                   (as.vector(df2[4, ]) %in% sector[[1]])]
  
  return (sum(as.numeric(VAD)))
}

#---------------- Test Code ----------------#

# Industry list (goods and services)
goods_lst <- list("goods" = paste0("c", 1:12))
services_lst <- list("services" = paste0("c", 13:25))
industry_lst <- list("goods" = goods_lst, "services" = services_lst)

df <- read_csv(sprintf("Eora_csv_files/Eora%d.csv", 2020))
country <- "KOR"
sector <- goods_lst

print(cal_VAD(df, country, sector))

# Running Time of Function
time_vec <- vector("numeric", 10)
index <- 1
for (i in 1:100) {
  time <- Sys.time()
  test <- cal_VAD(df, country, sector)
  time_vec[index] <- Sys.time() - time
  index <- index + 1
}
print(mean(time_vec))  # --> 0.09 sec


### Total bilateral trade flows for the entire period and country ###

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
goods_lst <- list("goods" = paste0("c", 1:12))
services_lst <- list("services" = paste0("c", 13:25))
industry_lst <- list("goods" = goods_lst, "services" = services_lst)


### Bilateral VAD for the entire period and country ###

# Create list
lst_length <- numeric(length(year_lst) * length(country_lst) * length(industry_lst))
VAD_lst <- list("Year" = lst_length, "Country" = lst_length, "Sector" = lst_length, "VAD" = lst_length)

index <- 1
# Read csv file with entire year
cat("#### [Create VAD variable] ####")
for (year in year_lst) {
  # Read csv file
  df <- read_csv(sprintf("Eora_csv_files/Eora%d.csv", year))
  cat(paste0("#### [Collecting VAD] Year: ", year, " ####\n"))
  
  # Calculate Bilateral VAD with the function and append to list
  for (country in country_lst) {
    for (sector in industry_lst) {
      VAD_lst[["Year"]][index] <- year
      VAD_lst[["Country"]][index] <- country
      VAD_lst[["Sector"]][index] <- names(sector)
      VAD_lst[["VAD"]][index] <- cal_VAD(df, country, sector)
      index <- index + 1
    }
  }
  # list -> DataFrame
  VAD_df <- data.frame(VAD_lst)
  
  # save as a csv file
  write_csv(VAD_df, "VAD/Eora_VAD.csv")
}

