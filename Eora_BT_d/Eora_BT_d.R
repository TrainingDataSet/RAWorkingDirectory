# [Last Modified: 2024-03-13]
# 2023-09-22: Add natural log by calculating time window variable, Remove hp filter
# 2023-09-25: read.csv -> read_csv (improve speed)
# 2023-10-11: write_csv(BT_d_df, "BT_d/TradeFlows.csv") Save trade flows for each year as a csv file
# 2023-12-26: Code optimization [running time improvement 2101.48sec -> 1648.17sec per year (21.57%)] 
# 2024-01-09: Add industry operation with goods and services
# 2024-02-29: Read Eora file instead of MRIO file and add intermediate save
# 2024-03-13: Replicate table which interchanged country & Industry pair and remove duplicated rows

library(dplyr)
library(readr)
library(readxl)

setwd("~/RAWorkingDirectory")

### Total trade flows between industry_i in country_s and industry_j in country_r ###

#---------------------------------
# Calculate total trade flows between industry_i in country_s and industry_j in country_r
#
# Args:
#   df: Preprocessed MRIO Data frame
#   country_s: Code of country_s 
#   country_r: Code of country_r
#   industry_i: Code of industry_i
#   industry_j: Code of industry_j
#
# Returns:
#   total_trade_flows: total trade flows between industry_i in country_s and industry_j in country_r
#---------------------------------

cal_total_trade_flows <- function(df, country_s, country_r, industry_i, industry_j) {
  # Data frame with trade flow from industry_i in country_s to industry_j in country_r
  MRIO_col_index <- df[1:4, ]
  inter_input <- df[(df$X2 == country_s) &
                      (df$X3 %in% industry_i), ]
  inter_input <- na.omit(inter_input)
  inter_input <- bind_rows(MRIO_col_index, inter_input)
  input_index <- inter_input[, 1:3]
  
  # Extract columns about country_r & industry_j
  consume_columns <- which((inter_input[3, ] == country_r) &
                             (inter_input[4, ] %in% industry_j))
  
  inter_input_consume <- inter_input[, consume_columns]
  
  # Sum of consumption (country_s -> country_r)
  s_to_r_inter_input_consume <- bind_cols(input_index, inter_input_consume)
  s_to_r_consumption_df <- s_to_r_inter_input_consume[5:nrow(s_to_r_inter_input_consume), 
                                                      4:ncol(s_to_r_inter_input_consume)]
  s_to_r_consumption_df <- as.data.frame(lapply(s_to_r_consumption_df, as.numeric))
  s_to_r_consumption_sum <- sum(s_to_r_consumption_df)
  
  # Data frame with trade flow from country_r to country_s
  MRIO_col_index <- df[1:4, ]
  inter_input <- df[(df$X2 == country_r) &
                      (df$X3 %in% industry_j), ]
  inter_input <- na.omit(inter_input)
  inter_input <- bind_rows(MRIO_col_index, inter_input)
  input_index <- inter_input[, 1:3]
  
  # Extract columns about country_s & industry_i
  consume_columns <- which((inter_input[3, ] == country_s) &
                             (inter_input[4, ] %in% industry_i))
  
  inter_input_consume <- inter_input[, consume_columns]
  
  # Sum of consumption (country_r -> country_s)
  r_to_s_inter_input_consume <- bind_cols(input_index, inter_input_consume)
  r_to_s_consumption_df <- r_to_s_inter_input_consume[5:nrow(r_to_s_inter_input_consume), 
                                                      4:ncol(r_to_s_inter_input_consume)]
  r_to_s_consumption_df <- as.data.frame(lapply(r_to_s_consumption_df, as.numeric))
  r_to_s_consumption_sum <- sum(r_to_s_consumption_df)
  
  # Bilateral total trade flows
  total_trade_flows = s_to_r_consumption_sum + r_to_s_consumption_sum 
  
  return (total_trade_flows)
}  

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
goods_lst <- paste0("c", 1:12)
services_lst <- paste0("c", 13:25)
industry_lst <- list("goods" = goods_lst, "services" = services_lst)

# copy
country_lst2 <- country_lst
industry_lst2 <- industry_lst

# Create list
lst_length <- numeric(length(year_lst) * sum(1:length(country_lst)) * length(industry_lst) * length(industry_lst2))
BT_I_lst <- list("Year" = lst_length, "Country_s" = lst_length, "Country_r" = lst_length)

# Create entire ordered pair
index <- 1
for (year in year_lst) {
  num <- 1
  for (country in country_lst) {
    for (country2 in country_lst2[num:length(country_lst2)]) {
      for (industry in names(industry_lst)) {
        for (industry2 in names(industry_lst2)) {
          BT_I_lst[["Year"]][index] <- year
          BT_I_lst[["Country_s"]][index] <- country
          BT_I_lst[["Country_r"]][index] <- country2
          BT_I_lst[["Industry_i"]][index] <- industry
          BT_I_lst[["Industry_j"]][index] <- industry2
          index <- index + 1
        }
      }
    }
    num <- num + 1
  }
}

# list -> DataFrame
BT_I_index <- data.frame(BT_I_lst)

# Create list
TradeFlows_lst <- list(
  "TradeFlows" = lst_length
)

# Read csv file with entire year
index <- 1
for (year in year_lst) {
  # Read csv file
  df <- read_csv(sprintf("Eora_csv_files/Eora%d.csv", year))
  cat(paste("#### [Collecting TradeFlows] Year: ", year, " ####\n", sep = ""))
  
  # Calculate total trade flows with the function and append to list
  num <- 1
  for (s in country_lst) {
    for (r in country_lst2[num:length(country_lst2)]) {
      for (i in industry_lst) {
        for (j in industry_lst2) {
          total_trade_flows <- cal_total_trade_flows(df, s, r, i, j)
          TradeFlows_lst[["TradeFlows"]][index] <- total_trade_flows
          index <- index + 1
        }
      }
    }
    num <- num + 1
  }
  # list -> DataFrame
  TradeFlows_df = data.frame(TradeFlows_lst)
  
  # concat
  BT_I_df <- bind_cols(BT_I_index, TradeFlows_df)
  
  # save as a csv file
  write_csv(BT_I_df, "Eora_BT_d/Eora_TradeFlows_IND_sample.csv")
}



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

cal_VAD <- function(df, country_s, industry_i, country_r, industry_j) {
  # Value added of (country_s, industry_i) and (country_r, industry_j)
  df2 <- df[, 5:ncol(df)-1]
  
  VAD_s_i <- df2[4927, (as.vector(df2[3, ]) == country_s) &
                  (as.vector(df2[4, ]) %in% industry_i)]
  VAD_r_j <- df2[4927, (as.vector(df2[3, ]) == country_r) &
                  (as.vector(df2[4, ]) %in% industry_j)]
  
  return (c(sum(as.numeric(VAD_s_i)), sum(as.numeric(VAD_r_j))))
}


### Bilateral VAD for the entire period and country ###

# Create list
VAD_lst <- list("VAD_s_i" = lst_length, "VAD_r_j" = lst_length)

index <- 1
# Read csv file with entire year
cat("#### [Create VAD variable] ####")
for (year in year_lst) {
  # Read csv file
  df <- read_csv(sprintf("Eora_csv_files/Eora%d.csv", year))
  cat(paste0("#### [Collecting VAD] Year: ", year, " ####\n"))
  
  # Calculate Bilateral VAD with the function and append to list
  num <- 1
  for (s in country_lst) {
    for (r in country_lst2[num:length(country_lst2)]) {
      for (i in industry_lst) {
        for (j in industry_lst2) {
          VAD_lst[["VAD_s_i"]][index] <- cal_VAD(df, s, i, r, j)[1]
          VAD_lst[["VAD_r_j"]][index] <- cal_VAD(df, s, i, r, j)[2]
          index <- index + 1
        }
      }
    }
    num <- num + 1
  }
  # list -> DataFrame
  VAD_df <- data.frame(VAD_lst)
  
  # concat
  VAD_df2 <- bind_cols(BT_I_index, VAD_df)
  
  # save as a csv file
  write_csv(VAD_df2, "Eora_BT_d/Eora_VAD_IND_sample.csv")
}

# concat
BT_I_df2 <- bind_cols(BT_I_df, VAD_df)


## Create BT_I by calculating total trade flows and VAD 
BT_I_df2$BT_I <- BT_I_df2$TradeFlows / (BT_I_df2$VAD_s_i + BT_I_df2$VAD_r_j)

# Replicate table which is interchanged country & Industry pair and remove duplicated rows
BT_I_df3 <- BT_I_df2
BT_I_df3[c("Country_s", "Country_r")] <- BT_I_df2[c("Country_r", "Country_s")]
BT_I_df3[c("Industry_i", "Industry_j")] <- BT_I_df2[c("Industry_j", "Industry_i")]
BT_I_df3[c("VAD_s_i", "VAD_r_j")] <- BT_I_df2[c("VAD_r_j", "VAD_s_i")]
BT_I_df4 <- rbind(BT_I_df2, BT_I_df3)
BT_I_df5 <- unique(BT_I_df4)

BT_I_df5$Country_s <- factor(BT_I_df5$Country_s, levels = country_lst)
BT_I_df5$Country_r <- factor(BT_I_df5$Country_r, levels = country_lst)
BT_I_df5$Industry_i <- factor(BT_I_df5$Industry_i, levels = names(industry_lst))
BT_I_df5$Industry_j <- factor(BT_I_df5$Industry_j, levels = names(industry_lst))

BT_I_df6 <- BT_I_df5 %>%
  arrange(Year, Country_s, Country_r, Industry_i, Industry_j)

# Save it as a csv file
output_file <- "Eora_BT_d/Eora_TradeFlows_IND_sample.csv"
write_csv(BT_I_df6, output_file)

# Create Time Window (ex.1990~1999, 2000~2009, 2010~2022)
t1 <- BT_I_df2 %>% filter(Year >= 1990 & Year <= 1999)
t2 <- BT_I_df2 %>% filter(Year >= 2000 & Year <= 2009)
t3 <- BT_I_df2 %>% filter(Year >= 2010 & Year <= 2022)


# Calculate average for each period
BT_I_t1 <- t1 %>% group_by(Country_s, Country_r, Industry_i, Industry_j) %>%
  summarise(
    BT_I = log(mean(BT_I)),
  )

BT_I_t2 <- t2 %>% group_by(Country_s, Country_r, Industry_i, Industry_j) %>%
  summarise(
    BT_I = log(mean(BT_I))
  )

BT_I_t3 <- t3 %>% group_by(Country_s, Country_r, Industry_i, Industry_j) %>%
  summarise(
    BT_I = log(mean(BT_I))
  )

# Create feature "t" for each variable
BT_I_t1 <- BT_I_t1 %>% mutate(t = 1)
BT_I_t2 <- BT_I_t2 %>% mutate(t = 2)
BT_I_t3 <- BT_I_t3 %>% mutate(t = 3)

# Bind rows and send a feature "t" to the front
BT_I <- bind_rows(BT_I_t1, BT_I_t2)
BT_I2 <- bind_rows(BT_I, BT_I_t3)
BT_I3 <- BT_I2 %>% select(t, everything())

# Save it as a csv file
write_csv(BT_I3, "Eora_BT_d/Eora_BT_IND_sample.csv")



#----------------[[Running time of functions]]----------------#

# 0.75(sec) * 2,370,060(rows) -> about 21 days (After multi-processing about 4-5days)

## ~0.63sec
time_vec <- c()
time <- Sys.time()
cal_total_trade_flows(df, country_s, country_r, industry_i, industry_j)
print(Sys.time()-time)

## ~0.12sec
time <- Sys.time()
cal_VAD(df, country_s, industry_i, country_r, industry_j)
print(Sys.time()-time)



# #------------------Export of specific industry------------------#
# 
# 
# Export <- Eora[(Eora$X2 == "AUS") & (Eora$X3 == "c13"), which((Eora[3, ] == "AUT") & (Eora[4, ] %in% goods_lst))]
# 
# Export1 <- sum(as.numeric(na.omit(Export)))
# 
# 
# #-----------------Export weighted of pollutants rate-----------------#
# 
# CO2_export = CO2_rate * Export1
# GHG_export = GHG_rate * Export1

