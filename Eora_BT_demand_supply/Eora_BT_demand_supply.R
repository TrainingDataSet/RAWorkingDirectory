# [Last Modified: 2024-03-15]
# 2024-03-14: Read Eora file instead of MRIO file and Add industry factor
# 2024-03-15: Add intermediate save & Replicate table which interchanged country & Industry pair and remove duplicated rows

library(readr)
library(dplyr)
library(readxl)

setwd("~/RAWorkingDirectory")

### BT_demand and BT_supply between countries s and r ###

#---------------------------------
# Create alpha between industry_i of country_s and industry_j of country_r
#
# Args:
#   df: Preprocessed MRIO Data frame
#   country_s: Code of country_s 
#   country_r: Code of country_r
#   industry_i: Type of industry (goods or services)
#   industry_j: Type of industry (goods or services)
#
# Returns:
#   alpha: alpha between industry_i of country_s and industry_j of country_r
#---------------------------------

cal_alpha <- function(df, country_s, country_r, industry_i, industry_j) {
  ## Create gross output in industry_j of country_r
  set_r <- subset(df, df$X2 == country_r & df$X3 %in% industry_j)
  output_r <- set_r[, length(set_r)]
  output_r <- as.double(output_r$X6059)
  GrossOutput_r <- sum(output_r)
  
  ## Create trade flow in intermediate inputs from s to r
  # Data frame with trade flow from industry_i of country_s to industry_j of country_r 
  Eora_col_index <- df[1:4, ]
  inter_input <- df[df$X2 == country_s & df$X3 %in% industry_i, ]
  inter_input <- na.omit(inter_input)
  inter_input <- bind_rows(Eora_col_index, inter_input)
  input_index <- inter_input[, 1:3]
  
  # Extract columns about intermediate input
  consume_columns <- which((inter_input[3, ] == country_r) & (inter_input[4, ] %in% industry_j))
  inter_input_consume <- inter_input[, consume_columns]
  
  # Sum of intermediate inputs (country_s -> country_r)
  s_to_r_inter_input_consume <- bind_cols(input_index, inter_input_consume)
  s_to_r_consumption_df <- s_to_r_inter_input_consume[5:nrow(s_to_r_inter_input_consume), 
                                                      4:ncol(s_to_r_inter_input_consume)]
  s_to_r_consumption_df <- as.data.frame(lapply(s_to_r_consumption_df, as.numeric))
  s_to_r_intermediate_inputs <- sum(s_to_r_consumption_df)
  
  ## Create alpha
  alpha <- s_to_r_intermediate_inputs / GrossOutput_r
  
  return (alpha)
}


#---------------------------------
# Create beta between country_s and country_r
#
# Args:
#   df: Preprocessed MRIO Data frame  
#   year: Years to collect data
#   country_s: Code of country_s 
#   country_r: Code of country_r
#   industry_i: Type of industry (goods or services)
#   industry_j: Type of industry (goods or services)
#
# Returns:
#   beta: beta between country_s and country_r
#---------------------------------

cal_beta <- function(df, country_s, country_r, industry_i, industry_j) {
  ## Create value added in industry_j of country_r
  rj_columns <- which((df[3, ] == country_r) & (df[4, ] %in% industry_j))
  VAD_vec <- df[4927, rj_columns]
  VAD_rj <- sum(as.data.frame(lapply(VAD_vec, as.numeric)))
  
  ## Create trade flow in final goods from s to r
  # Data frame with trade flow from industry_i of country_s to country_r
  MRIO_col_index <- df[1:4, ]
  inter_input <- df[(df$X2 == country_s) & (df$X3 %in% industry_i), ]
  inter_input <- na.omit(inter_input)
  inter_input <- bind_rows(MRIO_col_index, inter_input)
  input_index <- inter_input[, 1:3]
  
  # Extract columns about final goods
  consume_columns <- which((inter_input[3, ] == country_r) & (grepl("^F", inter_input[4, ])))
  inter_input_consume <- inter_input[, consume_columns]
  
  # Sum of final goods (country_s -> country_r)
  s_to_r_inter_input_consume <- bind_cols(input_index, inter_input_consume)
  s_to_r_consumption_df <- s_to_r_inter_input_consume[5:nrow(s_to_r_inter_input_consume), 
                                                      4:ncol(s_to_r_inter_input_consume)]
  s_to_r_consumption_df <- as.data.frame(lapply(s_to_r_consumption_df, as.numeric))
  s_to_r_final_goods <- sum(s_to_r_consumption_df)
  
  ## Create beta
  beta <- as.numeric(s_to_r_final_goods / VAD_rj)
  
  return (beta)
}


#----TODO: Convert function "cal_BT_demand_supply" to new version that added industry factor----#

#---------------------------------
# Create BT_demand and BT_supply between country_s and country_r
#
# Args:
#   df: Preprocessed MRIO Data frame
#   year: Years to collect data
#   country_s: Code of country_s
#   country_r: Code of country_r
#   industry_i: Type of industry (goods or services)
#   industry_j: Type of industry (goods or services)
#
# Returns:
#   BT_demand: beta between country_s and country_r
#---------------------------------

cal_BT_demand_supply <- function(df, country_s, country_r, industry_i, industry_j) {
  ## Calculate beta for each country pair
  beta_ss <- cal_beta(df, country_s, country_s, industry_i, industry_i)
  beta_sr <- cal_beta(df, country_s, country_r, industry_i, industry_j)
  beta_rs <- cal_beta(df, country_r, country_s, industry_j, industry_i)
  beta_rr <- cal_beta(df, country_r, country_r, industry_j, industry_j)

  ## Calculate alpha for each country pair
  alpha_sr <- cal_alpha(df, country_s, country_r, industry_i, industry_j)
  alpha_rs <- cal_alpha(df, country_r, country_s, industry_j, industry_i)

  ## Calculate BT_demand
  BT_demand <- (beta_ss + beta_rs * alpha_rs) * (beta_rs + beta_ss * alpha_sr) + (beta_sr + beta_rr * alpha_rs) * (beta_rr + beta_sr * alpha_sr)

  ## Calculate BT_supply
  BT_supply <- alpha_rs + alpha_sr

  return (c(BT_demand, BT_supply))
}


### Total bilateral trade flows for the entire period and country ###

# Year list (Entire year)
year_lst <- c(1990:2022)
# year_lst <- c(1990:2022)

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
country_lst <- c("AFG", "ALB", "DZA", "AND", "AGO")

# Industry list (goods and services)
goods_lst <- paste0("c", 1:12)
services_lst <- paste0("c", 13:24)
industry_lst <- list("goods" = goods_lst, "services" = services_lst)

# copy
country_lst2 <- country_lst
industry_lst2 <- industry_lst

# Create ordered pair of country_s, country_r
lst_length <- numeric(length(year_lst) * sum(1:length(country_lst)) * length(industry_lst) * length(industry_lst2))

BT_demand_supply_lst <- list("Year" = lst_length,
                             "Country_s" = lst_length,
                             "Country_r" = lst_length,
                             "Industry_i" = lst_length,
                             "Industry_j" = lst_length,
                             "BT_demand" = lst_length,
                             "BT_supply" = lst_length)

time <- Sys.time()
index <- 1
for (year in year_lst) {
  num <- 1
  cat(paste0("#### [Collecting BT_demand_supply] Year: ", year, " ####\n"))
  df <- read_csv(paste0("Eora_csv_files/Eora", year, ".csv"))

  for (s in country_lst) {
    for (r in country_lst2[num:length(country_lst2)]) {
      for (i in names(industry_lst)) {
        for (j in names(industry_lst2)) {
          BT_demand_supply <- cal_BT_demand_supply(df, s, r, industry_lst[[i]], industry_lst[[j]])
          BT_demand <- BT_demand_supply[1]
          BT_supply <- BT_demand_supply[2]

          BT_demand_supply_lst$Year[index] <- year
          BT_demand_supply_lst$Country_s[index] <- s
          BT_demand_supply_lst$Country_r[index] <- r
          BT_demand_supply_lst$Industry_i[index] <- i
          BT_demand_supply_lst$Industry_j[index] <- j
          BT_demand_supply_lst$BT_demand[index] <- BT_demand
          BT_demand_supply_lst$BT_supply[index] <- BT_supply
    
          index <- index + 1
        }
      }
    }
    num <- num + 1
  }
  # list -> DataFrame
  BT_EachYear <- as.data.frame(BT_demand_supply_lst)
  
  # Replicate table which is interchanged country & Industry pair and remove duplicated rows
  BT_EachYear1 <- BT_EachYear
  BT_EachYear1[c("Country_s", "Country_r")] <- BT_EachYear[c("Country_r", "Country_s")]
  BT_EachYear1[c("Industry_i", "Industry_j")] <- BT_EachYear[c("Industry_j", "Industry_i")]
  BT_EachYear2 <- rbind(BT_EachYear, BT_EachYear1)
  BT_EachYear3 <- unique(BT_EachYear2)
  
  BT_EachYear3$Country_s <- factor(BT_EachYear3$Country_s, levels = country_lst)
  BT_EachYear3$Country_r <- factor(BT_EachYear3$Country_r, levels = country_lst)
  BT_EachYear3$Industry_i <- factor(BT_EachYear3$Industry_i, levels = names(industry_lst))
  BT_EachYear3$Industry_j <- factor(BT_EachYear3$Industry_j, levels = names(industry_lst))
  
  BT_EachYear4 <- BT_EachYear3 %>%
    arrange(Year, Country_s, Country_r, Industry_i, Industry_j)
  
  # Save it as a csv file
  write_csv(BT_EachYear4, "Eora_BT_demand_supply/Eora_BT_demand_supply_EachYear_sample.csv")
}
print(Sys.time()-time)


# # Seperate data from 2007~2013 and 2014~2020
# t1 <- BT_EachYear %>% filter(Year >= 1990 & Year <= 1999)
# t2 <- BT_EachYear %>% filter(Year >= 2000 & Year <= 2009)
# t3 <- BT_EachYear %>% filter(Year >= 2010 & Year <= 2022)
# 
# # Calculate average for each period
# BT_demand_supply_t1 <- t1 %>% group_by(Country_s, Country_r, Industry_i, Industry_j) %>%
#   summarise(
#     BT_demand = log(mean(BT_demand)),
#     BT_supply = log(mean(BT_supply)),
#     .groups = "keep"
#   )
# 
# BT_demand_supply_t2 <- t2 %>% group_by(Country_s, Country_r, Industry_i, Industry_j) %>%
#   summarise(
#     BT_demand = log(mean(BT_demand)),
#     BT_supply = log(mean(BT_supply)),
#     .groups = "keep"
#   )
# 
# BT_demand_supply_t3 <- t3 %>% group_by(Country_s, Country_r, Industry_i, Industry_j) %>%
#   summarise(
#     BT_demand = log(mean(BT_demand)),
#     BT_supply = log(mean(BT_supply)),
#     .groups = "keep"
#   )
# 
# # Create feature "t" for each variable
# BT_demand_supply_t1 <- BT_demand_supply_t1 %>% mutate(t = 1)
# BT_demand_supply_t2 <- BT_demand_supply_t2 %>% mutate(t = 2)
# BT_demand_supply_t3 <- BT_demand_supply_t3 %>% mutate(t = 3)
# 
# # Bind rows and send a feature "t" to the front
# BT_demand_supply <- bind_rows(BT_demand_supply_t1, BT_demand_supply_t2)
# BT_demand_supply1 <- bind_rows(BT_demand_supply, BT_demand_supply_t3)
# BT_demand_supply2 <- BT_demand_supply1 %>% select(t, everything())
# 
# # Save it as a csv file
# write_csv(BT_demand_supply2, "Eora_BT_demand_supply/Eora_BT_demand_supply_sample.csv")


