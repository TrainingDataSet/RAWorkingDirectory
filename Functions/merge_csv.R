# [Last Modified: 2024-08-02]

library(dplyr)
library(readr)

# Function that merges all CSV files in the directory
merge_csv_files <- function(directory, key_columns) {
  
  # Get a list of all CSV files in that directory
  csv_files <- list.files(directory, pattern = "*.csv", full.names = TRUE)
  
  # Read the CSV file and save it to the list
  csv_list <- lapply(csv_files, read_csv)
  
  # Set the first data frame to the initial data frame
  merged_df <- csv_list[[1]]
  
  # Merges the remaining data frames in turn
  for (i in 2:length(csv_list)) {
    merged_df <- full_join(merged_df, csv_list[[i]], by = key_columns)
  }
  
  return(merged_df)
}


# Run
directory <- "Dataset"
# key_columns <- c("Country_s", "Country_r")
key_columns <- c("t", "Country_i", "Country_j")
merged_df <- merge_csv_files(directory, key_columns)

file_name <- "Dataset/data_eoracountry (2).csv"
write_csv(merged_df, file_name)

# merged_df2 <- merged_df %>%
#   select(Country_s, Country_r, corr_rgdp, corr_rgdp_co2, corr_co2, 
#          corr_rgdp_capita, corr_rgdp_co2_capita, corr_co2_capita, 
#          Within_country_corr_CO2_i, Within_country_corr_CO2_j,
#          Corr_prod_CO2, Corr_diff_CO2, total, inter, final, BT_demand, BT_supply,
#          MB_network_1st, LB_network_1st, MB_network_2nd, LB_network_2nd, sector_prox)
# 
# merged_df3 <- merged_df2 %>%
#   rename(country_s = Country_s, country_r = Country_r,
#          log_bt_d_total = total, log_bt_d_inter = inter, log_bt_d_final = final,
#          log_bt_demand = BT_demand, log_bt_supply = BT_supply,
#          mb_network_1st = MB_network_1st, lb_network_1st = LB_network_1st,
#          mb_network_2nd = MB_network_2nd, lb_network_2nd = LB_network_2nd)
# 
# merged_df4 <- merged_df3 %>%
#   arrange(country_s, country_r)
# 
# file_name <- "ADB/Dataset/globaltrade_country_entirePeriod.csv"
# write_csv(merged_df4, file_name)
