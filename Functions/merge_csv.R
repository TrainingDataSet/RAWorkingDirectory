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
directory <- "consideringSector/Time_window"
# key_columns <- c("t", "Country_i", "Country_j", "Sector_s", "Sector_u")
key_columns <- c("t", "Country_i", "Country_j")
merged_df <- merge_csv_files(directory, key_columns)

merged_df2 <- merged_df %>%
  select(t, Country_i, Country_j, Sector_s, Sector_u, Corr_VAD, everything())

file_name <- "consideringSector/Dataset/dataset2.csv"
write_csv(merged_df2, file_name)
