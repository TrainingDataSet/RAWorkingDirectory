# [Last Modified: 2024-05-09]

library(dplyr)
library(readr)

setwd("~/RAWorkingDirectory")

bind_csv <- function(directory, file_name) {
  
  # Get a list of all CSV files in that directory
  file_list <- list.files(directory, pattern = "\\.csv$", full.names = TRUE)
  
  # Read all CSV files and merge them into one data frame
  combined_data <- lapply(file_list, read_csv) %>%
    bind_rows()
  
  write_csv(combined_data, file_name)
}

# Run
directory = "MB_pollutants"
file_name = "MB_pollutants/MB_pollutants.csv"
bind_csv(directory, file_name)

