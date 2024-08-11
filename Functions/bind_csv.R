# [Last Modified: 2024-06-25]

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
directory = "consideringNothing/BT"
file_name = "consideringNothing/BT/BT.csv"
bind_csv(directory, file_name)

