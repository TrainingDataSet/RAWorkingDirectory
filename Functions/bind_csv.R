# [Last Modified: 2025-01-15]

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
directory = "consideringSector/BT2/"
file_name = "consideringSector/BT2/BT.csv"
bind_csv(directory, file_name)

