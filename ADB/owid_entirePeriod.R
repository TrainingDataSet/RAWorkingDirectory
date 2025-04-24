# [Last Modified: 2023-12-31]

library(dplyr)
library(hpfilter)
library(readxl)
library(readr)

setwd("~/RAWorkingDirectory")

df <- read_csv("ADB/sector_prox_EachYear_whole.csv")

# Calculate average for each period
BT_d_t1 <- df %>% group_by(Country_s, Country_r) %>%
  summarise(
    sector_prox = mean(sector_prox)
  )

# Save it as a csv file
write_csv(BT_d_t1, "ADB/Dataset/sector_prox.csv")
