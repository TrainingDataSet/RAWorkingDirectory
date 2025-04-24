# [Last Modified: 2023-12-31]

library(dplyr)
library(hpfilter)
library(readxl)
library(readr)

setwd("~/RAWorkingDirectory")

df <- read_csv("ADB/sector_prox_EachYear_whole.csv")

# Seperate data from 2007~2013 and 2014~2020
t1 <- df %>% filter(Year >= 2007 & Year <= 2013)
t2 <- df %>% filter(Year >= 2014 & Year <= 2020)

# Calculate average for each period
sector_prox_t1 <- t1 %>% group_by(Country_s, Country_r) %>%
  summarise(
    sector_prox = mean(sector_prox)
  )

sector_prox_t2 <- t2 %>% group_by(Country_s, Country_r) %>%
  summarise(
    sector_prox = mean(sector_prox)
  )

# Create feature "t" for each variable
sector_prox_t1 <- sector_prox_t1 %>% mutate(t = 1)
sector_prox_t2 <- sector_prox_t2 %>% mutate(t = 2)

# Bind rows and send a feature "t" to the front
sector_prox <- bind_rows(sector_prox_t1, sector_prox_t2)
sector_prox <- sector_prox %>% select(t, everything())

# Save it as a csv file
write_csv(sector_prox, "ADB/Dataset_tw/sector_prox.csv")

