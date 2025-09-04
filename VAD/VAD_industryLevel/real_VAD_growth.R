# [Last Modified: 2025-05-27]

library(dplyr)
library(readr)

setwd("~/RAWorkingDirectory")

df <- read_csv("VAD/real_VAD.csv")

df_growth <- df %>%
  group_by(Country, Sector) %>%
  arrange(Year, .by_group = TRUE) %>%
  mutate(
    VAD_growth = (VAD - lag(VAD)) / lag(VAD)
  )

write_csv(df_growth, "VAD/real_VAD_growth.csv")
