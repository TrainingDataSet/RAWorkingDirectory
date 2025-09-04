# [Last Modified: 2025-06-29]

library(dplyr)
library(readr)

setwd("~/RAWorkingDirectory")

df <- read_csv("VAD_countryLevel/real_VAD.csv")

df_growth <- df %>%
  group_by(Country) %>%
  arrange(Year, .by_group = TRUE) %>%
  mutate(
    VAD_growth = (real_VAD - lag(real_VAD)) / lag(real_VAD)
  )

write_csv(df_growth, "VAD_countryLevel/real_VAD_growth.csv")
