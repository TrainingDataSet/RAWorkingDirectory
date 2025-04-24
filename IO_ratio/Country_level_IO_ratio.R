# [Last Modified: 2024-10-06]
# 2024-10-06: sector-specific intermediate inputs-to-gross output ratio, 
#             weighted by the sector-level VAD/(sum of sector-level VAD) and averaged across sectors, for a given year.

library(dplyr)
library(readr)
library(parallel)
library(microbenchmark)

setwd("~/RAWorkingDirectory")


#----------------------- Industry level Dataset -----------------------#
df <- read_csv("IO_ratio/Industry_level_IO_ratio.csv")



#--------------------- Averaged Across Industries ---------------------#

df2 <- df %>%
  group_by(Year, Country) %>%
  summarize(IO_ratio = sum(IO_ratio, na.rm = TRUE))


#----------------------------- Write CSV -----------------------------#

write_csv(df2, "IO_ratio/Country_level_IO_ratio.csv")



