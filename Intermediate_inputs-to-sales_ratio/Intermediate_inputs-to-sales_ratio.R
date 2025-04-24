# [Last Modified: 2024-10-06]
# 2024-10-06: sector-specific intermediate inputs-to-gross output ratio, 
#             weighted by the sector-level VAD/(sum of sector-level VAD) and averaged across sectors, for a given year.

library(dplyr)
library(readr)
library(parallel)
library(microbenchmark)

setwd("~/RAWorkingDirectory")


#-------------------------- Dataset --------------------------#

vad <- read_csv("VAD/Eora_VAD.csv")
inter_inputs <- read_csv("Inter_inputs/Inter_inputs.csv")
gross_output <- read_csv("Gross_output/Gross_output.csv")

df <- merge(vad, inter_inputs, by = c("Year", "Country", "Sector"))
df <- merge(df, gross_output, by = c("Year", "Country", "Sector"))


#------------------- Inter Inputs / Gross Output -------------------#

df$IO_ratio <- df$Inter_inputs / df$Gross_output


#------------ Sector-level VAD / Sum of Sector-level VAD ------------#

vad_sum <- df%>%
  group_by(Year, Country) %>%
  summarize(Total_VAD = sum(VAD))

df <- df %>%
  left_join(vad_sum, by = c("Year", "Country"))

df$VAD_ratio = df$VAD / df$Total_VAD


#----------------------- IO_ratio * VAD_ratio -----------------------#

df$IO_weighted_VAD = df$IO_ratio * df$VAD_ratio


#---------------------- Averaged Across Sectors ----------------------#

df2 <- df %>%
  group_by(Year, Country) %>%
  summarize(Inter_inputs_to_Sales = mean(IO_weighted_VAD))


#----------------------------- Write CSV -----------------------------#

write_csv(df, "Intermediate_inputs-to-sales_ratio/Sector_level_IO_ratio.csv")
write_csv(df2, "Intermediate_inputs-to-sales_ratio/Country_level_IO_ratio.csv")



