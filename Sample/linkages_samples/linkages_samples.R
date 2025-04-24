# [Last Modified: 2025-01-03]

library(dplyr)
library(readr)

setwd("~/RAWorkingDirectory")

# Linkages_to_tradedSector_sample
linkages <- read_parquet("Linkages/Linkages_to_tradedSector/Linkages_to_tradedSector.parquet")
linkages_sample <- subset(linkages, Country_i %in% c("AGO", "KOR"))
write_parquet(linkages_sample, "Linkages_to_tradedSector_sample.parquet")

# Linkages_to_tradedSector_iSj_sample
linkages_iSj <- read_parquet("Linkages/Linkages_to_tradedSector/Linkages_to_tradedSector_iSj.parquet")
linkages_iSj_sample <- subset(linkages_iSj, Country_i %in% c("AGO", "KOR"))
write_parquet(linkages_iSj_sample, "Linkages_to_tradedSector_iSj_sample.parquet")

# Linkages_to_tradedSector_ij_sample
linkages_ij <- read_parquet("Linkages/Linkages_to_tradedSector/Linkages_to_tradedSector_ij.parquet")
linkages_ij_sample <- subset(linkages_ij, Country_i %in% c("AGO", "KOR"))
write_parquet(linkages_ij_sample, "Linkages_to_tradedSector_ij_sample.parquet")

# MB_linkages_S_Stilde_sample
MB_linkages_S_Stilde <- read_parquet("MB_linkages/MB_linkages_S_Stilde.parquet")
MB_linkages_S_Stilde_sample <- subset(MB_linkages_S_Stilde, Country %in% c("AGO", "KOR"))
write_parquet(MB_linkages_S_Stilde_sample, "MB_linkages_S_Stilde_sample.parquet")

# MB_linkages_S_k_Stilde_sample
MB_linkages_S_k_Stilde <- read_parquet("MB_linkages/MB_linkages_S_k_Stilde.parquet")
MB_linkages_S_k_Stilde_sample <- subset(MB_linkages_S_k_Stilde, Country %in% c("AGO", "KOR"))
write_parquet(MB_linkages_S_k_Stilde_sample, "MB_linkages_S_k_Stilde_sample.parquet")

# MB_linkages_i_S_Stilde_j_sample
MB_linkages_i_S_Stilde_j <- read_parquet("MB_linkages/MB_linkages_i_S_Stilde_j.parquet")
MB_linkages_i_S_Stilde_j_sample <- subset(MB_linkages_i_S_Stilde_j, Country_i %in% c("AGO", "KOR"))
write_parquet(MB_linkages_i_S_Stilde_j_sample, "MB_linkages_i_S_Stilde_j_sample.parquet")

# MB_linkages_i_S_j_sample
MB_linkages_i_S_j <- read_parquet("MB_linkages/MB_linkages_i_S_j.parquet")
MB_linkages_i_S_j_sample <- subset(MB_linkages_i_S_j, Country_i %in% c("AGO", "KOR"))
write_parquet(MB_linkages_i_S_j_sample, "MB_linkages_i_S_j_sample.parquet")

# MB_linkages_i_j
MB_linkages_i_j <- read_parquet("MB_linkages/MB_linkages_i_j.parquet")
MB_linkages_i_j_sample <- subset(MB_linkages_i_j, Country_i %in% c("AGO", "KOR"))
write_parquet(MB_linkages_i_j_sample, "MB_linkages_i_j_sample.parquet")





