# [Last Modified: 2024-02-29]

library(readr)
library(dplyr)

setwd("~/RAWorkingDirectory")


### Make Eora txt files similar to MRIOr csv files ###
Convert_Eora <- function(year) {

  #--------------[[Intermediate Trade]]--------------#
  
  # Intermediate Trade data set in Eora
  df_T <- read.table(sprintf("EoraProject/Eora26_1990to2022/Eora26_%d_bp/Eora26_%d_bp_T.txt", year, year),
                     header = FALSE, sep = "\t")
  
  # Intermediate Trade labels in Eora
  labels_T <- read.table(sprintf("EoraProject/Eora26_1990to2022/Eora26_%d_bp/labels_T.txt", year),
                         header = FALSE, sep = "\t")
  
  # Make Eora Intermediate Trade labels similar to MRIO 
  left_T <- labels_T %>% select(V4, V2)
  ind_level <- unique(left_T$V4)
  left_T$V3 <- paste0("c", as.numeric(factor(left_T$V4, levels = ind_level)))
  
  NA_df <- data.frame(matrix(NA, nrow = 3, ncol = 3))
  top_T <- cbind(NA_df, t(left_T))
  
  # Make Eora Intermediate Trade Data Frame similar to MRIO
  left_T <- cbind(left_T, df_T)
  names(left_T) <- names(top_T)
  Eora_T <- rbind(top_T, left_T)
  
  #-----------------[[Final Demand]]-----------------#
  
  # Final Demand data set in Eora
  df_FD <- read.table(sprintf("EoraProject/Eora26_1990to2022/Eora26_%d_bp/Eora26_%d_bp_FD.txt", year, year),
                     header = FALSE, sep = "\t")
  
  # Final Demand labels in Eora
  labels_FD <- read.table(sprintf("EoraProject/Eora26_1990to2022/Eora26_%d_bp/labels_FD.txt", year),
                         header = FALSE, sep = "\t")
  
  # Make Eora Final Demand labels similar to MRIO 
  left_FD <- labels_FD %>% select(V4, V2)
  fd_level <- unique(left_FD$V4)
  left_FD$V3 <- paste0("F", as.numeric(factor(left_FD$V4, levels = fd_level)))
  top_FD <- t(left_FD)
  Eora_FD <- rbind(top_FD, df_FD)
  
  # Bind FD to T
  Eora_T_FD <- cbind(Eora_T, Eora_FD)
  
  #-----------------[[Value Added]]-----------------#
  
  # Value Added data set in Eora
  df_VA <- read.table(sprintf("EoraProject/Eora26_1990to2022/Eora26_%d_bp/Eora26_%d_bp_VA.txt", year, year),
                      header = FALSE, sep = "\t")
  
  # Value Added labels in Eora
  labels_VA <- read.table(sprintf("EoraProject/Eora26_1990to2022/Eora26_%d_bp/labels_VA.txt", year),
                          header = FALSE, sep = "\t")
  
  # Make Eora Final Demand labels similar to MRIO 
  left_VA <- labels_VA %>% select(V2)
  NA_vec <- rep(NA, nrow(left_VA))
  left_VA$V3 <- NA_vec
  va_level <- unique(left_VA$V2)
  left_VA$V4 <- paste0("r", as.numeric(factor(left_VA$V2, levels = va_level)))
  
  # Make Eora Value Added Data Frame similar to MRIO
  Eora_VA <- cbind(left_VA, df_VA)
  
  # Bind VA to Eora_T_FD
  NA_df2 <- data.frame(matrix(NA, nrow = nrow(left_VA), ncol(df_FD)))
  Eora2_VA <- cbind(Eora_VA, NA_df2)
  names(Eora2_VA) <- names(Eora_T_FD)
  Eora <- rbind(Eora_T_FD, Eora2_VA)
  
  #--------------------[[TOTAL]]--------------------#
  
  # Calcuate TOTAL of Gross Product
  Eora_numeric <- as.data.frame(lapply(Eora[4:nrow(Eora), 4:ncol(Eora)], as.numeric))
  colSums_vec <- c("TOTAL", rep(NA, 2), colSums(Eora_numeric))
  Eora2 <- rbind(Eora, colSums_vec)
  
  Eora_numeric2 <- as.data.frame(lapply(Eora2[4:nrow(Eora2), 4:ncol(Eora2)], as.numeric))
  rowSums_vec <- c("TOTAL", rep(NA, 2), rowSums(Eora_numeric2, na.rm = TRUE))
  Eora3 <- cbind(Eora2, rowSums_vec)
  
  # Calcuate Sum of Value Added
  Eora_numeric2 <- as.data.frame(lapply(Eora3[4919:4924, 4:4918], as.numeric))
  colSums_vec2 <- c("Value Added", rep(NA, 2), colSums(Eora_numeric2))
  NA_vec2 <- c(rep(NA, 1141))
  colSums_vec3 <- c(colSums_vec2, NA_vec2)
  
  Eora4 <- rbind(Eora3, colSums_vec3)
  
  # Add first row of MRIO
  row1 <- c(rep(NA, 3), paste0("000", 1:(ncol(Eora4)-3)))
  Eora5 <- rbind(row1, Eora4)
  
  # Name in order
  colnames(Eora5) <- paste0("X", 1:ncol(Eora5))
  rownames(Eora5) <- 1:nrow(Eora5)
  
  # Save as a csv file
  write_csv(Eora5, sprintf("Eora_csv_files/Eora%d.csv", year))
  
}


### Make Eora txt files similar to MRIOr csv files ###
Convert_Eora_Q <- function(year) {

  #-------------------[[Pollutants Dataset]]-------------------#
  
  # Pollutants labels in Eora
  labels_Q <- read.table(sprintf("EoraProject/Eora26_1990to2022/Eora26_%d_bp/labels_Q.txt", year),
                         header = FALSE, sep = "\t")
  
  # Pollutants for each Industry data in Eora
  df_Q <- read.table(sprintf("EoraProject/Eora26_1990to2022/Eora26_%d_bp/Eora26_%d_bp_Q.txt", year, year),
                     header = FALSE, sep = "\t")
  
  # Pollutants for each Final Demand data in Eora
  df_QY <- read.table(sprintf("EoraProject/Eora26_1990to2022/Eora26_%d_bp/Eora26_%d_bp_QY.txt", year, year),
                     header = FALSE, sep = "\t")
  
  # Bind all data frame
  Eora_Q <- cbind(labels_Q[1:2], df_Q)
  Eora_Q2 <- cbind(Eora_Q, df_QY)
  
  #------------[[Intermediate & Final Demand Label]]------------#
  
  # Intermediate Trade labels in Eora
  labels_T <- read.table(sprintf("EoraProject/Eora26_1990to2022/Eora26_%d_bp/labels_T.txt", year),
                         header = FALSE, sep = "\t")
  
  # Make Eora Intermediate Trade labels similar to MRIO 
  left_T <- labels_T %>% select(V4, V2)
  ind_level <- unique(left_T$V4)
  left_T$V3 <- paste0("c", as.numeric(factor(left_T$V4, levels = ind_level)))
  
  NA_df <- data.frame(matrix(NA, nrow = 3, ncol = 2))
  top_T <- cbind(NA_df, t(left_T))
  
  # Final Demand labels in Eora
  labels_FD <- read.table(sprintf("EoraProject/Eora26_1990to2022/Eora26_%d_bp/labels_FD.txt", year),
                          header = FALSE, sep = "\t")
  
  # Make Eora Final Demand labels similar to MRIO 
  left_FD <- labels_FD %>% select(V4, V2)
  fd_level <- unique(left_FD$V4)
  left_FD$V3 <- paste0("F", as.numeric(factor(left_FD$V4, levels = fd_level)))
  top_FD <- t(left_FD)
  
  top_label <- cbind(top_T, top_FD)
  names(top_label) <- names(Eora_Q2)
  Eora_Q3 <- rbind(top_label, Eora_Q2)
  
  #--------------------[[TOTAL]]--------------------#
  
  # Calcuate TOTAL of Pollutants
  Eora_Q_numeric <- as.data.frame(lapply(Eora_Q3[4:nrow(Eora_Q3), 3:ncol(Eora_Q3)], as.numeric))
  colSums_vec <- c("TOTAL", NA, colSums(Eora_Q_numeric))
  Eora_Q4 <- rbind(Eora_Q3, colSums_vec)
  
  Eora_Q_numeric2 <- as.data.frame(lapply(Eora_Q4[4:nrow(Eora_Q4), 3:ncol(Eora_Q4)], as.numeric))
  rowSums_vec <- c("TOTAL", rep(NA, 2), rowSums(Eora_Q_numeric2, na.rm = TRUE))
  Eora_Q5 <- cbind(Eora_Q4, rowSums_vec)
  
  # Name in order
  colnames(Eora_Q5) <- paste0("X", 1:ncol(Eora_Q5))
  rownames(Eora_Q5) <- 1:nrow(Eora_Q5)
  
  # Save as a csv file
  write_csv(Eora_Q5, sprintf("Eora_csv_files/Eora_Q%d.csv", year))
  
}


### Convert all Eora txt files to csv files silmilar to MRIO ###

# Convert all Eora txt files to csv files similar to MRIOr.csv
for (year in c(1990:2022)) {
  print(sprintf("Converting file in %d", year)) 
  Convert_Eora(year)
}

# Convert all Eora_Q txt files to csv files similar to MRIOr.csv
for (year in c(1990:2022)) {
  print(sprintf("Converting file in %d", year)) 
  Convert_Eora_Q(year)
}







