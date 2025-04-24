# [Last Modified: 2025-03-23]

#################################################################
# Code for Global Value Chain Decomposition Program 
#      for International Trade and Environment Lecture,
# Writer: Bongseok Choi, Kookmin U.
#################################################################

library(readr)
library(dplyr)
library(parallel)

setwd("~/RAWorkingDirectory")

## The year currently considered.
year_lst <- list(c(1990:1992), c(1993:1995), c(1996:1998), c(1999:2001), c(2002:2004), c(2005:2007), c(2008:2010),
                 c(2011:2013), c(2014:2016), c(2017:2019), c(2020:2022))

task <- function(year_lst) {
  
  for (year in year_lst) {
    df <- as.matrix(read_csv(sprintf("Eora_csv_files/Eora%d.csv", year)))
    WIOT <- df[5:nrow(df), 4:ncol(df)]
    WIOT_name <- df[5:4918, 1:3]
    
    # Convert all data to numeric type & replace NA to zero
    WIOT <- as.matrix(as.data.frame(WIOT) %>% mutate(across(everything(), ~as.numeric(as.character(.)))))
    WIOT[is.na(WIOT)] <- 0
    
    Z <- WIOT[1:4914,1:4914]  # intermediate input (189 countries * 26 industries, 189 countries * 26 industries)
    VADD <-WIOT[4923,1:4914]  # Value Added (1, 189 countries * 26 industries)
    Ztotal<-WIOT[4922,1:4914] # total output (1, 189 countries * 26 industries)
    FD <- WIOT[1:4914,4916:6049] # Final Demand block (189 countries * 26 industries, 189 countries * 6 final consumption)
    
    YY <- matrix(0, nrow = 4914, ncol = 1)
    YD <- matrix(0, nrow = 4914, ncol = 1)
    YF <- matrix(0, nrow = 4914, ncol = 1)
    YY_ctry <- matrix(0, nrow = 4914, ncol = 189)
    ZF <- matrix(0, nrow = 4914, ncol = 4914)
    
    # Number of Countries: 189
    # Number of Industries: 26
    
    # Construct Y^F
    
    for (i in 1:4914) {
      index_row_ctry <- (i-1)%/%26 +1
      for (j in seq(1,1129,6)) {
        index_col_ctry <- j%/%6 +1
        if (index_row_ctry == index_col_ctry) {
          YY_ctry[i,index_col_ctry] <- 0
        }
        else {
          YY_ctry[i,index_col_ctry] <- sum(FD[i, j:(j+5)])
        }
      }
    }
    
    
    # Construct Z for A^F
    
    ZF <- Z
    
    for( i in seq(1,4889,26)) {  
      index_row_ctry <- (i-1)%/%26 +1  
      for(j in seq(1,4889,26)){  
        index_col_ctry <- (j-1)%/%26 +1 
        
        if(index_row_ctry == index_col_ctry){  
          ZF[i:(i+25),j:(j+25)] <- 0} 
      }
    }
    
    
    import <- matrix(0, nrow=4914, ncol=1)
    export <- matrix(0, nrow=4914, ncol=1)
    export[,1] <- rowSums(ZF)
    import[,1] <- colSums(ZF)
    
    # summation over gross output of intermediate sectors, not final good sectors.
    YY[,1] <- rowSums(FD)
    YF[,1] <- rowSums(YY_ctry)
    
    YD <- YY - YF 
    Xhat <- matrix(diag(Ztotal),ncol=4914)
    inv_Xhat <- matrix(diag(1/Ztotal),ncol=4914)
    I <- diag(4914)
    AA <- Z%*%inv_Xhat
    
    AA[is.na(AA)] = 0
    
    AF <- ZF%*%inv_Xhat
    
    AF[is.na(AF)] = 0
    
    AD <- AA - AF
    L <- solve(I-AD) 
    B <- solve(I-AA) 
    
    VV <- matrix(diag(VADD),ncol=4914)
    Vhat <- VV%*%inv_Xhat
    
    # another way to construt Vhat #
    VV2 <- VADD%*%inv_Xhat
    VV_mat<-  matrix(VV2,nrow = 4914,ncol = length(VV2),byrow = TRUE)
    VV_hat <- diag(diag(VV_mat))
    
    VV_check <- Vhat - VV_hat
    
    # Duplicate vector in matrix rows
    YY_mat<-  matrix(YY, nrow = 4914,ncol = length(YY),byrow = TRUE)
    YF_mat<-  matrix(YF, nrow = 4914,ncol = length(YF),byrow = TRUE)
    YD_mat<-  matrix(YD, nrow = 4914,ncol = length(YD),byrow = TRUE)
    YY_hat <- diag(diag(YY_mat))
    YF_hat <- diag(diag(YF_mat))
    YD_hat <- diag(diag(YD_mat))
    
    Vhat[is.na(Vhat)] = 0
    YY_hat[is.na(YY_hat)] = 0
    YF_hat[is.na(YF_hat)] = 0
    YD_hat[is.na(YD_hat)] = 0
    
    # 1) Value added domestically produced and consumed (not involved cross-border trade)
    DVAD <- Vhat%*%L%*%YD_hat
    # 2) Value added in final product export (traditonal cross-border trade)
    VAD_EXP_FIN <- Vhat%*%L%*%YF_hat
    # 3) Value added embodied exports/imports of intermediate goods and services
    VAD_EXIM_INT <- Vhat%*%L%*%AF%*%B%*%YY_hat
    # 4) Simple cross-country production sharing activities (Simple GVC)
    GVC_simple <- Vhat%*%L%*%AF%*%L%*%YD_hat
    # 5) Complex cross-country production sharing activities (Complex GVC)
    GVC_complex <- Vhat%*%L%*%AF%*%(B%*%YY_hat-L%*%YD_hat)
    
    #eqn (5)
    #Va_prime <- t(Vhat%*%B%*%YY)
    Va_prime <- rowSums(Vhat%*%B%*%YY_hat)
    #eqn (6)
    #Y_prime <- VADD%*%B%*%YY_hat
    Y_prime <- colSums(Vhat%*%B%*%YY_hat)
    
    DVAD_f <- rowSums(DVAD)/Va_prime
    VAD_EXP_FIN_f <- rowSums(VAD_EXP_FIN)/Va_prime
    VAD_EXIM_INT_f <- rowSums(VAD_EXIM_INT)/Va_prime
    
    
    GVC_s_f <- rowSums(GVC_simple)/Va_prime
    GVC_c_f <- rowSums(GVC_complex)/Va_prime
    GVC_f <- GVC_s_f + GVC_c_f
    
    DVAD_b <- colSums(DVAD)/Y_prime
    VAD_EXP_FIN_b <- colSums(VAD_EXP_FIN)/Y_prime
    VAD_EXIM_INT_b <- colSums(VAD_EXIM_INT)/Y_prime
    
    GVC_s_b <- colSums(GVC_simple)/Y_prime
    GVC_c_b <- colSums(GVC_complex)/Y_prime
    GVC_b <- GVC_s_b + GVC_c_b
    
    GVC_results <- matrix(0, nrow=4914, ncol=13)
    
    GVC_results[,1] <- year
    GVC_results[,2] <- t(GVC_s_f)
    GVC_results[,3] <- t(GVC_c_f)
    GVC_results[,4] <- t(GVC_f)
    GVC_results[,5] <- t(GVC_s_b)
    GVC_results[,6] <- t(GVC_c_b)
    GVC_results[,7] <- t(GVC_b)
    GVC_results[,8] <- t(DVAD_f)
    GVC_results[,9] <- t(VAD_EXP_FIN_f)
    GVC_results[,10] <- t(VAD_EXIM_INT_f)
    GVC_results[,11] <- t(DVAD_b)
    GVC_results[,12] <- t(VAD_EXP_FIN_b)
    GVC_results[,13] <- t(VAD_EXIM_INT_b)
    
    
    GVC_output <- cbind(Name = WIOT_name, GVC_results)
    col_names <- c("Industry", "Country", "Industry ID", "Year", "GVC_s_f", "GVC_c_f", "GVC_f", "GVC_s_b", "GVC_c_b", "GVC_b", "DVAD_f", "VAD_EXP_FIN_f", "VAD_EXIM_INT_f", "DVAD_b", "VAD_EXP_FIN_b", "VAD_EXIM_INT_b")
    GVC_table  <- rbind(col_names, GVC_output)
    
    write.table(GVC_table, file=paste0("Eora_GVC_Decomposition/GVC_table_", year,".csv"), sep=",", col.names=TRUE, row.names=FALSE)
  }
}

cl <- makeCluster(11)

clusterExport(cl, varlist = c("task", "year_lst", "read_csv", "%>%", "mutate", "across", "write.table", "sprintf"))

tasks <- list(
  function() task(year_lst[[1]]),
  function() task(year_lst[[2]]),
  function() task(year_lst[[3]]),
  function() task(year_lst[[4]]),
  function() task(year_lst[[5]]),
  function() task(year_lst[[6]]),
  function() task(year_lst[[7]]),
  function() task(year_lst[[8]]),
  function() task(year_lst[[9]]),
  function() task(year_lst[[10]]),
  function() task(year_lst[[11]])
)

time <- Sys.time()
parLapply(cl, tasks, function(f) f())
stopCluster(cl)
print(Sys.time()-time)  # --> running time 1:48:44
