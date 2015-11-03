#
# Script to implement MeTHODICAL for CCNX
#
#
#ssh  user@10.0.1.131
# 10.0.1.123
# 10.10.10.30

# rngBen <- 2:2
# rngCost <- 2:4
# myRNGBEN <- 1:2
# myRNGCOST <- 3:5
# myWrngBEN <- 1:1
# myWrngCost <- 2:4



aux_step03_Weight_of_Norm_matrix <-function(inMatrix, inWeight){
  
  outMat <- inMatrix
  ncols<-dim(inMatrix)[2]
  nrows<-dim(inMatrix)[1]
  
  #iMcalc2 <- matrix(inMatrix[,2:ncols], nrow=nrows, ncol=ncols-1, byrow=TRUE)
  iMcalc <- matrix(inMatrix[,2:ncols], nrow=nrows, ncol=ncols-1, byrow=FALSE)
  
  nC <- dim(iMcalc)[2] # Convert weigths into a matrix in order to perform Mult 
  aMmul <- matrix(rep(inWeight, times=nrows), ncol=nC, byrow=TRUE)
  
  auxCalc <- iMcalc * aMmul
  
  outMat[,2:ncols]<-auxCalc
  
  return(outMat)
}

FMC_WD <- function(mBen, mCost, wBen, wCost, namesList, name){

  mBen <- data.frame(matrix(unlist(mBen), ncol=2, byrow=F))
  colnames(mBen) <- c("id", "pop")

  mCost <- data.frame(matrix(unlist(mCost), ncol=4, byrow=F))
  colnames(mCost) <- c("id", "ratio", "size", "pop")

  namesList <- data.frame(matrix(unlist(namesList), ncol=2, byrow=F))
  colnames(namesList) <- c("id", "prefix")

  save(mBen, mCost, wBen, wCost, namesList, file=name)
}

FMC_LD <- function(dataFile, algorithm){
  load(dataFile)
  out <- FMC_MADM(mBen, mCost, wBen, wCost, algorithm)

  dfNames <- data.frame(matrix(unlist(namesList), ncol=2, byrow=F))
  colnames(dfNames) <- c("id", "prefix")

  dfOut <- data.frame(matrix(unlist(out), ncol=2, byrow=F))
  colnames(dfOut) <- c("id", "score")

  output <- merge(dfNames, dfOut, by="id")
  output <- output[2:3]
  output <- output[order(output$score),]
  return(output)
}

FMC_MADM <- function(mBen, mCost, wBen, wCost, algorithm=1){

  source("METH_related.r")

  #save(file="./input_data_.data", list=ls())

  auxBen_col <- dim(mBen)[2]
  auxCost_col <- dim(mCost)[2]

  if (dim(mBen)[1] < 1 || dim(mCost)[1] < 1){
    return(-2)
  }
  else if (dim(mBen)[1] < 2 || dim(mCost)[1] < 2){
    return(-1)
  }

  myRNGBEN <- seq(1, by=1, to=auxBen_col)
  rngBen <- seq(1, by=1, to=auxBen_col)

  #DANGER
  myRNGCOST <- seq(auxBen_col+1, by=1, to=auxBen_col + auxCost_col -1)
  rngCosts <- seq(2, by=1, to=auxCost_col)

  myWrngBEN <- seq(1, by=1, to=length(wBen))
  myWrngCost <- seq(length(wBen) +1, by=1, to=length(wBen) + length(wCost))

  if (is.list(mBen)){
    mBen <- matrix(unlist(mBen), ncol=auxBen_col, byrow=FALSE)
  }

  if (is.list(mCost)){
    mCost <- matrix(unlist(mCost), ncol=auxCost_col, byrow=FALSE)
  }

  if (algorithm == 1){
    
    iM <- mBen
    iM <- cbind(iM, mCost[,rngCosts])

    iVecWei <- wBen
    iVecWei <- append(iVecWei, wCost)

    aux_out <- METH_METHODICALv10(iM, iVecWei, rngBEN=myRNGBEN, rngCost=myRNGCOST, rngWBen=myWrngBEN, rngWCost=myWrngCost)
  }
  else if (algorithm == 2){
    aux_out <- METH_runTOPSIS(mBen, mCost, wBen, wCost)
  }
  else if (algorithm == 3){
    aux_out <- METH_runDiA(mBen, mCost, wBen, wCost)
  }
  else if (algorithm == 4){
    aux_out <- METH_runNMMD(mBen, mCost, wBen, wCost)
  }

  return(aux_out)
}



METH_METHODICALv9 <-function(mBen, mCost, weiBen_prefMultihoming, weiCost_prefMultihoming){
  
  auxBen_col <- dim(mBen)[2] - 1
  auxCost_col <- dim(mCost)[2] - 1

  myRNGBEN <- seq(1, by=1, to=auxBen_col)
  rngBen   <- seq(1, by=1, to=auxBen_col)
  # DANGER
  myRNGCOST <- seq(auxBen_col+1, by=1, to=auxBen_col + auxCost_col - 1 )
  rngCosts   <- seq(2, by=1, to=auxCost_col)
  
#   print("BEN")
#   print(auxBen_col)
#   print(myRNGBEN)
#   #print(rngBen)
#   
#   print("COST")
#   print(auxCost_col)
#   print(myRNGCOST)
#   print(rngCosts)
  
  myWrngBEN  <- seq(1, by=1, to=length(weiBen_prefMultihoming) )
  myWrngCost <- seq(length(weiBen_prefMultihoming) +1, by=1, to= length(weiBen_prefMultihoming) + length(weiCost_prefMultihoming) )
  
#   print("W BEN")
#   print(myWrngBEN)
#   print("W COST")
#   print(myWrngCost)
  
  iM <-mBen
  iM <- cbind(iM , mCost[,rngCosts])
  
  #str(iM)
  #print("#R")
  #print(iM)
  
  iVecWei <- weiBen_prefMultihoming
  iVecWei <- append(iVecWei, weiCost_prefMultihoming)
  
  aux_res<-METH_METHODICALv10(iM, iVecWei,  rngBEN=myRNGBEN, rngCost=myRNGCOST, rngWBen=myWrngBEN, rngWCost=myWrngCost )
  
  #str(aux_res)
  #print("#R")
  #print(aux_res)
  
  return (aux_res)
}



#
#
# Code for Version 10 uses Variance instead of standard deviation
# 
METH_METHODICALv10 <- function(iM, iVecWei, rngBEN=1:5, rngCost=6, rngWBen=1:4, rngWCost=5, MeTHBeta=0.5, MeTHOmega=0.5, itry=1 ){
  
  
  MINSUM_TOPSIS <- 1e-99 # To avoid divisions by zero in normalization
  
  mBen_Criteria <- iM[,rngBEN]
  mCost_Criteria <- iM[,c(1,rngCost)] # Put also the id in the matrix of costs.
  
  vBen_weight <- iVecWei[rngWBen]
  vCost_weight <- iVecWei[rngWCost]
  
  applyWeight <- TRUE
  applyNorm <- TRUE
  
  #norm_method <- "minmax"
  norm_method <- "vector"
  #norm_method <- "max"
  #norm_method <- "sum"
  
  fAuxPos <- function(iN){
    
    if (iN < 0) {
      iN <- iN * (-1)
    }
    return(iN)
  }
  
  fAux_Positive_Dif <- function(i1, i2){
    adif <- (i1 - i2)
    #if (adif < 0) {
    #  adif <- adif * (-1)
    #}
    adif<-fAuxPos(adif)
    return(adif)
  }
  
  # 
  # Determine RScore
  #
  calcRscorev10 <- function(inM, alpha=0.5, omega=0.5){
    nrow <- dim(inM)[1]
    outMat <- inM
    outMat <- outMat[,-3]
    
    for (nR in 1:nrow){
      #outMat[nR,2] <-  sqrt(alpha*(inM[nR,2]) + omega*(inM[nR,3])   ) # no square
      outMat[nR,2] <-  sqrt((inM[nR,2]) + (inM[nR,3])   ) # no square
      outMat[nR,2] <- outMat[nR,2] #+ auxSD
    }
    
    return(outMat)
  }
  
  #
  # It can't be leads to zeros.
  #
  calcRscorev10b_asTOPSIS <- function(inM, alpha=0.5, omega=0.5){
    nrow <- dim(inM)[1]
    outMat <- inM
    outMat <- outMat[,-3]
    
    for (nR in 1:nrow){
      #outMat[nR,2] <-  sqrt(alpha*(inM[nR,2]) + omega*(inM[nR,3])   ) # no square
      outMat[nR,2] <- inM[nR,3] / inM[nR,3] + inM[nR,2]
      outMat[nR,2] <- outMat[nR,2] #+ auxSD
    }
    
    return(outMat)
  }
  
  
  # I think this method produces a lot of zeros.
  aux_step02_MinMax_Normalization<- function(inMatrix, iTypeBen=TRUE, iRangeNorm=1){
    
    #internal function to help in normalization
    fAuxLinearBEN <- function(i, iMax, iMin){
      aa <- iMax - iMin
      if (aa != 0){
        aSu <- ( (i - iMin) / (iMax - iMin) ) 
      }else{
        aSu <- 0
      }
      return(aSu)
    }
    
    fAuxLinearCOST <- function(i, iMax, iMin){
      aa <- iMax - iMin
      if (aa != 0){
        aSu <- ( (iMax - i) / (iMax - iMin) ) 
      }else{
        aSu <- 0
      }
      return(aSu)
    }
    
    
    outMat <- inMatrix
    ncols<-dim(inMatrix)[2]
    nrows<-dim(inMatrix)[1]
    
    iMcalc <- matrix(inMatrix[,2:ncols], nrow=nrows, ncol=ncols-1, byrow=FALSE)
    
    #Apply Sum by col
    MaxPer_Row <- apply(iMcalc, 2, max)
    MinPer_Row <- apply(iMcalc, 2, min)
    
    nC <- dim(iMcalc)[2]
    nR <- dim(iMcalc)[1]
    aMMax <- matrix(rep(MaxPer_Row, times=nR), ncol=nC, byrow=TRUE)
    aMMin <- matrix(rep(MinPer_Row, times=nR), ncol=nC, byrow=TRUE)
    
    if (iTypeBen==TRUE){
      auxCalc <- mapply(FUN=fAuxLinearBEN, iMcalc, aMMax, aMMin)
    }else{
      auxCalc <- mapply(FUN=fAuxLinearCOST, iMcalc, aMMax, aMMin)
    }
    
    auxCalc <- auxCalc * iRangeNorm
    
    auxCalc <- matrix(auxCalc, nrow=nR, ncol=nC, byrow=FALSE)
    #nC <- dim(iMcalc)[2]
    #aMSum <- matrix(rep(auxSum, times=nrows), ncol=nC, byrow=TRUE)
    #auxCalc <- iMcalc / sqrt(aMSum)
    
    outMat[,2:ncols]<-auxCalc
    return(outMat)
    
  }
  
  
  aux_step02_VEC_Normalization <- function(inMatrix){
    
    #internal function to help in normalization
    fSum <- function(i){
      sumrow <- MINSUM_TOPSIS
      aSu <- sum(i^2) + sumrow
      return(aSu)
    }
    
    outMat <- inMatrix
    ncols<-dim(inMatrix)[2]
    nrows<-dim(inMatrix)[1]
    
    iMcalc <- matrix(inMatrix[,2:ncols], nrow=nrows, ncol=ncols-1, byrow=FALSE)
    #print(iMcalc)
    
    #Apply Sum by col
    auxSum <- apply(iMcalc, 2, FUN=fSum)
    #print(auxSum)
    
    nC <- dim(iMcalc)[2]
    aMSum <- matrix(rep(auxSum, times=nrows), ncol=nC, byrow=TRUE)
    auxCalc <- iMcalc / sqrt(aMSum)
    #print(auxCalc)
    
    outMat[,2:ncols]<-auxCalc
    return(outMat)
    
  }
  

  
  #
  # The opposite of minMax leads to a lot of one's if criterion is maximized.
  #
  aux_step02_Max_Normalization<- function(inMatrix, iTypeBen=TRUE, iRangeNorm=1){
    
    #internal function to help in normalization
    fAuxLinearBEN <- function(i, iMax){
      aa <- iMax 
      if (aa != 0){
        aSu <- ( i  / iMax ) 
      }else{
        aSu <- 0
      }
      return(aSu)
    }
    
    fAuxLinearCOST <- function(i, iMax){
      aa <- iMax 
      if (aa != 0){
        aSu <- 1 - ( i / iMax  ) 
      }else{
        aSu <- 0
      }
      return(aSu)
    }
    
    
    outMat <- inMatrix
    ncols<-dim(inMatrix)[2]
    nrows<-dim(inMatrix)[1]
    
    iMcalc <- matrix(inMatrix[,2:ncols], nrow=nrows, ncol=ncols-1, byrow=FALSE)
    
    #Apply Sum by col
    MaxPer_Row <- apply(iMcalc, 2, max)
    #MinPer_Row <- apply(iMcalc, 2, min)
    
    nC <- dim(iMcalc)[2]
    nR <- dim(iMcalc)[1]
    aMMax <- matrix(rep(MaxPer_Row, times=nR), ncol=nC, byrow=TRUE)
    #aMMin <- matrix(rep(MinPer_Row, times=nR), ncol=nC, byrow=TRUE)
    if (iTypeBen==TRUE){
      auxCalc <- mapply(FUN=fAuxLinearBEN, iMcalc, aMMax)
    }else{
      auxCalc <- mapply(FUN=fAuxLinearCOST, iMcalc, aMMax)
    }
    
    auxCalc <- auxCalc * iRangeNorm
    
    auxCalc <- matrix(auxCalc, nrow=nR, ncol=nC, byrow=FALSE)
    #nC <- dim(iMcalc)[2]
    #aMSum <- matrix(rep(auxSum, times=nrows), ncol=nC, byrow=TRUE)
    #auxCalc <- iMcalc / sqrt(aMSum)
    
    outMat[,2:ncols]<-auxCalc
    return(outMat)
    
  }
  
  
  aux_step02_SUM_Normalization <- function(inMatrix){
    
    #internal function to help in normalization
    fSum <- function(i){
      sumrow <- MINSUM_TOPSIS
      aSu <- sum(i) + sumrow
      return(aSu)
    }
    
    outMat <- inMatrix
    ncols<-dim(inMatrix)[2]
    nrows<-dim(inMatrix)[1]
    
    iMcalc <- matrix(inMatrix[,2:ncols], nrow=nrows, ncol=ncols-1, byrow=FALSE)
    #print(iMcalc)
    
    #Apply Sum by col
    auxSum <- apply(iMcalc, 2, FUN=fSum)
    #print(auxSum)
    
    nC <- dim(iMcalc)[2]
    aMSum <- matrix(rep(auxSum, times=nrows), ncol=nC, byrow=TRUE)
    auxCalc <- iMcalc / aMSum
    #print(auxCalc)
    
    outMat[,2:ncols]<-auxCalc
    return(outMat)
    
  }
  
  calcDist_METHODICALv10b<- function(inMatrix, inIdeal, inMin, inMax, BenCriteria=TRUE, ignFirstCol=TRUE){
    
    ncols<-dim(inMatrix)[2]
    nrows<-dim(inMatrix)[1]
    
    if (ignFirstCol){
      iMcalc <- matrix(inMatrix[,2:ncols], nrow=nrows, ncol=ncols-1, byrow=FALSE)
     
    }else{
      iMcalc <- inMatrix
     
    }
    
    fCalcAux <- function(iMcalc){
      auxMean <- mean(iMcalc) 
      auxSD   <- var(iMcalc) # MAIN difference in MeTH_v10
      #auxSD   <- sd(iMcalc) # MAIN difference in MeTH_v10
      
      if (BENC){
        SDMean <- auxMean + auxSD  
      }else{
        SDMean <- auxMean - auxSD
      }
      return(SDMean)
    }
    
    outMat <- matrix(ncol=2,nrow=nrows)
    outMat[,1] <- inMatrix[,1]
    
    nColMat <- dim(iMcalc)[2]
    ncolIdeal <- dim(inIdeal)[2]
    
    #}
    # Just to be sure
    stopifnot(nColMat == ncolIdeal)
    
    BENC <<- BenCriteria
    
    SDMean <- apply(iMcalc, 2, FUN=fCalcAux)
    
    mSDMean <- matrix(SDMean, byrow=TRUE, nrow=dim(iMcalc)[1], ncol=dim(iMcalc)[2])
    mIdeal  <- matrix(inIdeal, byrow=TRUE, nrow=dim(iMcalc)[1], ncol=dim(iMcalc)[2])
    mMax  <- matrix(inMax, byrow=TRUE, nrow=dim(iMcalc)[1], ncol=dim(iMcalc)[2])
    mMin  <- matrix(inMin, byrow=TRUE, nrow=dim(iMcalc)[1], ncol=dim(iMcalc)[2])
    
    #Perform Operation
    #auxTETA <- -1 *  (mSDMean - iMcalc)
    #Yaux <- (iMcalc - mIdeal)^2
    #auxDiffDeno <- (mMax - mMin)^2 #originally without sqrt
    #auxDeno <-  (auxTETA + auxDiffDeno)
    
    
    #auxTETA <- -1 *  (mSDMean - iMcalc)
    #Yaux <- (iMcalc - mIdeal)^2 + (mIdeal - mSDMean)^2
    #auxDiffDeno <- (mMax - mMin)^2 #originally without sqrt
    #auxDeno <-  (auxDiffDeno - auxTETA)
    
    Yaux <- (iMcalc - mIdeal)^2 
    auxDeno <-  abs(mIdeal - mSDMean) + 0.001
    
    
    auxCalcM <- (Yaux /  auxDeno )
    auxCalcM[is.na(auxCalcM)]<-0
    
    auxCalc <- apply(auxCalcM, 1, sum)
      
    outMat[,2] <- auxCalc
    
    return(outMat)  
  }
  
  
  
  ncolMB <- dim(mBen_Criteria)[2]
  MeTHTOPsisBenefits <- as.matrix(mBen_Criteria )
  
  ncolMC <- dim(mCost_Criteria)[2]
  MeTHTOPsisCosts <- as.matrix(mCost_Criteria )
  
  MeTHWeiBenTOP <- vBen_weight
  MeTHWeiCostTOP <- vCost_weight
  
  #
  # Step 01
  #
  
  # Needs libNormalization
  # apply Normalization
  if (applyNorm==TRUE){
    
    #ISSUE
    if (norm_method =="minmax"){
      MeTHTOPsisBenefits <- aux_step02_MinMax_Normalization(MeTHTOPsisBenefits, iTypeBen=TRUE)
      MeTHTOPsisCosts    <- aux_step02_MinMax_Normalization(MeTHTOPsisCosts, iTypeBen=FALSE)
    }
    
    if (norm_method =="vector"){
      MeTHTOPsisBenefits <- aux_step02_VEC_Normalization(MeTHTOPsisBenefits)
      MeTHTOPsisCosts    <- aux_step02_VEC_Normalization(MeTHTOPsisCosts)
    }
    
    if (norm_method =="sum"){
      MeTHTOPsisBenefits <- aux_step02_SUM_Normalization(MeTHTOPsisBenefits)
      MeTHTOPsisCosts    <- aux_step02_SUM_Normalization(MeTHTOPsisCosts)
    }
    
    if (norm_method =="max"){
      MeTHTOPsisBenefits <- aux_step02_Max_Normalization(MeTHTOPsisBenefits, iTypeBen=TRUE)
      MeTHTOPsisCosts    <- aux_step02_Max_Normalization(MeTHTOPsisCosts, iTypeBen=FALSE)
    }
    
  }
  
  
  
  #Apply Weighting
  if (applyWeight==TRUE){
    MeTHTOPsisBenefits <-aux_step03_Weight_of_Norm_matrix(MeTHTOPsisBenefits, MeTHWeiBenTOP) 
    MeTHTOPsisCosts    <-aux_step03_Weight_of_Norm_matrix(MeTHTOPsisCosts, MeTHWeiCostTOP) 
  }
  
  
  #
  # Step 02
  #
  #Retrieve Maximum and Minimum
  rngBBen <- seq(from=2, by=1, to=dim(MeTHTOPsisBenefits)[2])
  rngCCost <- seq(from=2, by=1, to=dim(MeTHTOPsisCosts)[2])
  
  
  MeTHMaxIdeal<- as.numeric(apply(as.matrix(MeTHTOPsisBenefits[,rngBBen]), 2, max))
  MeTHBenMin  <- as.numeric(apply(as.matrix(MeTHTOPsisBenefits[,rngBBen]), 2, min))
  MeTHBenMax  <- MeTHMaxIdeal
  #
  MeTHMinIdeal<- as.numeric(apply(as.matrix(MeTHTOPsisCosts[,rngCCost]), 2, min))
  MeTHCostMax <- as.numeric(apply(as.matrix(MeTHTOPsisCosts[,rngCCost]), 2, max))
  MeTHCostMin <- MeTHMinIdeal
  
  

  MeTHBenDist<-calcDist_METHODICALv10b(MeTHTOPsisBenefits,MeTHMaxIdeal, MeTHBenMin, MeTHBenMax, BenCriteria=TRUE)
  MeTHCostDist<-calcDist_METHODICALv10b(MeTHTOPsisCosts,MeTHMinIdeal, MeTHCostMin, MeTHCostMax, BenCriteria=FALSE)
  
  
  
  #
  # Step 04
  #
  inMatForR <- MeTHBenDist
  inMatForR <- cbind(inMatForR, MeTHCostDist[,2])
  #inMatForR
  
  
  MeTHRScore<-calcRscorev10(inMatForR, MeTHBeta, MeTHOmega)
  #RScoreANT <<- MeTHRScore
  
  orderRet  <-  MeTHRScore[order(MeTHRScore[,2]),]
  
  return(orderRet)
  #return(MeTHRScore) 
}




