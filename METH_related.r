
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


#' 
#' @param Matrices with Benefits and Costs, 
#' @param Vectors with Benefits and Costs weights,
#' @return Score of Paths 
#' @author Bruno Sousa
#' @note v1.0 no arguments validation
#' @title METH_runTOPSIS
#' @name METH_runTOPSIS
METH_runTOPSIS <- function(iMBen, iMCost, iVecBen,iVecCost ){
  # Important Global Variables
  MIN_COST_TOPSIS_TENDENCY <- 0.0001  # To avoid divisions by zero
  BETA_TOPSIS_TENDENCY_WEIGHTS <- 0.5  
  MINSUM_TOPSIS <- 1e-99 # To avoid divisions by zero in normalization
  
  #source("libNormalization.R")
  
  mBen_Criteria <- iMBen
  mCost_Criteria <- iMCost
  
  vBen_weight <- iVecBen
  vCost_weight <- iVecCost
 
  
  # Euclidean Distance
  fAux_Euclidean_Dist<- function(i1, i2){
    adif <- (i1 - i2)^2
    
    return(adif)
  }
  
  
  
  
  #
  # Convert weight costs into Benefit costs
  #
  aux_step01_Tendency_Cnv <- function(inCosts){
    nCol <- dim(inCosts)[2]
    aux <- matrix(inCosts[,2:nCol], ncol=nCol-1)
    
    aux[which(aux==0) ] <- MIN_COST_TOPSIS_TENDENCY 
    aux <- 1/aux
    
    return (aux)
  }
  
  
  # 
  # Append Benefits and Cost weigths 
  #
  aux_step01_Tendend_Weigths <- function(inWeBen, inWeCost){
    
    ret_Ben  <-    BETA_TOPSIS_TENDENCY_WEIGHTS * inWeBen
    ret_Cost <- (1 - BETA_TOPSIS_TENDENCY_WEIGHTS) * inWeCost
    
    ret_ <- c(ret_Ben, ret_Cost)
    
    return(ret_)
  }
  
  
  #
  # Normalize matrix
  # Using Vector normalization instead of min-max
  #
  aux_step02_Normalization <- function(inMatrix){
    
    #internal function to help in normalization
    fSum <- function(i){
      sumrow <- MINSUM_TOPSIS
      aSu <- sum(i^2) + sumrow
      return(aSu)
    }
    
    outMat <- inMatrix
    ncols<-dim(inMatrix)[2]
    nrows<-dim(inMatrix)[1]
    
    iMcalc <- matrix(inMatrix[,2:ncols], nrow=nrows, ncol=ncols-1, byrow=TRUE)
    
    #Apply Sum by col
    auxSum <- apply(iMcalc, 2, FUN=fSum)
    
    nC <- dim(iMcalc)[2]
    aMSum <- matrix(rep(auxSum, times=nrows), ncol=nC, byrow=TRUE)
    auxCalc <- iMcalc / sqrt(aMSum)
    
    outMat[,2:ncols]<-auxCalc
    return(outMat)
    
  }
  
  # 
  # Determine PIS according to DiA Method
  #
  aux_step04_PIS <-function(inM){
    ncolu <- dim(inM)[2]
    nrows <- dim(inM)[1]
    iMcalc <- matrix(inM[,2:ncolu], nrow=nrows, ncol=ncolu-1, byrow=TRUE)
    auxRet <- apply(iMcalc, 2, max)
    return (auxRet)  
  }
  
  aux_step04_NIS<-function(inM){
    ncolu <- dim(inM)[2]
    nrows <- dim(inM)[1]
    iMcalc <- matrix(inM[,2:ncolu], nrow=nrows, ncol=ncolu-1, byrow=TRUE)
    auxRet <- apply(iMcalc, 2, min)
    return (auxRet)	
  }
  
  
  #
  # Determine the distance to the ideal points (PIS and NIS)
  #
  aux_step05_distance_to_ideal <- function(inApos,  inMatrix){
    #Be careful with the idx of Ideal....
    ncols<- dim(inMatrix)[2]
    nrows<- dim(inMatrix)[1]
    
    auxMBenef <- matrix(ncol=2,nrow=nrows)
    auxMBenef[,1] <- inMatrix[,1] 
    
    mCalc <- matrix(inMatrix[,2:ncols], nrow=nrows, ncol=ncols-1, byrow=FALSE)
    
    for (nR in seq(from=1, to=nrows)){
      aSumBen <- 0
      for (nC in seq(from=1, to=ncol(mCalc))){
        aSumBen <- aSumBen + fAux_Euclidean_Dist(mCalc[nR, nC] , inApos[nC]) 
      }
      auxMBenef[nR,2] <- aSumBen
    }
    
    
    ret_ <- auxMBenef
    return (ret_)
  }
  
  
  #
  # Determine R score
  #
  aux_step06_Ranking<-function(inSepPos, inSepNeg){
    
    nRow <- dim(inSepPos)[1]
    nCol <- dim(inSepPos)[2]
    auxCi <- matrix(ncol=2, nrow=nRow)
    auxCi[,1] <- inSepPos[,1]
    mCalc <- matrix(c(inSepPos[,2], inSepNeg[,2]), ncol=2, nrow=nRow, byrow=FALSE)
    
    
    
    fDist <- function(i){ # Apply a function on pairwise columns
      ipos <- mCalc[i,1]
      ineg <- mCalc[i,2]
      auxCj <- ineg / (ipos + ineg)
      return (auxCj)
    }
    
    # dist
    aDist <- sapply(1:nrow(mCalc), fDist)
    auxCi[,2] <- aDist 
    return(auxCi)
  }
  
  
  ncolMB <- dim(mBen_Criteria)[2]
  TPSTOPsisBenefits <- as.matrix(mBen_Criteria )
  
  ncolMC <- dim(mCost_Criteria)[2]
  TPSTOPsisCosts <- as.matrix(mCost_Criteria )
  
  TPSWeiBenTOP <- vBen_weight
  TPSWeiCostTOP <- vCost_weight
  
  #
  # Step 01
  #
  TPSTOPsisCostsTended <- aux_step01_Tendency_Cnv(TPSTOPsisCosts)
  TPSWeiBenTOPall <- aux_step01_Tendend_Weigths(TPSWeiBenTOP, TPSWeiCostTOP)
  TPSTOPsisBenefits <- cbind(TPSTOPsisBenefits, TPSTOPsisCostsTended)
  TPSWeiBenTOP <- TPSWeiBenTOPall
  stopifnot(TPSTOPsisBenefits != NULL)
  
  #
  # Step 02
  #
  TPSTOPsisBenefitsTOP <- aux_step02_Normalization(TPSTOPsisBenefits)
  
  #
  # Step 03
  #
  TPSTOPsisBenefitsTOP <- aux_step03_Weight_of_Norm_matrix(TPSTOPsisBenefitsTOP, TPSWeiBenTOP)
  
  # Step 04 - Ideal Solutions
  TPSApos_Dia <- aux_step04_PIS(TPSTOPsisBenefitsTOP)
  TPSAneg_Dia <- aux_step04_NIS(TPSTOPsisBenefitsTOP)
  
  # Step 05 - Distance to Ideal
  TPSDposBen_Dia <- aux_step05_distance_to_ideal(TPSApos_Dia, TPSTOPsisBenefitsTOP )
  TPSDnegBen_Dia <- aux_step05_distance_to_ideal(TPSAneg_Dia, TPSTOPsisBenefitsTOP)
  
  #Step 06
  #RdistBen_Dia  <- aux_step06_Rscore(DposBen_Dia, DnegBen_Dia)
  TPStopsisRanking <- aux_step06_Ranking(TPSDposBen_Dia, TPSDnegBen_Dia) 
  #TPStopsisRanking
  
  #print(TPStopsisRanking)
  return(TPStopsisRanking[order(TPStopsisRanking[,2]),])
}



#' 
#' @param Matrices with Benefits and Costs, 
#' @param Vectors with Benefits and Costs weights,
#' @return Score of Paths 
#' @author Bruno Sousa
#' @note v1.0 no arguments validation
#' @title METH_runDiA
#' @name METH_runDiA
METH_runDiA <- function(iMBen, iMCost, iVecBen,iVecCost ){
  # Important Global Variables
  MIN_COST_TOPSIS_TENDENCY <- 0.0001  # To avoid divisions by zero
  BETA_TOPSIS_TENDENCY_WEIGHTS <- 0.5  
  MINSUM_TOPSIS <- 1e-99 # To avoid divisions by zero in normalization
  
  #source("libNormalization.R")
  
  mBen_Criteria <- iMBen
  mCost_Criteria <- iMCost
  
  vBen_weight <- iVecBen
  vCost_weight <- iVecCost
 
  
  #
  # Convert weight costs into Benefit costs
  #
  aux_step01_Tendency_Cnv <- function(inCosts){
    nCol <- dim(inCosts)[2]
    aux <- matrix(inCosts[,2:nCol], ncol=nCol-1)
    
    aux[which(aux==0) ] <- MIN_COST_TOPSIS_TENDENCY 
    aux <- 1/aux
    
    return (aux)
  }
  
  
  # 
  # Append Benefits and Cost weigths 
  #
  aux_step01_Tendend_Weigths <- function(inWeBen, inWeCost){
    
    ret_Ben  <-    BETA_TOPSIS_TENDENCY_WEIGHTS * inWeBen
    ret_Cost <- (1 - BETA_TOPSIS_TENDENCY_WEIGHTS) * inWeCost
    
    ret_ <- c(ret_Ben, ret_Cost)
    
    return(ret_)
  }
  
  
  #
  # Normalize matrix
  # Using Vector normalization instead of min-max
  #
  aux_step02_Normalization <- function(inMatrix){
    
    #internal function to help in normalization
    fSum <- function(i){
      sumrow <- MINSUM_TOPSIS
      aSu <- sum(i^2) + sumrow
      return(aSu)
    }
    
    outMat <- inMatrix
    ncols<-dim(inMatrix)[2]
    nrows<-dim(inMatrix)[1]
    
    iMcalc <- matrix(inMatrix[,2:ncols], nrow=nrows, ncol=ncols-1, byrow=TRUE)
    
    #Apply Sum by col
    auxSum <- apply(iMcalc, 2, FUN=fSum)
    
    nC <- dim(iMcalc)[2]
    aMSum <- matrix(rep(auxSum, times=nrows), ncol=nC, byrow=TRUE)
    auxCalc <- iMcalc / sqrt(aMSum)
    
    outMat[,2:ncols]<-auxCalc
    return(outMat)
    
  }
  
  
  # 
  # Determine PIS according to DiA Method
  #
  aux_step04_PIS <-function(inM){
    ncolu <- dim(inM)[2]
    nrows <- dim(inM)[1]
    iMcalc <- matrix(inM[,2:ncolu], nrow=nrows, ncol=ncolu-1, byrow=TRUE)
    auxRet <- apply(iMcalc, 2, max)
    return (auxRet)  
  }
  
  aux_step04_NIS<-function(inM){
    ncolu <- dim(inM)[2]
    nrows <- dim(inM)[1]
    iMcalc <- matrix(inM[,2:ncolu], nrow=nrows, ncol=ncolu-1, byrow=TRUE)
    auxRet <- apply(iMcalc, 2, min)
    return (auxRet)	
  }
  
  
  #
  # Determine the distance to the ideal points (PIS and NIS)
  #
  aux_step05_distance_to_ideal <- function(inApos,  inMatrix){
    #Be careful with the idx of Ideal....
    ncols<- dim(inMatrix)[2]
    nrows<- dim(inMatrix)[1]
    
    auxMBenef <- matrix(ncol=2,nrow=nrows)
    auxMBenef[,1] <- inMatrix[,1] 
    
    mCalc <- matrix(inMatrix[,2:ncols], nrow=nrows, ncol=ncols-1, byrow=FALSE)
    #internal fx
    fAux_Positive_Dif <- function(i1, i2){
      adif <- (i1 - i2)
      if (adif < 0) {
        adif <- adif * (-1)
      }
      return(adif)
    }
    
    
    
    for (nR in seq(from=1, to=nrows)){
      aSumBen <- 0
      for (nC in seq(from=1, to=ncol(mCalc))){
        aSumBen <- aSumBen + fAux_Positive_Dif(mCalc[nR, nC] , inApos[nC]) 
      }
      auxMBenef[nR,2] <- aSumBen
    }
    
    
    ret_ <- auxMBenef
    return (ret_)
  }
  
  
  #
  # Determine R score
  #
  aux_step06_Rscore<-function(inSepPos, inSepNeg){
    
    nRow <- dim(inSepPos)[1]
    nCol <- dim(inSepPos)[2]
    auxCi <- matrix(ncol=2, nrow=nRow)
    auxCi[,1] <- inSepPos[,1]
    mCalc <- matrix(c(inSepPos[,2], inSepNeg[,2]), ncol=2, nrow=nRow, byrow=FALSE)
    
    # get ideal point
    PIA_pos <- min(inSepPos[,2])
    PIA_neg <- max(inSepNeg[,2])
    
    fDist <- function(i){ # Apply a function on pairwise columns
      ipos <- mCalc[i,1]
      ineg <- mCalc[i,2]
      auxSqrt <- sqrt( (ipos - PIA_pos)^2 + (ineg - PIA_neg)^2 )
      return (auxSqrt)
    }
    
    # dist
    aDist <- sapply(1:nrow(mCalc), fDist)
    auxCi[,2] <- aDist 
    return(auxCi)
  }
  
  
  ncolMB <- dim(mBen_Criteria)[2]
  DiATOPsisBenefits <- as.matrix(mBen_Criteria )
  
  ncolMC <- dim(mCost_Criteria)[2]
  DiATOPsisCosts <- as.matrix(mCost_Criteria )
  
  DiAWeiBenTOP <- vBen_weight
  DiAWeiCostTOP <- vCost_weight
  
  #
  # Step 01
  #
  DiATOPsisCostsTended <- aux_step01_Tendency_Cnv(DiATOPsisCosts)
  DiAWeiBenTOPall <- aux_step01_Tendend_Weigths(DiAWeiBenTOP, DiAWeiCostTOP)
  DiATOPsisBenefits <- cbind(DiATOPsisBenefits, DiATOPsisCostsTended)
  DiAWeiBenTOP <- DiAWeiBenTOPall
  stopifnot(DiATOPsisBenefits != NULL)
  
  #
  # Step 02
  #
  DiATOPsisBenefitsTOP <- aux_step02_Normalization(DiATOPsisBenefits)
  DiATOPsisBenefitsTOPNorm <- DiATOPsisBenefitsTOP
  
  #
  # Step 03
  #
  DiATOPsisBenefitsTOP <- aux_step03_Weight_of_Norm_matrix(DiATOPsisBenefitsTOP, DiAWeiBenTOP)
  
  # Step 04
  DiAApos_Dia <- aux_step04_PIS(DiATOPsisBenefitsTOP)
  DiAAneg_Dia <- aux_step04_NIS(DiATOPsisBenefitsTOP)
  
  # Step 05
  DiADposBen_Dia <- aux_step05_distance_to_ideal(DiAApos_Dia, DiATOPsisBenefitsTOP )
  DiADnegBen_Dia <- aux_step05_distance_to_ideal(DiAAneg_Dia, DiATOPsisBenefitsTOP)
  
  #Step 06
  DiARdistBen_Dia  <- aux_step06_Rscore(DiADposBen_Dia, DiADnegBen_Dia)
  #DiARdistBen_Dia
  
  
  #print(DiARdistBen_Dia)
  return(DiARdistBen_Dia[order(DiARdistBen_Dia[,2]),])
  
  
}



#' 
#' @param Matrices with Benefits and Costs, 
#' @param Vectors with Benefits and Costs weights,
#' @return Score of Paths 
#' @author Bruno Sousa
#' @note v1.0 no arguments validation
#' @title METH_runNMMD
#' @name METH_runNMMD
METH_runNMMD <- function(iMBen, iMCost, iVecBen,iVecCost ){
  require("corpcor")
  require("HDMD")
  
  # Important Global Variables
  MIN_COST_TOPSIS_TENDENCY <- 0.0001  # To avoid divisions by zero
  BETA_TOPSIS_TENDENCY_WEIGHTS <- 0.5  
  MINSUM_TOPSIS <- 1e-99 # To avoid divisions by zero in normalization
  
  #source("libNormalization.R")
  
  mBen_Criteria <- iMBen
  mCost_Criteria <- iMCost
  
  vBen_weight <- iVecBen
  vCost_weight <- iVecCost
  
  
  
  # Function to check if Mahalanobis can be processed
  # This is to avoid errors on singular matrices.
  # TODO: need to understand why this can happen
  #
  # TODO2: Need a positive definite matrix... Probably due to the fact of having few data
  #        Some useful urls:
  #        http://www.r-bloggers.com/dealing-with-non-positive-definite-matrices-in-r/
  #   http://www2.gsu.edu/~mkteer/npdmatri.html
  fAux_Do_Mahalanobis <- function(iM){
    ret <- TRUE
    #auxCov  <- cov(iM)
    auxCov2 <- cov.shrink(iM,verbose=FALSE)
    
    #if (det(auxCov)==0){ ret <- FALSE }
    if (!is.positive.definite(auxCov2)){
      ret <- FALSE
    }
    return (ret)
  }
  
  #Mahalanobis Distance
  fAux_Mahalanobis_Dist <- function(iM, me=NULL){
    
    if (length(me)==0){
      auxMean <- apply(iM, 2, mean)
    }else{
      auxMean <- me
    }
    #auxCov  <- cov(iM)
    auxCov  <- cov.shrink(iM, verbose=FALSE)
    
    # To avoid errors
    if (fAux_Do_Mahalanobis(iM)){
      #print(paste("doing Maha for"  ))
      #print(iM)
      #print(auxCov)
      #print(det(auxCov))
      auxDistMaha <- mahalanobis(iM, auxMean, auxCov)
      return (auxDistMaha)
    }else{
      return (-1)
    }
  }
  
  
  
  myPairwise.mahalanobis <- function (x, grouping = NULL, cov = NULL, inverted = FALSE, digits = 5, ...) 
  {
    x <- if (is.vector(x)) 
      matrix(x, ncol = length(x))
    else as.matrix(x)
    if (!is.matrix(x)) 
      stop("x could not be forced into a matrix")
    if (length(grouping) == 0) {
      grouping = t(x[1])
      x = x[2:dim(x)[2]]
      cat("assigning grouping\n")
      print(grouping)
    }
    n <- nrow(x)
    p <- ncol(x)
    if (n != length(grouping)) {
      cat(paste("n: ", n, "and groups: ", length(grouping), 
                "\n"))
      stop("nrow(x) and length(grouping) are different")
    }
    g <- as.factor(grouping)
    g
    lev <- lev1 <- levels(g)
    counts <- as.vector(table(g))
    if (any(counts == 0)) {
      empty <- lev[counts == 0]
      warning(sprintf(ngettext(length(empty), "group %s is empty", 
                               "groups %s are empty"), paste(empty, collapse = " ")), 
              domain = NA)
      lev1 <- lev[counts > 0]
      g <- factor(g, levels = lev1)
      counts <- as.vector(table(g))
    }
    ng = length(lev1)
    group.means <- tapply(x, list(rep(g, p), col(x)), mean)
    if (missing(cov)) {
      inverted = FALSE
      cov = cor(x)
    }
    else {
      if (dim(cov)[1] != p && dim(cov)[2] != p) 
        stop("cov matrix not of dim = (p,p)\n")
    }
    Distance = matrix(nrow = ng, ncol = ng)
    dimnames(Distance) = list(names(group.means), names(group.means))
    Means = round(group.means, digits)
    Cov = round(cov, digits)
    Distance = round(Distance, digits)
    for (i in 1:ng) {
      Distance[i, ] = mahalanobis(group.means, group.means[i,], cov, inverted)
    }
    result <- list(means = group.means, cov = cov, distance = Distance)
    result
  }
  
  #Mahalanobis Distance
  fAux_Pairwise.Mahalanobis_Dist <- function(iM, me=NULL){
    
    if (length(me)==0){
      auxMean <- apply(iM, 2, mean)
    }else{
      auxMean <- me
    }
    #auxCov  <- cov(iM)
    auxCov  <- cov.shrink(iM, verbose=FALSE)
    
    # To avoid errors
    if (fAux_Do_Mahalanobis(iM)){
      #print(paste("doing Maha for"  ))
      #print(iM)
      #print(det(auxCov))
      #print(ncol(iM))
      #auxDistMaha <- pairwise.mahalanobis(iM, grouping=t(iM[,1]), digits=5, center=auxMean, cov=auxCov)
      auxDistMaha <- myPairwise.mahalanobis(iM, grouping=t(iM[,1]), digits=5, center=auxMean, cov=auxCov)
      return (auxDistMaha)
    }else{
      return (-1)
    }
  }
  
  
  #
  # Convert weight costs into Benefit costs
  #
  aux_step01_Tendency_Cnv <- function(inCosts){
    nCol <- dim(inCosts)[2]
    aux <- matrix(inCosts[,2:nCol], ncol=nCol-1)
    
    aux[which(aux==0) ] <- MIN_COST_TOPSIS_TENDENCY 
    aux <- 1/aux
    
    return (aux)
  }
  
  # 
  # Append Benefits and Cost weigths 
  #
  aux_step01_Tendend_Weigths <- function(inWeBen, inWeCost){
    
    ret_Ben  <-    BETA_TOPSIS_TENDENCY_WEIGHTS * inWeBen
    ret_Cost <- (1 - BETA_TOPSIS_TENDENCY_WEIGHTS) * inWeCost
    
    ret_ <- c(ret_Ben, ret_Cost)
    
    return(ret_)
  }
  
  
  #
  # Normalize matrix
  # Using Vector normalization instead of min-max
  #
  aux_step02_Normalization <- function(inMatrix){
    
    #internal function to help in normalization
    fSum <- function(i){
      sumrow <- MINSUM_TOPSIS
      aSu <- sum(i^2) + sumrow
      return(aSu)
    }
    
    outMat <- inMatrix
    ncols<-dim(inMatrix)[2]
    nrows<-dim(inMatrix)[1]
    
    iMcalc <- matrix(inMatrix[,2:ncols], nrow=nrows, ncol=ncols-1, byrow=TRUE)
    
    #Apply Sum by col
    auxSum <- apply(iMcalc, 2, FUN=fSum)
    
    nC <- dim(iMcalc)[2]
    aMSum <- matrix(rep(auxSum, times=nrows), ncol=nC, byrow=TRUE)
    auxCalc <- iMcalc / sqrt(aMSum)
    
    outMat[,2:ncols]<-auxCalc
    return(outMat)
    
  }
  
  
  #
  # Weight the normalized matrix, by multiplying each value by its weight
  #
  aux_step03_Weight_of_Norm_matrix <-function(inMatrix, inWeight){
    
    outMat <- inMatrix
    ncols<-dim(inMatrix)[2]
    nrows<-dim(inMatrix)[1]
    
    iMcalc <- matrix(inMatrix[,2:ncols], nrow=nrows, ncol=ncols-1, byrow=TRUE)
    
    nC <- dim(iMcalc)[2] # Convert weigths into a matrix in order to perform Mult 
    aMmul <- matrix(rep(inWeight, times=nrows), ncol=nC, byrow=TRUE)
    
    auxCalc <- iMcalc * aMmul
    
    outMat[,2:ncols]<-auxCalc
    
    return(outMat)
  }
  
  
  # 
  # Determine PIS according to DiA Method
  #
  aux_step04_PIS <-function(inM){
    ncolu <- dim(inM)[2]
    nrows <- dim(inM)[1]
    iMcalc <- matrix(inM[,2:ncolu], nrow=nrows, ncol=ncolu-1, byrow=TRUE)
    auxRet <- apply(iMcalc, 2, max)
    return (auxRet)	
  }
  
  aux_step04_NIS<-function(inM){
    ncolu <- dim(inM)[2]
    nrows <- dim(inM)[1]
    iMcalc <- matrix(inM[,2:ncolu], nrow=nrows, ncol=ncolu-1, byrow=TRUE)
    auxRet <- apply(iMcalc, 2, min)
    return (auxRet)	
  }
  
  
  #
  # Determine the distance to the ideal points (PIS and NIS)
  #
  aux_step05_distance_to_idealNMMD <- function(inMatrix){
    #Be careful with the idx of Ideal....
    ncols<- dim(inMatrix)[2]
    nrows<- dim(inMatrix)[1]
    
    #auxMBenef <- matrix(ncol=2,nrow=nrows)
    #auxMBenef[,1] <- inMatrix[,1] 
    
    mCalc <- matrix(inMatrix[,2:ncols], nrow=nrows, ncol=ncols-1, byrow=FALSE)
    
    auxMBenef <- fAux_Pairwise.Mahalanobis_Dist(mCalc, colMeans(mCalc))
   
    
    ret_ <- auxMBenef
    return (ret_)
  }
  
  #
  # Determine R score
  #
  aux_step06_RankingNMMD<-function(inPathsIDs, inDist ){
    
    nRow <- dim(inDist)[1]
    nCol <- dim(inDist)[2]
    auxCi <- matrix(ncol=nCol, nrow=nRow)
    #auxCi[,1] <- 1:nRow
    auxCi[,1] <- inPathsIDs 
    mCalc <- inDist 
    
    
    fDist <- function(i){ # Apply a function on pairwise columns
      auxCj <- sum(mCalc[i,]) / (ncol(mCalc))
      return (auxCj)
    }
    
    # dist
    aDist <- sapply(1:nrow(mCalc), fDist)
    auxCi[,2] <- aDist 
    return(auxCi)
  }
  
  ncolMB <- dim(mBen_Criteria)[2]
  NMMDTOPsisBenefits <- as.matrix(mBen_Criteria )
  
  ncolMC <- dim(mCost_Criteria)[2]
  NMMDTOPsisCosts <- as.matrix(mCost_Criteria )
  
  NMMDWeiBenTOP <- vBen_weight
  NMMDWeiCostTOP <- vCost_weight
  
  #
  # Step 01
  #
  
  #Keep Path Ids
  NMMD_PATHIds <- mBen_Criteria[,1]
  
  NMMDTOPsisCostsTended <- aux_step01_Tendency_Cnv(NMMDTOPsisCosts)
  NMMDWeiBenTOPall <- aux_step01_Tendend_Weigths(NMMDWeiBenTOP, NMMDWeiCostTOP)
  NMMDTOPsisBenefits <- cbind(NMMDTOPsisBenefits, NMMDTOPsisCostsTended)
  NMMDWeiBenTOP <- NMMDWeiBenTOPall 
  stopifnot(NMMDTOPsisBenefits != NULL)
  
  #
  # Step 02
  #
  #naCol <- dim(NMMDTOPsisBenefits)[2]
  #print(NMMDTOPsisBenefits[,2:naCol])
  #if (!fAux_Do_Mahalanobis(NMMDTOPsisBenefits[,2:naCol])){
  #	print("Doing Normalization")
  NMMDTOPsisBenefitsTOP <- aux_step02_Normalization(NMMDTOPsisBenefits)
  #}
  
  #
  # Step 03
  #
  NMMDTOPsisBenefitsTOP <- aux_step03_Weight_of_Norm_matrix(NMMDTOPsisBenefitsTOP, NMMDWeiBenTOP)
  
  # Step 04 - Ideal Solutions
 
  
  # Step 05 - Distance to Ideal
 
  NMMDDistBen_Dia <- aux_step05_distance_to_idealNMMD( NMMDTOPsisBenefitsTOP)
  
  #Step 06
  
  NMMDtopsisRanking <- aux_step06_RankingNMMD(NMMD_PATHIds, NMMDDistBen_Dia$distance) 
  #NMMDtopsisRanking
  
  #print(NMMDtopsisRanking)
  #print("NMMD")
  #print(NMMDtopsisRanking[order(-NMMDtopsisRanking[,2])])
  #print("\n")
  return(NMMDtopsisRanking[order(-NMMDtopsisRanking[,2]),])
  
  
}


