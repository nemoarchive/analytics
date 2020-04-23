
library(projectR) ## must use R 3.6
library(rhdf5)  ## for reading / writing h5 file from scanpy
library(Matrix)  ## for converting sparse matrix to dense matrix 
library(tictoc) ## for tracking comp time. from: devtools::install_github("jabiru/tictoc") 

tic("total") ## start timer for "total time"

tic("read Ref sample") ## start timer for "read Ref sample"


### define and read in reference sample - the sample that has PCs calculated

refSample = "K34_tsne" ## hopefully can tie to dropdown menu

refH5 <- H5Fopen(paste("/Users/bherb/Documents/UMB_work/Ament/NeMO_analytics/projectR/",refSample,".h5ad",sep=""))  ## link to file in directory

refDat <- refH5$X  ## count values (3495 cells X 10157 genes) 

refPCAcell <- refH5$uns$DimReduction  ## PC's per cell

refPCAperVar <- refH5$uns$pca ## "variance" and "variance_ratio"

refPCAgene <- t(refH5$uns$DimReductionGene) ## PC's per gene 

refGenes <- refH5$var$index ## list of genes 

rownames(refPCAgene) <- refGenes 

toc() ## stop timer for "read Ref sample"


tic("read Test sample") ## start timer for "read Test sample"

## test sample - data to project upon reference PC's

testSample = 'CS22_PFC_tsne' ## hopefully can tie to dropdown menu

testH5 <- H5Fopen(paste("/Users/bherb/Documents/UMB_work/Ament/NeMO_analytics/projectR/",testSample,".h5ad",sep=""))

testDat <- as.vector(testH5$X[['data']]) ## actual data values from sparse matrix

testIndex <- as.vector(testH5$X[['indices']]) ## column, or gene index - note: in zero based numbers 

testCellCount <- length(testH5$obs[,1]) ## number of cells

testGenes <-testH5$var$gene_symbol  ## gene names 

testGeneCount <- length(testGenes) ## number of genes 

## Reconstruct dense data matrix 

pdif = diff(testIndex+1)
p = which(pdif<0)
p=c(1,p+1) ## index of row starts (cell index)

## simply bc I don't know how p is set up
rowInd=rep(0,length(testIndex))
for(i in 1:length(p)){
    if(i!=length(p)){
    rowInd[(p[i]):p[i+1]-1]=i
    } else {
        rowInd[(p[i]):length(testIndex)]=i
    }
    }

testDatSM <- Matrix::sparseMatrix(j=testIndex+1,i=rowInd,x=as.vector(testDat),dims=c(testCellCount,testGeneCount)) ## same as adata in python! cells x genes

testDatDM = as.matrix(t(testDatSM)) ## genes x cells (33694 X 1945)

rownames(testDatDM)=testGenes

toc() ## stop timer for "read Test sample"

tic("Projection") ## start timer for "Projection"

testPro <- projectR(data=testDatDM,loadings=refPCAgene,full=TRUE) ## result is cells x PCs

toc() ## stop timer for "Projection"

tmpuns <- testH5$uns ## pull out uns slot from h5

tmpuns[['projection']][[refSample]] = testPro ## add projectR result to uns - note that 'projection' will be top level, and second level will be the name of the reference set used for projection (allows for comparison to multiple ref datasets) 

h5delete(testH5,'uns') ## need to delete uns from h5 file before new uns can be added

testH5$uns = tmpuns ## add back uns (with projectR result) to h5 

H5Fclose(testH5) ## save result

toc() ## stop timer for "total time"

## read Ref sample: 4.304 sec elapsed
## read Test sample: 9.051 sec elapsed
## Projection: 5.313 sec elapsed
## total time: 22.403 sec elapsed







