library(dplyr)
library(SqlRender)
library(CohortMethod)
library(xgboost)
library(data.table)
library(ROSE)

n_feature<-800

###reading aggr.cohort
dataFolder<-"S:/doctorai_data/new2"
#aggr_cohort<-readeRDS(paste0(dataFolder,"/aggr_cohort.rds"))

table(aggr_cohort$outcome)

##xgboost
label_xg<-aggr_cohort$outcome
train_xg<- data.table (aggr_cohort[,-c(1,2)])
train_xg<-sapply(train_xg,as.numeric)

bst<-xgboost(data=train_xg, label = label_xg, max_depth = 10
             ,num_parallel_tree = 100, subsample = 0.5, colsample_bytree=0.5
             ,eta= 0.1, nthread=10, nround=50)
saveRDS(bst,paste0(dataFolder,"/xgboost_model.rds"))

importance<-xgb.importance(feature_names=colnames(train_xg),model=bst)
saveRDS(importance,paste0(dataFolder,"/xgboost_importance.rds") )
xgb.plot.importance(importance_matrix=importance)

######################################################
##merge concept_id with concept_name in importance####
connectionDetails<-createConnectionDetails(dbms="sql server",
                                           server="128.1.99.53",
                                           schema="nhid.dbo",
                                           user="chandryou",
                                           password="dbtmdcks12#")
cdmDatabaseSchema <- "NHIS_NSC.dbo"
cdmVersion <- "5" 

concept_id_set<-gsub("gender,","",gsub("age,","",paste0("(", paste(importance$Feature,collapse=","),")")))

connection<-connect(connectionDetails)
sql <- "SELECT CONCEPT_ID, CONCEPT_NAME from @cdmDatabaseSchema.concept where concept_id in @concept_id_set"
sql <- renderSql(sql,
                 cdmDatabaseSchema=cdmDatabaseSchema,
                 concept_id_set=concept_id_set)$sql
sql <- translateSql(sql,
                    targetDialect=connectionDetails$dbms)$sql
#querySql(connection, sql)
ImpConcepts <- querySql(connection, sql)
colnames(ImpConcepts)<-tolower(colnames(ImpConcepts))
importance$Feature<-as.numeric(importance$Feature)
impconcept<-merge(x=importance,y=ImpConcepts,by.x="Feature",by.y="concept_id")
impconcept <- impconcept %>% arrange(desc(Gain))
write.csv(impconcept,paste0(dataFolder,"/impconcept.csv"))

#impconcept<-read.csv(paste0(dataFolder,"/impconcept.csv"))
#########################################################
#########################################################
feature<-impconcept$Feature[1:n_feature]
###Feature selection according to the importance



###dimesion redution by selected features

train.cohort.list<-list.files(dataFolder,"train_batch*",recursive=T, full.names=T)
test.cohort.list<-list.files(dataFolder,"test_batch*",recursive=T, full.names=T)

###test set
cohort<-c()
for (i in 1:length(test.cohort.list)){
  cohort_pre<-readRDS(test.cohort.list[i])
  #remove concept_id from column names
  colnames(cohort_pre)<-gsub("concept_id","",colnames(cohort_pre))
  cohort_pre<-cbind(cohort_pre[,1:8],cohort_pre[,match(feature, colnames(cohort_pre))])
  cohort<-rbind(cohort,cohort_pre)
  print(paste0(i, "th process was done, file size = ",round(object.size(cohort)/(1024*1024),3),"MB"))
}
saveRDS(cohort,paste0(dataFolder,"/cohort_test_reducted.rsd"))
gc()
###training set
cohort<-c()
for (i in 1:length(train.cohort.list)){
  cohort_pre<-readRDS(train.cohort.list[i])
  #remove concept_id from column names
  colnames(cohort_pre)<-gsub("concept_id","",colnames(cohort_pre))
  cohort_pre<-cbind(cohort_pre[,1:8],cohort_pre[,match(feature, colnames(cohort_pre))])
  cohort<-rbind(cohort,cohort_pre)
  print(paste0(i, "th process was done, file size = ",round(object.size(cohort)/(1024*1024),3),"MB"))
}
saveRDS(cohort,paste0(dataFolder,"/cohort_train_reducted.rds"))

#library(feather)
#write_feather(train,paste0(dataFolder,"/cohort_train_reducted.feather"))
#write_feather(test,paste0(dataFolder,"/cohort_test_reducted.feather"))
######################################################################