library(dplyr)
library(SqlRender)
library(CohortMethod)

dataFolder<-"S:/doctorai_data/new"
+
cohort<-c()
for (i in 1:19){
  if (object.size(cohort)/(1024*1024*1024)>10) break
  cohort_pre1<-readRDS(paste0(dataFolder,"/cohort_w_outcome_new_",i))
  #remove concept_id from column names
  colnames(cohort_pre1)<-gsub("concept_id","",colnames(cohort_pre1))
  #summing by person_id
  cohort_pre2<-aggregate(. ~person_id, cohort_pre1, sum)
  #ceiling
  cohort_pre2[9:ncol(cohort_pre2)] <- sapply(cohort_pre2[9:ncol(cohort_pre2)], function(x) { as.numeric(x > 0) })
  cohort<-rbind(cohort,cohort_pre2)
  print(paste0(i, "th process was done, file size = ",round(object.size(cohort)/(1024*1024),3),"MB"))
}


for (i in 1:1740){
  if (object.size(cohort)/(1024*1024*1024)>200) break
  cohort_pre1<-readRDS(paste0(dataFolder,"/cohort_wo_outcome_new_",i))
  #remove concept_id from column names
  colnames(cohort_pre1)<-gsub("concept_id","",colnames(cohort_pre1))
  #summing by person_id
  cohort_pre2<-aggregate(. ~person_id, cohort_pre1, sum)
  #ceiling
  cohort_pre2[9:ncol(cohort_pre2)] <- sapply(cohort_pre2[9:ncol(cohort_pre2)], function(x) { as.numeric(x > 0) })
  cohort<-rbind(cohort,cohort_pre2)
  print(paste0(i, "th process was done, file size = ",round(object.size(cohort)/(1024*1024),3),"MB"))
}
saveRDS(cohort,paste0(dataFolder,"/aggre_cohort.rds"))
#cohort<-readRDS(paste0(dataFolder,"/aggre_cohort.rds"))
cohort$outcome<-ifelse(cohort$outcome>=1,1,0)

#there are duplicated person_ids. This is required to be fixed
cohort$person_id<-1:nrow(cohort)

#test set with fraction of 0.1

te0<-cohort %>%
  filter(outcome==0) %>%
  sample_frac(0.1)

te1<-cohort %>%
  filter(outcome==1) %>%
  sample_frac(0.1)
te_cohort<-rbind(te0,te1)

te_cohort$person_id

sum(cohort[,"312327"]==1)

saveRDS(te_cohort,paste0(dataFolder,"/te_cohort.rds"))

test_person<-te_cohort$person_id

tr_cohort <-cohort%>%
  filter(!(person_id %in% te_cohort$person_id))
saveRDS(tr_cohort,paste0(dataFolder,"/tr_cohort.rds"))
#tr_cohort<-readRDS(paste0(dataFolder,"/tr_cohort.rds"))

colnames(tr_cohort)[1:10]
tr_cohort<-tr_cohort[,-c(3,4,5,6,7,8)]


#undersampling(making about 1:10)
tr_cohort1<-tr_cohort %>%
  filter(outcome ==1)

tr_cohort0<-tr_cohort%>%
  filter(outcome==0) %>%
  sample_frac(0.1)
sub_cohort<-rbind(tr_cohort0,tr_cohort1)

rm(cohort,cohort_pre1,cohort_pre2,sub_cohort,te_cohort,te0,te1,tr_cohort,tr_cohort0,tr_cohort1,tr_cohort2)
rm(boruta.cohort)
rm(bst)
test_person

##xgboost
library(xgboost)
library(data.table)
tr_cohort2<-data.table(tr_cohort[,-c(1,2)])
tr_cohort2<-sapply(tr_cohort2,as.numeric)
bst<-xgboost(data=tr_cohort2, label = tr_cohort$outcome, max_depth = 10
             ,num_parallel_tree = 100, subsample = 0.5, colsample_bytree=0.5
             ,eta= 0.1, nthread=10, nround=50)
saveRDS(bst,"S:/doctorai_data/new/xgboost_model.rds")

importance<-xgb.importance(feature_names=colnames(tr_cohort2),model=bst)
saveRDS(importance,"S:/doctorai_data/new/xgboost_importance.rds")
xgb.plot.importance(importance_matrix=importance)

######################################################
##merge concept_id with concept_name in importance####
library(SqlRender)
library(CohortMethod)

connectionDetails<-createConnectionDetails(dbms="sql server",
                                           server="128.1.99.53",
                                           schema="nhid.dbo",
                                           user="chandryou",
                                           password="dbtmdcks12#")
cdmDatabaseSchema <- "NHIS_NSC.dbo"
cdmVersion <- "5" 

concept_id_set<-paste0("(", paste(as.numeric(importance$Feature),collapse=","),")")

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
#########################################################
#########################################################

#excepting concpets(MI, stroke etc...)
Exc_concept<-c(4110197, 4112026, 4110961, 312327, 372924, 321318, 315286)
features<-as.numeric(importance$Feature[1:850])
feature<- features[!(features %in% Exc_concept)]
feature<-as.character(feature)

cohort<-c()
te_cohort<-c()
for (i in 1:19){
  if (object.size(cohort)/(1024*1024*1024)>10) break
  cohort_pre1<-readRDS(paste0(dataFolder,"/cohort_w_outcome_new_",i))
  #remove concept_id from column names
  colnames(cohort_pre1)<-gsub("concept_id","",colnames(cohort_pre1))
  cohort_pre2<-cbind(cohort_pre1[,1:8],cohort_pre1[,match(feature, colnames(cohort_pre1))])
  
  te_cohort_pre<- cohort_pre2[cohort_pre2$person_id %in% test_person,]
  
  cohort_pre<-cohort_pre2[ !(cohort_pre2$person_id %in% test_person),]
  
  te_cohort<-rbind(te_cohort,te_cohort_pre)
  cohort<-rbind(cohort,cohort_pre)
  print(paste0(i, "th process was done, file size = ",round(object.size(cohort)/(1024*1024),3),"MB"))
}
saveRDS(cohort,paste0(dataFolder,"/cohort_w_outcome_cut.rds"))

j<-1
cohort<-c()
for (i in 1:1740){
  if (object.size(cohort)/(1024*1024*1024)>200) {
    saveRDS(cohort,paste0(dataFolder,"/cohort_wo_outcome_cut",j,".rds"))
    j<-j+1
    cohort<-c()
    }
  cohort_pre1<-readRDS(paste0(dataFolder,"/cohort_wo_outcome_new_",i))
  #remove concept_id from column names
  colnames(cohort_pre1)<-gsub("concept_id","",colnames(cohort_pre1))
  cohort_pre2<-cbind(cohort_pre1[,1:8],cohort_pre1[,match(feature, colnames(cohort_pre1))])
  
  te_cohort_pre<- cohort_pre2[cohort_pre2$person_id %in% test_person,]
  
  cohort_pre<-cohort_pre2[ !(cohort_pre2$person_id %in% test_person),]
  
  te_cohort<-rbind(te_cohort,te_cohort_pre)
  cohort<-rbind(cohort,cohort_pre)
  print(paste0(i, "th process was done, file size = ",round(object.size(cohort)/(1024*1024),3),"MB"))
}
saveRDS(cohort,paste0(dataFolder,"/cohort_wo_outcome_cut",j+1,".rds"))
saveRDS(te_cohort,paste0(dataFolder,"/test_cohort_cut.rds"))

#j<-1
cohort_wo_outcome<-readRDS(paste0(dataFolder,"/cohort_wo_outcome_cut",j+1,".rds"))
cohort_w_outcome<-readRDS(paste0(dataFolder,"/cohort_w_outcome_cut.rds"))
te_cohort<-readRDS(paste0(dataFolder,"/test_cohort_cut.rds"))


##sub cohort of cohort_wo_outcome #####
sub_personid<-sample(unique(cohort_wo_outcome$person_id),floor(length(unique(cohort_wo_outcome$person_id))/100))

sub_cohort_wo_outcome<-cohort_wo_outcome %>%
  filter(person_id %in% sub_personid)
######################################

saveRDS(sub_cohort_wo_outcome,paste0(dataFolder,"/sub_cohort_wo_outcome.rds"))

install.packages("feather")
library("feather")

write_feather(cohort_wo_outcome,paste0(dataFolder,"/cohort_wo_outcome_cut.feather"))
write_feather(cohort_w_outcome,paste0(dataFolder,"/cohort_w_outcome_cut.feather"))
write_feather(te_cohort,paste0(dataFolder,"/te_cohort.feather"))

write.csv(cohort_wo_outcome,paste0(dataFolder,"/cohort_wo_outcome_cut.csv"))
write.csv(cohort_w_outcome,paste0(dataFolder,"/cohort_w_outcome_cut.csv"))
write.csv(te_cohort,paste0(dataFolder,"/te_cohort.csv"))
########################################################
####hdf5################################################
source("http://bioconductor.org/biocLite.R")
biocLite("rhdf5")

library(rhdf5)

h5write(cohort_wo_outcome,paste0(dataFolder,"/cohort_wo_outcome_cut",".h5"),"df")
H5close()
h5createFile(paste0(dataFolder,"/cohort_w_outcome_cut.h5"))
h5write(cohort_w_outcome,paste0(dataFolder,"/cohort_w_outcome_cut.h5"),"df")
H5close()

h5write(te_cohort,paste0(dataFolder,"/te_cohort",".h5"),"df")


########################################
########################################
########################################
#install.packages("Boruta")
library(Boruta)
boruta.cohort<-Boruta(outcome~.-person_id, data=sub_cohort, doTrace=2, ntree = 500,maxRuns=100)
saveRDS(boruta.cohort,"boruta.cohort")
plot(boruta.cohort)
getConfirmedFormula(boruta.cohort)
##>No attributes deemed important
