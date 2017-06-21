library(CohortMethod) 
library(SqlRender)
library(doParallel)
library(dplyr)
library(ROSE)
######################################################
##YOU NEED TO FILL !!#################################

#set working folder as git
workFolder<-"S:/doctorai_data/new2"
dataFolder<-"S:/doctorai_data/new"

#Mac: workFolder<-"/Users/chan/git/ajou_deep/doctorai_cdm"
#Mac : dataFolder<-"/Users/chan/Google ????????????/Study/doctor_ai_data"
#set working folder as git
setwd(workFolder)

connectionDetails<-createConnectionDetails(dbms="sql server",
                                           server="128.1.99.53",
                                           schema="nhis_nsc.dbo",
                                           user="",
                                           password="")
cdmVersion <- "5" 
cdmDatabaseSchema <- "nhis_nsc.dbo"
resultsDatabaseSchema <- "chan_NHID_CVD.dbo"
exposureTable = "exposureTable"
outcomeTable  = "outcomeTable"
ml_cohort = "ml_cohort"

target_cohort_definition_id<-170607
MI_id<-4444
stroke_id<-5555
CVD_death_id<-6666
revas_id<-7777
outcome_id<-4567
outcome_code_set<-paste0( "(",paste(MI_id,stroke_id,CVD_death_id,revas_id,sep=','),")")
batch_size=100

cluster_no<-20 #for now, it should be 10!
seed_no<-1

#######################################################
#######################################################

###connection##########################################
connection<-connect(connectionDetails)
#######################################################

sql<-readSql( paste(workFolder,"/sql/condition_extraction_outcome.sql",sep="") )
sql <- renderSql(sql,
                 cdmDatabaseSchema=cdmDatabaseSchema,
                 resultsDatabaseSchema=resultsDatabaseSchema,
                 exposureTable=exposureTable,
                 outcomeTable=outcomeTable,
                 cohort_definition_id = target_cohort_definition_id,
                 ml_cohort=ml_cohort)$sql
sql <- translateSql(sql,
                    targetDialect=connectionDetails$dbms)$sql
sql<-readRDS("S:/doctorai_data/R/CIReNN/condition.sql.rds")
condition<-querySql(connection, sql)
saveRDS(condition,paste0(dataFolder,"/condition_data.rds"))


sql<-readSql( paste(workFolder,"/sql/drug_extraction_outcome.sql",sep="") )
sql <- renderSql(sql,
                 cdmDatabaseSchema=cdmDatabaseSchema,
                 resultsDatabaseSchema=resultsDatabaseSchema,
                 cohort_definition_id = target_cohort_definition_id,
                 ml_cohort=ml_cohort)$sql
sql <- translateSql(sql,
                    targetDialect=connectionDetails$dbms)$sql

sql<-readRDS("S:/doctorai_data/R/CIReNN/drug.sql.rds")
drug<-querySql(connection, sql)
saveRDS(drug,paste0(dataFolder,"/drug_data.rds"))

#drug<-readRDS(paste0(dataFolder,"/drug_data.rds"))
#condition<-readRDS(paste0(dataFolder,"/condition_data.rds"))

cohort_tab <- rbind(condition,drug)
#cohort_tab<-readRDS(paste0(dataFolder,"/raw_cohort_tab.rds"))
rm(drug,condition)

##change column names into lower case
colnames(cohort_tab)<-tolower(colnames(cohort_tab))
##remove na values in table

#nrow(cohort_tab) #95903847
cohort_tab<-(na.omit(cohort_tab))
#nrow(cohort_tab) #95903847

#factorization and numerization of whole concept_id in whole cohort.
cohort_tab$concept_id <- as.factor(cohort_tab$concept_id)
cohort_tab<-cohort_tab %>% 
    group_by (person_id) %>% 
    arrange(desc(visit_seq)) %>% 
    mutate(reversed_seq= dense_rank(-visit_seq) )
cohort_tab<-data.frame(cohort_tab)
#nrow(cohort_tab) #95903847

 #age and gender -> regularization
cohort_tab$year_of_birth<-as.numeric(format(cohort_tab$visit_start_date, '%Y'))-cohort_tab$year_of_birth
cohort_tab$year_of_birth <- cohort_tab$year_of_birth/50
#gender concept_id 8532 : female / 8507 : male
cohort_tab$gender_concept_id<-ifelse(cohort_tab$gender_concept_id == 8532, 1, 0)
saveRDS(cohort_tab,paste(dataFolder,"/cohort_tab.rds",sep=""))
#cohort_tab<-readRDS(paste(dataFolder,"/cohort_tab.rds",sep=""))

##making person_list
set.seed(seed_no)
person.outcome<-aggregate(outcome~person_id, FUN=min,data=cohort_tab)
#nrow(person.outcome)
#length(unique(person.outcome$person_id))
#sum(person.outcome$outcome==0)
#sum(person.outcome$outcome==1)

set.seed(seed_no)
per.out.test<-sample_frac(person.outcome,0.1)
per.out.train<-person.outcome %>% filter (!person_id %in% per.out.test$person_id)

nrow(per.out.test) #30946
nrow(per.out.train) #278512
nrow(person.outcome) #309458

#under sampling (ratio = 1:10)
per.out.train$outcome<-as.factor(per.out.train$outcome)
train.sample<-ovun.sample(outcome~.,data=per.out.train,p=0.1,method="under")
train.sample<-train.sample$data

train.sample$outcome<-as.integer(train.sample$outcome)
per.out.train$outcome<-as.integer(per.out.train$outcome)

person.list<-list(test=per.out.test,train=per.out.train,train_sample=train.sample)

saveRDS(person.list,paste0(dataFolder,"/person.list.rds"))

##test set
#split whole patients into batch_size
options(cores=15)
cl <- makeCluster(cluster_no)
registerDoParallel(cl)
total_start_time<-Sys.time()

##train set
person<-person.list$test$person_id
foreach(i = seq(ceiling(length(person)/batch_size))) %dopar% {
  #for (i in seq(ceiling(length(person_test)/batch_size))){
  start_time<-Sys.time()
  if (i<ceiling(length(person)/batch_size)){
    target_person<-person[ (batch_size*(i-1)+1): (batch_size*(i))]
  } else {target_person<-person[ (batch_size*(i-1)+1):length(person)]}
  target_cohort<-cohort_tab[cohort_tab$person_id %in%target_person,]
  
  #make one-hot vector matrix (df)
  df<-data.frame(with(target_cohort, model.matrix(~ concept_id + 0)))
  encoded<-aggregate(df, by = list(target_cohort$person_id, target_cohort$outcome, target_cohort$gender_concept_id, target_cohort$year_of_birth, target_cohort$visit_seq, target_cohort$visit_start_date, target_cohort$reversed_seq),FUN=sum )
  colnames(encoded)[1:7]<-c("person_id", "outcome", "gender", "age" ,"visit_seq", "visit_start_date", "rev_seq")
  
  #ceiling one-hot vector 1 or underN
  encoded[8:ncol(encoded)] <- sapply(encoded[8:ncol(encoded)], function(x) { as.numeric(x > 0) })
  
  #adding date difference between visit(day)
  encoded <- encoded[order(encoded$person_id, encoded$visit_seq),]
  encoded$index <- 1L:nrow(encoded)
  j <- by(encoded, encoded$person_id, function(x) x$index[which.min(x$visit_seq)] )
  
  encoded$datediff = (encoded$visit_seq>1) * as.integer(encoded$visit_start_date - append(0, encoded$visit_start_date)[1:length(encoded$visit_start_date)])
  #fixing datediff of first visit to 0
  encoded[j,]$datediff<-0
  encoded$index<-NULL #delete index column
  encoded<-encoded[, c(1:6, ncol(encoded), 7: (ncol(encoded)-1))]
  
  colnames(encoded)<-gsub("concept_id","",colnames(encoded))
  
  saveRDS(encoded,paste(dataFolder,"/test_batch","_",i,".rds",sep=""))
  end_time<-Sys.time()
  elapsedtime<-end_time-start_time
  print(paste(i,"th process was done, total = ",ceiling(length(person)/batch_size),"elapsed time=",elapsedtime,attributes(elapsedtime)$units),sep="")
}
stopCluster(cl)
gc()

##train set
#split whole patients into batch_size
options(cores=15)
cl <- makeCluster(cluster_no)
registerDoParallel(cl)
total_start_time<-Sys.time()

##train set
person<-person.list$train_sample$person_id
length(person)
length(unique(person))
foreach(i = seq(ceiling(length(person)/batch_size))) %dopar% {
  start_time<-Sys.time()
  if (i<ceiling(length(person)/batch_size)){
    target_person<-person[ (batch_size*(i-1)+1): (batch_size*(i))]
  } else {target_person<-person[ (batch_size*(i-1)+1):length(person)]}
  target_cohort<-cohort_tab[cohort_tab$person_id %in%target_person,]
  
  #make one-hot vector matrix (df)
  df<-data.frame(with(target_cohort, model.matrix(~ concept_id + 0)))
  encoded<-aggregate(df, by = list(target_cohort$person_id, target_cohort$outcome, target_cohort$gender_concept_id, target_cohort$year_of_birth, target_cohort$visit_seq, target_cohort$visit_start_date, target_cohort$reversed_seq),FUN=sum )
  colnames(encoded)[1:7]<-c("person_id", "outcome", "gender", "age" ,"visit_seq", "visit_start_date", "rev_seq")
  
  #ceiling one-hot vector 1 or underN
  encoded[8:ncol(encoded)] <- sapply(encoded[8:ncol(encoded)], function(x) { as.numeric(x > 0) })
  
  #adding date difference between visit(day)
  encoded <- encoded[order(encoded$person_id, encoded$visit_seq),]
  encoded$index <- 1L:nrow(encoded)
  j <- by(encoded, encoded$person_id, function(x) x$index[which.min(x$visit_seq)] )
  
  encoded$datediff = (encoded$visit_seq>1) * as.integer(encoded$visit_start_date - append(0, encoded$visit_start_date)[1:length(encoded$visit_start_date)])
  #fixing datediff of first visit to 0
  encoded[j,]$datediff<-0
  encoded$index<-NULL #delete index column
  encoded<-encoded[, c(1:6, ncol(encoded), 7: (ncol(encoded)-1))]
  
  colnames(encoded)<-gsub("concept_id","",colnames(encoded))
  
  saveRDS(encoded,paste(dataFolder,"/train_batch","_",i,".rds",sep=""))
  end_time<-Sys.time()
  elapsedtime<-end_time-start_time
  print(paste(i,"th process was done, total = ",ceiling(length(person)/batch_size),"elapsed time=",elapsedtime,attributes(elapsedtime)$units),sep="")
}
stopCluster(cl)
gc()


##summing by person_id for feature extraction

train.cohort.list=list.files(dataFolder,"train_batch*",recursive=T, full.names=T)

aggr_cohort<-c()
for(i in 1:length(train.cohort.list)){
  encoded<-readRDS(train.cohort.list[i])
  #summing by person_id
  cohort_pre1<-aggregate( .~person_id, encoded[,-c(2,3,4,5,6,7,8)] , sum )
  cohort_pre1[2:ncol(cohort_pre1)] <- sapply(cohort_pre1[2:ncol(cohort_pre1)], function(x) { as.numeric(x > 0) })
  #extract gender, max age and etc.
  cohort_pre2<-aggregate(.~person_id, encoded[,c(1,2,3,4)],max)
  cohort_pre<-cbind(cohort_pre2, cohort_pre1[,-1])
  aggr_cohort<-rbind(aggr_cohort, cohort_pre)
}
saveRDS(aggr_cohort,paste0(dataFolder,"/aggr_cohort.rds"))
