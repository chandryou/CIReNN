library(CohortMethod) 
library(SqlRender)
library(doParallel)
library(plyr)
######################################################
##YOU NEED TO FILL !!#################################

#set working folder as git
workFolder<-"S:/doctorai_data/R/CIReNN"
dataFolder<-"S:/doctorai_data/R/CIReNN"

#Mac: workFolder<-"/Users/chan/git/ajou_deep/doctorai_cdm"
#Mac : dataFolder<-"/Users/chan/Google ????????????/Study/doctor_ai_data"
#set working folder as git
setwd(workFolder)

connectionDetails<-createConnectionDetails(dbms="sql server",
                                           server="128.1.99.53",
                                           schema="nhis_nsc.dbo",
                                           user="chandryou",
                                           password="dbtmdcks12#")
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
batch_size=5000

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
sql<-readRDS("S:/doctorai_data/R/CIReNN/condition.rds")
condition<-querySql(connection, sql)
saveRDS(condition,"S:/doctorai_data/R/CIReNN/condition_data.rds")



sql<-readSql( paste(workFolder,"/sql/drug_extraction_outcome.sql",sep="") )
sql <- renderSql(sql,
                 cdmDatabaseSchema=cdmDatabaseSchema,
                 resultsDatabaseSchema=resultsDatabaseSchema,
                 cohort_definition_id = target_cohort_definition_id,
                 ml_cohort=ml_cohort)$sql
sql <- translateSql(sql,
                    targetDialect=connectionDetails$dbms)$sql

sql<-readRDS("S:/doctorai_data/R/CIReNN/drug.rds")
drug<-querySql(connection, sql)
saveRDS(drug,"S:/doctorai_data/R/CIReNN/drug_data.rds")

cohort_tab <- rbind(condition,drug)

##change column names into lower case
colnames(cohort_tab)<-tolower(colnames(cohort_tab))
##remove na values in table
cohort_tab<-(na.omit(cohort_tab))

saveRDS(cohort_tab,paste(dataFolder,"/cohort_tab.rds",sep=""))

length(unique(cohort_tab$person_id))==length(cohort_tab$person_id)
cohort_tab$person_id<-as.numeric(as.factor(cohort_tab$person_id))
#factorization and numerization of whole concept_id in whole cohort.
cohort_tab$concept_id <- as.factor(cohort_tab$concept_id)
cohort_tab<-cohort_tab %>% 
    group_by (person_id) %>% 
    arrange(desc(visit_seq)) %>% 
    mutate(reversed_seq= dense_rank(-visit_seq) )
cohort_tab<-data.frame(cohort_tab)
saveRDS(cohort_tab,paste(dataFolder,"/cohort_tab.rds",sep=""))

#cohort_tab<-readRDS("C:/Users/chan/Google Drive/Study/doctor_ai_data/cohort_tab.rds")


#age and gender -> regularization
cohort_tab$year_of_birth<-as.numeric(format(cohort_tab$visit_start_date, '%Y'))-cohort_tab$year_of_birth
hist(cohort_tab$year_of_birth)
cohort_tab$year_of_birth <- cohort_tab$year_of_birth/50
#hist(encoded$age)
#gender concept_id 8532 : female / 8507 : male
cohort_tab$gender_concept_id<-ifelse(cohort_tab$gender_concept_id == 8532, 1, 0)


#split whole patients into batch_size
cohort_w_out<-cohort_tab[cohort_tab$outcome==1,]
cohort_wo_out<-cohort_tab[cohort_tab$outcome==0,]

saveRDS(cohort_w_out,paste(dataFolder,"/cohort_w_out.rds",sep=""))
cohort_w_out<-readRDS(paste(dataFolder,"/cohort_w_out.rds",sep=""))
saveRDS(cohort_wo_out,paste(dataFolder,"/cohort_wo_out.rds",sep=""))
cohort_wo_out<-readRDS(paste(dataFolder,"/cohort_wo_out.rds",sep=""))

#rm(cohort_wo_out,cohort_tab) ## for saving memory

##COHORT WITHOUT OUTCOME

person_w_out<-unique(cohort_w_out$person_id)
#shuffle person_id
person_w_out<-sample(person_w_out)

options(cores=10)
cl <- makeCluster(10)
registerDoParallel(cl)

total_start_time<-Sys.time()
foreach(i = seq(ceiling(length(person_w_out)/batch_size))) %dopar% {
    #for (i in seq(ceiling(length(person_w_out)/batch_size))){
    start_time<-Sys.time()
    if (i<ceiling(length(person_w_out)/batch_size)){
        target_person<-person_w_out[ (batch_size*(i-1)+1): (batch_size*(i)+1)]
    } else {target_person<-person_w_out[ (batch_size*(i-1)+1):length(person_w_out)]}
    cohort_tab<-cohort_w_out[cohort_w_out$person_id %in% target_person,]
    
    #make one-hot vector matrix (df)
    df<-data.frame(with(cohort_tab, model.matrix(~ concept_id + 0)))
    encoded<-aggregate(df, by = list(cohort_tab$person_id, cohort_tab$outcome, cohort_tab$gender_concept_id, cohort_tab$year_of_birth, cohort_tab$visit_seq, cohort_tab$visit_start_date, cohort_tab$reversed_seq),FUN=sum )
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
    
    saveRDS(encoded,paste(dataFolder,"/cohort_w_outcome_batch_no",i,".rds",sep=""))
    end_time<-Sys.time()
    elapsedtime<-end_time-start_time
    print(paste(i,"th process was done, total = ",ceiling(length(person_w_out)/batch_size),"elapsed time=",elapsedtime,attributes(elapsedtime)$units),sep="")
    
}
stopCluster(cl)
rm(cohort_w_out) #for saving memory

#cohort_wo_out<-readRDS(paste(dataFolder,"cohort_wo_out.rds",sep=""))

person_wo_out<-unique(cohort_wo_out$person_id)
#shuffle person_id
person_wo_out<-sample(person_wo_out)

options(cores=10)
cl <- makeCluster(15)
registerDoParallel(cl)

total_start_time<-Sys.time()
#for (i in seq(ceiling(length(person_wo_out)/batch_size))){
foreach(i = seq(ceiling(length(person_wo_out)/batch_size))) %dopar% {
    start_time<-Sys.time()
    if (i<ceiling(length(person_wo_out)/batch_size)){
        target_person<-person_wo_out[ (batch_size*(i-1)+1): (batch_size*(i)+1)]
    } else {target_person<-person_wo_out[ (batch_size*(i-1)+1):length(person_wo_out)]}
    cohort_tab<-cohort_wo_out[cohort_wo_out$person_id %in% target_person,]
    
    #make one-hot vector matrix (df)
    df<-data.frame(with(cohort_tab, model.matrix(~ concept_id + 0)))
    encoded<-aggregate(df, by = list(cohort_tab$person_id, cohort_tab$outcome, cohort_tab$gender_concept_id, cohort_tab$year_of_birth, cohort_tab$visit_seq, cohort_tab$visit_start_date, cohort_tab$reversed_seq),FUN=sum )
    colnames(encoded)[1:7]<-c("person_id", "outcome", "gender", "age" ,"visit_seq", "visit_start_date", "rev_seq")
    
    #age and gender -> regularization
    encoded$age<-as.numeric(format(encoded$visit_start_date, '%Y'))-encoded$age
    encoded$age <- (encoded$age - mean(encoded$age)) / sd(encoded$age)
    #hist(encoded$age)
    #gender concept_id 8532 : female / 8507 : male
    encoded$gender<-ifelse(encoded$age == 8532, 1, 0)
    
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
    
    saveRDS(encoded,paste(dataFolder,"/cohort_wo_outcome_batch_no",i,".rds",sep=""))
    end_time<-Sys.time()
    elapsedtime<-end_time-start_time
    print(paste(i,"th process was done, total = ",ceiling(length(person_wo_out)/batch_size),"elapsed time=",elapsedtime,attributes(elapsedtime)$units),sep="")
    
}
stopCluster(cl)

