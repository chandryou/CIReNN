library(ROSE)
library(dplyr)
library(data.table)

timestep<-50

test<-readRDS(paste0(dataFolder,"/cohort_test_reducted.rds"))
train<-readRDS(paste0(dataFolder,"/cohort_train_reducted.rds"))

person_outcome<-tapply(train$outcome, train$person_id,mean)
person_outcome<-data.frame(person_id=names(person_outcome),outcome=person_outcome)

#over + undersampling to 10,000
person_outcome$outcome<-as.factor(person_outcome$outcome)
person_ovun<-ovun.sample(outcome~.,data=person_outcome,N=20000 ,p=0.5,method='both' )
sampled<-person_ovun$data
sampled$person_id<-as.integer(as.character(sampled$person_id))

##limit max visit according to the timestep
pre_train<-train[train$rev_seq<=50,]
pre_train<-pre_train[pre_train$person_id %in% sampled$person_id, ]
pre_test<-test[test$rev_seq<=50,]


#removing visit_date and visit_seq etc.
#oversampling of train /w outcome by x5
pre_train<-pre_train[,-c(5,6,7)]
pre_test<-pre_test[,-c(5,6,7)]

length(unique(pre_train$person_id))#104100
length(unique(pre_test$person_id)) #11501

##zero_padding
mat.padding.y<-function(data=data,padding_num=0, timestep=50){
  ncol=ncol(data)
  max_seq<-tapply(data$rev_seq,data$person_id,max)
  df<-data.frame(person_id=names(max_seq),max_seq=max_seq)
  df$seqsub<-timestep-df$max_seq
  df<-data.table(cbind(df,matrix(padding_num,ncol=ncol-2,nrow=nrow(df))))
  df<-data.frame( df[,list(freq=rep(1,seqsub)),by=c(colnames(df))] )
  df$max_seq=NULL
  df$seqsub=NULL
  colnames(df)<-colnames(data)
  df$rev_seq<-0
  result<-rbind(data,df)
  result<-result%>%arrange(desc(person_id,rev_seq))
  
  ##remove duplicated person_id
  a<-tapply(result$person_id,result$person_id,length)
  dup_person_id<-dimnames(a[a>timestep])
  result<-result[!(result$person_id %in% dup_person_id[[1]]),]
  
  ##result
  result.x<-result
  result.x$outcome=NULL
  return( list(x=result.x,y=result$outcome[c(TRUE,rep(FALSE,timestep-1))],
               person_id = result$person_id[c(TRUE,rep(FALSE,timestep-1))] ) )
}

gc()
train.list<-mat.padding.y(data=pre_train,padding_num=0,timestep=timestep)
rm(pre_train);gc()
test.list <-mat.padding.y(data=pre_test, padding_num=0,timestep=timestep)
rm(pre_test);gc()

#remove person_id, rev_seq from x
randompi<-sample(sampled$person_id)
trainpi<-match(randompi,train.list$person_id)

train.x<-train.list$x[,-c(1,4)]
train.y<-train.list$y
train.person_id<-train.list$person_id

t.train.x<-t(train.x)
no_feature<-ncol(train.x)
no_subject_tr<-nrow(train.x)/timestep

dim(t.train.x)<-c(no_feature,timestep,no_subject_tr)
t.train.x<-aperm(t.train.x,c(3,2,1))

#sampling of array by under/oversampling
dim(t.train.x) #6689     50   802
t.train.x<-t.train.x[trainpi,,]
dim(t.train.x) #10000    50   802
train.y<-train.y[trainpi]
train.person_id<-train.list$person_id[trainpi]
length(unique(train.person_id))

##put into list
train.list$x<-t.train.x
train.list$y<-train.y
train.list$person_id<-train.person_id

test.x<-test.list$x[,-c(1,4)]
test.y<-test.list$y
test.person_id<-test.list$person_id

t.test.x<-t(test.x)
no_subject_te<-nrow(test.x)/timestep
dim(t.test.x)<-c(no_feature,timestep,no_subject_te)
t.test.x<-aperm(t.test.x,c(3,2,1))

test.list$x<-t.test.x
test.list$y<-test.y

##saveRDS
saveRDS(train.list,paste0(dataFolder,"/train.list.rds"))
saveRDS(test.list,paste0(dataFolder,"/test.list.rds"))