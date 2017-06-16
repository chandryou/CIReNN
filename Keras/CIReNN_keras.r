library(keras)
library(AUC)
library(dplyr)
library(data.table)
library(PRROC)
library(abind)

#for Mac: 
dataFolder<-"/Users/chan/OneDrive/Study/CVDPrediction/data/new"
#14ho
#dataFolder<-"/data/chan"
#pc dataFolder<-"F:/17.6.11"
timestep<-50

pre_train.w<-readRDS(paste0(dataFolder,"/cohort_train_w_out_reducted.rds"))
pre_train.wo<-readRDS(paste0(dataFolder,"/cohort_train_wo_out_reducted.rds"))
pre_test<-readRDS(paste0(dataFolder,"/cohort_test_reducted.rds"))

##limit max visit according to the timestep

pre_train.w<-pre_train.w %>% filter(rev_seq<=timestep)
pre_train.wo<-pre_train.wo %>% filter(rev_seq<=timestep)
pre_test<-pre_test %>% filter(rev_seq<=timestep)


#removing visit_date and visit_seq etc.
#oversampling of train /w outcome by x5
pre_train<-rbind(pre_train.wo,pre_train.w,pre_train.w,pre_train.w,pre_train.w,pre_train.w)[,-c(5,6,7)]
pre_test<-pre_test[,-c(5,6,7)]

rm(pre_train.w,pre_train.wo)

length(unique(pre_train$person_id))#34534
length(unique(pre_test$person_id)) #30945

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

train.list<-mat.padding.y(data=pre_train,padding_num=0,timestep=timestep)
test.list<-mat.padding.y(data=pre_test,padding_num=0,timestep=timestep)


#remove person_id, rev_seq from x

train.x<-train.list$x[,-c(1,4)]
train.y<-train.list$y
train.person_id<-train.list$person_id

test.x<-test.list$x[,-c(1,4)]
test.y<-test.list$y
test.person_id<-test.list$person_id

t.train.x<-t(train.x)
no_feature<-ncol(train.x)
no_subject_tr<-nrow(train.x)/timestep

dim(t.train.x)<-c(no_feature,timestep,no_subject_tr)
t.train.x<-aperm(t.train.x,c(3,2,1))
#shuffle the array (mix the subject /w outcome and w/o outcome)
set.seed(1)
shuffle_ord<-sample(1:no_subject_tr)
t.train.x<-t.train.x[shuffle_ord,,]
train.y<-train.y[shuffle_ord]

t.test.x<-t(test.x)
no_subject_te<-nrow(test.x)/timestep
dim(t.test.x)<-c(no_feature,timestep,no_subject_te)
t.test.x<-aperm(t.test.x,c(3,2,1)) #no_subject, timestep, no_feature

train.list$x<-t.train.x
train.list$y<-train.y

test.list$x<-t.test.x
test.list$y<-test.y

saveRDS(train.list,paste0(dataFolder,"/train.list.rds"))
saveRDS(test.list,paste0(dataFolder,"/test.list.rds"))

train.list<-readRDS(paste0(dataFolder,"/train.list.rds"))

t.train.x<-train.list$x
train.y<-train.list$y
rm(train.list)

##oversampling of cohort /w outcome x5
wo.outcome<-t.train.x[train.y==0,,]
wo.y<-train.y[train.y==0]
w.outcome<-t.train.x[train.y==1,,]
w.y<-train.y[train.y==1]

train.x.over<-abind(wo.outcome,w.outcome,w.outcome,w.outcome,w.outcome,w.outcome, along=1)
train.y.over<-c(wo.y, w.y, w.y, w.y, w.y, w.y)

set.seed(1)
shuffle_ord<-sample(1:dim(train.x.over)[1])
t.train.x<-train.x.over[shuffle_ord,,]
train.y<-train.y.over[shuffle_ord]

rm(train.x.over,train.y.over)

saveRDS(t.train.x,paste0(dataFolder,"/train.x.over.rds"))
saveRDS(train.y,paste0(dataFolder,"/train.y.over.rds"))

train.list<-readRDS(paste0(dataFolder,"/train.list.rds"))
test.list<-readRDS(paste0(dataFolder,"/test.list.rds"))

t.train.x<-train.list$x
train.y<-train.list$y

t.test.x<-test.list$x
test.y<-test.list$y

no_feature<-dim(t.test.x)[3]
timestep<-50

##single-layer gru
model <- keras_model_sequential()
model  %>%
    layer_gru(units=32, recurrent_dropout = 0.2,input_shape = c(timestep,no_feature),
              return_sequences=TRUE#,stateful=TRUE
    ) %>%
    layer_dropout(0.4) %>%
    layer_dense(units=1, activation='sigmoid')

model %>% compile(
    loss = 'binary_crossentropy',
    optimizer = optimizer_rmsprop(lr=0.0001, decay=1e-5),
    metrics = c('binary_accuracy')
)

history<-model %>% fit(t.train.x, train.y, epochs=20,
                       batch_size =100,
                       validation_split=0.2,
                       shuffle=TRUE)

summary(model)

saveRDS(model,paste0(dataFolder,"/singleGRU_model.rds"))
saveRDS(history,paste0(dataFolder,"/singleGRU_model.rds"))


#multi-layer
model <- keras_model_sequential()
model  %>%
    layer_gru(units=64, recurrent_dropout = 0.2,input_shape = c(timestep,no_feature),
              return_sequences=TRUE#,stateful=TRUE
              ) %>%
    layer_dropout(0.4) %>%
    layer_gru(units=32, recurrent_dropout = 0.2,
              return_sequences=TRUE) %>%
    layer_dropout(0.4) %>%
    layer_gru(units=2, recurrent_dropout = 0.2
              #,stateful=TRUE
    ) %>%
    layer_dropout(0.4) %>%
    layer_dense(units=1, activation='sigmoid')

model %>% compile(
    loss = 'binary_crossentropy',
    optimizer = optimizer_rmsprop(lr=0.0001, decay=1e-5),
    metrics = c('binary_accuracy')
)

history<-model %>% fit(t.train.x, train.y, epochs=250,
                  batch_size =100,
                  validation_split=0.2,
                  shuffle=TRUE)

summary(model)

saveRDS(model,paste0(dataFolder,"/multi_model.rds"))
saveRDS(history,paste0(dataFolder,"/multi_model_history.rds"))
################################################################
##RESULT EXPORT ###############################################

outcomeFolder<-file.path(dataFolder,"multi_model")
hist<-history$history

pdf(file=paste0(dataFolder,"/history.pdf"))
matplot(cbind(hist$val_binary_accuracy,hist$val_loss,hist$binary_accuracy,hist$loss),type = c("b"),pch=1,col = 1:4)
legend("topleft", legend = c('val_acc','val_loss','acc','loss'), col=1:4, pch=1)
dev.off()

y_pred<-predict(model, t.test.x)
y_true<-as.factor(test.y)
auprc<-pr.curve(scores.class0 = y_pred[y_true==1],
                scores.class1 = y_pred[y_true==0],
                curve = T)
pdf(file=paste0(dataFolder,"/AUPRC.pdf"))
plot(auprc)
dev.off()

auroc<-roc.curve(scores.class0 = y_pred[y_true==1],
                scores.class1 = y_pred[y_true==0],
                curve = T)
pdf(file=paste0(dataFolder,"/AUROC.pdf"))
plot(auroc)
dev.off()

str(roc(y_pred,y_true))
auc(roc(y_pred,y_true))
auc(AUC::sensitivity(y_pred,y_true))
auc(AUC::specificity(y_pred,y_true))

pred_output<-data.frame(person_id=test.list$person_id,label=test.y,pred=y_pred, diff=abs(test.y-y_pred))
pred_output<-pred_output %>% arrange(desc(diff))

write.csv(pred_output,paste0(dataFolder,"/pred_ouptut.csv"))

###########################################################
###########################################################