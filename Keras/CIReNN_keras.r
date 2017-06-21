library(keras)
library(AUC)
library(dplyr)
library(data.table)
library(PRROC)

#for Mac: 
#dataFolder<-"/Users/chan/OneDrive/Study/CVDPrediction/data/new"
#14ho
dataFolder<-"/data/chan"
#pc dataFolder<-"F:/17.6.11"
timestep<-50

train.list<-readRDS(paste0(dataFolder,"/train.list.rds"))
test.list<-readRDS(paste0(dataFolder,"/test.list.rds"))

t.train.x<-train.list$x
train.y<-train.list$y

t.test.x<-test.list$x
test.y<-test.list$y

no_feature<-dim(t.test.x)[3]
timestep<-50

##single-layer gru
#model <- keras_model_sequential()
#model  %>%
#    layer_gru(units=32, recurrent_dropout = 0.2,input_shape = c(timestep,no_feature),
#              return_sequences=TRUE#,stateful=TRUE
#    ) %>%
#    layer_dropout(0.4) %>%
#    layer_dense(units=1, activation='sigmoid')

#model %>% compile(
#    loss = 'binary_crossentropy',
#    optimizer = optimizer_rmsprop(lr=0.0001, decay=1e-5),
#    metrics = c('binary_accuracy')
#)

#history<-model %>% fit(t.train.x, train.y, epochs=20,
#                       batch_size =100,
#                       validation_split=0.2,
#                       shuffle=TRUE)

#summary(model)

#saveRDS(model,paste0(dataFolder,"/singleGRU_model.rds"))
#saveRDS(history,paste0(dataFolder,"/singleGRU_model.rds"))


#multi-layer
model <- keras_model_sequential()
model  %>%
    layer_gru(units=128, recurrent_dropout = 0.2,input_shape = c(timestep,no_feature),
              return_sequences=TRUE#,stateful=TRUE
              ) %>%
    layer_dropout(0.4) %>%
    layer_gru(units=64, recurrent_dropout = 0.2,
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
hist<-history$metrics

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