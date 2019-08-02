
## Verifica lista_output modello CHURN DIGITAL


#apri sessione
source("connection_R.R")
options(scipen = 1000)




path_lista <- "/user/stefano.mazzucca/churn_digital/output_modello_CD_20180717_enriched.csv"
# "/user/stefano.mazzucca/churn_digital/output_modello_CD_20180717_enriched.csv"
# "/user/stefano.mazzucca/churn_digital/output_modello_CD_20180724_enriched.csv"
# "/user/stefano.mazzucca/churn_digital/output_modello_CD_20180731_enriched.csv"

data_inizio_test <- as.Date('2018-07-17')
data_fine_test <- as.Date('2018-08-21')


lista <- read.df(path_lista, source = "csv",  header = "true", delimiter = ",")
# View(head(lista,100))
nrow(lista)
# 1.279.135

lista_1 <- withColumn(lista, "decile", cast(lista$decile, "double"))
lista_1 $flg_pred <- ifelse(lista_1$flg_pred == "TRUE", 1, 0)
lista_2 <- withColumn(lista_1, "dat_fine_dt", cast(cast(unix_timestamp(lista_1$DAT_FINE, 'ddMMMyyyy:HH:mm:ss'), 'timestamp'), 'date'))
# View(head(lista_2,100))
lista_3 <- filter(lista_2, lista_2$dat_fine_dt >= data_inizio_test | isNull(lista_2$dat_fine_dt))
lista_4 <- withColumn(lista_3, "data_test", lit(data_inizio_test))
lista_5 <- withColumn(lista_4, "gg_dalla_pred_a_effettivo_pdisc", datediff(lista_4$dat_fine_dt, lista_4$data_test))
lista_5$churn <- ifelse(lista_5$dat_fine_dt >= data_inizio_test, # & lista_5$dat_fine_dt <= data_fine_test, 
                        1, 0)
View(head(lista_5,100))
nrow(lista_5)
# 1.275.791


## Elaborazioni ###############################################################################################################################################
###############################################################################################################################################################

lista_pred_true <- filter(lista_5, "flg_pred = 1")
View(head(lista_pred_true,10000))
nrow(lista_pred_true)
# 2.683

media <- summarize(lista_pred_true, media = mean(lista_pred_true$gg_dalla_pred_a_effettivo_pdisc))
View(head(media,100))

createOrReplaceTempView(lista_pred_true, "lista_pred_true")
quartili <- sql("select percentile_approx(gg_dalla_pred_a_effettivo_pdisc, 0.25) as q1,
                        percentile_approx(gg_dalla_pred_a_effettivo_pdisc, 0.50) as q2, 
                        percentile_approx(gg_dalla_pred_a_effettivo_pdisc, 0.75) as q3,
                        percentile_approx(gg_dalla_pred_a_effettivo_pdisc, 1) as q4
                from lista_pred_true 
                where gg_dalla_pred_a_effettivo_pdisc is NOT NULL")
View(head(quartili,100)) 


lista_pred_true_churned <- filter(lista_pred_true, "dat_fine_dt is NOT NULL")
lista_pred_true_churned <- arrange(lista_pred_true_churned, asc(lista_pred_true_churned$dat_fine_dt))
View(head(lista_pred_true_churned,1000))
nrow(lista_pred_true_churned)
# 890
lista_pred_true_churned_week <- filter(lista_pred_true_churned, lista_pred_true_churned$dat_fine_dt <= data_fine_test & 
                                                                lista_pred_true_churned$dat_fine_dt >= data_inizio_test)
View(head(lista_pred_true_churned_week,1000))
nrow(lista_pred_true_churned_week)
# 377   # (665 considerando che c'erano gia' 288 utenti che tra il 02JUL e il 17JUL avevano gia' inviato disdetta.)


lista_churned <- filter(lista_5, "dat_fine_dt is NOT NULL")
nrow(lista_churned)
# 38.711
lista_churned_week <- filter(lista_churned, lista_churned$dat_fine_dt <= data_fine_test & 
                                            lista_churned$dat_fine_dt >= data_inizio_test)
nrow(lista_churned_week)
# 10.370   # (13.714 considerando che prima del 17JUL c'erano gia' 3.344 utenti in pdisc (13.714 - 10.370).)
###############################################################################################################################################################


## Creao dataset e funzioni per il calcolo delle performance di predizione: ####################################################################################

## FUNZIONE PER CALCOLARE L'ACCURACY
score_accuracy <- function(tp, tn, fp, fn){
  accuracy <- round((tp + tn)/(tp + tn + fp + fn) * 100, 1)
  return(accuracy)
}

## FUNZIONE PER CALCOLARE LA PRECISION
score_precision <- function(tp, fp){
  precision <- round(tp/(tp + fp) * 100, 1)
  return(precision)
}

## FUNZIONE PER CALCOLARE LA RECALL
score_recall <- function(tp, fn){
  recall <- round(tp/(tp + fn) * 100, 1)
  return(recall)
}


# View(head(lista_2,100))
ds <- select(lista_5, c("churn", "flg_pred"))
createOrReplaceTempView(ds, "ds")
cm <- sql("SELECT churn as Real, flg_pred, count(*) as Freq FROM ds GROUP BY 1,2")
cm <- as.data.frame(cm)
# View(cm)
true_positive <- cm[cm$Real == 1 & cm$flg_pred == 1,]$Freq
true_negative <- cm[cm$Real == 0 & cm$flg_pred == 0,]$Freq
false_positive <- cm[cm$Real == 0 & cm$flg_pred == 1,]$Freq
false_negative <- cm[cm$Real == 1 & cm$flg_pred == 0,]$Freq

accuracy <- score_accuracy(true_positive, true_negative, false_positive, false_negative)
precision <- score_precision(true_positive, false_positive)
recall <- score_recall(true_positive, false_negative)

View(cm)
print("--- PERFORMANCE ---")
print(paste0("ACCURACY --> ", accuracy))
print(paste0("PRECISION --> ", precision))
print(paste0("RECALL --> ", recall))


## Calcolo distribuzioni #######################################################################################################################################

media <- summarize(lista_5, media = mean(lista_5$gg_dalla_pred_a_effettivo_pdisc))
View(head(media,100))

createOrReplaceTempView(lista_5, "lista_5")
quartili <- sql("select percentile_approx(gg_dalla_pred_a_effettivo_pdisc, 0.25) as q1,
                        percentile_approx(gg_dalla_pred_a_effettivo_pdisc, 0.50) as q2, 
                        percentile_approx(gg_dalla_pred_a_effettivo_pdisc, 0.75) as q3,
                        percentile_approx(gg_dalla_pred_a_effettivo_pdisc, 1) as q4
                from lista_5 
                where gg_dalla_pred_a_effettivo_pdisc is NOT NULL")
View(head(quartili,100)) 


decile_1 <- filter(lista_5, lista_5$decile == 1)
# View(head(decile_1,100))
# nrow(decile_1)

createOrReplaceTempView(decile_1, "decile_1")
quartili <- sql("select percentile_approx(gg_dalla_pred_a_effettivo_pdisc, 0.25) as q1,
                        percentile_approx(gg_dalla_pred_a_effettivo_pdisc, 0.50) as q2, 
                        percentile_approx(gg_dalla_pred_a_effettivo_pdisc, 0.75) as q3,
                        percentile_approx(gg_dalla_pred_a_effettivo_pdisc, 1) as q4
                from decile_1 
                where gg_dalla_pred_a_effettivo_pdisc is NOT NULL")
View(head(quartili,100))

decile_2_3 <- filter(lista_5, lista_5$decile == 2 | lista_5$decile == 3)
# View(head(decile_2_3,100))
# nrow(decile_2_3)

createOrReplaceTempView(decile_2_3, "decile_2_3")
quartili <- sql("select percentile_approx(gg_dalla_pred_a_effettivo_pdisc, 0.25) as q1,
                        percentile_approx(gg_dalla_pred_a_effettivo_pdisc, 0.50) as q2, 
                        percentile_approx(gg_dalla_pred_a_effettivo_pdisc, 0.75) as q3,
                        percentile_approx(gg_dalla_pred_a_effettivo_pdisc, 1) as q4
                from decile_2_3 
                where gg_dalla_pred_a_effettivo_pdisc is NOT NULL")
View(head(quartili,100))

decile_maggiore_3 <- filter(lista_5, lista_5$decile >= 4)
# View(head(decile_maggiore_3,100))
# nrow(decile_maggiore_3)

createOrReplaceTempView(decile_maggiore_3, "decile_maggiore_3")
quartili <- sql("select percentile_approx(gg_dalla_pred_a_effettivo_pdisc, 0.25) as q1,
                        percentile_approx(gg_dalla_pred_a_effettivo_pdisc, 0.50) as q2, 
                        percentile_approx(gg_dalla_pred_a_effettivo_pdisc, 0.75) as q3,
                        percentile_approx(gg_dalla_pred_a_effettivo_pdisc, 1) as q4
                from decile_maggiore_3 
                where gg_dalla_pred_a_effettivo_pdisc is NOT NULL")
View(head(quartili,100))


createOrReplaceTempView(lista_5, "lista_5")
distr_gg <- sql("select gg_dalla_pred_a_effettivo_pdisc, count(COD_CLIENTE_CIFRATO) as count_extid
                from lista_5
                group by gg_dalla_pred_a_effettivo_pdisc")
distr_gg <- arrange(distr_gg, asc(distr_gg$gg_dalla_pred_a_effettivo_pdisc))
View(head(distr_gg,100))


lista_flg_1 <- filter(lista_5, "flg_pred = 1")
createOrReplaceTempView(lista_flg_1, "lista_flg_1")
distr_gg_flg_1 <- sql("select gg_dalla_pred_a_effettivo_pdisc, count(COD_CLIENTE_CIFRATO) as count_extid
                      from lista_flg_1
                      group by gg_dalla_pred_a_effettivo_pdisc")
distr_gg_flg_1 <- arrange(distr_gg_flg_1, asc(distr_gg_flg_1$gg_dalla_pred_a_effettivo_pdisc))
View(head(distr_gg_flg_1,100))


distr_gg_flg <- sql("select gg_dalla_pred_a_effettivo_pdisc, flg_pred, 
                    count(COD_CLIENTE_CIFRATO) as count_extid
                    from lista_5
                    group by gg_dalla_pred_a_effettivo_pdisc, flg_pred")
distr_gg_flg <- arrange(distr_gg_flg, asc(distr_gg$gg_dalla_pred_a_effettivo_pdisc), asc(distr_gg_flg$flg_pred))
View(head(distr_gg_flg,100))


distr_decile_churn <- sql("select decile, churn, 
                          count(COD_CLIENTE_CIFRATO) as count_extid
                          from lista_5
                          group by decile, churn")
distr_decile_churn <- arrange(distr_decile_churn, asc(distr_decile_churn$decile), asc(distr_decile_churn$churn))
View(head(distr_decile_churn,100))





# ## Join fra le 3 lista #######################################################################################################################################
# 
# path_1 <- "/user/stefano.mazzucca/churn_digital/output_modello_CD_20180717_enriched.csv"
# path_2 <- "/user/stefano.mazzucca/churn_digital/output_modello_CD_20180724_enriched.csv"
# path_3 <- "/user/stefano.mazzucca/churn_digital/output_modello_CD_20180731_enriched.csv"
# 
# lista_1 <- read.df(path_1, source = "csv", header = "true", delimiter = ",")
# lista_2 <- read.df(path_2, source = "csv", header = "true", delimiter = ",")
# lista_3 <- read.df(path_3, source = "csv", header = "true", delimiter = ",")
# View(head(lista_1,100))
# 
# lista_1 <- withColumn(lista_1, "decile", cast(lista_1$decile, "double"))
# lista_1 $flg_pred <- ifelse(lista_1$flg_pred == "TRUE", 1, 0)
# lista_1 <- withColumn(lista_1, "dat_fine_dt", cast(cast(unix_timestamp(lista_1$DAT_FINE, 'ddMMMyyyy:HH:mm:ss'), 'timestamp'), 'date'))
# 
# lista_2 <- withColumn(lista_2, "decile", cast(lista_2$decile, "double"))
# lista_2 $flg_pred <- ifelse(lista_2$flg_pred == "TRUE", 1, 0)
# lista_2 <- withColumn(lista_2, "dat_fine_dt", cast(cast(unix_timestamp(lista_2$DAT_FINE, 'ddMMMyyyy:HH:mm:ss'), 'timestamp'), 'date'))
# 
# lista_3 <- withColumn(lista_3, "decile", cast(lista_3$decile, "double"))
# lista_3 $flg_pred <- ifelse(lista_3$flg_pred == "TRUE", 1, 0)
# lista_3 <- withColumn(lista_3, "dat_fine_dt", cast(cast(unix_timestamp(lista_3$DAT_FINE, 'ddMMMyyyy:HH:mm:ss'), 'timestamp'), 'date'))
# 
# 
# lista_tot <- rbind(lista_1, lista_2, lista_3)
# View(head(lista_tot,100))
# nrow(lista_tot)
# # 3.790.997
# createOrReplaceTempView(lista_tot, "lista_tot")
# lista_tot_1 <- sql("select COD_CLIENTE_CIFRATO, DAT_PRIMA_ATTIVAZIONE_dt, dat_fine_dt,
#                             max(flg_pred) as flg_pred, 
#                             min(decile) as decile,
#                             count(COD_CLIENTE_CIFRATO) as count_utente_in_liste
#                    from lista_tot
#                    group by COD_CLIENTE_CIFRATO, DAT_PRIMA_ATTIVAZIONE_dt, dat_fine_dt")
# View(head(lista_tot_1,100))
# nrow(lista_tot_1)
# # 1.352.33
# lista_tot_2 <- arrange(lista_tot_1, desc(lista_tot_1$flg_pred), desc(lista_tot_1$dat_fine_dt))
# View(head(lista_tot_2,100))
# 
# 
# data_inizio_test <- '2018-07-01'
# data_fine_test <- '2018-08-22'
# 
# lista_tot_2$churn <- ifelse(lista_tot_2$dat_fine_dt >= data_inizio_test & lista_tot_2$dat_fine_dt <= data_fine_test, 1, 0)
# # View(head(lista_tot_2,100))
# ds <- select(lista_tot_2, c("churn", "flg_pred"))
# createOrReplaceTempView(ds, "ds")
# cm <- sql("SELECT churn as Real, flg_pred, count(*) as Freq FROM ds GROUP BY 1,2")
# cm <- as.data.frame(cm)
# # View(cm)
# true_positive <- cm[cm$Real == 1 & cm$flg_pred == 1,]$Freq
# true_negative <- cm[cm$Real == 0 & cm$flg_pred == 0,]$Freq
# false_positive <- cm[cm$Real == 0 & cm$flg_pred == 1,]$Freq
# false_negative <- cm[cm$Real == 1 & cm$flg_pred == 0,]$Freq
# 
# accuracy <- score_accuracy(true_positive, true_negative, false_positive, false_negative)
# precision <- score_precision(true_positive, false_positive)
# recall <- score_recall(true_positive, false_negative)
# 
# View(cm)
# print("--- PERFORMANCE ---")
# print(paste0("ACCURACY --> ", accuracy))    # 96.4
# print(paste0("PRECISION --> ", precision))  # 42.3
# print(paste0("RECALL --> ", recall))        # 8.5




## ROC and Lift curve #######################################################################################################################################

lista_6 <- arrange(lista_5, asc(lista_5$decile), desc(lista_5$flg_pred))
lista_7 <- select(lista_6, c("flg_pred", "churn"))
View(head(lista_7,100))
nrow(lista_7)
# 1.275.791

primo_decile <- filter(lista_6, lista_6$decile == 1)
View(head(primo_decile,100))
nrow(primo_decile)
# 129.284
# filt_primo_decile <- filter(primo_decile, primo_decile$churn == 1)
# nrow(filt_primo_decile)
# # 12.858

createOrReplaceTempView(primo_decile, "primo_decile")
cm <- sql("SELECT churn as Real, flg_pred, count(*) as Freq FROM primo_decile GROUP BY 1,2")
cm <- as.data.frame(cm)
View(cm)
true_positive <- cm[cm$Real == 1 & cm$flg_pred == 1,]$Freq
true_negative <- cm[cm$Real == 0 & cm$flg_pred == 0,]$Freq
false_positive <- cm[cm$Real == 0 & cm$flg_pred == 1,]$Freq
false_negative <- cm[cm$Real == 1 & cm$flg_pred == 0,]$Freq
#
accuracy <- score_accuracy(true_positive, true_negative, false_positive, false_negative)
precision <- score_precision(true_positive, false_positive)
recall <- score_recall(true_positive, false_negative)
# print("--- PERFORMANCE ---")
print(paste0("ACCURACY --> ", accuracy))    # 96.9
print(paste0("PRECISION --> ", precision))  # 33.2
print(paste0("RECALL --> ", recall))        # 2.3


require(ROCR)
library(ggplot2)

df <- as.data.frame(primo_decile)

pred <- prediction(df$flg_pred, df$churn)
gain <- performance(pred, "tpr", "rpp")
lift <- performance(pred, "lift", "rpp")
plot(gain, main = "Gain Chart")
plot(lift, main = "Lift Chart", colorize=TRUE)



perf <- performance(pred,"tpr","fpr")
plot(perf, colorize=TRUE, main = "ROC curve")

auc_ROCR <- performance(pred, measure = "auc")
auc_ROCR <- auc_ROCR@y.values[[1]]
cat(auc_ROCR)
# 0.6185925





## altre prove

lift.perf <- performance(pred, 'lift', 'rpp')
lift.df <- data.frame(predicted = lift.perf@alpha.values[[1]], # alpha is cutoff
                      lift = lift.perf@y.values[[1]])
lift.df <- lift.df[2:nrow(lift.df), ]


perf<-performance(pred, 'ecost')
plot(perf)


perf <- performance(pred, "acc", "lift")
plot(perf, colorize=T)
plot(perf, colorize=T,
     print.cutoffs.at=seq(0,1,by=0.1),
     add=T, text.adj=c(1.2, 1.2),
     avg="threshold", lwd=3)


perf <- performance(pred, "pcmiss", "lift")
plot(perf, colorize = T, print.cutoffs.at = seq(0, 1, by = 0.1),
     text.adj = c(1.2, 1.2), avg = "threshold", lwd = 3)


perf <- performance(pred,"acc")
perf@y.values
# 0.90054454 0.89355991 0.09945546
plot(perf, avg= "vertical",
     spread.estimate="boxplot",
     show.spread.at= seq(0.1, 0.9, by=0.1))










#chiudi sessione
sparkR.stop()
