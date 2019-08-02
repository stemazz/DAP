

# CJ_PDISC_2
# modello predittivo 


###################################################################################################################################################################################
## su SparkR #####################################################################################################################################################################
###################################################################################################################################################################################


#apri sessione
source("connection_R.R")
options(scipen = 1000)



export_data_grouped <- read.df("/user/stefano.mazzucca/export_data_cj_pdisc_grouped.csv", source = "csv", header = "true", delimiter = ",")
View(head(export_data_grouped,100))
nrow(export_data_grouped)
# 36.542


df_ <- withColumn(export_data_grouped, "flg_31gg", cast(export_data_grouped$flg_31gg, "integer"))
df_ <- withColumn(df_, "assistenza_conosci", cast(df_$assistenza_conosci, "integer"))
df_ <- withColumn(df_, "pagine_di_servizio", cast(df_$pagine_di_servizio, "integer"))
df_ <- withColumn(df_, "faidate_home", cast(df_$faidate_home, "integer"))
df_ <- withColumn(df_, "faidate_fatture_pagamenti", cast(df_$faidate_fatture_pagamenti, "integer"))
df_ <- withColumn(df_, "faidate_gestisci_dati_servizi", cast(df_$faidate_gestisci_dati_servizi, "integer"))
df_ <- withColumn(df_, "faidate_webtracking", cast(df_$faidate_webtracking, "integer"))
df_ <- withColumn(df_, "faidate_extra", cast(df_$faidate_extra, "integer"))
df_ <- withColumn(df_, "faidate_arricchisci_abbonamento", cast(df_$faidate_arricchisci_abbonamento, "integer"))
df_ <- withColumn(df_, "tecnologia", cast(df_$tecnologia, "integer"))
df_ <- withColumn(df_, "app_home", cast(df_$app_home, "integer"))
df_ <- withColumn(df_, "app_comunicazioni", cast(df_$app_comunicazioni, "integer"))
df_ <- withColumn(df_, "app_impostazioni", cast(df_$app_impostazioni, "integer"))
df_ <- withColumn(df_, "app_i_miei_dati", cast(df_$app_i_miei_dati, "integer"))
df_ <- withColumn(df_, "app_il_mio_abbonamento", cast(df_$app_il_mio_abbonamento, "integer"))
df_ <- withColumn(df_, "app_widget_dispositivi", cast(df_$app_widget_dispositivi, "integer"))
df_ <- withColumn(df_, "app_widget_ultimafattura", cast(df_$app_widget_ultimafattura, "integer"))
df_ <- withColumn(df_, "app_widget", cast(df_$app_widget, "integer"))
df_ <- withColumn(df_, "app_arricchisci_abbonamento", cast(df_$app_arricchisci_abbonamento, "integer"))
df_ <- withColumn(df_, "app_widget_gestisci", cast(df_$app_widget_gestisci, "integer"))
df_ <- withColumn(df_, "assistenza_sky_expert", cast(df_$assistenza_sky_expert, "integer"))
df_ <- withColumn(df_, "app_assistenza_home", cast(df_$app_assistenza_home, "integer"))
df_ <- withColumn(df_, "app_assistenza_ricerca", cast(df_$app_assistenza_ricerca, "integer"))
df_ <- withColumn(df_, "app_assistenza_gestisci", cast(df_$app_assistenza_gestisci, "integer"))
df_ <- withColumn(df_, "pacchetti_offerte", cast(df_$pacchetti_offerte, "integer"))
df_ <- withColumn(df_, "app_widget_contatta_sky", cast(df_$app_widget_contatta_sky, "integer"))
df_ <- withColumn(df_, "app_assistenza_contatta", cast(df_$app_assistenza_contatta, "integer"))
df_ <- withColumn(df_, "assistenza_contatta", cast(df_$assistenza_contatta, "integer"))
df_ <- withColumn(df_, "assistenza_home", cast(df_$assistenza_home, "integer"))
df_ <- withColumn(df_, "assistenza_richiedi_assistenza_ok", cast(df_$assistenza_richiedi_assistenza_ok, "integer"))
df_ <- withColumn(df_, "app_assistenza_conosci", cast(df_$app_assistenza_conosci, "integer"))
df_ <- withColumn(df_, "assistenza_richiedi_assistenza_ok_appview", cast(df_$assistenza_richiedi_assistenza_ok_appview, "integer"))
df_ <- withColumn(df_, "homepage_sky", cast(df_$homepage_sky, "integer"))
df_ <- withColumn(df_, "assistenza_conferma_richiesta_assistenza", cast(df_$assistenza_conferma_richiesta_assistenza, "integer"))
df_ <- withColumn(df_, "app_assistenza_risolvi", cast(df_$app_assistenza_risolvi, "integer"))
df_ <- withColumn(df_, "assistenza_ricerca", cast(df_$assistenza_ricerca, "integer"))
df_ <- withColumn(df_, "assistenza_gestisci", cast(df_$assistenza_gestisci, "integer"))
df_ <- withColumn(df_, "assistenza_conosci_disdetta", cast(df_$assistenza_conosci_disdetta, "integer"))
df_ <- withColumn(df_, "assistenza_info_disdetta", cast(df_$assistenza_info_disdetta, "integer"))
df_ <- withColumn(df_, "assistenza_moduli_contrattuali", cast(df_$assistenza_moduli_contrattuali, "integer"))
df_ <- withColumn(df_, "extra", cast(df_$extra, "integer"))
df_ <- withColumn(df_, "assistenza_risolvi", cast(df_$assistenza_risolvi, "integer"))
df_ <- withColumn(df_, "assistenza_trova_sky_service", cast(df_$assistenza_trova_sky_service, "integer"))
df_ <- withColumn(df_, "assistenza_benvenuto", cast(df_$assistenza_benvenuto, "integer"))
df_ <- withColumn(df_, "assistenza_trasloca_sky", cast(df_$assistenza_trasloca_sky, "integer"))
df_ <- withColumn(df_, "assistenza_info_disdetta_modulo_recesso_no_scadenza", cast(df_$assistenza_info_disdetta_modulo_recesso_no_scadenza, "integer"))
df_ <- withColumn(df_, "assistenza_info_disdetta_modulo_scadenza_contratto", cast(df_$assistenza_info_disdetta_modulo_scadenza_contratto, "integer"))
df_ <- withColumn(df_, "assistenza_assistenza_skyq", cast(df_$assistenza_assistenza_skyq, "integer"))
df_ <- withColumn(df_, "app_dati_e_fatture", cast(df_$app_dati_e_fatture, "integer"))
df_ <- withColumn(df_, "app_sky_extra", cast(df_$app_sky_extra, "integer"))
df_ <- withColumn(df_, "app_assistenza_e_supporto", cast(df_$app_assistenza_e_supporto, "integer"))
df_ <- withColumn(df_, "assistenza_trasparenza_tariffaria", cast(df_$assistenza_trasparenza_tariffaria, "integer"))
df_ <- withColumn(df_, "assistenza_skyready", cast(df_$assistenza_skyready, "integer"))
df_ <- withColumn(df_, "assistenza_faq_homepack", cast(df_$assistenza_faq_homepack, "integer"))
df_ <- withColumn(df_, "assistenza_vo", cast(df_$assistenza_vo, "integer"))
df_ <- withColumn(df_, "assistenza_info_disdetta_modulo_recesso_entro_14_giorni", cast(df_$assistenza_info_disdetta_modulo_recesso_entro_14_giorni, "integer"))
df_ <- withColumn(df_, "assistenza_print", cast(df_$assistenza_print, "integer"))
df_ <- withColumn(df_, "assistenza_schedeservice", cast(df_$assistenza_schedeservice, "integer"))
df_ <- withColumn(df_, "assistenza_disdetta", cast(df_$assistenza_disdetta, "integer"))
df_ <- withColumn(df_, "assistenza_sms", cast(df_$assistenza_sms, "integer"))
df_ <- withColumn(df_, "assistenza_info_disdetta_modulo_scadenza_contratto_timsky", cast(df_$assistenza_info_disdetta_modulo_scadenza_contratto_timsky, "integer"))
df_ <- withColumn(df_, "assistenza_gsa", cast(df_$assistenza_gsa, "integer"))
df_ <- withColumn(df_, "assistenza_riepilogo_homepack", cast(df_$assistenza_riepilogo_homepack, "integer"))
df_ <- withColumn(df_, "assistenza_info_disdetta_modulo_recesso_no_scadenza_timsky", cast(df_$assistenza_info_disdetta_modulo_recesso_no_scadenza_timsky, "integer"))
df_ <- withColumn(df_, "assistenza_info_disdetta_modulo_recesso_entro_14_giorni_timsky", cast(df_$assistenza_info_disdetta_modulo_recesso_entro_14_giorni_timsky, "integer"))
df_ <- withColumn(df_, "assistenza_informazioni_energetiche_sky_q", cast(df_$assistenza_informazioni_energetiche_sky_q, "integer"))
df_ <- withColumn(df_, "assistenza_Other", cast(df_$assistenza_Other, "integer"))
df_ <- withColumn(df_, "assistenza_sky_q", cast(df_$assistenza_sky_q, "integer"))
df_ <- withColumn(df_, "Home_Guidatv", cast(df_$Home_Guidatv, "integer"))
df_ <- withColumn(df_, "assistenza_piusky_success", cast(df_$assistenza_piusky_success, "integer"))
df_ <- withColumn(df_, "assistenza_sky_id", cast(df_$assistenza_sky_id, "integer"))
df_ <- withColumn(df_, "app_assistenza", cast(df_$app_assistenza, "integer"))
df_ <- withColumn(df_, "assistenza_clienti", cast(df_$assistenza_clienti, "integer"))
df_ <- withColumn(df_, "assistenza_registration_modifica", cast(df_$assistenza_registration_modifica, "integer"))
df_ <- withColumn(df_, "assistenza_fattura_elettronica", cast(df_$assistenza_fattura_elettronica, "integer"))
df_ <- withColumn(df_, "assistenza_piu_giorni_sky", cast(df_$assistenza_piu_giorni_sky, "integer"))


df <- select(df_, "flg_31gg", "assistenza_conosci", "pagine_di_servizio", "faidate_home", "faidate_fatture_pagamenti",
             "faidate_gestisci_dati_servizi", "faidate_webtracking", "faidate_extra", "faidate_arricchisci_abbonamento", "tecnologia",
             "app_home", "app_comunicazioni", "app_impostazioni", "app_i_miei_dati", "app_il_mio_abbonamento", "app_widget_dispositivi",
             "app_widget_ultimafattura", "app_widget", "app_arricchisci_abbonamento", "app_widget_gestisci", "assistenza_sky_expert",
             "app_assistenza_home", "app_assistenza_ricerca", "app_assistenza_gestisci", "pacchetti_offerte", "app_widget_contatta_sky",
             "app_assistenza_contatta", "assistenza_contatta", "assistenza_home", "assistenza_richiedi_assistenza_ok", "app_assistenza_conosci",
             "assistenza_richiedi_assistenza_ok_appview", "homepage_sky", "assistenza_conferma_richiesta_assistenza", "app_assistenza_risolvi",
             "assistenza_ricerca", "assistenza_gestisci", "assistenza_conosci_disdetta", "assistenza_info_disdetta", "assistenza_moduli_contrattuali",
             "extra", "assistenza_risolvi", "assistenza_trova_sky_service", "assistenza_benvenuto", "assistenza_trasloca_sky",
             "assistenza_info_disdetta_modulo_recesso_no_scadenza", "assistenza_info_disdetta_modulo_scadenza_contratto", "assistenza_assistenza_skyq",
             "app_dati_e_fatture", "app_sky_extra", "app_assistenza_e_supporto", "assistenza_trasparenza_tariffaria", "assistenza_skyready",
             "assistenza_faq_homepack", "assistenza_vo", "assistenza_info_disdetta_modulo_recesso_entro_14_giorni", "assistenza_print", "assistenza_schedeservice",
             "assistenza_disdetta", "assistenza_sms", "assistenza_info_disdetta_modulo_scadenza_contratto_timsky", "assistenza_gsa", "assistenza_riepilogo_homepack",
             "assistenza_info_disdetta_modulo_recesso_no_scadenza_timsky", "assistenza_info_disdetta_modulo_recesso_entro_14_giorni_timsky",
             "assistenza_informazioni_energetiche_sky_q", "assistenza_Other", "assistenza_sky_q", "Home_Guidatv", "assistenza_piusky_success",
             "assistenza_sky_id", "app_assistenza", "assistenza_clienti", "assistenza_registration_modifica", "assistenza_fattura_elettronica",
             "assistenza_piu_giorni_sky")
View(head(df,100))
nrow(df)
# 36.542

index <- 1:nrow(df) # sample works on vectors

train <- sample(df, withReplacement=FALSE, fraction=0.7, seed=42)
test <- except(df, train)
cache(train)
cache(test)


############################################################################################
## model with XGBoost ######################################################################
############################################################################################

model <- spark.gbt(df, flg_31gg ~ .,
                    type = "regression", maxDepth = 5, maxBins = 16)

# assistenza_conosci + pagine_di_servizio + faidate_home + faidate_fatture_pagamenti +
# faidate_gestisci_dati_servizi + faidate_webtracking + faidate_extra + faidate_arricchisci_abbonamento + tecnologia +
# app_home + app_comunicazioni + app_impostazioni + app_i_miei_dati + app_il_mio_abbonamento + app_widget_dispositivi +
# app_widget_ultimafattura + app_widget + app_arricchisci_abbonamento + app_widget_gestisci + assistenza_sky_expert +
# app_assistenza_home + app_assistenza_ricerca + app_assistenza_gestisci + pacchetti_offerte + app_widget_contatta_sky +
# app_assistenza_contatta + assistenza_contatta + assistenza_home + assistenza_richiedi_assistenza_ok + app_assistenza_conosci +
# assistenza_richiedi_assistenza_ok_appview + homepage_sky + assistenza_conferma_richiesta_assistenza + app_assistenza_risolvi +
# assistenza_ricerca + assistenza_gestisci + assistenza_conosci_disdetta + assistenza_info_disdetta + assistenza_moduli_contrattuali +
# extra + assistenza_risolvi + assistenza_trova_sky_service + assistenza_benvenuto + assistenza_trasloca_sky +
# assistenza_info_disdetta_modulo_recesso_no_scadenza + assistenza_info_disdetta_modulo_scadenza_contratto + assistenza_assistenza_skyq +
# app_dati_e_fatture + app_sky_extra + app_assistenza_e_supporto + assistenza_trasparenza_tariffaria + assistenza_skyready +
# assistenza_faq_homepack + assistenza_vo + assistenza_info_disdetta_modulo_recesso_entro_14_giorni + assistenza_print + assistenza_schedeservice +
# assistenza_disdetta + assistenza_sms + assistenza_info_disdetta_modulo_scadenza_contratto_timsky + assistenza_gsa + assistenza_riepilogo_homepack +
# assistenza_info_disdetta_modulo_recesso_no_scadenza_timsky + assistenza_info_disdetta_modulo_recesso_entro_14_giorni_timsky +
# assistenza_informazioni_energetiche_sky_q + assistenza_Other + assistenza_sky_q + Home_Guidatv + assistenza_piusky_success +
# assistenza_sky_id + app_assistenza + assistenza_clienti + assistenza_registration_modifica + assistenza_fattura_elettronica +
# assistenza_piu_giorni_sky

summary(model)

# make predictions
predictions <- predict(model, test)

results <- select(predictions, "flg_31gg", "prediction")
View(head(results,1000))
cache(results)

# save and load the model
path <- "/user/stefano.mazzucca/model_sparkR_gbt"
write.ml(model, path)
savedModel <- read.ml(path)
summary(savedModel)


# Confusion Matrix ######################################################

total_rows <- nrow(test)
results$prediction <- ifelse(results$prediction > 0.5, 1, 0)

results$tp_tn <- ifelse((results$flg_31gg == results$prediction), "TRUE", "FALSE")
df_tp_tn <- nrow(results[results$tp_tn == "TRUE",])

accuracy <- df_tp_tn/total_rows
cat(accuracy)
# 0.7509243

results$tp <- ifelse((results$prediction == 1 & results$flg_31gg == 1), "TRUE", "FALSE")
df_tp <- nrow(results[results$tp == "TRUE",])

results$fp <- ifelse((results$prediction == 1 & results$flg_31gg == 0), "TRUE", "FALSE")
df_fp <- nrow(results[results$fp == "TRUE",])

results$tn <- ifelse((results$prediction == 0 & results$flg_31gg == 0), "TRUE", "FALSE")
df_tn <- nrow(results[results$tn == "TRUE",])

results$fn <- ifelse((results$prediction == 0 & results$flg_31gg == 1), "TRUE", "FALSE")
df_fn <- nrow(results[results$fn == "TRUE",])

precision <- (df_tp/(df_tp+df_fp))
# 0.749247
recall <- (df_tp/(df_tp+df_fn))
# 0.6871547
accuracy = (df_tp+df_tn)/(df_tp+df_tn+df_fp+df_fn)
# 0.7509243

## auROC ??

#           prediction
# flg_31gg    0    1
#       0   4124 999
#       1   1359 2985



############################################################################################
## model with RandomForest #################################################################
############################################################################################

model_rf <- spark.randomForest(df, flg_31gg ~ .,
                                type = "regression", maxDepth = 5, maxBins = 16)

# make predictions
predictions_rf <- predict(model_rf, test)

results_rf <- select(predictions_rf, "flg_31gg", "prediction")
View(head(results_rf,1000))
cache(results_rf)


# Confusion Matrix ######################################################

total_rows <- nrow(test)
results_rf$prediction <- ifelse(results_rf$prediction > 0.5, 1, 0)

results_rf$tp_tn <- ifelse((results_rf$flg_31gg == results_rf$prediction), "TRUE", "FALSE")
df_tp_tn <- nrow(results_rf[results_rf$tp_tn == "TRUE",])

accuracy <- df_tp_tn/total_rows
cat(accuracy)
# 0.7173339

results_rf$tp <- ifelse((results_rf$prediction == 1 & results_rf$flg_31gg == 1), "TRUE", "FALSE")
df_tp <- nrow(results_rf[results_rf$tp == "TRUE",])

results_rf$fp <- ifelse((results_rf$prediction == 1 & results_rf$flg_31gg == 0), "TRUE", "FALSE")
df_fp <- nrow(results_rf[results_rf$fp == "TRUE",])

results_rf$tn <- ifelse((results_rf$prediction == 0 & results_rf$flg_31gg == 0), "TRUE", "FALSE")
df_tn <- nrow(results_rf[results_rf$tn == "TRUE",])

results_rf$fn <- ifelse((results_rf$prediction == 0 & results_rf$flg_31gg == 1), "TRUE", "FALSE")
df_fn <- nrow(results_rf[results_rf$fn == "TRUE",])

precision <- (df_tp/(df_tp+df_fp))
# 0.7141757
recall <- (df_tp/(df_tp+df_fn))
# 0.6401934
accuracy = (df_tp+df_tn)/(df_tp+df_tn+df_fp+df_fn)
# 0.7173339

## auROC ??

#           prediction
# flg_31gg    0    1
#       0   4010 1113
#       1   1563 2781


install.packages("ROCR",dep=T)
install.packages("ROCR")
require(ROCR)

df <- as.data.frame(predictions_rf)

pred <- prediction(df$prediction, df$label) #"flg_31gg"
gain <- performance(pred, "tpr", "rpp")
lift <- performance(pred, "lift", "rpp")
plot(gain, main = "Gain Chart")
plot(lift, main = "Lift Chart")



perf <- performance(pred,"tpr","fpr")
plot(perf,colorize=TRUE, main = "ROC curve")

auc_ROCR <- performance(pred, measure = "auc")
auc_ROCR <- auc_ROCR@y.values[[1]]
cat(auc_ROCR)
# 0.7905575




#chiudi sessione
sparkR.stop()







###################################################################################################################################################################################
## su sparklyr ###################################################################################################################################################################
###################################################################################################################################################################################

#USER
SPARK_USER="stefano.mazzucca"

# connection #
Sys.setenv(SPARK_HOME="/home/rstudio/spark/spark-2.1.0-bin-hadoop2.7/")
Sys.setenv(SPARK_HOME_VERSION="2.1.0")
Sys.setenv(SPARK_HDFS_CONFIG_URL="/usr/lib/rstudio-server/hadoop/")
Sys.setenv(SPARK_USER=SPARK_USER)

# Conf obbligatorie
conf <- list()
conf$spark.mesos.executor.docker.image <- "mesosphere/spark:1.0.9-2.1.0-1-hadoop-2.7"
conf$spark.mesos.executor.home <- "/opt/spark/dist"

# Puoi aggiungere le opzioni aggiuntive ad es.:

conf$spark.executor.cores <-"2"
conf$spark.executor.memory <- "15G"
conf$spark.executor.heartbeat <- "60s"
conf$spark.cores.max <- "10"
conf$spark.driver.maxResultSize <- "40G"
conf$spark.driver.memory <- "6G"
conf$spark.driver.cores <- "2"

library(sparklyr)
sc <- spark_connect(master = 'mesos://leader.mesos:5050', config = conf, app_name = 'sparklyr_SM')

library("dplyr", lib.loc="/usr/local/lib/R/site-library")
library(ggplot2)


export_data_grouped <- spark_read_csv(sc, "export_data_grouped", "/user/stefano.mazzucca/export_data_cj_pdisc_grouped.csv")
View(head(export_data_grouped,100))
nrow(export_data_grouped)
# 36.542


data_tbl <- tbl(sc, "export_data_grouped")

partition <- data_tbl %>% 
  select(flg_31gg, assistenza_conosci, pagine_di_servizio, faidate_home, faidate_fatture_pagamenti,
         faidate_gestisci_dati_servizi, faidate_webtracking, faidate_extra, faidate_arricchisci_abbonamento, tecnologia,
         app_home, app_comunicazioni, app_impostazioni, app_i_miei_dati, app_il_mio_abbonamento, app_widget_dispositivi,
         app_widget_ultimafattura, app_widget, app_arricchisci_abbonamento, app_widget_gestisci, assistenza_sky_expert,
         app_assistenza_home, app_assistenza_ricerca, app_assistenza_gestisci, pacchetti_offerte, app_widget_contatta_sky,
         app_assistenza_contatta, assistenza_contatta, assistenza_home, assistenza_richiedi_assistenza_ok, app_assistenza_conosci,
         assistenza_richiedi_assistenza_ok_appview, homepage_sky, assistenza_conferma_richiesta_assistenza, app_assistenza_risolvi,
         assistenza_ricerca, assistenza_gestisci, assistenza_conosci_disdetta, assistenza_info_disdetta, assistenza_moduli_contrattuali,
         extra, assistenza_risolvi, assistenza_trova_sky_service, assistenza_benvenuto, assistenza_trasloca_sky,
         assistenza_info_disdetta_modulo_recesso_no_scadenza, assistenza_info_disdetta_modulo_scadenza_contratto, assistenza_assistenza_skyq,
         app_dati_e_fatture, app_sky_extra, app_assistenza_e_supporto, assistenza_trasparenza_tariffaria, assistenza_skyready,
         assistenza_faq_homepack, assistenza_vo, assistenza_info_disdetta_modulo_recesso_entro_14_giorni, assistenza_print, assistenza_schedeservice,
         assistenza_disdetta, assistenza_sms, assistenza_info_disdetta_modulo_scadenza_contratto_timsky, assistenza_gsa, assistenza_riepilogo_homepack,
         assistenza_info_disdetta_modulo_recesso_no_scadenza_timsky, assistenza_info_disdetta_modulo_recesso_entro_14_giorni_timsky,
         assistenza_informazioni_energetiche_sky_q, assistenza_Other, assistenza_sky_q, Home_Guidatv, assistenza_piusky_success,
         assistenza_sky_id, app_assistenza, assistenza_clienti, assistenza_registration_modifica, assistenza_fattura_elettronica,
         assistenza_piu_giorni_sky) %>%
  sdf_partition(train = 0.70, test = 0.30, seed = 8585)

# Create table references
train_tbl <- partition$train
#View(head(train_tbl,100))
#nrow(train_tbl)
# 25.590
test_tbl <- partition$test
#nrow(test_tbl)
# 10.952



ml_formula <- formula(flg_31gg ~ .)


############################################################################################
## model with XGBoost ######################################################################
############################################################################################

ml_gbt_model <- ml_gradient_boosted_trees(train_tbl, ml_formula)

summary(ml_gbt_model)


pred <- sdf_predict(ml_gbt_model, test_tbl)

results <- select(pred, flg_31gg, prediction)


# Confusion Matrix ######################################################

df_tp <- filter(results,(prediction >= 0.5 && flg_31gg == 1))
df_fp <- filter(results,(prediction >= 0.5 && flg_31gg == 0))
df_tn <- filter(results,(prediction < 0.5 && flg_31gg == 0))
df_fn <- filter(results,(prediction < 0.5 && flg_31gg == 1))


tp <- df_tp %>%  summarise(n = n()) %>% collect() %>% as.integer()
fp <- df_fp %>%  summarise(n = n()) %>% collect() %>% as.integer()
tn <- df_tn %>% summarise(n = n()) %>% collect() %>% as.integer()
fn <- df_fn %>%  summarise(n = n()) %>% collect() %>% as.integer()

df_precision <- (tp/(tp+fp))
# 0.7560137
df_recall <- (tp/(tp+fn))
# 0.7286109
df_accuracy = (tp+tn)/(tp+tn+fp+fn)
# 0.7486304
c_AUC <- ml_binary_classification_eval(results, label = "flg_31gg",
                                       score = "prediction", metric = "areaUnderROC")
# 0.8339858


results_df <- data.frame(results)
results_df$prediction <- ifelse(results_df$prediction > 0.5, 1, 0)


tb <- with(results_df, table(flg_31gg, prediction))
View(tb)
#           prediction
# flg_31gg    0    1
#       0   4239 1278
#       1   1475 3960

prop_tb <- prop.table(tb)
View(prop_tb)
#                   prediction
# flg_31gg         0         1
#       0     0.3870526 0.1166910
#       1     0.1346786 0.3615778


# Lift Chart ####################################################

calculate_lift <- results %>%
    mutate(bin = ntile(desc(prediction), 10)) %>% 
    group_by(bin) %>% 
    summarize(count = sum(flg_31gg)) %>% 
    mutate(prop = count / sum(count)) %>% 
    arrange(bin) %>% 
    mutate(prop = cumsum(prop)) %>% 
    select(-count) %>% 
    collect() %>% 
    as.data.frame()

ggplot(calculate_lift, aes(x = bin, y = prop)) +
  geom_point() +
  geom_line() +
  scale_color_brewer(type = "qual") +
  labs(title = "Lift Chart for Predicting Model",
       subtitle = "Test Data Set",
       x = NULL,
       y = NULL)


# Importanza variabili ###########################################

feature_importance <- ml_tree_feature_importance(sc, ml_gbt_model) %>%
                          mutate(importance = as.numeric(levels(importance))[importance]) %>%
                          mutate(feature = as.character(feature))

feature_importance %>%
  ggplot(aes(reorder(feature, importance), importance)) + 
  geom_bar(stat = "identity") + 
  coord_flip() +
  labs(title = "Feature importance",
       x = NULL) +
  theme(legend.position = "none")


############################################################################################
## model with RandomForest #################################################################
############################################################################################

ml_rf_model <- ml_random_forest(train_tbl, ml_formula)

summary(ml_rf_model)


pred <- sdf_predict(ml_rf_model, test_tbl)

results <- select(pred, flg_31gg, prediction)


# Confusion Matrix ######################################################

df_tp <- filter(results,(prediction >= 0.5 && flg_31gg == 1))
df_fp <- filter(results,(prediction >= 0.5 && flg_31gg == 0))
df_tn <- filter(results,(prediction < 0.5 && flg_31gg == 0))
df_fn <- filter(results,(prediction < 0.5 && flg_31gg == 1))


tp <- df_tp %>%  summarise(n = n()) %>% collect() %>% as.integer()
fp <- df_fp %>%  summarise(n = n()) %>% collect() %>% as.integer()
tn <- df_tn %>% summarise(n = n()) %>% collect() %>% as.integer()
fn <- df_fn %>%  summarise(n = n()) %>% collect() %>% as.integer()

df_precision <- (tp/(tp+fp))
# 0.7452885
df_recall <- (tp/(tp+fn))
# 0.7057958
df_accuracy = (tp+tn)/(tp+tn+fp+fn)
# 0.7342951
c_AUC <- ml_binary_classification_eval(results, label = "flg_31gg",
                                       score = "prediction", metric = "areaUnderROC")
# 0.8147921


results_df <- data.frame(results)
results_df$prediction <- ifelse(results_df$prediction > 0.5, 1, 0)


tb <- with(results_df, table(flg_31gg, prediction))
View(tb)
#           prediction
# flg_31gg    0    1
#       0   4206 1311
#       1   1599 3836

prop_tb <- prop.table(tb)
View(prop_tb)
#                   prediction
# flg_31gg         0         1
#       0     0.3840394 0.1197042
#       1     0.1460007 0.3502557


# Lift Chart ####################################################

calculate_lift <- results %>%
  mutate(bin = ntile(desc(prediction), 10)) %>% 
  group_by(bin) %>% 
  summarize(count = sum(flg_31gg)) %>% 
  mutate(prop = count / sum(count)) %>% 
  arrange(bin) %>% 
  mutate(prop = cumsum(prop)) %>% 
  select(-count) %>% 
  collect() %>% 
  as.data.frame()

ggplot(calculate_lift, aes(x = bin, y = prop)) +
  geom_point() +
  geom_line() +
  scale_color_brewer(type = "qual") +
  labs(title = "Lift Chart for Predicting Model",
       subtitle = "Test Data Set",
       x = NULL,
       y = NULL)


# Importanza variabili ###########################################

feature_importance <- ml_tree_feature_importance(sc, ml_rf_model) %>%
  mutate(importance = as.numeric(levels(importance))[importance]) %>%
  mutate(feature = as.character(feature))

feature_importance %>%
  ggplot(aes(reorder(feature, importance), importance)) + 
  geom_bar(stat = "identity") + 
  coord_flip() +
  labs(title = "Feature importance",
       x = NULL) +
  theme(legend.position = "none")








spark_disconnect(sc)





