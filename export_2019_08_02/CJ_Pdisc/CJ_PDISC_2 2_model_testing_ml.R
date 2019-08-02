
# CJ_PDISC_2
# modello predittivo - testing sui vari modelli con sparklyr


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

# library("dplyr", lib.loc="/usr/local/lib/R/site-library")
library(dplyr)
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


# Train a logistic regression model
ml_log <- ml_logistic_regression(train_tbl, ml_formula)

# Decision Tree
ml_dt <- ml_decision_tree(train_tbl, ml_formula)

# Random Forest
ml_rf <- ml_random_forest(train_tbl, ml_formula)

# Gradient Boosted Tree
ml_gbt <- ml_gradient_boosted_trees(train_tbl, ml_formula)

# Naive Bayes
ml_nb <- ml_naive_bayes(train_tbl, ml_formula)

# Neural Network
ml_nn <- ml_multilayer_perceptron(train_tbl, ml_formula, layers = c(75, 15, 2))


# Validation data
# Bundle the models into a single list object
ml_models <- list(
  "Logistic" = ml_log,
  "Decision Tree" = ml_dt,
  "Random Forest" = ml_rf,
  "Gradient Boosted Trees" = ml_gbt,
  "Naive Bayes" = ml_nb,
  "Neural Net" = ml_nn
)

# Create a function for scoring
score_test_data <- function(model, data = test_tbl){
  pred <- sdf_predict(model, data)
  select(pred, flg_31gg, prediction)
}

# Score all the models
ml_score <- lapply(ml_models, score_test_data)


## Compare results

# Lift function
calculate_lift <- function(scored_data) {
  scored_data %>%
    mutate(bin = ntile(desc(prediction), 10)) %>% 
    group_by(bin) %>% 
    summarize(count = sum(flg_31gg)) %>% 
    mutate(prop = count / sum(count)) %>% 
    arrange(bin) %>% 
    mutate(prop = cumsum(prop)) %>% 
    select(-count) %>% 
    collect() %>% 
    as.data.frame()
}

# Initialize results
ml_gains <- data_frame(
  bin = 1:10,
  prop = seq(0, 1, len = 10),
  model = "Base"
)

# Calculate lift
for(i in names(ml_score)){
  ml_gains <- ml_score[[i]] %>%
    calculate_lift %>%
    mutate(model = i) %>%
    bind_rows(ml_gains, .)
}

# Plot results
ggplot(ml_gains, aes(x = bin, y = prop, color = model)) +
  geom_point() +
  geom_line() +
  scale_color_brewer(type = "qual") +
  labs(title = "Lift Chart for Predicting Models",
       subtitle = "Test Data Set",
       x = NULL,
       y = NULL)


# Function for calculating accuracy
calc_accuracy <- function(data, cutpoint = 0.5){
  data %>% 
    mutate(prediction = if_else(prediction > cutpoint, 1.0, 0.0)) %>%
    ml_classification_eval("prediction", "flg_31gg", "accuracy")
}

# Calculate AUC and accuracy
perf_metrics <- data_frame(
  model = names(ml_score),
  AUC = 100 * sapply(ml_score, ml_binary_classification_eval, "flg_31gg", "prediction"),
  Accuracy = 100 * sapply(ml_score, calc_accuracy)
)
perf_metrics

#                   model     AUC   Accuracy
# 
# 1               Logistic 74.99120 74.96348
# 2          Decision Tree 79.42851 72.66253
# 3          Random Forest 81.47921 73.42951
# 4 Gradient Boosted Trees 83.39054 74.86304
# 5            Naive Bayes 69.34076 69.46676
# 6             Neural Net 76.06443 76.06830

library(tidyr)

# Plot results
gather(perf_metrics, metric, value, AUC, Accuracy) %>%
  ggplot(aes(reorder(model, value), value, fill = metric)) + 
  geom_bar(stat = "identity", position = "dodge") + 
  coord_flip() +
  xlab("") +
  ylab("Percent") +
  ggtitle("Performance Metrics")



## Feature importance

# Initialize results
feature_importance <- data_frame()

# Calculate feature importance
for(i in c("Decision Tree", "Random Forest", "Gradient Boosted Trees")){
  feature_importance <- ml_tree_feature_importance(sc, ml_models[[i]]) %>%
    mutate(Model = i) %>%
    mutate(importance = as.numeric(levels(importance))[importance]) %>%
    mutate(feature = as.character(feature)) %>%
    rbind(feature_importance, .)
}

# Plot results
feature_importance %>%
  ggplot(aes(reorder(unlist(feature), unlist(importance)), unlist(importance), fill = unlist(Model))) + 
  facet_wrap(~unlist(Model)) +
  geom_bar(stat = "identity") + 
  coord_flip() +
  labs(title = "Feature importance", x = NULL) +
  theme(legend.position = "none")


# ## Compare run times
# 
# # Number of reps per model
# n <- 10
# 
# # Format model formula as character
# format_as_character <- function(x){
#   x <- paste(deparse(x), collapse = "")
#   x <- gsub("\\s+", " ", paste(x, collapse = ""))
#   x
# }
# 
# # Create model statements with timers
# format_statements <- function(y){
#   y <- format_as_character(y[[".call"]])
#   y <- gsub('ml_formula', ml_formula_char, y)
#   y <- paste0("system.time(", y, ")")
#   y
# }
# 
# # Convert model formula to character
# ml_formula_char <- format_as_character(ml_formula)
# 
# # Create n replicates of each model statements with timers
# all_statements <- sapply(ml_models, format_statements) %>%
#   rep(., n) %>%
#   parse(text = .)
# 
# # Evaluate all model statements
# res <- map(all_statements, eval)
# 
# # Compile results
# result <- data_frame(model = rep(names(ml_models), n),
#                      time = sapply(res, function(x){as.numeric(x["elapsed"])})) 
# 
# # Plot
# result %>%
#   ggplot(aes(time, reorder(model, time))) + 
#   geom_boxplot() + 
#   geom_jitter(width = 0.4, aes(color = model)) +
#   scale_color_discrete(guide = FALSE) +
#   labs(title = "Model training times",
#        x = "Seconds",
#        y = NULL)
















spark_disconnect(sc)

