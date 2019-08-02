# OPTIONS & LIBRARY -------------------------------------------------------------------

source("connection_R.R")
options(scipen = 10000)
source("Churn_Digital/00_Utils_3.R")


# CREATE DF ---------------------------------------------------------------


source("Churn_Digital/22_PDISC_18.R")
source("Churn_Digital/22_NOPDISC_18.R")

# IMPORT MODEL
model <- read.ml("/user/stefano.mazzucca/churn_digital/xgb_new_CD_model")



# TESTING -----------------------------------------------------------------
#nopdisc18 <- df_kpi_nopdisc_18
nopdisc18 <- read.parquet("/user/stefano.mazzucca/churn_digital/CD_df_kpi_nopdisc_18.parquet")

nopdisc18 <- drop(nopdisc18, "COD_CLIENTE_CIFRATO")
nopdisc18 <- drop(nopdisc18, "DAT_PRIMA_ATTIVAZIONE_dt")
#nopdisc18 <- drop(nopdisc18, "INGR_PDISC_dt")
nopdisc18$meno1M_FASCIA_SDM_PDISC_M1 <- cast(nopdisc18$meno1M_FASCIA_SDM_PDISC_M1, "integer")
nopdisc18$meno1M_FASCIA_SDM_PDISC_M4 <- cast(nopdisc18$meno1M_FASCIA_SDM_PDISC_M4, "integer")

pdisc18 <- read.parquet("/user/stefano.mazzucca/churn_digital/CD_df_kpi_pdisc_18.parquet")

nopdisc18 <- withColumn(nopdisc18, "CHURN", lit("FALSE"))
pdisc18 <- withColumn(pdisc18, "CHURN", lit("TRUE"))

pdisc18 <- withColumn(pdisc18, "month_pdisc", month(pdisc18$INGR_PDISC_dt))
pdisc_dicembre <- filter(pdisc18, "month_pdisc == 12")
pdisc_gennaio <- filter(pdisc18, "month_pdisc == 1")
pdisc_febbraio <- filter(pdisc18, "month_pdisc == 2")
pdisc_marzo <- filter(pdisc18, "month_pdisc == 3")
pdisc_aprile <- filter(pdisc18, "month_pdisc == 4")

#### DICEMBRE
pdisc_dicembre <- test_cleaning(pdisc_dicembre)
pdisc_dicembre <- select(pdisc_dicembre, names(nopdisc18))
test <- rbind(nopdisc18, pdisc_dicembre)
test <- filter(test, test$visite_totali_2m > 0)
test <- dropna(test, how = "any") #, cols = c("meno1M_FASCIA_SDM_PDISC_M1", "meno1M_FASCIA_SDM_PDISC_M4"))
prediction <- predict(model, test)
collected <- print_performance(prediction, 0.95)

#### GENNAIO
pdisc_gennaio <- test_cleaning(pdisc_gennaio)
pdisc_gennaio <- select(pdisc_gennaio, names(nopdisc18))
test <- rbind(nopdisc18, pdisc_gennaio)
test <- filter(test, test$visite_totali_2m > 0)
test <- dropna(test, how = "any") #, cols = c("meno1M_FASCIA_SDM_PDISC_M1", "meno1M_FASCIA_SDM_PDISC_M4"))
prediction <- predict(model, test)
collected <- print_performance(prediction, 0.95)

#### FEBBRAIO
pdisc_febbraio <- test_cleaning(pdisc_febbraio)
pdisc_febbraio <- select(pdisc_febbraio, names(nopdisc18))
test <- rbind(nopdisc18, pdisc_febbraio)
test <- filter(test, test$visite_totali_2m > 0)
test <- dropna(test, how = "any") #, cols = c("meno1M_FASCIA_SDM_PDISC_M1", "meno1M_FASCIA_SDM_PDISC_M4"))
prediction <- predict(model, test)
collected <- print_performance(prediction, 0.95)

#### MARZO
pdisc_marzo <- test_cleaning(pdisc_marzo)
pdisc_marzo <- select(pdisc_marzo, names(nopdisc18))
test <- rbind(nopdisc18, pdisc_marzo)
test <- filter(test, test$visite_totali_2m > 0)
test <- dropna(test, how = "any") #, cols = c("meno1M_FASCIA_SDM_PDISC_M1", "meno1M_FASCIA_SDM_PDISC_M4"))
prediction <- predict(model, test)
collected <- print_performance(prediction, 095)

#### APRILE
pdisc_aprile <- test_cleaning(pdisc_aprile)
pdisc_aprile <- select(pdisc_aprile, names(nopdisc18))
test <- rbind(nopdisc18, pdisc_aprile)
test <- filter(test, test$visite_totali_2m > 0)
test <- dropna(test, how = "any") #, cols = c("meno1M_FASCIA_SDM_PDISC_M1", "meno1M_FASCIA_SDM_PDISC_M4"))
prediction <- predict(model, test)
collected <- print_performance(prediction, 0.95)


#### EXPORT PER ANALISI
pred <- select(prediction, c("CHURN", "label", "prediction", "meno1M_FASCIA_SDM_PDISC_M4"))
write.df(repartition(pred, 1), path = "/user/riccardo.motta/churn_digital/analisi_test", "csv")
