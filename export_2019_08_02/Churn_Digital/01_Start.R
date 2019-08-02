# OPTIONS & LIBRARY -------------------------------------------------------------------

source("connection_R.R")
options(scipen = 10000)
source("Churn_Digital/00_Utils_3.R")


# CREATE DF ---------------------------------------------------------------

#source("Churn_Digital/22_PDISC_17.R")
#source("Churn_Digital/22_NOPDISC_17.R")

# IMPORT DF
pdisc17 <- read.parquet("/user/stefano.mazzucca/churn_digital/df_kpi_pdisc_17.parquet")
nopdisc17 <- read.parquet("/user/stefano.mazzucca/churn_digital/df_kpi_nopdisc_17.parquet")


# SPLIT PREPARATION -------------------------------------------------------
nopdiscxtrain <- read.parquet("/user/riccardo.motta/churn_digital/sampled_nopdisc.parquet")
nopdiscxtrain <- select(nopdiscxtrain, "SKY_ID")
nopdiscxtrain <- withColumnRenamed(nopdiscxtrain, "SKY_ID", "COD_CLIENTE_CIFRATO")

nopdisc17 <- join_COD_CLIENTE_CIFRATO(nopdiscxtrain, nopdisc17)
nopdisc17 <- withColumn(nopdisc17, "CHURN", lit("FALSE"))
pdisc17 <- withColumn(pdisc17, "CHURN", lit("TRUE"))
pdisc17 <- drop(pdisc17, "INGR_PDISC_dt")

nopdisc17 <- select(nopdisc17, names(pdisc17))

pdisc_17_train <- sample(pdisc17, withReplacement=FALSE, fraction=0.8, seed = seed)
nopdisc_17_train <- sample(nopdisc17, withReplacement=FALSE, fraction=0.8, seed = seed)


createOrReplaceTempView(pdisc17, "pdisc17")
createOrReplaceTempView(pdisc_17_train, "pdisc_17_train")
pdisc_17_test <- sql("select t1.*
                     from pdisc17 t1 left join pdisc_17_train t2 on t1.COD_CLIENTE_CIFRATO = t2.COD_CLIENTE_CIFRATO
                     where t2.COD_CLIENTE_CIFRATO is null")
# nrow(pdisc_17_test)
# #23.613

createOrReplaceTempView(nopdisc17, "nopdisc17")
createOrReplaceTempView(nopdisc_17_train, "nopdisc_17_train")
nopdisc_17_test <- sql("select t1.*
                       from nopdisc17 t1 left join nopdisc_17_train t2 on t1.COD_CLIENTE_CIFRATO = t2.COD_CLIENTE_CIFRATO
                       where t2.COD_CLIENTE_CIFRATO is null")
# nrow(nopdisc_17_test)
# # 80.070


train <- rbind(pdisc_17_train, nopdisc_17_train)
test <- rbind(pdisc_17_test, nopdisc_17_test)

train <- filter(train, train$visite_totali_2m > 0)

train <- drop(train, "COD_CLIENTE_CIFRATO")
train <- drop(train, "DAT_PRIMA_ATTIVAZIONE_dt")
train$meno1M_FASCIA_SDM_PDISC_M1 <- cast(train$meno1M_FASCIA_SDM_PDISC_M1, "integer")
train$meno1M_FASCIA_SDM_PDISC_M4 <- cast(train$meno1M_FASCIA_SDM_PDISC_M4, "integer")
train <- dropna(train, how = "any", cols = c("meno1M_FASCIA_SDM_PDISC_M1", "meno1M_FASCIA_SDM_PDISC_M4"))

# train <- select(train, retain)


# TRAINING ----------------------------------------------------------------

model <- spark.gbt(data = train,
                   formula = CHURN ~ .,
                   type = "regression",
                   maxDepth = 5,
                   maxBins = 5,
                   maxIter = 20,
                   stepSize = 0.01,
                   subsamplingRate = 0.9,
                   seed = seed
)

# write.ml(model, path = "/user/stefano.mazzucca/churn_digital/xgb_new_CD_model")
write.ml(model, path = "/user/stefano.mazzucca/churn_digital/xgb_new_CD_model_400var")

model <- read.ml("/user/stefano.mazzucca/churn_digital/xgb_new_CD_model_400var")

summary_model <- summary(model)
imp_var_model <- summary_model$featureImportances
View(head(imp_var_model,100))


#### TEST
test <- test_cleaning(test)
test <- filter(test, test$visite_totali_2m > 0)
prediction <- predict(model, test)
collected <- print_performance(prediction, 0.95)



# TESTING ----------------------------------------------------------------- VAI al file TEST_R2.r ----------------------------------------------------------





#chiudi sessione
sparkR.stop()

