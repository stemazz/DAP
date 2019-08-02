# OPTIONS & LIBRARY -------------------------------------------------------------------

## Eseguo il file di configurazione di SparkR
source("connection_R.R")
options(scipen = 10000)

## Carico le funzioni necessarie al corretto run del modello
source("Churn_Digital/Production_Code/00_CD_UTILS.R")


# PARAMETRI ---------------------------------------------------------------

## Inserire la data target a partire da cui calcolare i kpi.
## Default: la data di oggi
# data_modello <- Sys.Date() 
data_modello <- as.Date("2018-07-25")
# modello valido dal 2018-08-01 al 2018-08-15


# PATH PER LETTURA --------------------------------------------------------

## Inserire i path dei file da importare
## Lasciare vuoto path delta quando si aggiorna il full

path_full_csv <- "/user/stefano.mazzucca/churn_digital_ver/1808.csv" 
path_full <- "/user/stefano.mazzucca/churn_digital_ver/2018_08/full_201808.parquet" 

## Default: path vuoto
path_full_new <- ""
# path_full_new <- "/user/stefano.mazzucca/churn_digital_ver/CD_full_20180731.parquet"

path_delta <- ""
# path_delta <- "/user/stefano.mazzucca/churn_digital_ver/cb_delta.parquet"


path_navigazione_sito <- "/user/stefano.mazzucca/churn_digital_ver/2018_08/nav_sito_180801.parquet"
path_navigazione_app <- "/user/stefano.mazzucca/churn_digital_ver/2018_08/nav_app_180801.parquet"
path_mappatura_cookie_skyid <- "/user/stefano.mazzucca/churn_digital_ver/2018_08/mappatura_skyid_coo_180801.parquet"


# PATH PER SCRITTURA ------------------------------------------------------

## Inserire i path dove scrivere i paruqte. Inserire anche il nome del file
## Default: No default

path_kpi_sito <- "/user/stefano.mazzucca/churn_digital_ver/2018_08/df_kpi_sito_180801_2.parquet"
path_kpi_app <- "/user/stefano.mazzucca/churn_digital_ver/2018_08/df_kpi_app_180801_2.parquet"
path_kpi_finale <- "/user/stefano.mazzucca/churn_digital_ver/2018_08/df_kpi_180801_2.parquet"

path_lista_output <- "/user/stefano.mazzucca/churn_digital_ver/output_modello_CD_180801_2.csv"

# path_true_list_output <- "/user/stefano.mazzucca/churn_digital_ver/2017_12/output_true_modello_CD_171201.csv"



# SCARICO NAVIGAZIONI  --------------------------------------------------------------

source("Churn_Digital/Production_Code/pre_01_scarico_navigazioni.R")


# UPDATE DMP --------------------------------------------------------------

## Update della dmp andr?? fatto solo se non c'?? un nuovo full.

if(path_delta != ""){
  source("Churn_Digital/Production_Code/02_CD_UPDATE_DMP.R")
  path_full <- path_full_new
}

# CREATE DF ---------------------------------------------------------------

## Creo la base dati con i kpi aggiornati alla data impostata precedentemente
source("Churn_Digital/Production_Code/03_CD_UPDATE_DF.R")

# TRAINING ----------------------------------------------------------------

# model <- spark.gbt(data = train, 
#                    formula = CHURN ~ .,
#                    type = "regression",
#                    maxDepth = 5,
#                    maxBins = 5,
#                    maxIter = 20,
#                    stepSize = 0.01,
#                    subsamplingRate = 0.9,
#                    seed = seed
#                    )

#write.ml(model, path = "/user/stefano.mazzucca/churn_digital/xgb_v1")


# IMPORTO MODELLO ---------------------------------------------------------

model <- read.ml("/user/stefano.mazzucca/churn_digital/xgb_new_CD_model")

df_kpi <- read.parquet(path_kpi_finale)

#### TEST su dati settimanali
df_kpi_test <- test_cleaning(df_kpi)
df_kpi_test <- filter(df_kpi_test, df_kpi_test$visite_totali_2m > 0)
# df_kpi_test <- select(df_kpi_test, retain)
prediction <- predict(model, df_kpi_test)
collected <- print_performance(prediction, 0.95)

output <- arrange(select(collected, "COD_CLIENTE_CIFRATO", "DAT_PRIMA_ATTIVAZIONE_dt", "prediction", "Pred"), desc(collected$prediction))
createOrReplaceTempView(output, "output")
lista_output <- sql("select COD_CLIENTE_CIFRATO, DAT_PRIMA_ATTIVAZIONE_dt, Pred as flg_pred, prediction, 
                        NTILE(10) OVER(ORDER BY prediction DESC NULLS LAST) as decile
                from output")
nrow(lista_output)
# 1.489.198 aprile
# 1.505.072 maggio
# 1.400.650 giugno
# 1.270.184 luglio
# 1.260.477 agosto
# 1.570.313 settembre
# 1.208.130 (2.638)   # 1.189.889 (2.960)   # 1.186.756 (4.192)   # 1.256.949 (5.941)   # 1.280.175 (4.635)

# true_list <- filter(lista_output, "Pred == 'TRUE'")
# nrow(true_list)


write.df(repartition( lista_output, 1), path = path_lista_output, "csv", sep=";", mode = "overwrite", header=TRUE)

# write.df(repartition( true_list, 1), path = path_true_list_output, "csv", sep=";", mode = "overwrite", header=TRUE)


