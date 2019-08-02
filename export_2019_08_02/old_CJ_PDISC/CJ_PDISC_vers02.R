
# CJ_PDISC vers.02

#apri sessione
source("connection_R.R")
options(scipen = 1000)


## Lista PDISC ricevuta da MARETING AUTOMATION

pdisc_path <- "/user/stefano.mazzucca/WAVERD_PDISC_ma_completo.csv"
pdisc_waverd <- read.df(pdisc_path, source = "csv", header = "true", delimiter = ";")
View(head(pdisc_waverd,100))
nrow(pdisc_waverd)
# 351.554

createOrReplaceTempView(pdisc_waverd,"pdisc_waverd")
valdist_waverd <- sql("select distinct CODICE_CONTRATTO__C
                      from pdisc_waverd")
View(head(valdist_waverd))
nrow(valdist_waverd)
# 114.347

# ## verifica sulla precedente lista ricevuta
# pdisc_path <- "/user/stefano.mazzucca/WAVERD_completo.csv"
# old_pdisc_waverd <- read.df(pdisc_path, source = "csv", header = "true", delimiter = ";")
# View(head(old_pdisc_waverd,100))
# nrow(old_pdisc_waverd)
# # 171.712
# 
# createOrReplaceTempView(old_pdisc_waverd,"old_pdisc_waverd")
# old_valdist_waverd <- sql("select distinct CODICE_CONTRATTO__C
#                       from old_pdisc_waverd")
# View(head(old_valdist_waverd))
# nrow(old_valdist_waverd)
# # 57.803


View(head(pdisc_completo,100))
nrow(pdisc_completo)
# 42.526 records

## Alcune verifiche sulla lista dei psdic #####################################################################################

pdisc_completo <- withColumn(pdisc_completo,"data_ingr_pdisc_formdate",cast(cast(unix_timestamp(pdisc_completo$DATA_INGR_PDISC, 'ddMMMyy'), 'timestamp'), 'date'))
View(head(pdisc_completo,100))

pdisc_filter_data_ant1dec <- filter(pdisc_completo, "data_ingr_pdisc_formdate<'2017-12-01'")
View(head(pdisc_filter_data_ant1dec,100))
nrow(pdisc_filter_data_ant1dec)
# 7.970

################################################################################################################################

## Join tra la base pdisc_completa e i valore di digitalizzaizone degli utenti

base_score_digital <- read.parquet("hdfs:///user/alessandro/clusterfinalscore_conApp.parquet")

View(head(base_score_digital, 100))
nrow(base_score_digital)
# 4.325.832

## Calcolo la "digitalizzazione_bin" a seconda del livello_digitalizzazione

createOrReplaceTempView(base_score_digital, "base_score_digital")

base_score_digital_2 <- sql("select distinct *
                            from base_score_digital")
nrow(base_score_digital_2)
# 4.178.841

base_liv_digital <- sql("select distinct *,
                        case when livello_digitalizzazione = 0 then 0 
                        when livello_digitalizzazione > 0 and livello_digitalizzazione <=3 then 1 
                        when livello_digitalizzazione > 3 and livello_digitalizzazione <=6 then 2 
                        when livello_digitalizzazione > 6 and livello_digitalizzazione <=9 then 3 
                        when livello_digitalizzazione > 9 and livello_digitalizzazione <=12 then 4 
                        when livello_digitalizzazione > 12 and livello_digitalizzazione <=15 then 5
                        when livello_digitalizzazione > 15 and livello_digitalizzazione <=18 then 6
                        when livello_digitalizzazione > 18 and livello_digitalizzazione <=21 then 7
                        when livello_digitalizzazione > 21 and livello_digitalizzazione <=24 then 8
                        when livello_digitalizzazione > 24 and livello_digitalizzazione <=27 then 9
                        when livello_digitalizzazione > 27 and livello_digitalizzazione <=30 then 10
                        else NULL end as digitalizzazione_bin
                        from base_score_digital
                        ")
View(head(base_liv_digital,100))
nrow(base_liv_digital)
# 4.178.841 (ceck OK)

## JOIN tra base_pdisc e livello di digitalizzazione

createOrReplaceTempView(pdisc_completo,"pdisc_completo")
createOrReplaceTempView(base_liv_digital,"base_liv_digital")

base_pdisc_digital <- sql("select t1.*, t2.digitalizzazione_bin
                             from pdisc_completo t1
                             left join base_liv_digital t2
                              on t1.COD_CLIENTE_CIFRATO = t2.skyid")
View(head(base_pdisc_digital, 100))
nrow(base_pdisc_digital)
# 42.526

# verifica2_ <- filter(base_pdisc_digital, "digitalizzazione_bin is NULL")
# nrow(verifica2_)
# # 1.445
# # 41.081 (rispetto ai 42.526 della base -->> 1.445 utenti SENZA livello digitalizzazione)
# ver2_ <- distinct(select(verifica2_, "COD_CONTRATTO"))
# nrow(ver2_)
# # 1.327 utenti
# 
# createOrReplaceTempView(verifica2_,"verifica2_")
# verifica3_ <- sql("select distinct DAT_PRIMA_ATTIV_CNTR
#                   from verifica2_")
# View(head(verifica3_,100))
# nrow(verifica3_)
# # 1.023 ?????????????????? NON sono nuovi utenti???????????????????????????????????????



## Join tra la lista e la base_pdisc_completa

createOrReplaceTempView(pdisc_waverd,"pdisc_waverd")
createOrReplaceTempView(base_pdisc_digital,"base_pdisc_digital")

join_pdisc_ma <- sql("select distinct t1.COD_CONTRATTO, t1.DES_ULTIMA_CAUSALE_CESSAZIONE
                     from base_pdisc_digital t1
                     inner join pdisc_waverd t2
                      on t1.COD_CONTRATTO = t2.CODICE_CONTRATTO__C")
View(head(join_pdisc_ma,100))
nrow(join_pdisc_ma)
# 31.029 





#chiudi sessione
sparkR.stop()




