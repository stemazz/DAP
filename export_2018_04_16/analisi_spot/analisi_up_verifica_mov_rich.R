
## Verifica MOVIMENTI ATTIVAZIONI up_downgrade VS RICHIESTE up_downgrade

#apri sessione
source("connection_R.R")
options(scipen = 1000)

#################################################################################################################################################################################

# Richieste di UP e DDOWNGRADE
rich_up_down <- read.parquet("/STAGE/CMDM/DettaglioRichiesteUpDowngradeReali/full/vs_rich_up_downgrade_SHOT_2.parquet")
View(head(rich_up_down,1000))
printSchema(rich_up_down)

# root
# |-- COD_CONTRATTO: string (nullable = true)

# |-- DAT_RICHIESTA: string (nullable = true)
# |-- DES_PACCHETTO_DA: string (nullable = true)
# |-- DES_PACCHETTO_A: string (nullable = true)
# |-- DAT_SCHEDULAZIONE_RICHIESTA: string (nullable = true)
# |-- FLG_UPG_PACK: string (nullable = true)
# |-- FLG_DWG_PACK: string (nullable = true)

# |-- FLG_UPGRADE_CALCIO: string (nullable = true)
# |-- FLG_UPGRADE_SPORT: string (nullable = true)
# |-- FLG_UPGRADE_CINEMA: string (nullable = true)
# |-- FLG_DOWNGRADE_CALCIO: string (nullable = true)
# |-- FLG_DOWNGRADE_SPORT: string (nullable = true)
# |-- FLG_DOWNGRADE_CINEMA: string (nullable = true)
# |-- NUM_UPG_PACK: string (nullable = true)
# |-- NUM_DWG_PACK: string (nullable = true)

# |-- FLG_UPG_HD: string (nullable = true)
# |-- FLG_DWG_HD: string (nullable = true)
# |-- FLG_UPG_SKYTV: string (nullable = true)
# |-- FLG_DWG_SKYTV: string (nullable = true)
# |-- FLG_UPG_SKYFAMIGLIA: string (nullable = true)
# |-- FLG_DWG_SKYFAMIGLIA: string (nullable = true)


rich_up_down_1 <- select(rich_up_down, "COD_CONTRATTO", "DAT_RICHIESTA", "FLG_UPGRADE_CALCIO", "FLG_UPGRADE_SPORT", "FLG_UPGRADE_CINEMA", 
                         "FLG_UPG_HD", "FLG_UPG_SKYFAMIGLIA", "DES_PACCHETTO_DA", "DES_PACCHETTO_A")
rich_up_down_2 <- withColumn(rich_up_down_1, "date", cast(cast(unix_timestamp(rich_up_down_1$DAT_RICHIESTA, 'dd/MM/yyyy'), 'timestamp'), 'date'))
nrow(rich_up_down_2)
# 17.440.445

rich_up_down_3 <- filter(rich_up_down_2, "date >= '2017-07-01' and date <= '2017-10-01'")
View(head(rich_up_down_3, 1000))
nrow(rich_up_down_3)
# 599.663

# Selezione dei soli UPGRADE di pacchetti premium
rich_up_down_4 <- filter(rich_up_down_3, "FLG_UPGRADE_CALCIO = 1 or
                         FLG_UPGRADE_SPORT = 1 or
                         FLG_UPGRADE_CINEMA = 1 or
                         FLG_UPG_HD = 1 or
                         FLG_UPG_SKYFAMIGLIA = 1")
View(head(rich_up_down_4,1000))
nrow(rich_up_down_4)
# 375.080


#################################################################################################################################################################################

# Movimenti/Attivazioni di UP e DOWN GRADE
mov_up_down <- read.parquet("/STAGE/CMDM/DettaglioAttivazioniUpDowngrade/full/vs_attiv_up_downgrade_SHOT_2.parquet")
View(head(mov_up_down,1000))
printSchema(mov_up_down)

# root
# |-- COD_CONTRATTO: string (nullable = true)
# |-- DAT_EVENTO: string (nullable = true)
# |-- FLG_UPGRADE_GENERICO: string (nullable = true)
# |-- FLG_UPGRADE_CALCIO: string (nullable = true)
# |-- FLG_UPGRADE_SPORT: string (nullable = true)
# |-- FLG_UPGRADE_CINEMA: string (nullable = true)
# |-- FLG_DOWNGRADE_GENERICO: string (nullable = true)
# |-- FLG_DOWNGRADE_CALCIO: string (nullable = true)
# |-- FLG_DOWNGRADE_SPORT: string (nullable = true)
# |-- FLG_DOWNGRADE_CINEMA: string (nullable = true)
# |-- DES_PACCHETTO_DA: string (nullable = true)
# |-- DES_PACCHETTO_A: string (nullable = true)

# |-- FLG_UPG_HD: string (nullable = true)
# |-- FLG_DWG_HD: string (nullable = true)
# |-- FLG_UPG_SKYTV: string (nullable = true)
# |-- FLG_DWG_SKYTV: string (nullable = true)
# |-- FLG_UPG_SKYFAMIGLIA: string (nullable = true)
# |-- FLG_DWG_SKYFAMIGLIA: string (nullable = true)


mov_up_down_1 <- select(mov_up_down, "COD_CONTRATTO", "DAT_EVENTO", "FLG_UPGRADE_CALCIO", "FLG_UPGRADE_SPORT", "FLG_UPGRADE_CINEMA", 
                        "FLG_UPG_HD", "FLG_UPG_SKYFAMIGLIA", "DES_PACCHETTO_DA", "DES_PACCHETTO_A")
mov_up_down_2 <- withColumn(mov_up_down_1, "date", cast(cast(unix_timestamp(mov_up_down_1$DAT_EVENTO, 'dd/MM/yyyy'), 'timestamp'), 'date'))
nrow(mov_up_down_2)
# 19.723.578

mov_up_down_3 <- filter(mov_up_down_2, "date >= '2017-07-01' and date <= '2017-10-01'")
View(head(mov_up_down_3, 1000))
nrow(mov_up_down_3)
# 574.514

# Selezione dei soli UPGRADE di pacchetti premium
mov_up_down_4 <- filter(mov_up_down_3, "FLG_UPGRADE_CALCIO = 1 or
                        FLG_UPGRADE_SPORT = 1 or
                        FLG_UPGRADE_CINEMA = 1 or
                        FLG_UPG_HD = 1 or
                        FLG_UPG_SKYFAMIGLIA = 1")
View(head(mov_up_down_4,1000))
nrow(mov_up_down_4)
# 384.964 



######################
# 599.663 richieste di movimento pacchetto
# 574.514 attivazioni di movimento pacchetto
# -> caduta di circa il 4,2 %
######################

######################
# 375.080 richieste di upselling
# 384.964 attivazioni di upselling
# -> "caduta" (in realt?? aumentano) di circa il 2,6 %
######################



################################################################################################################################################################################

## Conversione dei COD_CONTRATTO in COD_CLIENTE

tab_conv <- read.parquet("hdfs:///OBF/CMDM/STGContrattoCliente/full/stg_contratto_cliente_SHOT_2.parquet")
View(head(tab_conv,100))


createOrReplaceTempView(tab_conv, "tab_conv")
createOrReplaceTempView(rich_up_down_4, "rich_up_down_4")
createOrReplaceTempView(mov_up_down_4, "mov_up_down_4")

rich_up_down_5 <- sql("select t1.*, t2.COD_CLIENTE_CIFRATO
                      from rich_up_down_4 t1
                      inner join tab_conv t2
                        on t1.COD_CONTRATTO = t2.COD_CONTRATTO_CIFRATO
                      order by t2.COD_CLIENTE_CIFRATO")
View(head(rich_up_down_5,100))
nrow(rich_up_down_5) 
# 375.080 (Ceck OK)
rich_up_down_5_ver <- filter(rich_up_down_5, "COD_CLIENTE_CIFRATO is NULL")
nrow(rich_up_down_5_ver)
# 0

valdist_cl_rich <- distinct(select(rich_up_down_5, "COD_CLIENTE_CIFRATO"))
nrow(valdist_cl_rich)
# 353.001
valdist_cl_date_rich <- distinct(select(rich_up_down_5, "COD_CLIENTE_CIFRATO", "date"))
nrow(valdist_cl_date_rich)
# 374.384


mov_up_down_5 <- sql("select t1.*, t2.COD_CLIENTE_CIFRATO
                      from mov_up_down_4 t1
                      inner join tab_conv t2
                        on t1.COD_CONTRATTO = t2.COD_CONTRATTO
                     order by t2.COD_CLIENTE_CIFRATO")
View(head(mov_up_down_5,100))
nrow(mov_up_down_5)
# 384.964 (Ceck OK)
mov_up_down_5_ver <- filter(mov_up_down_5, "COD_CLIENTE_CIFRATO is NULL")
nrow(mov_up_down_5_ver)
# 0

valdist_cl_mov <- distinct(select(mov_up_down_5, "COD_CLIENTE_CIFRATO"))
nrow(valdist_cl_mov)
# 362.023
valdist_cl_date_mov <- distinct(select(mov_up_down_5, "COD_CLIENTE_CIFRATO", "date"))
nrow(valdist_cl_date_mov)
# 384.325



################################################################################################################################################################################
#### Come ?? possibile che le attivazioni siano di pi?? delle richieste?
################################################################################################################################################################################

createOrReplaceTempView(rich_up_down_5, "rich_up_down_5")
createOrReplaceTempView(mov_up_down_5, "mov_up_down_5")

innerjoin_extid_date <- sql("select distinct t1.COD_CLIENTE_CIFRATO, t2.COD_CLIENTE_CIFRATO
                            from rich_up_down_5 t1
                            inner join mov_up_down_5 t2
                              on t1.COD_CLIENTE_CIFRATO = t2.COD_CLIENTE_CIFRATO and t1.date = t2.date
                            order by t1.COD_CLIENTE_CIFRATO")
View(head(innerjoin_extid_date,1000))
nrow(innerjoin_extid_date)
# 351.898
######################
# 353.001 clienti in rich
# 363.522 clienti in mov
######################

innerjoin_extid_date_2 <- sql("select distinct t1.COD_CLIENTE_CIFRATO, t1.date
                            from rich_up_down_5 t1
                            inner join mov_up_down_5 t2
                              on t1.COD_CLIENTE_CIFRATO = t2.COD_CLIENTE_CIFRATO and t1.date = t2.date
                            order by t1.COD_CLIENTE_CIFRATO")
View(head(innerjoin_extid_date_2,1000))
nrow(innerjoin_extid_date_2)
# 372.642
######################
# 374.384 clienti+date distinct in rich
# 384.325 clienti+date distinct in mov
######################





#chiudi sessione
sparkR.stop()
