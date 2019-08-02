
## Lenti-veloci (6)
## Conversioni -- interessi DMP


#apri sessione
source("connection_R.R")
options(scipen = 1000)


######################################################################################################################
######################################################################################################################
# aggiungo dati DMP  x interessi
######################################################################################################################
######################################################################################################################

################### interest_attributes_2 ############################################################################
interest_attributes_2_1 <- read.parquet("/STAGE/DMP/simple/rawdata_interest_attributes_2.parquet")
View(head(interest_attributes_2_1,200))
printSchema(interest_attributes_2_1)

interest_attributes_2_2 <- filter(interest_attributes_2_1,"data_condivisione >= 20171201")
interest_attributes_2_3 <- filter(interest_attributes_2_2,"data_condivisione <= 20180531")

createOrReplaceTempView(interest_attributes_2_3,"interest_attributes_2_3")

# #verifica date
# date_inter_2 <- sql("select distinct data_condivisione,week from interest_attributes_2_3 order by data_condivisione")
# View(head(date_inter_2,500))

createOrReplaceTempView(base_clienti_chiavi,"base_clienti_chiavi")
interest_attributes_2_4 <- sql("select *
                               from base_clienti_chiavi t1 
                               inner join interest_attributes_2_3 t2
                               on t1.COD_CLIENTE_CIFRATO = t2.External_id")
nrow(interest_attributes_2_4) 


# #verifica campi
# seleziona_inter2 <- distinct(select(interest_attributes_2_4,"categoria_secondo_livello"))
# View(head(seleziona_inter2,500))

interest_attributes_2_5 <- filter(interest_attributes_2_4,"categoria_secondo_livello is not NULL")
nrow(interest_attributes_2_5) 

interest_attributes_2_6 <- select(interest_attributes_2_5,"COD_CLIENTE_CIFRATO","Cookie",
                                  "External_id","week","categoria_secondo_livello","data_condivisione",
                                  "numero_pagine_categoria","numero_pagine_totali")

createOrReplaceTempView(interest_attributes_2_6,"interest_attributes_2_6")
View(head(interest_attributes_2_6,200))

interest_attributes_2_7 <- sql("select *,
                               case when categoria_secondo_livello like '%_economiche_commerciali%' then 'attivita_economiche_commerciali' 
                               when categoria_secondo_livello like '%societ%' then 'societa'
                               when categoria_secondo_livello like '%famiglia_genitoriali%' then 'famiglia_genitorialita'
                               else categoria_secondo_livello end as categoria_LEV2
                               from interest_attributes_2_6")
View(head(interest_attributes_2_7,200))

interest_attributes_2_7$numero_pagine_categoria <- cast(interest_attributes_2_7$numero_pagine_categoria, 'integer')
interest_attributes_2_7$numero_pagine_totali <- cast(interest_attributes_2_7$numero_pagine_totali, 'integer')
nrow(interest_attributes_2_7) 

write.parquet(interest_attributes_2_7,"/user/valentina/PROG_ADVOC_scarico_Interessi_LEV2.parquet")


##### elaborazioni ##########################################################################################################################

elabora_lev2 <- read.parquet("/user/valentina/PROG_ADVOC_scarico_Interessi_LEV2.parquet")
names(elabora_lev2)
View(head(elabora_lev2,500))

createOrReplaceTempView(elabora_lev2,"elabora_lev2")

### num week in cui naviga
num_week_di_navigaz <- sql("select COD_CLIENTE_CIFRATO,
                           count(distinct week) as num_week
                           from elabora_lev2
                           group by COD_CLIENTE_CIFRATO")
nrow(num_week_di_navigaz)  

# createOrReplaceTempView(num_week_di_navigaz,"num_week_di_navigaz")
# min_max_week <- sql("select min(num_week) as min, max(num_week) as max from num_week_di_navigaz")
# View(head(min_max_week,200))  #1 / 25

### tot pagine
tot_pagine <- sql("select COD_CLIENTE_CIFRATO,
                  sum(distinct numero_pagine_totali) as sum_pagine_totali
                  from elabora_lev2
                  group by COD_CLIENTE_CIFRATO")
View(head(tot_pagine,500))
nrow(tot_pagine) 


### pagine x categoria
pagine_x_categoria_1 <- sql("select COD_CLIENTE_CIFRATO,categoria_LEV2,
                            sum(numero_pagine_categoria) as sum_pagine_xcategoria
                            from elabora_lev2
                            group by COD_CLIENTE_CIFRATO,categoria_LEV2")
names(pagine_x_categoria_1) #"COD_CLIENTE_CIFRATO"   "categoria_LEV2"        "sum_pagine_xcategoria"
View(head(pagine_x_categoria_1,100))


createOrReplaceTempView(pagine_x_categoria_1,"pagine_x_categoria_1")

pagine_x_categoria_2 <- sum(pivot(groupBy(pagine_x_categoria_1, "COD_CLIENTE_CIFRATO"), "categoria_LEV2"), "sum_pagine_xcategoria")
View(head(group_sum,200))
nrow(group_sum)  
names(pagine_x_categoria_2)

#### unisci tutto
createOrReplaceTempView(num_week_di_navigaz,"num_week_di_navigaz")  #"COD_CLIENTE_CIFRATO" "num_week"   
createOrReplaceTempView(tot_pagine,"tot_pagine") #"COD_CLIENTE_CIFRATO" "sum_pagine_totali"  
createOrReplaceTempView(pagine_x_categoria_2,"pagine_x_categoria_2")

join_1 <- sql("select distinct t1.COD_CLIENTE_CIFRATO, 
              t1.num_week,
              t2.sum_pagine_totali,
              t2.sum_pagine_totali/t1.num_week as num_pag_x_week
              from num_week_di_navigaz t1 inner join tot_pagine t2
              on t1.COD_CLIENTE_CIFRATO=t2.COD_CLIENTE_CIFRATO")
View(head(join_1,200))

join_1 <- withColumn(join_1,"round_num_pag_x_week",round(join_1$num_pag_x_week))

createOrReplaceTempView(join_1,"join_1")
join_2 <- sql("select distinct t1.*,
              case when t2.alimentazione is NULL then 0 else alimentazione end as alimentazione,
              case when t2.animali_domestici is NULL then 0 else animali_domestici end as animali_domestici,
              case when t2.arte_intrattenimento is NULL then 0 else arte_intrattenimento end as arte_intrattenimento,
              case when t2.attivita_economiche_commerciali is NULL then 0 else attivita_economiche_commerciali end as attivita_economiche_commerciali,
              case when t2.automobilismo is NULL then 0 else automobilismo end as automobilismo,
              case when t2.casa_giardinaggio is NULL then 0 else casa_giardinaggio end as casa_giardinaggio,
              case when t2.diritto_governo_politica is NULL then 0 else diritto_governo_politica end as diritto_governo_politica,
              case when t2.economia_finanza is NULL then 0 else economia_finanza end as economia_finanza,
              case when t2.famiglia_genitorialita is NULL then 0 else famiglia_genitorialita end as famiglia_genitorialita,
              case when t2.hobby is NULL then 0 else hobby end as hobby,
              case when t2.immobiliare is NULL then 0 else immobiliare end as immobiliare,
              case when t2.informazione is NULL then 0 else informazione end as informazione,
              case when t2.intenzioni is NULL then 0 else intenzioni end as intenzioni,
              case when t2.istruzione is NULL then 0 else istruzione end as istruzione,
              case when t2.lavoro is NULL then 0 else lavoro end as lavoro,
              case when t2.moda_stile is NULL then 0 else moda_stile end as moda_stile,
              case when t2.religione is NULL then 0 else religione end as religione,
              case when t2.salute_benessere is NULL then 0 else salute_benessere end as salute_benessere,
              case when t2.scienze is NULL then 0 else scienze end as scienze,
              case when t2.shopping is NULL then 0 else shopping end as shopping,
              case when t2.societa is NULL then 0 else societa end as societa,
              case when t2.sport is NULL then 0 else sport end as sport,
              case when t2.tecnologia_informatica is NULL then 0 else tecnologia_informatica end as tecnologia_informatica,
              case when t2.viaggi is NULL then 0 else viaggi end as viaggi
              from join_1 t1 inner join pagine_x_categoria_2 t2
              on t1.COD_CLIENTE_CIFRATO=t2.COD_CLIENTE_CIFRATO")

View(head(join_2,200))

names(join_2)
createOrReplaceTempView(join_2,"join_2")
join_3 <- sql("select distinct *,
              case when alimentazione>0 then round(alimentazione/num_week) else 0 end as alimentazione_xweek,
              case when animali_domestici>0 then round(animali_domestici/num_week) else 0 end as animali_domestici_xweek,
              case when arte_intrattenimento>0 then round(arte_intrattenimento/num_week) else 0 end as arte_intrattenimento_xweek,
              case when attivita_economiche_commerciali>0 then round(attivita_economiche_commerciali/num_week) else 0 end as attivita_econom_comm_xweek,
              case when automobilismo>0 then round(automobilismo/num_week) else 0 end as automobilismo_xweek,
              case when casa_giardinaggio>0 then round(casa_giardinaggio/num_week) else 0 end as casa_giardinaggio_xweek,
              case when diritto_governo_politica>0 then round(diritto_governo_politica/num_week) else 0 end as diritto_gov_politica_xweek,
              case when economia_finanza>0 then round(economia_finanza/num_week) else 0 end as economia_finanza_xweek,
              case when famiglia_genitorialita>0 then round(famiglia_genitorialita/num_week) else 0 end as famiglia_genitorialita_xweek,
              case when hobby>0 then round(hobby/num_week) else 0 end as hobby_xweek,
              case when immobiliare>0 then round(immobiliare/num_week) else 0 end as immobiliare_xweek,
              case when informazione>0 then round(informazione/num_week) else 0 end as informazione_xweek,
              case when intenzioni>0 then round(intenzioni/num_week) else 0 end as intenzioni_xweek,
              case when istruzione>0 then round(istruzione/num_week) else 0 end as istruzione_xweek,
              case when lavoro>0 then round(lavoro/num_week) else 0 end as lavoro_xweek,
              case when moda_stile>0 then round(moda_stile/num_week) else 0 end as moda_stile_xweek,
              case when religione>0 then round(religione/num_week) else 0 end as religione_xweek,
              case when salute_benessere>0 then round(salute_benessere/num_week) else 0 end as salute_benessere_xweek,
              case when scienze>0 then round(scienze/num_week) else 0 end as scienze_xweek,
              case when societa>0 then round(societa/num_week) else 0 end as societa_xweek,
              case when sport>0 then round(sport/num_week) else 0 end as sport_xweek,
              case when tecnologia_informatica>0 then round(tecnologia_informatica/num_week) else 0 end as tecnologia_informatica_xweek,
              case when viaggi>0 then round(viaggi/num_week) else 0 end as viaggi_xweek,
              case when shopping>0 then round(shopping/num_week) else 0 end as shopping_xweek
              from join_2")

View(head(join_3,100))


#calcolo la mediana per colonna ----> i valori devono essere aggiornati qui e poi nel codice sottostante (a mano!!!)
alimentazione_xweek <- sql("select percentile_approx(alimentazione_xweek, 0.75) from join_3 where alimentazione_xweek>0")
View(head(alimentazione_xweek,100)) #2

animali_domestici_xweek <- sql("select percentile_approx(animali_domestici_xweek, 0.75) from join_3 where animali_domestici_xweek>0")
View(head(animali_domestici_xweek,100)) #2

arte_intrattenimento_xweek <- sql("select percentile_approx(arte_intrattenimento_xweek, 0.75) from join_3 where arte_intrattenimento_xweek>0")
View(head(arte_intrattenimento_xweek,100))  #3

attivita_econom_comm_xweek <- sql("select percentile_approx(attivita_econom_comm_xweek, 0.75) from join_3 where attivita_econom_comm_xweek>0")
View(head(attivita_econom_comm_xweek,100)) #2

automobilismo_xweek <- sql("select percentile_approx(automobilismo_xweek, 0.75) from join_3 where automobilismo_xweek>0")
View(head(automobilismo_xweek,100)) #3

casa_giardinaggio_xweek <- sql("select percentile_approx(casa_giardinaggio_xweek, 0.75) from join_3 where casa_giardinaggio_xweek>0")
View(head(casa_giardinaggio_xweek,100)) #1

diritto_gov_politica_xweek <- sql("select percentile_approx(diritto_gov_politica_xweek, 0.75) from join_3 where diritto_gov_politica_xweek>0")
View(head(diritto_gov_politica_xweek,100)) #3

economia_finanza_xweek <- sql("select percentile_approx(economia_finanza_xweek, 0.75) from join_3 where economia_finanza_xweek>0")
View(head(economia_finanza_xweek,100)) #3

famiglia_genitorialita_xweek <- sql("select percentile_approx(famiglia_genitorialita_xweek, 0.75) from join_3 where famiglia_genitorialita_xweek>0")
View(head(famiglia_genitorialita_xweek,100)) #1

hobby_xweek <- sql("select percentile_approx(hobby_xweek, 0.75) from join_3 where hobby_xweek>0")
View(head(hobby_xweek,100)) #2

immobiliare_xweek <- sql("select percentile_approx(immobiliare_xweek, 0.75) from join_3 where immobiliare_xweek>0")
View(head(immobiliare_xweek,100)) #3

informazione_xweek <- sql("select percentile_approx(informazione_xweek, 0.75) from join_3 where informazione_xweek>0")
View(head(informazione_xweek,100)) #3

intenzioni_xweek <- sql("select percentile_approx(intenzioni_xweek, 0.75) from join_3 where intenzioni_xweek>0")
View(head(intenzioni_xweek,100)) #2

istruzione_xweek <- sql("select percentile_approx(istruzione_xweek, 0.75) from join_3 where istruzione_xweek>0")
View(head(istruzione_xweek,100))  #2

lavoro_xweek <- sql("select percentile_approx(lavoro_xweek, 0.75) from join_3 where lavoro_xweek>0")
View(head(lavoro_xweek,100))  #2

moda_stile_xweek <- sql("select percentile_approx(moda_stile_xweek, 0.75) from join_3 where moda_stile_xweek>0")
View(head(moda_stile_xweek,100))  #2

religione_xweek <- sql("select percentile_approx(religione_xweek, 0.75) from join_3 where religione_xweek>0")
View(head(religione_xweek,100)) #1

salute_benessere_xweek <- sql("select percentile_approx(salute_benessere_xweek, 0.75) from join_3 where salute_benessere_xweek>0")
View(head(salute_benessere_xweek,100))  #1

scienze_xweek <- sql("select percentile_approx(scienze_xweek, 0.75) from join_3 where scienze_xweek>0")
View(head(scienze_xweek,100))  #2

societa_xweek <- sql("select percentile_approx(societa_xweek, 0.75) from join_3 where societa_xweek>0")
View(head(societa_xweek,100)) #2

sport_xweek <- sql("select percentile_approx(sport_xweek, 0.75) from join_3 where sport_xweek>0")
View(head(sport_xweek,100))  #5

tecnologia_informatica_xweek <- sql("select percentile_approx(tecnologia_informatica_xweek, 0.75) from join_3 where tecnologia_informatica_xweek>0")
View(head(tecnologia_informatica_xweek,100))  #4

viaggi_xweek <- sql("select percentile_approx(viaggi_xweek, 0.75) from join_3 where viaggi_xweek>0")
View(head(viaggi_xweek,100)) #2

shopping_xweek <- sql("select percentile_approx(shopping_xweek, 0.75) from join_3 where shopping_xweek>0")
View(head(shopping_xweek,100)) #3

num_week <- sql("select percentile_approx(num_week, 0.30) from join_3 where num_week>0")
View(head(num_week,100))  #25%=2 ;30%=3


### calcolo le % relative x riga

names(join_3)

createOrReplaceTempView(join_3,"join_3")
join_4 <- sql("select *,
              (alimentazione+animali_domestici+arte_intrattenimento+attivita_economiche_commerciali+automobilismo+                  
              casa_giardinaggio+diritto_governo_politica+economia_finanza+famiglia_genitorialita+hobby+                          
              immobiliare+informazione+intenzioni+istruzione+lavoro+                         
              moda_stile+religione+salute_benessere+scienze+shopping+                       
              societa+sport+tecnologia_informatica+viaggi) as  tot_pag_xcategoria
              from join_3")
View(head(join_4,300))


createOrReplaceTempView(join_4,"join_4")
join_5 <- sql("select *,
              round((alimentazione /tot_pag_xcategoria)*100) as p_alimentazione,
              round((animali_domestici /tot_pag_xcategoria)*100) as p_animali_domestici ,  
              round((arte_intrattenimento /tot_pag_xcategoria)*100) as p_arte_intrattenimento ,
              round((attivita_economiche_commerciali /tot_pag_xcategoria)*100) as p_attivita_economiche_commerciali ,
              round((automobilismo /tot_pag_xcategoria)*100) as p_automobilismo ,
              round((casa_giardinaggio /tot_pag_xcategoria)*100) as p_casa_giardinaggio ,
              round((diritto_governo_politica /tot_pag_xcategoria)*100) as p_diritto_governo_politica ,
              round((economia_finanza /tot_pag_xcategoria)*100) as p_economia_finanza ,
              round((famiglia_genitorialita /tot_pag_xcategoria)*100) as p_famiglia_genitorialita ,
              round((hobby /tot_pag_xcategoria)*100) as p_hobby ,
              round((immobiliare /tot_pag_xcategoria)*100) as p_immobiliare ,
              round((informazione /tot_pag_xcategoria)*100) as p_informazione ,
              round((intenzioni /tot_pag_xcategoria)*100) as p_intenzioni ,
              round((istruzione /tot_pag_xcategoria)*100) as p_istruzione ,
              round((lavoro /tot_pag_xcategoria)*100) as p_lavoro ,
              round((moda_stile /tot_pag_xcategoria)*100) as p_moda_stile ,
              round((religione /tot_pag_xcategoria)*100) as p_religione ,
              round((salute_benessere /tot_pag_xcategoria)*100) as p_salute_benessere ,
              round((scienze /tot_pag_xcategoria)*100) as p_scienze ,
              round((shopping /tot_pag_xcategoria)*100)as p_shopping ,
              round((societa /tot_pag_xcategoria)*100) as p_societa ,
              round((sport /tot_pag_xcategoria)*100) as p_sport ,
              round((tecnologia_informatica /tot_pag_xcategoria)*100) as p_tecnologia_informatica ,
              round((viaggi /tot_pag_xcategoria)*100) as p_viaggi
              from join_4")
View(head(join_5,200))


###### calcolo kpi x interesse  #####################################################################################################################

createOrReplaceTempView(join_5,"join_5")

join_6 <- sql("select *,
              case when num_week>=3 and alimentazione_xweek>=2 and (alimentazione_xweek>=2 or p_alimentazione>=30) then 1 else 0 end as flg_int_alimentazione,
              case when num_week>=3 and animali_domestici_xweek>=2 and (animali_domestici_xweek>=2 or p_animali_domestici>=30) then 1 else 0 end as flg_int_animali_domestici,
              case when num_week>=3 and arte_intrattenimento_xweek>=2 and (arte_intrattenimento_xweek>=3 or p_arte_intrattenimento>=30) then 1 else 0 end as flg_int_arte_intrat,
              case when num_week>=3 and attivita_econom_comm_xweek>=2 and (attivita_econom_comm_xweek>=2 or p_attivita_economiche_commerciali>=30) then 1 else 0 end as flg_int_attiv_econom_comm,
              case when num_week>=3 and automobilismo_xweek>=2 and (automobilismo_xweek>=3 or p_automobilismo>=30) then 1 else 0 end as flg_int_automobilismo,
              case when num_week>=3 and casa_giardinaggio_xweek>=2 and (casa_giardinaggio_xweek>=1 or p_casa_giardinaggio>=30) then 1 else 0 end as flg_int_casa_giardinag,
              case when num_week>=3 and diritto_gov_politica_xweek>=2 and (diritto_gov_politica_xweek>=3 or p_diritto_governo_politica>=30) then 1 else 0 end as flg_int_diritto_gov_politica,
              case when num_week>=3 and economia_finanza_xweek>=2 and (economia_finanza_xweek>=3 or p_economia_finanza>=30) then 1 else 0 end as flg_int_economia_finanza,
              case when num_week>=3 and famiglia_genitorialita_xweek>=2 and (famiglia_genitorialita_xweek>=1 or p_famiglia_genitorialita>=30) then 1 else 0 end as flg_int_famiglia_genitorialita,
              case when num_week>=3 and hobby_xweek>=2 and (hobby_xweek>=2 or p_hobby>=30) then 1 else 0 end as flg_int_hobby,
              case when num_week>=3 and immobiliare_xweek>=2 and (immobiliare_xweek>=3 or p_immobiliare>=30) then 1 else 0 end as flg_int_immobiliare,
              case when num_week>=3 and informazione_xweek>=2 and (informazione_xweek>=3 or p_informazione>=30) then 1 else 0 end as flg_int_informazione,
              case when num_week>=3 and intenzioni_xweek>=2 and (intenzioni_xweek>=2 or p_intenzioni>=30) then 1 else 0 end as flg_int_intenzioni,
              case when num_week>=3 and istruzione_xweek>=2 and (istruzione_xweek>=2 or p_istruzione>=30) then 1 else 0 end as flg_int_istruzione,
              case when num_week>=3 and lavoro_xweek>=2 and (lavoro_xweek>=2 or p_lavoro>=30) then 1 else 0 end as flg_int_lavoro,
              case when num_week>=3 and moda_stile_xweek>=2 and (moda_stile_xweek>=2 or p_moda_stile>=30) then 1 else 0 end as flg_int_moda_stile,
              case when num_week>=3 and religione_xweek>=2 and (religione_xweek>=1 or p_religione>=30) then 1 else 0 end as flg_int_religione,
              case when num_week>=3 and salute_benessere_xweek>=2 and (salute_benessere_xweek>=1 or p_salute_benessere>=30) then 1 else 0 end as flg_int_salute_benessere,
              case when num_week>=3 and scienze_xweek>=2 and (scienze_xweek>=2 or p_scienze>=30) then 1 else 0 end as flg_int_scienze,
              case when num_week>=3 and societa_xweek>=2 and (societa_xweek>=2 or p_societa>=30) then 1 else 0 end as flg_int_societa,
              case when num_week>=3 and sport_xweek>=2 and (sport_xweek>=5 or p_sport>=30) then 1 else 0 end as flg_int_sport,
              case when num_week>=3 and tecnologia_informatica_xweek>=2 and (tecnologia_informatica_xweek>=4 or p_tecnologia_informatica>=30) then 1 else 0 end as flg_int_tec_informatica,
              case when num_week>=3 and shopping_xweek>=2 and (shopping_xweek>=3 or p_shopping>=30) then 1 else 0 end as flg_int_shopping,
              case when num_week>=3 and viaggi_xweek>=2 and (viaggi_xweek>=2 or p_viaggi>=30) then 1 else 0 end as flg_int_viaggi
              from join_5")

View(head(join_6,600))
nrow(join_6) # 






#chiudi sessione
sparkR.stop()
