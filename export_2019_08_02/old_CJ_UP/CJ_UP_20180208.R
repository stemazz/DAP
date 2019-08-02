
#CJ_UP

#apri sessione
source("connection_R.R")
options(scipen = 1000)


base_up <- read.parquet("/user/valentina/CJ_UP_base_filtrata.parquet")

View(head(base_up, 100))
nrow(base_up)
# 3.779.807


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


## JOIN tra base_up e livello di digitalizzazione

createOrReplaceTempView(base_up,"base_up")
createOrReplaceTempView(base_liv_digital,"base_liv_digital")

base_up_score_digital <- sql("select t1.*, t2.digitalizzazione_bin
                             from base_up t1
                             left join base_liv_digital t2
                              on t1.SKY_ID = t2.skyid")
View(head(base_up_score_digital, 100))
nrow(base_up_score_digital)
# 3.779.807

## Verifica livello digitalizzazione di tutti gli utenti

ver <- filter(base_up_score_digital, "digitalizzazione_bin is NULL")
nrow(ver)
# 0 (ceck OK!!)



## Distribuzione flg_up_3m

createOrReplaceTempView(base_up_score_digital,"base_up_score_digital")
distr_base_up_flg <- sql("select flg_up_3m, count(*)
                            from base_up_score_digital
                            group by flg_up_3m")
View(head(distr_base_up_flg,100))

#oppure (con summarize)

distr_base_up_flg_2 <-summarize(groupBy(base_up_score_digital, base_up_score_digital$flg_up_3m), COUNT = count(base_up_score_digital$flg_up_3m))
View(head(distr_base_up_flg_2))

##Distribuzione flg_up_3m + digitalizzazione_bin

distr_base_up_flg_dig <- sql("select flg_up_3m, digitalizzazione_bin, count(*)
                            from base_up_score_digital
                            group by flg_up_3m, digitalizzazione_bin")
View(head(distr_base_up_flg_dig,100))

#oppure (con summarize)

distr_base_up_flg_dig_2 <- summarize(groupBy(base_up_score_digital, base_up_score_digital$flg_up_3m, base_up_score_digital$digitalizzazione_bin), COUNT = count(base_up_score_digital$SKY_ID))
View(head(distr_base_up_flg_dig_2,100))



## Campionamento stratificato della cb

base_up_0 <- filter(base_up_score_digital, "flg_up_3m = 0")
nrow(base_up_0)
# 3.741.803

base_up_1 <- filter(base_up_score_digital, "flg_up_3m = 1")
nrow(base_up_1)
# 38.004




fractions <- list(
  "0" <- 0.0,
  "1" <- 0.0,
  "2" <- 0.0, 
  "3" <- 0.01, 
  "4" <- 0.02, 
  "5" <- 0.04, 
  "6" <- 0.07, 
  "7" <- 0.13, 
  "8" <- 0.17, 
  "9" <- 0.19, 
  "10" <- 0.37
)
fractions2 <- setNames(as.list(c(0.0,0.0,0.0,0.01,0.02,0.04,0.07,0.13,0.17,0.19,0.37)),c(0,1,2,3,4,5,6,7,8,9,10))

fractions3 <- list(
  "0" = c(0.0), 
  "1" = c(0.0),
  "2" = c(0.0),
  "3" = c(0.0),
  "4" = c(0.01),
  "5" = c(0.04),
  "6" = c(0.07),
  "7" = c(0.13),
  "8" = c(0.17),
  "9" = c(0.19),
  "10" = c(0.37)
)


prova_sample <- sampleBy(base_up_0, "digitalizzazione_bin", fractions3, 36)
View(head(prova_sample))
nrow(prova_sample)


prova <- sample(base_up_0, FALSE, 0.2)
nrow(prova)
# 748.319




#chiudi sessione
sparkR.stop()
