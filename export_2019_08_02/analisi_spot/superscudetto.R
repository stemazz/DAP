
## Superscudetto: info clienti sky


source("connection_R.R")
options(scipen = 10000)



path_dizionario <- "/user/stefano.mazzucca/task_force_pdisc/dizionario.parquet"
path_scarico <- '/user/emma.cambieri/scarico_skyitdev_20180701_20190124'


scarico <- read.parquet(path_scarico)
View(head(scarico,100))

scarico_1 <- filter(scarico, scarico$post_channel == 'sport')
View(head(scarico_1,100))

scarico_2 <- filter(scarico_1, scarico_1$secondo_livello_post_prop == 'sport-fantagiochi')
scarico_3 <- filter(scarico_2, scarico_2$terzo_livello_post_prop == 'sport-superscudetto')
View(head(scarico_3,100))

date <- summarize(scarico_3, min_data = min(scarico_3$date_time_dt), max_data = max(scarico_3$date_time_dt))
View(head(date,100))
# min_data     max_data
# 2018-07-01   2019-01-24

scarico_4 <- filter(scarico_3, scarico_3$date_time_dt <= "2018-12-31")

write.parquet(scarico_4, "/user/stefano.mazzucca/scarico_superscudetto.parquet")


superscudetto <- read.parquet("/user/stefano.mazzucca/scarico_superscudetto.parquet")
View(head(superscudetto,100))
nrow(superscudetto)
# 11.266.238

# valdist_extid <- summarize(groupBy(superscudetto, superscudetto$external_id_post_evar), 
#                            count_visit = countDistinct(concat(superscudetto$post_visid_concatenated, superscudetto$visit_num)), 
#                            min_data = min(superscudetto$date_time_dt), 
#                            max_data = max(superscudetto$date_time_dt))
# View(head(valdist_extid,100))
# nrow(valdist_extid)
# # 109.559


dizionario <- read.parquet(path_dizionario)
View(head(dizionario,100))
nrow(dizionario)
# 760.698.524


createOrReplaceTempView(superscudetto, "superscudetto")
createOrReplaceTempView(dizionario, "dizionario")

diz_super <- sql("select distinct t2.external_id_post_evar as skyid, t1.*
                 from superscudetto t1
                 left join dizionario t2
                 on t1.post_visid_concatenated = t2.post_visid_concatenated")

write.parquet(diz_super, "/user/stefano.mazzucca/diz_superscudetto.parquet")



superscudetto <- read.parquet("/user/stefano.mazzucca/diz_superscudetto.parquet")
View(head(superscudetto,100))
nrow(superscudetto)
# 11.708.512


valdist_extid <- summarize(groupBy(superscudetto, superscudetto$skyid), 
                           count_visit = countDistinct(concat(superscudetto$post_visid_concatenated, superscudetto$visit_num)), 
                           count_d_cookie_associati = countDistinct(superscudetto$post_visid_concatenated),
                           min_data = min(superscudetto$date_time_dt), 
                           max_data = max(superscudetto$date_time_dt))
View(head(valdist_extid,100))
nrow(valdist_extid)
# 113.585

valdist_cookie <- summarize(groupBy(superscudetto, superscudetto$post_visid_concatenated), 
                           count_visit = countDistinct(concat(superscudetto$post_visid_concatenated, superscudetto$visit_num)), 
                           min_data = min(superscudetto$date_time_dt), 
                           max_data = max(superscudetto$date_time_dt))
View(head(valdist_cookie,100))
nrow(valdist_cookie)
# 624.450


