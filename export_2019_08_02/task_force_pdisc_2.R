
## Task Force PDISC (2) ##

source("connection_R.R")
options(scipen = 10000)

path_kpi_sito <- "/user/stefano.mazzucca/task_force_pdisc/kpi_sito.parquet"
path_kpi_app <- "/user/stefano.mazzucca/task_force_pdisc/kpi_app.parquet"
path_click_adform <- "/user/stefano.mazzucca/task_force_pdisc/click.parquet"

path_kpi_sito_finale <- "/user/stefano.mazzucca/task_force_pdisc/kpi_sito_finale.parquet"
path_kpi_app_finale <- "/user/stefano.mazzucca/task_force_pdisc/kpi_app_finale.parquet"
path_click_adform_finale <- "/user/stefano.mazzucca/task_force_pdisc/click_finale.parquet"

path_kpi_finale <- "/user/stefano.mazzucca/task_force_pdisc/kpi_finale.parquet"





kpi_sito <- read.parquet(path_kpi_sito)
View(head(kpi_sito,100))
nrow(kpi_sito)
# 42.023.596

kpi_app <- read.parquet(path_kpi_app)
View(head(kpi_app,100))
nrow(kpi_app)
# 24.687.104





click_nav <- read.parquet(path_click_adform)
View(head(click_nav,100))
nrow(click_nav)
# 16.435.283
# 18.194.449


click_nav_with_domain <- filter(click_nav, isNotNull(click_nav$PublisherDomain))
View(head(click_nav_with_domain,100))
nrow(click_nav_with_domain)
# 12.410.173
# 13.427.594

gb_domain <- summarize(groupBy(click_nav_with_domain, click_nav_with_domain$PublisherDomain), count = count(click_nav_with_domain$skyid))
gb_domain <- arrange(gb_domain, desc(gb_domain$count))
View(head(gb_domain,100))


## cookie positivi ##
click_nav_coo_positivi <- filter(click_nav, "CookieID NOT LIKE '-%'")
View(head(click_nav_coo_positivi,100))
nrow(click_nav_coo_positivi)
# 1.517.298

click_nav_coo_with_domain <- filter(click_nav_coo_positivi, isNotNull(click_nav_coo_positivi$PublisherDomain))
View(head(click_nav_coo_with_domain,100))
nrow(click_nav_coo_with_domain)
# 948.742

gb_domain_coo <- summarize(groupBy(click_nav_coo_with_domain, click_nav_coo_with_domain$PublisherDomain), count = count(click_nav_coo_with_domain$skyid))
gb_domain_coo <- arrange(gb_domain_coo, desc(gb_domain_coo$count))
View(head(gb_domain_coo,100))


############################################################################################################################################################
############################################################################################################################################################


kpi_sito <- read.parquet(path_kpi_sito)
View(head(kpi_sito,100))
nrow(kpi_sito)
# 42.023.596

kpi_sito$touchpoint <- ifelse(kpi_sito$secondo_livello_post_prop == 'landing', 'landing', kpi_sito$touchpoint)
kpi_sito$touchpoint <- ifelse(kpi_sito$terzo_livello_post_prop == 'faidate home', 'wsc_home', kpi_sito$touchpoint)
kpi_sito$touchpoint <- ifelse(kpi_sito$terzo_livello_post_prop == 'faidate primafila', 'wsc_primafila', kpi_sito$touchpoint)
kpi_sito$touchpoint <- ifelse(kpi_sito$secondo_livello_post_prop == 'homepage sky', 'homepage_sito', kpi_sito$touchpoint)
kpi_sito$touchpoint <- ifelse(kpi_sito$secondo_livello_post_prop == 'shop', 'shop_soundbox', kpi_sito$touchpoint)
kpi_sito$touchpoint <- ifelse(kpi_sito$secondo_livello_post_prop == 'callmenow', 'call_me_now', kpi_sito$touchpoint)
kpi_sito$touchpoint <- ifelse(kpi_sito$secondo_livello_post_prop == 'acquista' | kpi_sito$secondo_livello_post_prop == 'aol', 
                              'acquista', kpi_sito$touchpoint)
kpi_sito$touchpoint <- ifelse(kpi_sito$secondo_livello_post_prop == 'assistenza' & isNull(kpi_sito$touchpoint), 'assistenza_altro', kpi_sito$touchpoint)

# test <- filter(kpi_sito, isNotNull(kpi_sito$post_visid_concatenated))
# View(head(test,100))
# nrow(test)
# # 40.283.147
# 
# test_1 <- filter(test, isNull(test$touchpoint))
# View(head(test_1,100))
# nrow(test_1)
# # 8.552.095
# test_2 <- summarize(groupBy(test_1, test_1$secondo_livello_post_prop), count_3_livello = countDistinct(test_1$terzo_livello_post_prop),
#                     count_records = count(test_1$post_visid_concatenated))
# View(head(test_2,100))
# # OK

kpi_sito_finale <- filter(kpi_sito, isNotNull(kpi_sito$touchpoint))
View(head(kpi_sito_finale,100))
nrow(kpi_sito_finale)
# 31.731.052

write.parquet(kpi_sito_finale, path_kpi_sito_finale)


############################################################################################################################################################
############################################################################################################################################################


kpi_app <- read.parquet(path_kpi_app)
View(head(kpi_app,100))
nrow(kpi_app)
# 24.687.104

kpi_app$touchpoint <- ifelse(kpi_app$site_section == 'fatture', 'app_dati_fatture', kpi_app$touchpoint)
kpi_app$touchpoint <- ifelse(kpi_app$site_section == 'intro', 'app_intro', kpi_app$touchpoint)
kpi_app$touchpoint <- ifelse(kpi_app$site_section == 'login', 'app_login', kpi_app$touchpoint)
kpi_app$touchpoint <- ifelse(kpi_app$site_section == 'extra', 'app_extra', kpi_app$touchpoint)
kpi_app$touchpoint <- ifelse(kpi_app$site_section == 'primafila', 'app_primafila', kpi_app$touchpoint)


# test <- filter(kpi_app, isNotNull(kpi_app$post_visid_concatenated))
# View(head(test,100))
# nrow(test)
# # 20.962.647
# 
# test_1 <- filter(test, isNull(test$touchpoint))
# View(head(test_1,100))
# nrow(test_1)
# # 294.749

kpi_app_finale <- filter(kpi_app, isNotNull(kpi_app$touchpoint))
View(head(kpi_app_finale,100))
nrow(kpi_app_finale)
# 20.667.898

write.parquet(kpi_app_finale, path_kpi_app_finale)


############################################################################################################################################################
############################################################################################################################################################

# unisco 

# click_nav_with_domain
# kpi_app_finale
# kpi_sito_finale

kpi_sito_finale <- read.parquet(path_kpi_sito_finale)
View(head(kpi_sito_finale,100))
nrow(kpi_sito_finale)
# 31.731.052

kpi_sito_finale_1 <- select(kpi_sito_finale, kpi_sito_finale$COD_CLIENTE_CIFRATO, kpi_sito_finale$date_time_ts, kpi_sito_finale$touchpoint)

kpi_app_finale <- read.parquet(path_kpi_app_finale)
View(head(kpi_app_finale,100))
nrow(kpi_app_finale)
# 20.667.898

kpi_app_finale_1 <- select(kpi_app_finale, kpi_app_finale$COD_CLIENTE_CIFRATO, kpi_app_finale$date_time_ts, kpi_app_finale$touchpoint)



kpi_sito_app <- union(kpi_sito_finale_1, kpi_app_finale_1)
kpi_sito_app <- arrange(kpi_sito_app, kpi_sito_app$COD_CLIENTE_CIFRATO, asc(kpi_sito_app$date_time_ts))
View(head(kpi_sito_app,100))
nrow(kpi_sito_app)
# 52.398.950



click_nav_with_domain <- select(click_nav_with_domain, click_nav_with_domain$skyid, 
                                click_nav_with_domain$Timestamp, click_nav_with_domain$PublisherDomain)
click_nav_with_domain <- withColumn(click_nav_with_domain, "date_time_ts", cast(click_nav_with_domain$Timestamp, "timestamp"))

# createOrReplaceTempView(kpi_sito_app, "kpi_sito_app")
# createOrReplaceTempView(click_nav_with_domain, "click_nav_with_domain")
# 
# kpi_final <- sql("select distinct t1.*, t2.date_time_dt as date_click, t2.PublisherDomain as domain_click
#                  from kpi_sito_app t1
#                  full outer join click_nav_with_domain t2
#                  on t1.COD_CLIENTE_CIFRATO = t2.skyid")

click_nav_with_domain <- withColumnRenamed(click_nav_with_domain, "skyid", "COD_CLIENTE_CIFRATO")
# click_nav_with_domain <- withColumnRenamed(click_nav_with_domain, "date_time_dt", "date_time_ts")
click_nav_with_domain <- withColumnRenamed(click_nav_with_domain, "PublisherDomain", "touchpoint")

click_nav_with_domain <- withColumn(click_nav_with_domain, "appoggio", lit("external_nav "))

click_nav_with_domain <- withColumn(click_nav_with_domain, "touchpoint_2", concat(click_nav_with_domain$appoggio, click_nav_with_domain$touchpoint))

click_nav_with_domain$touchpoint <- NULL
click_nav_with_domain <- withColumnRenamed(click_nav_with_domain, "touchpoint_2", "touchpoint")

click_nav_with_domain <- select(click_nav_with_domain, click_nav_with_domain$COD_CLIENTE_CIFRATO, 
                                click_nav_with_domain$date_time_ts, click_nav_with_domain$touchpoint)


kpi_final <- union(kpi_sito_app, click_nav_with_domain)
# View(head(kpi_final,100))
# nrow(kpi_final)
# # 64.809.123
kpi_final <- arrange(kpi_final, kpi_final$COD_CLIENTE_CIFRATO, asc(kpi_final$date_time_ts))


write.parquet(kpi_final, path_kpi_finale, mode = "overwrite")


kpi_final <- read.parquet(path_kpi_finale)
View(head(kpi_final,1000))
nrow(kpi_final)
# 64.809.123
# 65.826.544


write.df(repartition( kpi_final, 1), path = "/user/stefano.mazzucca/task_force_pdisc/kpi_final.csv", "csv", sep=";", mode = "overwrite", header=TRUE)


kpi_final <- read.df("/user/stefano.mazzucca/task_force_pdisc/kpi_final.csv", source = "csv", header = "true", delimiter = ";")
View(head(kpi_final,100))
nrow(kpi_final)
# 64.809.123
# 65.826.544



#chiudi sessione
sparkR.stop()