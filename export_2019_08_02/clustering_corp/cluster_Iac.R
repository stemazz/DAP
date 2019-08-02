library(dplyr)
library(sparklyr)
devtools::install_github("hoxo-m/SparkRext")


# #USER
# SPARK_USER="stefano.mazzucca"
# 
# # connection #
# Sys.setenv(SPARK_HOME="/home/rstudio/spark/spark-2.1.0-bin-hadoop2.7/")
# Sys.setenv(SPARK_HOME_VERSION="2.1.0")
# Sys.setenv(SPARK_HDFS_CONFIG_URL="/usr/lib/rstudio-server/hadoop/")
# Sys.setenv(SPARK_USER=SPARK_USER)
# 
# # Conf obbligatorie
# conf <- list()
# conf$spark.mesos.executor.docker.image <- "mesosphere/spark:1.0.9-2.1.0-1-hadoop-2.7"
# conf$spark.mesos.executor.home <- "/opt/spark/dist"
# 
# # Puoi aggiungere le opzioni aggiuntive ad es.:
# 
# conf$spark.executor.cores <-"2"
# conf$spark.executor.memory <- "15G"
# conf$spark.executor.heartbeat <- "60s"
# conf$spark.cores.max <- "10"
# conf$spark.driver.maxResultSize <- "40G"
# conf$spark.driver.memory <- "6G"
# conf$spark.driver.cores <- "2"
# 
# library(sparklyr)
# sc <- spark_connect(master = 'mesos://leader.mesos:5050', config = conf, app_name = 'sparklyr_SM')



sc <- sparkR.session(master = 'mesos://leader.mesos:5050', sparkConfig = conf, appName = 'SM_Sparklyr')



Sys.setenv(SPARK_HOME="/home/rstudio/spark/spark-2.1.0-bin-hadoop2.7/")
Sys.setenv(SPARK_HOME_VERSION="2.1.0")
Sys.setenv(SPARK_HDFS_CONFIG_URL="/usr/lib/rstudio-server/hadoop/")
Sys.setenv(SPARK_USER=SPARK_USER)
conf <- spark_config()

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

# sc = spark_connect(master='mesos://leader.mesos:5050', config=conf, version="2.1.0")
# data_feed_local = spark_read_table(sc = sc,
#                                  name = "data_feed",
#                                  path = "/RAW/adobe/reportsuite=skyappanywhere/table=hitdata")

skygo = read.df("/RAW/adobe/reportsuite=skyitdev/table=hitdata", "parquet") ## funziona 

data_feed_local$merged_visitor_ids <- concat(data_feed_local$post_visid_high,data_feed_local$post_visid_low)

skygo_tbl = 
  select(data_feed_local,c("merged_visitor_ids","visit_num","visit_page_num","browser","post_event_list","geo_country",
                           "geo_region","first_hit_time_gmt", "hit_time_gmt","mobile_id","post_page_event","post_pagename",
                           "username","post_evar6", "post_evar19",
                           "daily_visitor","weekly_visitor","monthly_visitor","yearly_visitor"))
 

clienti <-  select(data_feed_tbl, c("merged_visitor_ids", "post_evar50"))
clienti <- agg(groupBy(clienti, clienti$merged_visitor_ids, clienti$post_evar50), "post_evar50", "merged_visitor_ids")

clienti <- unique(clienti)
clienti <- dropna(clienti, cols="post_evar50")
clienti <- filter(clienti, clienti$merged_visitor_ids!='NANA')
clienti$post_evar50 <- lit(1)

fillna(data_feed_tbl, 0)
data_feed_tbl <- withColumn(data_feed_tbl, "daily_visitor", cast(data_feed_tbl$daily_visitor, "double"))
data_feed_tbl <- withColumn(data_feed_tbl, "weekly_visitor", cast(data_feed_tbl$weekly_visitor, "double"))
data_feed_tbl <- withColumn(data_feed_tbl, "monthly_visitor", cast(data_feed_tbl$monthly_visitor, "double"))
data_feed_tbl <- withColumn(data_feed_tbl, "yearly_visitor", cast(data_feed_tbl$yearly_visitor, "double"))
data_feed_tbl <- withColumn(data_feed_tbl, "visit_num", cast(data_feed_tbl$visit_num, "double"))
data_feed_tbl <- withColumn(data_feed_tbl, "visit_page_num", cast(data_feed_tbl$visit_page_num, "double"))


frequency <- select(data_feed_tbl, c("merged_visitor_ids", "daily_visitor", "weekly_visitor", "monthly_visitor", "yearly_visitor"))

createOrReplaceTempView(frequency, "frequency")
frequency3 <- sql("SELECT merged_visitor_ids, max(daily_visitor) as daily_visitor, max(weekly_visitor) as weekly_visitor, max(monthly_visitor) as monthly_visitor, max(yearly_visitor) as yearly_visitor FROM frequency GROUP BY merged_visitor_ids")


basic_counts <- summarize(groupBy(data_feed_tbl, data_feed_tbl$merged_visitor_ids), hit_count = n(data_feed_tbl$merged_visitor_ids), lifetime_visits = max(data_feed_tbl$visit_num),
                            visits = n_distinct(data_feed_tbl$visit_num))

basic_counts_desktop <- summarize(groupBy(filter(data_feed_tbl, data_feed_tbl$post_evar22=="desktop site"), data_feed_tbl$merged_visitor_ids),
                          visits_to_desktop = n_distinct(data_feed_tbl$visit_num))

basic_counts_mobile <- summarize(groupBy(filter(data_feed_tbl, data_feed_tbl$post_evar22=="mobile site"), data_feed_tbl$merged_visitor_ids),
                                  visits_to_mobile = n_distinct(data_feed_tbl$visit_num))


page_view_counts_tot = data_feed_tbl %>%
  group_by(visitor_id) %>%
  filter(post_page_event == 0) %>%
  summarize(
    page_views_to_tot = n()
  )

page_view_counts_desktop = data_feed_tbl %>%
  group_by(visitor_id) %>%
  filter(post_evar22=="desktop site" & post_page_event == 0) %>%
  summarize(
    page_views_to_desktop = n()
  )

page_view_counts_mobile = data_feed_tbl %>%
  group_by(visitor_id) %>%
  filter(post_evar22=="mobile site" & post_page_event == 0) %>%
  summarize(
    page_views_to_mobile = n()
  )


visit_counts_sport = data_feed_tbl %>%
  group_by(visitor_id) %>%
  filter(grepl("sport", post_evar13)) %>%
  summarize(
    visits_to_sport = n_distinct(visit_num)
  )


visit_counts_tg24 = data_feed_tbl %>%
  group_by(visitor_id) %>%
  filter(grepl("tg24", post_evar13)) %>%
  summarize(
    visits_to_tg24 = n_distinct(visit_num)
  )


visit_counts_video = data_feed_tbl %>%
  group_by(visitor_id) %>%
  filter(grepl("^video", post_evar13)) %>%
  summarize(
    visits_to_video = n_distinct(visit_num)
  )


visit_counts_skygo = data_feed_tbl %>%
  group_by(visitor_id) %>%
  filter(grepl("skygo", post_evar13)) %>%
  summarize(
    visits_to_skygo = n_distinct(visit_num)
  )

visit_counts_meteo = data_feed_tbl %>%
  group_by(visitor_id) %>%
  filter(grepl("Meteo", post_evar13)) %>%
  summarize(
    visits_to_meteo = n_distinct(visit_num)
  )

visit_counts_oroscopo = data_feed_tbl %>%
  group_by(visitor_id) %>%
  filter(grepl("Oroscopo", post_evar13)) %>%
  summarize(
    visits_to_oroscopo = n_distinct(visit_num)
  )

visit_counts_guidatv = data_feed_tbl %>%
  group_by(visitor_id) %>%
  filter(grepl("guidatv", post_evar13)) %>%
  summarize(
    visits_to_guidatv = n_distinct(visit_num)
  )

visit_counts_xfactor = data_feed_tbl %>%
  group_by(visitor_id) %>%
  filter(grepl("xfactor", post_evar13)) %>%
  summarize(
    visits_to_xfactor = n_distinct(visit_num)
  )

visit_counts_masterchef = data_feed_tbl %>%
  group_by(visitor_id) %>%
  filter(grepl("masterchef", post_evar13)) %>%
  summarize(
    visits_to_masterchef = n_distinct(visit_num)
  )

page_view_counts_sport = data_feed_tbl %>%
  group_by(visitor_id) %>%
  filter(grepl("sport", post_evar13) & post_page_event == 0) %>%
  summarize(
    page_views_to_sport = n()
  )

page_view_counts_tg24 = data_feed_tbl %>%
  group_by(visitor_id) %>%
  filter(grepl("tg24", post_evar13) & post_page_event == 0) %>%
  summarize(
    page_views_to_tg24 = n()
  )

page_view_counts_video = data_feed_tbl %>%
  group_by(visitor_id) %>%
  filter(grepl("^video", post_evar13) & post_page_event == 0) %>%
  summarize(
    page_views_to_video = n()
  )

page_view_counts_skygo = data_feed_tbl %>%
  group_by(visitor_id) %>%
  filter(grepl("skygo", post_evar13) & post_page_event == 0) %>%
  summarize(
    page_views_to_skygo = n()
  )


page_view_counts_meteo = data_feed_tbl %>%
  group_by(visitor_id) %>%
  filter(grepl("Meteo", post_evar13)) %>%
  summarize(
    page_views_to_meteo = n()
  )

page_view_counts_oroscopo = data_feed_tbl %>%
  group_by(visitor_id) %>%
  filter(grepl("Oroscopo", post_evar13)) %>%
  summarize(
    page_views_to_oroscopo = n()
  )

page_view_counts_guidatv = data_feed_tbl %>%
  group_by(visitor_id) %>%
  filter(grepl("guidatv", post_evar13)) %>%
  summarize(
    page_views_to_guidatv = n()
  )

page_view_counts_xfactor = data_feed_tbl %>%
  group_by(visitor_id) %>%
  filter(grepl("xfactor", post_evar13)) %>%
  summarize(
    page_views_to_xfactor = n()
  )

page_view_counts_masterchef = data_feed_tbl %>%
  group_by(visitor_id) %>%
  filter(grepl("masterchef", post_evar13)) %>%
  summarize(
    page_views_to_masterchef = n()
  )

data_feed_tbl$hit_time_gmt <- as.POSIXct(data_feed_tbl$hit_time_gmt, 
                                         origin = '1970-01-01', tz = 'GMT')

data_feed_tbl$mese <- as.data.frame(format(as.Date(data_feed_tbl$hit_time_gmt, format = '%Y%m%d'), "%Y-%m"))

date_counts = data_feed_tbl %>%
  group_by(visitor_id) %>%
  summarize(
    days_visited = n_distinct(as.Date(hit_time_gmt, format = '%Y%m%d')),
    months_visited = n_distinct(as.character(format(as.Date(hit_time_gmt, format = '%Y%m%d'), "%Y-%m")))
  )


visitor_rollup <- list(basic_counts, frequency3) %>%
  Reduce(function(...) merge(..., all=TRUE, by="merged_visitor_ids"), .)

visitor_rollup$merged_visitor_ids_y <- NULL

visitor_rollup <- list(visitor_rollup, clienti) %>%
  Reduce(function(...) merge(..., all=TRUE, by="merged_visitor_ids"), .)




visitor_rollup = list(basic_counts, frequency, clienti, page_view_counts_tot, page_view_counts_desktop, page_view_counts_mobile,
                      visit_counts_skygo, visit_counts_sport, visit_counts_tg24, visit_counts_video, visit_counts_meteo, visit_counts_oroscopo, visit_counts_guidatv, visit_counts_masterchef,
                      visit_counts_xfactor,
                      page_view_counts_skygo, page_view_counts_sport, page_view_counts_tg24, page_view_counts_video, page_view_counts_meteo, page_view_counts_oroscopo, page_view_counts_guidatv, page_view_counts_masterchef, page_view_counts_xfactor,
                      date_counts) %>%
  Reduce(function(...) merge(..., all=TRUE, by="visitor_id"), .)

visitor_rollup[is.na(visitor_rollup)] <- 0
visitor_rollup <- subset(visitor_rollup, visitor_rollup$visitor_id!="NANA")
visitor_rollup$visits_to_skygo <- as.numeric(visitor_rollup$visits_to_skygo)
visitor_rollup$page_views_to_skygo <- as.numeric(visitor_rollup$page_views_to_skygo)

sample_size = 20000
sample_visitors = visitor_rollup %>%
  top_n(sample_size, wt=visitor_id) %>%
  collect()

clean_sample = sample_visitors %>%
  filter(hit_count > 1) %>%
  select(-visitor_id)

clean_sample$lifetime_visits <- as.numeric(clean_sample$lifetime_visits)
clean_sample[is.na(clean_sample)] <- 0

# Principle Component Analysis
components = prcomp(clean_sample)
rotation_matrix = components$rotation
percents = components$sdev^2/sum(components$sdev^2)

# View the percentages to pick the number of components to use
# then apply the rotation matrix to the sample dataset
percents
pseudo_vars = 3
pseudo_observations = as.data.frame(as.matrix(clean_sample) %*% 
                                      as.matrix(rotation_matrix[,1:pseudo_vars]))

library(mclust)
cluster_count = 4
cluster_model = Mclust(pseudo_observations, G=1:cluster_count)

table(cluster_model$classification) / length(cluster_model$classification)

# Prepping to apply cluster model to entire dataset
visitor_rollup$lifetime_visits <- as.numeric(visitor_rollup$lifetime_visits)
clean_rollup = visitor_rollup %>%
  select(-visitor_id)

# Transform total dataset into principle components
transformed_rollup = as.data.frame(as.matrix(clean_rollup) %*% 
                                     as.matrix(rotation_matrix[,1:pseudo_vars]))

# Apply cluster model to total dataset
model_output = predict(cluster_model, newdata=transformed_rollup)
cluster_assignments = as.numeric(as.character(model_output$classification))

visitor_rollup = visitor_rollup %>%
  mutate(cluster = cluster_assignments)


summary_table = visitor_rollup %>%
  group_by(cluster) %>%
  summarize(
    population = n(),
    median_hit_count = median(hit_count),
    median_visits = median(visits),
    median_days_visited = median(days_visited),
    percent_of_population = n() / dim(visitor_rollup)[1],
    percent_of_visits = sum(visits) / sum(visitor_rollup$visits),
    percent_of_visits_desktop = sum(visits_to_desktop) / sum(visits),
    percent_of_visits_mobile = sum(visits_to_mobile) / sum(visits),
    percent_of_pageviews_sport = sum(page_views_to_sport) / sum(visitor_rollup$page_views_to_sport),
    percent_of_pageviews_sky_go = sum(page_views_to_skygo) / sum(visitor_rollup$page_views_to_skygo),
    percent_of_pageviews_sky_tg24 = sum(page_views_to_tg24) / sum(visitor_rollup$page_views_to_tg24),
    percent_of_pageviews_sky_video = sum(page_views_to_video) / sum(visitor_rollup$page_views_to_video),
    percent_of_pageviews_sky_meteo = sum(page_views_to_meteo) / sum(visitor_rollup$page_views_to_meteo),
    percent_of_pageviews_sky_oroscopo = sum(page_views_to_oroscopo) / sum(visitor_rollup$page_views_to_oroscopo),
    percent_of_pageviews_sky_guidatv = sum(page_views_to_guidatv) / sum(visitor_rollup$page_views_to_guidatv),
    percent_of_pageviews_sky_masterchef = sum(page_views_to_masterchef) / sum(visitor_rollup$page_views_to_masterchef),
    percent_of_pageviews_sky_xfactor = sum(page_views_to_xfactor) / sum(visitor_rollup$page_views_to_xfactor),
    percent_of_sport_visits = sum(visits_to_sport) / sum(visitor_rollup$visits_to_sport),
    percent_of_skygo_visits = sum(visits_to_skygo) / sum(visitor_rollup$visits_to_skygo),
    percent_of_tg24_visits = sum(visits_to_tg24) / sum(visitor_rollup$visits_to_tg24),
    percent_of_video_visits = sum(visits_to_video) / sum(visitor_rollup$visits_to_video),
    percent_of_meteo_visits = sum(visits_to_meteo) / sum(visitor_rollup$visits_to_meteo),
    percent_of_oroscopo_visits = sum(visits_to_oroscopo) / sum(visitor_rollup$visits_to_oroscopo),
    percent_of_guidatv_visits = sum(visits_to_guidatv) / sum(visitor_rollup$visits_to_guidatv),
    percent_of_masterchef_visits = sum(visits_to_masterchef) / sum(visitor_rollup$visits_to_masterchef),
    percent_of_xfactor_visits = sum(visits_to_xfactor) / sum(visitor_rollup$visits_to_xfactor),
    percent_of_daily_visitors = sum(daily_visitor) / sum(visitor_rollup$daily_visitor),
    percent_of_weekly_visitors = sum(weekly_visitor) / sum(visitor_rollup$weekly_visitor),
    percent_of_monthly_visitors = sum(monthly_visitor) / sum(visitor_rollup$monthly_visitor),
    percent_of_yearly_visitors = sum(yearly_visitor) / sum(visitor_rollup$yearly_visitor),
    pv_vv = sum(page_views_to_tot) / sum(visits)
  )


trasposta <- t(round(summary_table, 2))
colnames(trasposta) <- c("Cluster_1", "Cluster_2", "Cluster_3", "Cluster_4")
options(scipen = 999)

library(cluster)
packageurl <- "https://cran.rstudio.com/src/contrib/ggrepel_0.7.0.tar.gz"
install_github("cran/ggrepel")
install_github("cran/ggsci")
install_github("cran/ggpubr")
install_github("cran/factoextra")
install_github("cran/dplyr")

library(factoextra)
df <- scale(USArrests)
pam <- pam(visitor_rollup[1:10536,2:34], 4, metric = "euclidean", stand = FALSE)


optim <- fviz_nbclust(sample_n(visitor_rollup[,2:34], 10000), FUNcluster = cluster::pam, k.max=8, method = "silhouette")+
  theme_classic()

pam.res <- pam(visitor_rollup[1:10536,2:34], 3)
print(pam.res)

fviz_cluster(pam.res, 
             palette = c("#00AFBB", "#FC4E07"), # color palette
             ellipse.type = "t", # Concentration ellipse
             repel = TRUE, # Avoid label overplotting (slow)
             ggtheme = theme_classic()
)

fviz_mclust(cluster_model, "BIC", palette = "jco") 


fviz_mclust(cluster_model, "classification", geom = "point", 
            pointsize = 1.5, palette = "jco") + ylim(c(-5, 1)) + xlim(c(-5, 1))

fviz_mclust(cluster_model, "uncertainty", palette = "jco")
