
## chatbot
## click al "contatta sky"


source("connection_R.R")
options(scipen = 10000)



skyitdev <- read.parquet("/STAGE/adobe/reportsuite=skyitdev")
skyit_1 <- select(skyitdev, "external_id_post_evar",
                  "post_visid_high",
                  "post_visid_low",
                  "post_visid_concatenated",
                  "visit_num",
                  "date_time",
                  "post_channel", 
                  "page_name_post_evar",
                  "page_url_post_evar",
                  "secondo_livello_post_prop",
                  "terzo_livello_post_prop",
                  "hit_source",
                  "exclude_hit",
                  "post_page_event_value",
                  "zone_id_post_evar"
)
skyit_2 <- withColumn(skyit_1, "date_time_dt", cast(skyit_1$date_time, "date"))
skyit_3 <- withColumn(skyit_2, "date_time_ts", cast(skyit_2$date_time, "timestamp"))
skyit_4 <- filter(skyit_3, "post_channel = 'corporate' or post_channel = 'Guidatv'")

# filtro temporale
skyit_5 <- filter(skyit_4, skyit_4$date_time_dt >= '2018-09-10') 
skyit_6 <- filter(skyit_5, "zone_id_post_evar LIKE 'menu_assistenza_contatta'") # %chatbot%

ext_id_count_chatbot <- summarize(groupBy(skyit_6, "external_id_post_evar", "date_time_dt"), 
                          count_visit = countDistinct(concat(skyit_6$post_visid_concatenated, skyit_6$visit_num)))

write.parquet(ext_id_count_chatbot, "/user/stefano.mazzucca/count_contatta_user_x_day.parquet")


#####################################################################################################################################################################

user_chatbot_count <- read.parquet("/user/stefano.mazzucca/count_contatta_user_x_day.parquet")
View(head(user_chatbot_count,100))
nrow(user_chatbot_count)
# 21.905 # 58.266

test <- summarize(user_chatbot_count, 
                       max_data = max(user_chatbot_count$date_time_dt), min_data = min(user_chatbot_count$date_time_dt),
                       max_count = max(user_chatbot_count$count_visit), min_count = min(user_chatbot_count$count_visit))
View(head(test,100))


user_chatbot_agg <- summarize(groupBy(user_chatbot_count, "external_id_post_evar"),
                              count_visit = sum(user_chatbot_count$count_visit),
                              min_data = min(user_chatbot_count$date_time_dt),
                              max_data = max(user_chatbot_count$date_time_dt))

user_chatbot_agg <- arrange(user_chatbot_agg, desc(user_chatbot_agg$count_visit))
View(head(user_chatbot_agg,100))
nrow(user_chatbot_agg)
# 20.475 # 52.556

createOrReplaceTempView(user_chatbot_agg, "user_chatbot_agg")
quartili <- sql("select percentile_approx(count_visit, 0.25) as q1,
                        percentile_approx(count_visit, 0.50) as q2, -- q2 = mediana
                        percentile_approx(count_visit, 0.75) as q3,
                        percentile_approx(count_visit, 1) as q4,
                        percentile_approx(count_visit, 0.99) as 99percentile
                from user_chatbot_agg 
                ")
View(head(quartili,100)) 

write.df(repartition( user_chatbot_agg, 1), path = "/user/stefano.mazzucca/count_contatta_user.csv", "csv", sep=";", mode = "overwrite", header=TRUE)





