
## Impatto ADV su vendite offline 5

## Estrazione dell'ultimo ADV visto prima della conversione


p2c_no_digital_completo_def <- read.parquet("/user/stefano.mazzucca/p2c_no_digital_def.parquet")
#p2c_no_digital_completo_def_2 <- arrange(p2c_no_digital_completo_def, "skyid")
View(head(p2c_no_digital_completo_def,100))
nrow(p2c_no_digital_completo_def)
# 31.773



# #carico campagne da considerare per impression e click
# campaign_attribute_path='hdfs:///user/silvia/campaign_attribute.csv'
# campaign_attribute= spark.read.csv(campaign_attribute_path, sep=';',header='TRUE')
# campaign_attribute.printSchema()
# campaign_attribute.show(1,False)
# campaign_attribute.createOrReplaceTempView("campaign_attribute")
# 
# campaign_attribute_unique=spark.sql("select `Campaign ID`,`Line Item ID`, Label, Search from campaign_attribute group by 1,2,3,4").persist()
# campaign_attribute_unique.createOrReplaceTempView("campaign_attribute_unique")
# campaign_attribute_unique.show(1,False)
#
# #leggo impression e filtro: cookie non zero, impression valide e visibility vera
# impression_df= spark.read.format('parquet').load(impression_path)
# impression_df.printSchema()
# impression_df = impression_df.filter(impression_df.IsRobot == 'No')
# impression_df = impression_df.filter(impression_df.CookieID != 0)
# impression_df = impression_df.where("yyyymmdd <= 20180228 and yyyymmdd >= 20170401")
# impression_df = impression_df.where("regexp_extract(unloadvars.visibility, '\"visible1\":(\\\w+)',1)='true'").persist()
# impression_df.createOrReplaceTempView("impression")
# #seleziono le impression relative alle campagne di interesse per l'analisi (caricate dal file campaign_attribute)
# impression_filter=spark.sql("select a.*,b.label, b.search from impression a join campaign_attribute_unique b 
#                                      # on a.CampaignId=b.`Campaign ID` and a.`PlacementId-ActivityId`=b.`Line Item ID`").persist()
#
# #leggo clicks e filtro: cookie non zero, impression valide e visibility vera
# click_df= spark.read.format('parquet').load(click_path)
# click_df.printSchema()
# click_df = click_df.filter(click_df.IsRobot == 'No')
# click_df = click_df.filter(click_df.CookieID != 0)
# click_df = click_df.where("yyyymmdd <= 20180228 and yyyymmdd >= 20170401")
# click_df = click_df.where("regexp_extract(unloadvars.visibility, '\"visible1\":(\\\w+)',1)='true'").persist()
# click_df.createOrReplaceTempView("click")
# #seleziono i click relativi alle campagne di interesse per l'analisi (caricate dal file campaign_attribute)
# click_filter=spark.sql("select a.*,b.label, b.search from click a join campaign_attribute_unique b 
#                                      # on a.CampaignId=b.`Campaign ID` and a.`PlacementId-ActivityId`=b.`Line Item ID`").persist()





imp <- read.parquet("/STAGE/adform/table=Impression/Impression.parquet")
View(head(imp,100))

