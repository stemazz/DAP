
# Estrazione dati da Adform


#apri sessione
source("connection_R.R")
options(scipen = 1000)


adform_tp_1 <- read.parquet('hdfs:///STAGE/adform/table=Trackingpoint/Trackingpoint.parquet')

createOrReplaceTempView(adform_tp_1, "adform_tp_1")

adform_tp_2 <- sql( "SELECT regexp_extract(customvars.systemvariables, '(\"13\":\")(.+)(\")',2)  AS skyid, 
                                  cookieid, 
                                  'device-name',
                                  'os-name', 
                                  'browser-name'
                                  from adform_tp_1
                                  where(customvars.systemvariables like '%13%' and cookieid <> '0' )
                                  having (skyid <> '' and length(skyid)= 48)")


View(head(adform_tp_2,200))








#chiudi sessione
sparkR.stop()
