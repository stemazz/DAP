#test_sample_DAMA

sample <- read.csv("sample100.csv", header = TRUE, sep = ",")
#View(head(sample))

sample2 <- createDataFrame(sample)


triplette <- summarize(groupBy(sample2, sample2$Fascia_eta, sample2$Sesso, sample2$Sezione_2001), new_col=avg(sample2$progressivo_famiglia))
#, new_col=distinct(select((sample2$Presenza.di.prestiti))))
View(triplette)



