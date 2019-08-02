
## clustering_corporate

## test sul modello non supervisionato

source("connection_R.R")
options(scipen = 10000)



## KMEANS

data(iris)
df <- createDataFrame(iris)
model <- spark.kmeans(df, Sepal_Length ~ Sepal_Width, 
                      #  ~ Sepal_Length + Sepal_Width,
                      k = 4, initMode = "random")
summary(model)

# fitted values on training data
fitted <- predict(model, df)
head(select(fitted, "Sepal_Length", "prediction"))

# save fitted model to input path
path <- "/user/stefano.mazzucca/clustering_corporate/kmean_iris"
write.ml(model, path)

# can also read back the saved model and print
savedModel <- read.ml(path)
summary(savedModel)


# write.ml(model, path = "...")
# 
# model <- read.ml("...")
# 
# summary_model <- summary(model)
# imp_var_model <- summary_model$featureImportances
# View(head(imp_var_model,100))

iris_m <- as.factor(iris)
library(psych)
fa_ultima  <- psych::fa(iris, nfactors = 2, rotate = "varimax",
                        scores = "tenBerge", fm = "pa") 
# target_ultimo[,-c(1,2,38:55)]
scores_ult     <- as.data.frame(fa_ultima$scores)
clust_k3       <- kmeans(scores_ult, centers = 3, iter.max = 100000) 






?fa




#chiudi sessione
sparkR.stop()
