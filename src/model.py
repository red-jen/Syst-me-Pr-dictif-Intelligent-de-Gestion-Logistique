"""Model utilities (placeholders)"""

from pyspark.ml.classification import RandomForestClassifier


def get_rf_estimator(labelCol="label", featuresCol="features"):
    return RandomForestClassifier(labelCol=labelCol, featuresCol=featuresCol)
