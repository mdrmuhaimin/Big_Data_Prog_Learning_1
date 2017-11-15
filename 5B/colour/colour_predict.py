import sys
from colour_tools import colour_schema, rgb2lab_query, plot_predictions
from pyspark.sql import SparkSession, functions, types
from pyspark.ml.feature import VectorAssembler, StringIndexer, SQLTransformer
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.classification import RandomForestClassifier, MultilayerPerceptronClassifier
from pyspark.ml.linalg import Vectors



spark = SparkSession.builder.appName('colour predicter').getOrCreate()
# sc = spark.sparkContext
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+
assert spark.version >= '2.2'  # make sure we have Spark 2.2+


def main(inputs):
    #TODO: Remove this comment block
    # df = spark.createDataFrame([(0.0, Vectors.dense([0.0, 0.0])),
    #                             (1.0, Vectors.dense([0.0, 1.0])),
    #                             (1.0, Vectors.dense([1.0, 0.0])),
    #                             (0.0, Vectors.dense([1.0, 1.0]))], ["label", "features"])
    # df.show()

    data = spark.read.csv(inputs, header=True, schema=colour_schema)
    numlabels = data.distinct().count()
    lab_query = rgb2lab_query(passthrough_columns=['labelword'])

    sqlTrans = SQLTransformer(statement=lab_query)
    rgb_assembler = VectorAssembler(inputCols=["R", "G", "B"], outputCol="features")
    lab_assembler = VectorAssembler(inputCols=["lL", "lA", "lB"], outputCol="features")
    indexer = StringIndexer(inputCol="labelword", outputCol="color_index", handleInvalid='error')

    rf = RandomForestClassifier(numTrees=3, maxDepth=2, labelCol="color_index", seed=42)
    mlp = MultilayerPerceptronClassifier(labelCol="color_index", maxIter=100, layers=[3, 10, numlabels])

    models = [
        ('RGB-forest', Pipeline(stages=[rgb_assembler, indexer, rf])),
        ('LAB-forest', Pipeline(stages=[sqlTrans, lab_assembler, indexer, rf])),
        ('RGB-MLP', Pipeline(stages=[rgb_assembler, indexer, mlp])),
        ('LAB-MLP', Pipeline(stages=[sqlTrans, lab_assembler, indexer, mlp]))
    ]

    evaluator = MulticlassClassificationEvaluator(predictionCol="prediction", labelCol='color_index')


    # TODO: split data into training and testing
    train, test = data.randomSplit([0.9, 0.1])
    train = train.cache()
    test = test.cache()

    for label, pipeline in models:
        model = pipeline.fit(train)
        
        # Output a visual representation of the predictions we're
        # making: uncomment when you have a model working
        # TODO: uncomment it
        plot_predictions(model, label)

        predictions = model.transform(test)
        # predictions.select('features').show()
        # calculate a score
        score = evaluator.evaluate(predictions)
        print(label, score)


if __name__ == "__main__":
    inputs = sys.argv[1]
    #output = sys.argv[2]
    main(inputs)
