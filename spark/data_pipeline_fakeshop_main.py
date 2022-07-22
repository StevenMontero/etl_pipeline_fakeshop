import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import monotonically_increasing_id, row_number
from pyspark.sql import Window
from random import randint


def create_purchase(row):
    quantity = randint (1,10)
    return (row[0],row[1],row[2],row[3],quantity,quantity*row[3])


if __name__ == "__main__":
    spark = SparkSession.builder.getOrCreate()
    df_users = spark.read.json(sys.argv[1])
    df_product = spark.read.json(sys.argv[2])

    df_users = df_users.selectExpr("name.first as FirstName","name.last as LastName",
                                                "location.country as Country","email as Email","gender as Gender")\
                                                    .withColumn("row_idx", row_number().over(Window.orderBy(monotonically_increasing_id())))
    
    df_product = df_product.selectExpr("id as IdProduct","title as ProductName",
                                                "category.name as Category","price as Price")
    
    df_product = df_product.rdd.map(create_purchase) \
        .toDF(["IdProduct","ProductName","Category","Price","Quantity","Total"])\
            .withColumn("row_idx", row_number().over(Window.orderBy(monotonically_increasing_id())))

    df_orders = df_users.join(df_product, df_users.row_idx == df_product.row_idx).drop("row_idx")

    df_orders.write.format('bigquery') \
        .option('table', 'fake_shop.orders') \
        .mode('append') \
        .option("temporaryGcsBucket","fake_shop_bucket") \
        .save()



