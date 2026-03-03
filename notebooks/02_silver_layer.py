import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *


##### Creation of fact_orders ####

items_schema = ArrayType(
    StructType([
        StructField('item_id', StringType(), True),
        StructField('name', StringType(), True),
        StructField('category', StringType(), True),
        StructField('quantity', IntegerType(), True),
        StructField('unit_price', DoubleType(), True),
        StructField('subtotal', DoubleType(), True),
    ])
)

@dlt.table(
    name="fact_orders",
    comment="Cleaned data for orders"
)

@dlt.expect_all_or_fail({
    "valid_order_id": "order_id IS NOT NULL",
    "valid_order_timestamp": "order_timestamp IS NOT NULL",
    "valid_restaurant_id": "restaurant_id IS NOT NULL",
    "valid_customer_id": "customer_id IS NOT NULL",
    "valid_item_count": "item_count > 0",
    "valid_total_amount": "total_amount > 0",
    "valid_payment_method": "payment_method IS NOT NULL",
    "valid_order_status": "order_status IS NOT NULL",
})

def fact_orders():
    df = (
        dlt.read_stream("landing_orders_incremental")
          .withColumnRenamed("timestamp", "order_timestamp")
          .withColumn("order_date", to_date(col("order_timestamp")))
          .withColumn("order_hour", hour(col("order_timestamp")))
          .withColumn("day_of_week", date_format(col("order_timestamp"), "EEEE"))
          .withColumn("is_weekend", col("day_of_week").isin("Saturday", "Sunday"))
          .withColumn("items_parsed", from_json(col("items"), items_schema))
          .withColumn("item_count", size(col("items_parsed")))
          .select(
              "order_id", "order_timestamp", "order_date", "order_hour", "day_of_week", "is_weekend",
              "restaurant_id", "customer_id", "order_type", "total_amount", "payment_method",
              "order_status", "item_count"
          )
    )
    return df



##### Creation of fact_order_items ####


items_schema = ArrayType(
    StructType([
        StructField("item_id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("category", StringType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("unit_price", DoubleType(), True),
        StructField("subtotal", DoubleType(), True),
    ])
)


@dlt.table(
    name="fact_order_items",
    comment="Cleaned data for order items"
)


@dlt.expect_all_or_fail({
    "valid_order_id": "order_id IS NOT NULL",
    "valid_order_timestamp": "order_timestamp IS NOT NULL",
    "valid_restaurant_id": "restaurant_id IS NOT NULL",
    "valid_customer_id": "customer_id IS NOT NULL",
    "valid_item_id": "item_id IS NOT NULL",
    "valid_quantity": "item_quantity > 0",
    "valid_unit_price": "item_unit_price > 0",
    "valid_subtotal": "item_subtotal > 0"
})


def fact_order_items():
    return (
        dlt.read_stream("landing_orders_incremental")
          .withColumnRenamed("timestamp", "order_timestamp")
          .withColumn("order_date", to_date(col("order_timestamp")))
          .withColumn("items_parsed", from_json(col("items"), items_schema))
          .withColumn("item", explode(col("items_parsed")))
          .select(
              col("order_id"),
              col("order_timestamp"),
              col("order_date"),
              col("restaurant_id"),
              col("customer_id"),
              col("item.item_id").alias("item_id"),
              col("item.name").alias("item_name"),
              col("item.category").alias("item_category"),
              col("item.quantity").alias("item_quantity"),
              col("item.unit_price").alias("item_unit_price"),
              col("item.subtotal").alias("item_subtotal"),
          )
    )



##### Creation of dim_customers ####

@dlt.table(
    name="dim_customers",
    comment="Cleaned data for customers"
)


@dlt.expect_all_or_fail({
    "valid_customer_id": "customer_id IS NOT NULL",
})


def dim_customers():
    return (
        dlt.read_stream("landing_customers_incremental")
        .withColumnRenamed("join_date", "join_timestamp")
        .withColumn("join_date", to_date(col("join_timestamp")))
        .withColumn("join_month", month(col("join_timestamp")))
        .withColumn("join_year", year(col("join_timestamp")))
        .withColumn("join_day", dayofmonth(col("join_timestamp")))
        .withColumn("join_day_of_week", date_format(col("join_timestamp"), "EEEE"))
    )



##### Creation of dim_menu_items ####

@dlt.table(
    name="dim_menu_items",
    comment="Cleaned data for menu items"
)


@dlt.expect_all_or_fail({
    "valid_item_id": "item_id IS NOT NULL",
    "valid_restaurant_id": "restaurant_id IS NOT NULL",
    "valid_price": "price > 0"
})


def dim_menu_items():
    return (
        dlt.read_stream("landing_menu_items_incremental")
    )



##### Creation of dim_restaurant ####

@dlt.table(
    name="dim_restaurant",
    comment="Cleaned data for restaurant"
)


@dlt.expect_all_or_fail({
    "valid_restaurant_id": "restaurant_id IS NOT NULL",
    "valid_opening_date": "opening_date IS NOT NULL"
})


def dim_restaurant():
    return (
        dlt.read_stream("landing_restaurants_incremental")
    )