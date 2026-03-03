import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *


##### Ingestion of Customer Data ####

customer_schema = StructType([
    StructField('customer_id', StringType(), True),
    StructField('name', StringType(), True),
    StructField('email', StringType(), True),
    StructField('phone', StringType(), True),
    StructField('city', StringType(), True),
    StructField('join_date', TimestampType(), True)
])



@dlt.table(
    name = 'landing_customers_incremental',
    comment = 'Raw data for customers'
)


def landing_customers_incremental():
  return (spark.readStream.format('cloudFiles')
          .option('cloudFiles.format', 'csv')
          .option('cloudFiles.includeExistingFiles', 'true')
          .option('header', 'true')
          .schema(customer_schema)
          .load('/Volumes/restaurant/restaurant_ops/restaurant_lakehouse/customers/'))
 

##### Ingestion of Orders Data ####

order_schema = StructType([
    StructField('order_id', StringType(), True),
    StructField('timestamp', TimestampType(), True),
    StructField('restaurant_id', StringType(), True),
    StructField('customer_id', StringType(), True),
    StructField('order_type', StringType(), True),
    StructField('items', StringType(), True),
    StructField('total_amount', DoubleType(), True),
    StructField('payment_method', StringType(), True),
    StructField('order_status', StringType(), True),
    StructField('created_at', TimestampType(), True)
])


@dlt.table(
    name = 'landing_orders_incremental',
    comment = 'Raw data for orders'
)


def landing_orders_incremental():
  return (spark.readStream.format('cloudFiles')
          .option('cloudFiles.format', 'csv')
          .option('cloudFiles.includeExistingFiles', 'true')
          .option('header', 'true')
          .option('multiLine', 'true')
          .option('escape', '"')
          .option('quote', '"')
          .schema(order_schema)
          .load('/Volumes/restaurant/restaurant_ops/restaurant_lakehouse/orders/'))
  
 
##### Ingestion of Menu Items Data ####

menu_schema = StructType([
    StructField('restaurant_id', StringType(), True),
    StructField('item_id', StringType(), True),
    StructField('name', StringType(), True),
    StructField('category', StringType(), True),
    StructField('price', DoubleType(), True),
    StructField('ingredients', StringType(), True),
    StructField('is_vegetarian', BooleanType(), True),
    StructField('spice_level', StringType(), True)
])


@dlt.table(
    name = 'landing_menu_items_incremental',
    comment = 'Raw data for menu'
)


def landing_menu_items_incremental():
    return (spark.readStream.format('cloudFiles')
            .option('cloudFiles.format', 'csv')
            .option('cloudFiles.includeExistingFiles', 'true')
            .option('header', 'true')
            .schema(menu_schema)
            .load('/Volumes/restaurant/restaurant_ops/restaurant_lakehouse/menu_items/'))
    

##### Ingestion of Restaurant Data ####

restaurant_schema = StructType([
    StructField('restaurant_id', StringType(), True),
    StructField('name', StringType(), True),
    StructField('city', StringType(), True),
    StructField('country', StringType(), True),
    StructField('address', StringType(), True),
    StructField('opening_date', DateType(), True),
    StructField('phone', StringType(), True)
])


@dlt.table(
    name = 'landing_restaurants_incremental',
    comment = 'Raw data for restaurants'
)


def landing_restaurants_incremental():
    return (spark.readStream.format('cloudFiles')
            .option('cloudFiles.format', 'csv')
            .option('cloudFiles.includeExistingFiles', 'true')
            .option('header', 'true')
            .schema(restaurant_schema)
            .load('/Volumes/restaurant/restaurant_ops/restaurant_lakehouse/restaurants/'))
    

##### Ingestion of Review Data ####

inventory_schema = StructType([
    StructField('review_id', StringType(), True),
    StructField('order_id', StringType(), True),
    StructField('customer_id', StringType(), True),
    StructField('restaurant_id', StringType(), True),
    StructField('review_text', StringType(), True),
    StructField('rating', IntegerType(), True),
    StructField('review_timestamp', TimestampType(), True)
])


@dlt.table(
    name = 'landing_review_incremental',
    comment = 'Raw data for review'
)


def landing_review_incremental():
    return (spark.readStream.format('cloudFiles')
            .option('cloudFiles.format', 'csv')
            .option('cloudFiles.includeExistingFiles', 'true')
            .option('header', 'true')
            .schema(inventory_schema)
            .load('/Volumes/restaurant/restaurant_ops/restaurant_lakehouse/reviews/'))
