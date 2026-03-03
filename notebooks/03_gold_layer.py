import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window


##### Creation of daily_sales_summary ####

@dlt.table(
  name = 'daily_sales_summary',
  comment = "Daily Sales Summary Tables"
)
def daily_sales_summary():
    df = (
        dlt.read("fact_orders")
        .groupBy("order_date")
        .agg(
            count_distinct("order_id").alias("total_orders"),
            round(sum("total_amount"),2).alias("total_revenue"),
            round(try_divide(sum("total_amount"), count_distinct("order_id")), 2).alias("avg_order_value"),
            count_distinct("customer_id").alias("unique_customers"),
            count_distinct("restaurant_id").alias("unique_restaurants"),
            count_distinct("order_id", when(col("order_status") == "delivered", True)).alias("orders_delivered"),
            count_distinct("order_id", when(col("order_status") == "completed", True)).alias("orders_completed"),
            count_distinct("order_id", when(col("order_type") == "dine_in", True)).alias("dine_in_orders"),
            count_distinct("order_id", when(col("order_type") == "delivery", True)).alias("delivery_orders"),
            count_distinct("order_id", when(col("order_type") == "takeaway", True)).alias("takeaway_orders"),
            )
        .select(
            col("order_date"),
            col("total_orders"),
            col("total_revenue"),
            col("avg_order_value"),
            col("unique_customers"),
            col("unique_restaurants"),
            col("orders_delivered"),
            col("orders_completed"),
            col("dine_in_orders"),
            col("delivery_orders"),
            col("takeaway_orders")
            )
        )
    return df



##### Creation of monthly_restaurant_reviews #####

@dlt.table(
  name = 'monthly_restaurant_reviews',
  comment = "Daily Restaurant Reviews Tables"
)
def monthly_restaurant_reviews():
    df_fact_reviews = dlt.read("fact_reviews")
    df_dim_restaurant = dlt.read("dim_restaurant")
    df = (
        df_fact_reviews
        .join(df_dim_restaurant, on = "restaurant_id")
        .groupBy("review_year_month", "restaurant_id", "name", "city")
        .agg(
            count_distinct("review_id").alias("total_reviews"),
            round(avg("rating"),2).alias("avg_rating"),
            count_distinct("review_id", when(col("rating") == 5, True)).alias("total_5_stars"),
            count_distinct("review_id", when(col("rating") == 4, True)).alias("total_4_stars"),
            count_distinct("review_id", when(col("rating") == 3, True)).alias("total_3_stars"),
            count_distinct("review_id", when(col("rating") == 2, True)).alias("total_2_stars"),
            count_distinct("review_id", when(col("rating") == 1, True)).alias("total_1_stars"),
            count_distinct("review_id", when(col("sentiment") == "positive", True)).alias("total_positive"),
            count_distinct("review_id", when(col("sentiment") == "negative", True)).alias("total_negative"),
            count_distinct("review_id", when(col("sentiment") == "neutral", True)).alias("total_neutral"),
            count_distinct("review_id", when(col("issue_delivery") == True, True)).alias("total_issue_delivery"),
            count_distinct("review_id", when(col("issue_food_quality") == True, True)).alias("total_issue_food_quality"),
            count_distinct("review_id", when(col("issue_pricing") == True, True)).alias("total_issue_pricing"),
            count_distinct("review_id", when(col("issue_portion_size") == True, True)).alias("total_issue_portion_size"),
            )
        .select(
            col("review_year_month"),
            col("restaurant_id"),
            col("name").alias("restaurant_name"),
            col("city").alias("restaurant_city"),
            col("total_reviews"),
            col("avg_rating"),
            col("total_5_stars"),
            col("total_4_stars"),
            col("total_3_stars"),
            col("total_2_stars"),
            col("total_1_stars"),
            col("total_positive"),
            col("total_negative"),
            col("total_neutral"),
            col("total_issue_delivery"),
            col("total_issue_food_quality"),
            col("total_issue_pricing"),
            col("total_issue_portion_size")
            )
    )
    return df


#### Creation of customer_360 ####

@dlt.table(
  name = 'customer_360',
  comment = "Customer 360 Tables"
)


def customer_360():
    df_orders = dlt.read("fact_orders")
    df_customers = dlt.read("dim_customers")
    df_order_items = dlt.read("fact_order_items")
    df_reviews = dlt.read("fact_reviews")
    df_restaurant = dlt.read("dim_restaurant")

    df_order_stats = (
        df_orders
        .groupBy("customer_id")
        .agg(
            count_distinct(df_orders.order_id).alias("total_orders"),
            round(sum("total_amount"),2).alias("lifetime_spends"),
            round(try_divide(sum("total_amount"), count_distinct(df_orders.order_id)), 2).alias("avg_order_value"),
            max(df_orders.order_date).alias("last_order_date")
            )
        .withColumn(
                    "loyalty_tier",
                     when(col("lifetime_spends") >= 5000, "Platinum")
                    .when(col("lifetime_spends") >= 2000, "Gold")
                    .when(col("lifetime_spends") >= 1000, "Silver")
                    .otherwise("Bronze")
                    )
        .select(
                col("customer_id"),
                col("total_orders"),
                col("lifetime_spends"),
                col("avg_order_value"),
                col("last_order_date"),
                col("loyalty_tier")
                )            
    )


    df_review_stats = (
        df_reviews
        .groupBy("customer_id")
        .agg(
            count_distinct(df_reviews.review_id).alias("total_reviews"),
            round(avg("rating"),2).alias("avg_rating")
            )
    )
            

    df_fav_restaurant = (
        df_orders
        .join(df_restaurant, on="restaurant_id")
        .groupBy("customer_id", "restaurant_id", "name")
        .agg(
            count_distinct("order_id").alias("total_orders")
            )
        .withColumn("rank", row_number().over(Window.partitionBy("customer_id").orderBy(col("customer_id"), col("restaurant_id"), col("total_orders").desc())))
        .filter(col("rank") == 1)
        .select(
            col("customer_id"),
            col("restaurant_id"),
            col("name").alias("restaurant_name"),
            col("total_orders").alias("total_fav_orders")
            )
    )



    df_fav_item = (
        df_order_items
        .join(df_orders, df_order_items.order_id == df_orders.order_id)
        .groupBy(df_order_items.customer_id, "item_id", "item_name")
        .agg(
            sum("item_quantity").alias("total_quantity")
            )
        .orderBy(col("customer_id"), col("total_quantity").desc())
        .withColumn("rank", row_number().over(Window.partitionBy("customer_id").orderBy(col("customer_id"), col("total_quantity").desc())))
        .filter(col("rank") == 1)
        .select(
            "customer_id",
            "item_id",
            "item_name",
            "total_quantity"
            )
    )   


    df_customer_360 = (
        df_order_stats
        .join(df_review_stats, on="customer_id", how="left")
        .join(df_fav_restaurant, on="customer_id", how="left")
        .join(df_fav_item, on="customer_id", how="left")
        .join(df_customers, on="customer_id", how="left")
        .select(
                col("customer_id"),
                col("name").alias("customer_name"),
                col("email"),
                col("city"),
                col("join_date"),
                col("total_orders"),
                col("lifetime_spends"),
                col("avg_order_value"),
                col("last_order_date"),
                col("loyalty_tier"),
                col("total_reviews"),
                col("avg_rating"),
                col("restaurant_id"),
                col("restaurant_name").alias("favorite_restaurant"),
                col("total_fav_orders"),
                col("item_id"),
                col("item_name").alias("favorite_item"),
                col("total_quantity").alias("total_fav_quantity"),
                when(col("lifetime_spends") >= 5000, True).otherwise(False).alias("is_vip")
            )

    )

    return df_customer_360
