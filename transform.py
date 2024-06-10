import pandas as pd

orders = pd.read_csv("data/olist_orders_dataset.csv")
order_items = pd.read_csv("data/olist_order_items_dataset.csv")
category_translation = pd.read_csv("data/product_category_name_translation.csv")
products = pd.read_csv("data/olist_products_dataset.csv")
customers = pd.read_csv("data/olist_customers_dataset.csv")
geolocation = pd.read_csv("data/olist_geolocation_dataset.csv")
reviews = pd.read_csv("data/olist_order_reviews_dataset.csv")[
    ["order_id", "review_score"]
]

# cut off date for last full month
cutoff_date = pd.Timestamp("2018-09-01")
orders["order_purchase_timestamp"] = pd.to_datetime(
    orders["order_purchase_timestamp"], errors="coerce"
)
orders = orders[orders["order_purchase_timestamp"] < cutoff_date]

geolocation = (
    geolocation.groupby(by=["geolocation_city"])
    .agg({"geolocation_lat": "mean", "geolocation_lng": "mean"})
    .reset_index()
)

customer_locations = customers.merge(
    geolocation, left_on="customer_city", right_on="geolocation_city"
)

# Replace the original category names with English names
merged = products.merge(category_translation, on="product_category_name", how="left")
merged["product_category_name"] = merged["product_category_name_english"]
selected_columns = [
    "product_id",
    "product_category_name",
]
products_translated = merged[selected_columns]

orders_categorised = order_items.merge(products_translated, on="product_id")

orders_final = orders_categorised.merge(orders, on="order_id")
orders_final["order_purchase_timestamp"] = pd.to_datetime(
    orders_final["order_purchase_timestamp"]
)
orders_final["order_delivered_customer_date"] = pd.to_datetime(
    orders_final["order_delivered_customer_date"]
)

# Calculate delivery time in days
orders_final["delivery_days"] = orders_final.apply(
    lambda row: (
        (row["order_delivered_customer_date"] - row["order_purchase_timestamp"]).days
        if pd.notnull(row["order_delivered_customer_date"])
        else None
    ),
    axis=1,
)

orders_final = orders_final.merge(customer_locations, on="customer_id")
orders_final = orders_final.merge(reviews, on="order_id")

selected_columns = [
    "price",
    "product_category_name",
    "order_status",
    "order_purchase_timestamp",
    "delivery_days",
    "order_id",
    "customer_zip_code_prefix",
    "customer_city",
    "customer_state",
    "geolocation_lat",
    "geolocation_lng",
    "review_score",
]
orders_final = orders_final[selected_columns]

orders_final.to_csv("output/consolidated_orders.csv", index=False)
