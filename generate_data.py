"""Generate synthetic e-commerce data and save to ./data/ as CSV files."""

import os
import random
import csv
from datetime import datetime, timedelta
from faker import Faker

fake = Faker()
random.seed(42)
Faker.seed(42)

DATA_DIR = "./data"
os.makedirs(DATA_DIR, exist_ok=True)

NUM_CUSTOMERS = 200
NUM_PRODUCTS = 50
NUM_ORDERS = 1000
NUM_ORDER_ITEMS = 2000

CATEGORIES = ["Electronics", "Clothing", "Home & Garden", "Sports", "Books", "Toys", "Food"]
STATUSES = ["pending", "processing", "shipped", "delivered", "cancelled"]


def generate_customers():
    rows = []
    for i in range(1, NUM_CUSTOMERS + 1):
        rows.append({
            "customer_id": i,
            "first_name": fake.first_name(),
            "last_name": fake.last_name(),
            "email": fake.unique.email(),
            "phone": fake.phone_number(),
            "city": fake.city(),
            "state": fake.state_abbr(),
            "country": "US",
            "created_at": fake.date_time_between(start_date="-3y", end_date="-1y").isoformat(),
        })
    return rows


def generate_products():
    rows = []
    for i in range(1, NUM_PRODUCTS + 1):
        price = round(random.uniform(5.0, 500.0), 2)
        rows.append({
            "product_id": i,
            "product_name": fake.catch_phrase(),
            "category": random.choice(CATEGORIES),
            "price": price,
            "cost": round(price * random.uniform(0.3, 0.7), 2),
            "stock_quantity": random.randint(0, 500),
            "created_at": fake.date_time_between(start_date="-3y", end_date="-2y").isoformat(),
        })
    return rows


def generate_orders(customer_ids):
    rows = []
    for i in range(1, NUM_ORDERS + 1):
        order_date = fake.date_time_between(start_date="-1y", end_date="now")
        rows.append({
            "order_id": i,
            "customer_id": random.choice(customer_ids),
            "order_date": order_date.isoformat(),
            "status": random.choice(STATUSES),
            "shipping_address": fake.street_address(),
            "city": fake.city(),
            "state": fake.state_abbr(),
            "country": "US",
            "total_amount": 0.0,  # filled after order items are generated
        })
    return rows


def generate_order_items(order_ids, product_prices):
    rows = []
    order_totals = {oid: 0.0 for oid in order_ids}
    used = set()

    for i in range(1, NUM_ORDER_ITEMS + 1):
        order_id = random.choice(order_ids)
        product_id = random.randint(1, NUM_PRODUCTS)
        key = (order_id, product_id)
        # avoid exact duplicate (order_id, product_id) combos
        attempts = 0
        while key in used and attempts < 10:
            product_id = random.randint(1, NUM_PRODUCTS)
            key = (order_id, product_id)
            attempts += 1
        used.add(key)

        qty = random.randint(1, 5)
        unit_price = product_prices[product_id]
        subtotal = round(qty * unit_price, 2)
        order_totals[order_id] = round(order_totals[order_id] + subtotal, 2)

        rows.append({
            "order_item_id": i,
            "order_id": order_id,
            "product_id": product_id,
            "quantity": qty,
            "unit_price": unit_price,
            "subtotal": subtotal,
        })
    return rows, order_totals


def write_csv(filename, rows):
    path = os.path.join(DATA_DIR, filename)
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)
    print(f"  Wrote {len(rows):>5} rows -> {path}")


def main():
    print("Generating e-commerce data...")

    customers = generate_customers()
    products = generate_products()

    customer_ids = [c["customer_id"] for c in customers]
    product_prices = {p["product_id"]: p["price"] for p in products}

    orders = generate_orders(customer_ids)
    order_ids = [o["order_id"] for o in orders]
    order_items, order_totals = generate_order_items(order_ids, product_prices)

    for order in orders:
        order["total_amount"] = order_totals[order["order_id"]]

    write_csv("customers.csv", customers)
    write_csv("products.csv", products)
    write_csv("orders.csv", orders)
    write_csv("order_items.csv", order_items)

    print("Done.")


if __name__ == "__main__":
    main()
