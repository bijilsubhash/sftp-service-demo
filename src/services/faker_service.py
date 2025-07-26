import polars as pl
from faker import Faker
from random import randint
from pathlib import Path
from datetime import datetime
from utils.logging import Logger


class FakerService:
    def __init__(self, current_date: datetime):
        self.fake = Faker()
        self.logger = Logger(__name__)
        self.current_date = current_date
        self.output_dir = Path("output") / current_date.strftime("%Y%m%d")

    def generate_data(self, size: int = 1000) -> None:
        self.output_dir.mkdir(parents=True, exist_ok=True)

        if size <= 0:
            raise ValueError("Size must be greater than 0")

        customers = self._generate_customers(min(size // 10, 100))
        products = self._generate_products(min(size // 20, 50))
        orders = self._generate_orders(size, customers, products)

        for data in [customers, products, orders]:
            df = pl.DataFrame(data)
            output_file = self.output_dir / f"{data[0]['record_type']}.csv"
            df.write_csv(output_file)
            self.logger.info(f"{len(data)} records generated to {output_file}")

    def _generate_customers(self, count: int) -> list[dict]:
        return [
            {
                "record_type": "customer",
                "customer_id": i + 1,
                "customer_name": self.fake.name(),
                "email": self.fake.email(),
                "phone": self.fake.phone_number(),
                "company": self.fake.company(),
                "address": self.fake.address().replace("\n", ", "),
                "city": self.fake.city(),
                "country": self.fake.country(),
                "registration_date": self.fake.date_between(
                    start_date="-2y", end_date="today"
                ).isoformat(),
                "credit_limit": round(self.fake.random.uniform(1000, 50000), 2),
            }
            for i in range(count)
        ]

    def _generate_products(self, count: int) -> list[dict]:
        categories = {
            "Electronics": [
                "iPhone 15 Pro",
                "Samsung Galaxy S24",
                "MacBook Air M3",
                "Dell XPS 13",
                "iPad Pro",
                "Sony WH-1000XM5",
                "AirPods Pro",
                "Nintendo Switch",
                "Google Pixel 8",
                "Apple Watch Series 9",
                "Bose QuietComfort 45",
                "Sony WH-1000XM5",
                "AirPods Pro",
                "Nintendo Switch",
            ],
            "Clothing": [
                "Levi's 501 Jeans",
                "Nike Air Max 270",
                "Adidas Ultraboost",
                "Zara Basic T-Shirt",
                "H&M Hoodie",
                "Uniqlo Down Jacket",
                "Converse Chuck Taylor",
                "Nike Air Force 1",
                "Adidas Stan Smith",
                "Vans Old Skool",
                "Puma Suede",
                "Reebok Classic",
                "New Balance 574",
            ],
            "Books": [
                "The Great Gatsby",
                "To Kill a Mockingbird",
                "1984",
                "Pride and Prejudice",
                "The Catcher in the Rye",
                "Lord of the Flies",
                "Harry Potter",
            ],
            "Home & Garden": [
                "IKEA Billy Bookshelf",
                "Dyson V15 Vacuum",
                "KitchenAid Stand Mixer",
                "Instant Pot Duo",
                "Philips Hue Bulbs",
                "Weber Genesis Grill",
                "Instant Pot Duo",
                "Philips Hue Bulbs",
                "Weber Genesis Grill",
            ],
            "Sports": [
                "Wilson Tennis Racket",
                "Spalding Basketball",
                "Nike Running Shoes",
                "Yeti Water Bottle",
                "Under Armour Gym Bag",
                "Fitbit Charge 5",
            ],
        }

        products = []
        for i in range(count):
            category = self.fake.random_element(list(categories.keys()))
            product_name = self.fake.random_element(categories[category])

            products.append(
                {
                    "record_type": "product",
                    "product_id": i + 1,
                    "product_name": product_name,
                    "category": category,
                    "price": round(self.fake.random.uniform(10, 1000), 2),
                    "cost": round(self.fake.random.uniform(5, 500), 2),
                    "sku": self.fake.bothify(text="???-####"),
                    "supplier": self.fake.company(),
                    "weight": round(self.fake.random.uniform(0.1, 25.0), 2),
                }
            )

        return products

    def _generate_orders(
        self, count: int, customers: list[dict], products: list[dict]
    ) -> list[dict]:
        if not customers or not products:
            return []

        orders = []
        for i in range(count):
            customer = self.fake.random_element(customers)
            product = self.fake.random_element(products)
            quantity = randint(1, 5)
            unit_price = product["price"]
            total_amount = round(quantity * unit_price, 2)

            orders.append(
                {
                    "record_type": "order",
                    "order_id": i + 1,
                    "customer_id": customer["customer_id"],
                    "product_id": product["product_id"],
                    "order_date": self.fake.date_between(
                        start_date="-1y", end_date="today"
                    ).isoformat(),
                    "quantity": quantity,
                    "unit_price": unit_price,
                    "total_amount": total_amount,
                    "status": self.fake.random_element(
                        ["pending", "shipped", "delivered", "cancelled"]
                    ),
                    "payment_method": self.fake.random_element(
                        ["credit_card", "debit_card", "paypal", "bank_transfer"]
                    ),
                }
            )

        return orders
