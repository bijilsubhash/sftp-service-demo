import polars as pl
import hashlib
from faker import Faker
from random import randint
from pathlib import Path
from datetime import datetime
from common.utils.logging_util import Logger


class FakerService:
    def __init__(self, current_date: str):
        self.fake = Faker()
        self.logger = Logger(__name__)
        self.current_date = datetime.strptime(current_date, "%d-%m-%Y")
        self.output_dir = Path("output") / self.current_date.strftime("%Y%m%d")

    def generate_data(self, size: int = 1000) -> None:
        self.output_dir.mkdir(parents=True, exist_ok=True)

        if size <= 0:
            raise ValueError("Size must be greater than 0")

        customers = self._load_customers()
        products = self._load_products()
        orders = self._generate_orders(size, customers, products)

        df = pl.DataFrame(orders)
        output_file = self.output_dir / "order.csv"
        df.write_csv(output_file)
        self.logger.debug(f"{len(orders)} records generated to {output_file}")
        self.logger.info(
            f"Generated order data for {self.output_dir.name.split('/')[-1]}"
        )

    def _load_customers(self) -> list[dict]:
        customers_file = Path("data/customers.csv")
        if not customers_file.exists():
            raise FileNotFoundError(f"Customers file not found: {customers_file}")

        df = pl.read_csv(customers_file)
        return [{"record_type": "customer", **row} for row in df.to_dicts()]

    def _load_products(self) -> list[dict]:
        products_file = Path("data/products.csv")
        if not products_file.exists():
            raise FileNotFoundError(f"Products file not found: {products_file}")

        df = pl.read_csv(products_file)
        return [{"record_type": "product", **row} for row in df.to_dicts()]

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
                    "order_id": hashlib.sha256(
                        self.current_date.isoformat().encode() + str(i + 1).encode()
                    ).hexdigest(),
                    "customer_id": customer["customer_id"],
                    "product_id": product["product_id"],
                    "order_date": self.current_date,
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
