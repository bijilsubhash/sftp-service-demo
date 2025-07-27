import pytest
import polars as pl
from pathlib import Path
from src.sftp.services.faker_service import FakerService


@pytest.fixture
def faker_service() -> FakerService:
    test_date = "15-01-2024"
    service = FakerService(test_date)
    service.output_dir = Path("tests/output") / test_date.replace("-", "")
    return service


def test_generate_data(faker_service: FakerService) -> None:
    faker_service.generate_data(size=100)
    assert faker_service.output_dir.exists()
    assert (faker_service.output_dir / "order.csv").exists()


def test_generate_data_with_invalid_size(faker_service: FakerService) -> None:
    with pytest.raises(ValueError):
        faker_service.generate_data(size=-1)


def test_generate_data_with_zero_size(faker_service: FakerService) -> None:
    with pytest.raises(ValueError):
        faker_service.generate_data(size=0)


def test_order_csv_file_created(faker_service: FakerService) -> None:
    faker_service.generate_data(size=100)

    # Check that order CSV file is created
    order_df = pl.read_csv(faker_service.output_dir / "order.csv")
    assert order_df.height > 0


def test_order_data_fields(faker_service: FakerService) -> None:
    faker_service.generate_data(size=100)
    order_df = pl.read_csv(faker_service.output_dir / "order.csv")

    required_columns = [
        "order_id",
        "customer_id",
        "product_id",
        "order_date",
        "quantity",
        "unit_price",
        "total_amount",
    ]
    for col in required_columns:
        assert col in order_df.columns


def test_order_relationships(faker_service: FakerService) -> None:
    faker_service.generate_data(size=100)

    # Load static customer and product data
    customers_df = pl.read_csv("data/customers.csv")
    products_df = pl.read_csv("data/products.csv")
    orders_df = pl.read_csv(faker_service.output_dir / "order.csv")

    # Verify foreign key relationships exist
    order_customer_ids = orders_df["customer_id"].unique().to_list()
    customer_ids = customers_df["customer_id"].unique().to_list()

    order_product_ids = orders_df["product_id"].unique().to_list()
    product_ids = products_df["product_id"].unique().to_list()

    # All order customer_ids should exist in customers
    assert all(cid in customer_ids for cid in order_customer_ids)
    # All order product_ids should exist in products
    assert all(pid in product_ids for pid in order_product_ids)


def test_load_customers(faker_service: FakerService) -> None:
    customers = faker_service._load_customers()
    assert len(customers) > 0
    assert all("customer_id" in customer for customer in customers)
    assert all("customer_name" in customer for customer in customers)


def test_load_products(faker_service: FakerService) -> None:
    products = faker_service._load_products()
    assert len(products) > 0
    assert all("product_id" in product for product in products)
    assert all("product_name" in product for product in products)
