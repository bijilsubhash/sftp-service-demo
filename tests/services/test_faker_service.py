import pytest
import polars as pl
from pathlib import Path
from datetime import datetime
from src.services.faker_service import FakerService


@pytest.fixture
def faker_service() -> FakerService:
    test_date = datetime(2024, 1, 15)
    service = FakerService(test_date)
    service.output_dir = Path("tests/output") / test_date.strftime("%Y%m%d")
    return service


def test_generate_data(faker_service: FakerService) -> None:
    faker_service.generate_data(size=100)
    assert faker_service.output_dir.exists()
    assert (faker_service.output_dir / "customer.csv").exists()
    assert (faker_service.output_dir / "product.csv").exists()
    assert (faker_service.output_dir / "order.csv").exists()


def test_generate_data_with_invalid_size(faker_service: FakerService) -> None:
    with pytest.raises(ValueError):
        faker_service.generate_data(size=-1)


def test_generate_data_with_zero_size(faker_service: FakerService) -> None:
    with pytest.raises(ValueError):
        faker_service.generate_data(size=0)


def test_separate_csv_files_created(faker_service: FakerService) -> None:
    faker_service.generate_data(size=100)

    # Check that separate CSV files are created
    customer_df = pl.read_csv(faker_service.output_dir / "customer.csv")
    product_df = pl.read_csv(faker_service.output_dir / "product.csv")
    order_df = pl.read_csv(faker_service.output_dir / "order.csv")

    assert customer_df.height > 0
    assert product_df.height > 0
    assert order_df.height > 0


def test_customer_data_fields(faker_service: FakerService) -> None:
    faker_service.generate_data(size=100)
    customer_df = pl.read_csv(faker_service.output_dir / "customer.csv")

    required_columns = ["customer_id", "customer_name", "email", "phone", "company"]
    for col in required_columns:
        assert col in customer_df.columns


def test_product_data_fields(faker_service: FakerService) -> None:
    faker_service.generate_data(size=100)
    product_df = pl.read_csv(faker_service.output_dir / "product.csv")

    required_columns = ["product_id", "product_name", "category", "price", "sku"]
    for col in required_columns:
        assert col in product_df.columns


def test_order_relationships(faker_service: FakerService) -> None:
    faker_service.generate_data(size=100)

    customers_df = pl.read_csv(faker_service.output_dir / "customer.csv")
    products_df = pl.read_csv(faker_service.output_dir / "product.csv")
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
