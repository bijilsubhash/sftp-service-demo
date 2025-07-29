from setuptools import find_packages, setup

setup(
    name="dagster_project",
    packages=find_packages(),
    install_requires=[
        "dagster",
        "faker>=37.4.2",
        "paramiko>=3.3.1",
        "polars>=1.23.0",
        "pydantic>=2.10.6",
        "pydantic-settings>=2.5.2",
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
