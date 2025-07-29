from setuptools import find_packages, setup

setup(
    name="dagster_project",
    package_dir={"": "src"},
    packages=find_packages(where="src"),
    py_modules=["main"],
    install_requires=[
        "dagster",
        "dagster-cloud",
        "faker>=37.4.2",
        "paramiko>=3.3.1",
        "polars>=1.23.0",
        "polars-lts-cpu>=1.23.0",
        "pydantic>=2.10.6",
        "pydantic-settings>=2.5.2",
        "python-json-logger>=2.0.9",
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
