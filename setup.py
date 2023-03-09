"""
This file configures the Python package with entrypoints used for future runs on Databricks.

Please follow the `entry_points` documentation for more details on how to configure the entrypoint:
* https://setuptools.pypa.io/en/latest/userguide/entry_point.html
"""

from setuptools import find_packages, setup

PACKAGE_REQUIREMENTS = [
    "pyyaml",
    "pytest",
    "mlflow",
    "scikit-learn",
    "flake8",
    "pre-commit",
    "black",
    "databricks-cli",
    "dbx>=0.7,<0.8",
]

# packages for local development and unit testing
# please note that these packages are already available in DBR, there is no need to install them on DBR.
LOCAL_REQUIREMENTS = [
    "pyspark==3.2.1",
    "delta-spark==1.1.0",
    "scikit-learn",
    "pandas",
    "mlflow",
]

TEST_REQUIREMENTS = [
    # development & testing tools
    "pytest",
    "coverage[toml]",
    "pytest-cov",
    "dbx>=0.7,<0.8",
]

setup(
    name="dlt_pipelines",
    packages=find_packages(exclude=["tests", "tests.*"]),
    setup_requires=["setuptools", "wheel"],
    install_requires=PACKAGE_REQUIREMENTS,
    extras_require={"local": LOCAL_REQUIREMENTS, "test": TEST_REQUIREMENTS},
    entry_points={
        "console_scripts": [
            "etl = dlt_pipeline1.tasks.sample_etl_task:entrypoint",
            "ml = dlt_pipeline1.tasks.sample_ml_task:entrypoint",
        ]
    },
    description="",
    author="",
)
