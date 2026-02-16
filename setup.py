from setuptools import setup, find_packages


setup(
    name="stock_dividend_monitor",
    version="0.1.0",
    description="ETL pipeline for stock dividend monitoring",
    author="Your Name",
    packages=find_packages(),
    install_requires=[
        "pandas>=1.5.0",
        "requests>=2.28.0",
        "pytest>=7.0.0",
        "python-dotenv>=0.19.0",  # For config management
    ],
    python_requires=">=3.8",
)