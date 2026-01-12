from setuptools import setup, find_packages

setup(
    name="baostock-analyzer",
    version="0.1.0",
    description="基于baostock的公司财务分析工具",
    author="Your Name",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    install_requires=[
        "baostock>=0.8.8",
        "pandas>=1.3.0",
        "python-dateutil>=2.8.0",
    ],
    python_requires=">=3.7",
)
