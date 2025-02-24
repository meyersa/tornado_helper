from setuptools import setup, find_packages

setup(
    name="tornado-helper",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        "aria2c",
        "requests",
        "b2sdk",
        "dotenv"
    ],
    author="August Meyers",
    description="A helper package for uploading and downloading Tornado training data",
    url="https://github.com/meyersa/tornado-helper",
)