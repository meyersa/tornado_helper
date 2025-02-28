from setuptools import setup, find_packages

setup(
    name="tornado_helper",
    version="0.6.0",
    packages=find_packages(),
    install_requires=[
        "requests",
        "b2sdk",
        "dotenv",
        "aria2",
        "tqdm"
    ],
    author="August Meyers",
    description="A helper package for uploading and downloading Tornado training data",
    url="https://github.com/meyersa/tornado_helper",
)