from setuptools import setup, find_packages

setup(
    name="tornado_helper",
    version="1.0.1",
    packages=find_packages(),
    install_requires=[
        "requests",
        "b2sdk",
        "dotenv",
        "aria2p",
        "tqdm"
    ],
    author="August Meyers",
    description="A helper package for uploading and downloading Tornado training data",
    url="https://github.com/meyersa/tornado_helper",
)