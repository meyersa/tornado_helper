from setuptools import setup, find_packages

setup(
    name="tornado_helper",
    version="2.1.7",
    packages=find_packages(),
    install_requires=[
        "requests",
        "b2sdk",
        "python-dotenv",
        "aria2p",
        "tqdm",
        "boto3",
        "pandas",
        "pyproj",
        "netcdf4",
        "xarray",
        "folium"
    ],
    author="August Meyers",
    description="A helper package for uploading and downloading Tornado training data",
    url="https://github.com/meyersa/tornado_helper",
)