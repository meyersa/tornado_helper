from setuptools import setup, find_packages

setup(
    name="tornado-helper",
    version="0.1.2",
    packages=find_packages(),
    package_dir={'': 'src'},
    packages=find_packages(where='src'),
    install_requires=[
        "requests",
        "b2sdk",
        "dotenv"
    ],
    author="August Meyers",
    description="A helper package for uploading and downloading Tornado training data",
    url="https://github.com/meyersa/tornado-helper",
)