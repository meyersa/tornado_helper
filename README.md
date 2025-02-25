# Tornado Helper 

Based on a loader from the [Tornado prediction](https://github.com/meyersa/tornado-prediction) project, downloads learning files from S3. 

[![Build Package and publish to PyPi](https://github.com/meyersa/tornado_helper/actions/workflows/ci.yml/badge.svg)](https://github.com/meyersa/tornado_helper/actions/workflows/ci.yml)&nbsp;![PyPI - Version](https://img.shields.io/pypi/v/tornado_helper)

## Installation 

Ensure Aria2c is installed for downloading, this can be done with your preferred package manager 

> apt install aria2 

> brew install aria2 

Then simply install the package with pip

> pip install tornado_helper 

## Using

The package can then be used in the project by importing the Helper you need, for example 

```python
from tornado_helper import TorNet

TorNet.download()
```

Will then download TorNet's files to your computer at a specified directory. 

See the example.ipynb file for some additional context.