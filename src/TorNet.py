from typing import List
from .Helper import Helper

"""
Class for Tornet

Includes a method to download all data, upload data to s3, and to download just a small part 

Stores in links arr so the download all just calls all while download small calls just one 
"""
class TorNet(Helper):
    BUCKET_NAME = "TorNetBecauseZenodoSlow"
    LINKS = [
            "tornet_2013.tar.gz",
            "tornet_2014.tar.gz",
            "tornet_2015.tar.gz",
            "tornet_2016.tar.gz",
            "tornet_2017.tar.gz",
            "tornet_2018.tar.gz",
            "tornet_2019.tar.gz",
            "tornet_2020.tar.gz",
            "tornet_2021.tar.gz",
            "tornet_2022.tar.gz",
        ]
    
    """
    Override upload using TorNet specifics
    """

    def upload(self, files: List[str], application_key: str, application_key_id: str) -> bool:
        return super().upload(files, self.BUCKET_NAME, application_key, application_key_id)

    """
    Override Download using Tornet specifics
    """
    def download(self, output_dir: str = None) -> bool:
        return super().download(self.LINKS, self.BUCKET_NAME, output_dir)
    
    """
    Download just one part 
    """
    def downloadSeg(self, start=0, end=0, output_dir: str=None) -> bool: 
        return super().download(self.LINKS[start:end], self.BUCKET_NAME, output_dir)
