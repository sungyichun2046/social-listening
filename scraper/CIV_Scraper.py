import abc
from abc import ABC, abstractmethod

class CIV_Scraper(ABC):

    def __init__(self):
        pass

    @abstractmethod
    def get_data(self):
        pass

