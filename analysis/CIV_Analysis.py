import abc
from abc import ABC, abstractmethod

class CIV_Analysis(ABC):

    def __init__(self, lang=None, text=None):
        self.lang = lang
        self.text = text

    @abstractmethod
    def get_score(self, type=None):
        pass