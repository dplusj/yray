from abc import ABC, abstractmethod


class Executor(ABC):

    @abstractmethod
    def submit(self, node, dep_results):
        pass

    @abstractmethod
    def gather(self, handle):
        pass