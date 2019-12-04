from abc import ABC, abstractmethod
from typing import Union, List, Dict


class Http(ABC):
    @abstractmethod
    def post(self, url: str, data: Union[List, Dict, str] = None,
             headers: Dict[str, str] = None
             ) -> Union[List, Dict]:
        pass

    @abstractmethod
    def get(self, url, params: Union[List, Dict] = None,
            headers: Dict[str, str] = None
            ) -> Union[List, Dict]:
        pass
