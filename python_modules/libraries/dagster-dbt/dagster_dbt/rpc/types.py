from __future__ import annotations

from typing import TYPE_CHECKING, Any, Mapping

from ..types import DbtOutput

if TYPE_CHECKING:
    import requests


class DbtRpcOutput(DbtOutput):
    """The output from executing a dbt command via the dbt RPC server.

    Attributes:
        result (Dict[str, Any]): The parsed contents of the "result" field of the JSON response from
            the rpc server (if any).
        response_dict (Dict[str, Any]): The entire contents of the JSON response from the rpc server.
        response (requests.Response): The original Response from which this output was generated.
    """

    def __init__(self, response: requests.Response):
        self._response = response
        self._response_dict = response.json()

        super().__init__(result=self._response_dict.get("result", {}))

    @property
    def response(self) -> requests.Response:
        return self._response

    @property
    def response_dict(self) -> Mapping[str, Any]:
        return self._response_dict
