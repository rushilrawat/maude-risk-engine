from email.message import Message
from typing import Protocol, Self, cast
from urllib.request import Request, urlopen


class HttpResponse(Protocol):
    headers: Message

    def read(self, amount: int = -1) -> bytes: ...

    def geturl(self) -> str: ...

    def __enter__(self) -> Self: ...

    def __exit__(self, *args: object) -> None: ...


class UrlOpener(Protocol):
    def __call__(self, request: Request, timeout: float) -> HttpResponse: ...


def open_url(request: Request, timeout: float) -> HttpResponse:
    return cast(HttpResponse, urlopen(request, timeout=timeout))
