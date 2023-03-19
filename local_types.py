from dataclasses import dataclass
from datetime import datetime

@dataclass
class Dialog:
    name:  str
    tg_id: str



@dataclass
class Msg:
    dlg_id: str
    dt: datetime
    text: str
    tg_id: str


@dataclass
class Book:
    id: str
    filename: str
    size: str
    doctype: str
    tg_id : str



