from dataclasses import dataclass
from datetime import datetime
from telethon.tl.custom.file import File
from telethon.tl.custom.message import Message
from telethon.tl.custom.dialog import Dialog

@dataclass
class LDialog:
    name:  str
    tg_id: int
    dlg_type: str
    max_msg_dt: datetime


@dataclass
class LMessage:
    dlg_id: int
    dt: datetime
    text: str
    tg_id: str



@dataclass
class LBook:
    filename: str
    size: int
    doctype: str
    tg_id : int
    dlg_id: int
    msg_id: int


def get_dialog(d: Dialog) -> LDialog:
    dlg_type = "unknown"
    if d.is_user:
        dlg_type = "user"
    elif d.is_group:
        dlg_type = "group"
    elif d.is_channel:
        dlg_type = "channel"

    return LDialog(name=d.name, tg_id = d.id, dlg_type=dlg_type,max_msg_dt=None)


def get_message(d: Message, dlg_id) -> LMessage:
    return LMessage(dlg_id=dlg_id, dt=d.date, text=d.text,tg_id=d.id )

def get_file(file: File, dlg_id, msg_id) -> LMessage:
    return LBook(filename=file.name, tg_id=file.id, size = file.size, doctype=file.mime_type, dlg_id=dlg_id, msg_id = msg_id)

