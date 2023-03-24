from telethon import TelegramClient, events
import asyncio
import logging
import dao
import sys

import local_types
from local_types import *
from telethon.tl.custom.file import File
from telethon.tl.custom.message import Message
from telethon.tl.custom.dialog import Dialog

logging.basicConfig(encoding='utf-8', format='%(asctime)s %(levelname)s %(message)s', level=logging.ERROR)

api_id = 29093757
api_hash = "0d64c0d51b514dd14b4efad572cbb5db"

dao = dao.BooksDAO()


async def main(client: TelegramClient):
    try:
        for d in await client.get_dialogs():
            dialog = local_types.get_dialog(d)

            if dialog.dlg_type == "channel":
                dialog = dao.add_dialog(dialog)
                print(f"Walk through channel {dialog.name}")

                max_msg_dt = dialog.max_msg_dt
                msg_iter = client.iter_messages(d, reverse=True) if max_msg_dt is None else \
                    client.iter_messages(d, offset_date=max_msg_dt, reverse=True)

                async for m in msg_iter:
                    msg: Message = m
                    message: LMessage = local_types.get_message(m, dialog.tg_id)

                    if msg.file is not None and msg.file.name is not None and msg.file.ext in [".pdf", ".epub", ".fb2"]:
                        file: LBook = local_types.get_file(msg.file, dialog.tg_id, message.tg_id)
                        lbook = dao.get_book_by_name(file.filename)

                        if lbook is None:
                            dao.add_message(message)
                            dao.add_book(file)
                            await msg.download_media("/Volumes/SSD/download")
                            print(f"add file {file.filename}")
                        else:
                            # dao.add_message(message)
                            if lbook.msg_id is None:
                                dao.add_message(message)

                                dao.update_by_name(file.filename, file)
                                print(f"update file {file.filename}")

                    dialog.max_msg_dt = message.dt
                    #dao.update_dialog(dialog)
                dao.update_dialog(dialog)

    except Exception as e:
        logging.error(e)
        print(e)
        raise e

    print ("Finished")



client = TelegramClient("im", api_id, api_hash)
with client:
    client.loop.run_until_complete(main(client))
