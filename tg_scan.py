from telethon import TelegramClient, events
import asyncio
import logging
from  dao import BooksDAO
import sys

import local_types
from local_types import *
from telethon.tl.custom.file import File
from telethon.tl.custom.message import Message
from telethon.tl.custom.dialog import Dialog

logging.basicConfig(stream=sys.stdout,encoding='utf-8', format='%(asctime)s %(levelname)s %(message)s', level=logging.INFO)

api_id = 29093757
api_hash = "0d64c0d51b514dd14b4efad572cbb5db"


class TgScanner:
    def __init__(self, client:TelegramClient, booksDao: BooksDAO):
        self.client = client
        self.dao = booksDao

    def tg_scan(self):
        client = self.client
        with client:
            client.loop.run_until_complete(self.__scan__(client, self.dao))

    async def __scan__(self, client: TelegramClient, dao:BooksDAO):
        books = 0
        try:
            for d in await client.get_dialogs():
                dialog = local_types.get_dialog(d)

                if dialog.dlg_type == "channel":
                    dialog = dao.add_dialog(dialog)
                    logging.info(f"Scanning channel {dialog.name}")

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
                                books += 1
                                await msg.download_media("/Volumes/SSD/download")
                                logging.info(f"add file {file.filename}")
                            else:
                                # dao.add_message(message)
                                if lbook.msg_id is None:
                                    dao.add_message(message)

                                    dao.update_by_name(file.filename, file)
                                    logging.info(f"update file {file.filename}")

                        dialog.max_msg_dt = message.dt

                    dao.update_dialog(dialog)
            client.disconnect()

        except Exception as e:
            logging.error(e)
            print(e)
            raise e

        logging.info(f"Scan finished, {books} books added")


def main():
    dao = BooksDAO()
    client = TelegramClient("im", api_id, api_hash)

    TgScanner(client,dao).tg_scan()

if __name__ == "__main__":
    main()
