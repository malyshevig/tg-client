from telethon import TelegramClient

import logging

from books_meta.bmeta import Indexer
from books_storage.storage import BooksStorage
import sys

import local_types
from local_types import *
from telethon.tl.custom.message import Message
from hash import book_hash
from books_meta import bmeta

logging.basicConfig(stream=sys.stdout,encoding='utf-8', format='%(asctime)s %(levelname)s %(message)s', level=logging.INFO)

api_id = 29093757
api_hash = "0d64c0d51b514dd14b4efad572cbb5db"


class TgScanner:
    def __init__(self, client:TelegramClient, booksDao: BooksStorage):
        self.client = client
        self.dao = booksDao

    def tg_scan(self):
        client = self.client
        with client:
            client.loop.run_until_complete(self.__scan__(client, self.dao))

    def check_duplicate(self, tg_id, dlg_id, filename):
        logging.info(f"check_duplicate_file  tg_id={tg_id} file={filename}")
        lbook = self.dao.get_book_by_name(filename) if filename else None
        old_msg = self.dao.get_message_by_id(dlg_id=dlg_id, tg_id=tg_id)

        r = lbook is None and old_msg is None
        logging.info(f"check_duplicate res:{r} lbook={lbook} old_msg={old_msg}")
        return r

    async def __scan__(self, client: TelegramClient, dao:BooksStorage):
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
                            if self.check_duplicate(dlg_id=message.dlg_id, tg_id=message.tg_id, filename=msg.file.name):
                                await msg.download_media("/Volumes/SSD/download")
                                file: LBook = local_types.get_file(msg.file, dialog.tg_id, message.tg_id)
                                file.keys = bmeta.get_keys(file.filename)

                                bhash = book_hash.hash_file("/Volumes/SSD/download" + "/" + file.filename)
                                file.hash = bhash

                                if lb := self.dao.get_book_by_hash(bhash):
                                    logging.info(f"duplicate found:{bhash} new_file:{file.filename} existing file:{lb.filename}")
                                else:
                                    dao.add_book(file)
                                    logging.info(f"add file {file.filename}")
                                    books += 1

                        dao.add_message_if_not_exists(message)
                        dialog.max_msg_dt = message.dt

                    dao.update_dialog(dialog)
            client.disconnect()

        except Exception as e:
            logging.error(e)
            print(e)
            raise e

        logging.info(f"Scan finished, {books} books added")


def main():
    dao = BooksStorage()
    client = TelegramClient("im", api_id, api_hash)

    TgScanner(client,dao).tg_scan()

    indexer = Indexer(dao)
    indexer.update_keys()
    indexer.update_hash()
    dao.close()

if __name__ == "__main__":
    main()
