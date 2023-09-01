
from local_types import *
import logging
from books_storage.storage import BooksStorage
import re
import pymorphy2 as pm
from . import mq
from hash import book_hash

logging.basicConfig(level=logging.INFO)

ldir = "/Volumes/SSD/download"

anal_uk = pm.MorphAnalyzer(lang="uk")
anal_ru = pm.MorphAnalyzer(lang="ru")


def norm(s):
    if ord(s[0]) < 255:
        return anal_uk.parse(s)[0].normal_form
    else:
        return anal_ru.parse(s)[0].normal_form


def get_keys(name: str):
    idx = name.rfind(".", )
    s = name[:idx]

    keys = re.split(r"[-;,._\s]\s*", s)
    keys = list({norm(k) for k in keys if k != ""})
    return keys


class Indexer:
    def __init__(self, storage:BooksStorage):
        self.dao = storage
        self.task_mgr = mq.RabbitTaskMgr()

    def cleanup (self):
        books = self.dao.get_books()
        for c, b in enumerate(books):
            path = ldir + "/" + b.filename
            try:
                f = open(path, "rb")
                f.close()
            except FileNotFoundError as ex:
                logging.error(f"missing file {b}  - removing")
                self.dao.delete_dummy(b.tg_id)

    def update_keys(self):
        keys_set = {k for k in self.dao.get_keys()}

        books = self.dao.get_books_empty_keys()
#        books = self.dao.get_books()
        num = len(books)

        logging.info(f"adding keys {num}")

        for c, b in enumerate(books):
            keys = get_keys(b.filename)
            for k in keys:
                if k not in keys_set:
                    self.dao.add_key(k)
                    keys_set.add(k)

            self.dao.update_keys(b.tg_id, keys)
            if c % 100 == 0:
                logging.info(f"Keys added to {c} of {num}")

    def update_hash(self):
        books = self.dao.get_books_empty_hash()
        #books = self.dao.get_books()
        num = len(books)

        logging.info(f"adding hash {num}")

        for c, b in enumerate(books):
            path = ldir +"/"+b.filename
            try:
                bhash = book_hash.hash_file(path)

                self.dao.update_hash(b.tg_id, bhash)
                if c % 100 == 0:
                    logging.info(f"hash added to {c} of {num}")
            except BaseException as ex:
                logging.error(f"update_hash {b} {ex}")


    def update_meta_info(self):
        books: [LBook] = self.dao.get_books()
        self.task_mgr.connect()

        for c, b in enumerate(books):
            if not b.meta:
                self.task_mgr.add_task(b)

        self.task_mgr.close()



if __name__ == "__main__":

    indexer = Indexer(storage := BooksStorage())
    try:
        indexer.cleanup()
        indexer.update_keys()
        indexer.update_hash()
        #  indexer.update_meta_info()

        storage.close()
    except BaseException as ex:
        logging.error(f"bmeta main {ex}")
        storage.close()



