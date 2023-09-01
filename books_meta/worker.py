import time

from . import mq
from local_types import *
import logging
import isbnlib
from books_storage.storage import BooksStorage
import pymorphy2 as pm
from . import pdf

logging.basicConfig(level=logging.INFO)

ldir = "/Volumes/SSD/download"

anal_uk = pm.MorphAnalyzer(lang="uk")
anal_ru = pm.MorphAnalyzer(lang="ru")


class UpdateMetaWorker:

    def __init__(self, storage: BooksStorage):
        self.reader = mq.RabbitTaskMgr()
        self.storage = storage

    def work(self):
        self.reader.connect()
        self.reader.read_tasks(lambda b: self.process_book(b))

    def update_book_meta(self, b: LBook, meta):
        self.storage.update_books_meta(b, meta)

    def add_publisher(self, publisher):
        self.storage.add_publisher(publisher)

    def  process_book(self, b: LBook):
        try:
            if b.doctype == "application/pdf":
                file = ldir + "/" + b.filename

                isbn_list = pdf.get_isbn(file)
                if isbn_list is None or len(isbn_list) == 0:
                    logging.error(f"for {b.filename} isbn is empty")
                else:
                    logging.info(f"for {b.filename} isbn = {isbn_list}")

                    meta = None
                    for isbn in isbn_list:
                        if isbn[-1] == "-":
                            isbn = isbn[:-1]

                        services:dict = isbnlib._metadata.get_services()
                        for srv in services.keys():
                            try:
                                logging.info(f" trying isbn:{isbn} srv:{srv}")
                                meta = isbnlib.meta(isbn, service=srv)
                            except BaseException as ex:
                                logging.error(f" get_meta failed isbn:{isbn} {srv} {ex}")
                                pass

                            if meta is not None and len(meta)>0:
                                break

                    time.sleep(1)
                    if meta is not None and len(meta)>0:
                        self.update_book_meta(b, meta)
                        publisher = meta.get("Publisher")
                        if publisher:
                            self.add_publisher(publisher)
                        else:
                            logging.warning(f"book = {b.filename} meta={meta} publisher is None")

        except Exception as ex:
            logging.error(f" book:{b} error: {ex}")


def main ():
    UpdateMetaWorker().work()


if __name__ == "__main__":
    main()