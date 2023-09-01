
import pymongo as pm
import logging, unittest
from pymongo.cursor import Cursor
from local_types import *


class BooksStorage:

    def __init__(self, database = "books"):
        self.client = pm.mongo_client.MongoClient()

        self.database = database
        self.db = self.client.get_database(database)
        self.books = self.db.get_collection("books")
        self.dialogs = self.db.get_collection("dialogs")
        self.messages = self.db.get_collection("messages")
        self.publishers = self.db.get_collection("publishers")
        self.keys = self.db.get_collection("keys")

    def close(self):
        self.db.client.close()

    def __get_local_book__(self, book: dict) -> LBook:
        filename = book.get("fileName")
        tg_id = book.get("tg_id")
        msg_id = book.get("msg_id")
        size = book.get("size")
        doctype = book.get("doctype")
        keys = book.get("keys=")
        dlg_id = book.get("dlg_id")
        book_hash = book.get("hash")
        meta = book.get("books_meta")
        if meta is not None and len(meta) == 0:
            meta = None

        return LBook(filename=filename, tg_id=tg_id, msg_id=msg_id, dlg_id=dlg_id, size=size, doctype=doctype,
                     keys= keys, hash = book_hash, meta=meta)

    def __get_local_message__(self, msg: dict) -> LMessage:
        text = msg.get("text")
        tg_id = msg.get("tg_id")
        dt = msg.get("dt")
        dlg_id = msg.get("dlg_id")

        return LMessage(dlg_id,dt,text,tg_id)

    def __get_local_publisher__(self, pub: dict) -> LMessage:
        text = msg.get("text")
        tg_id = msg.get("tg_id")
        dt = msg.get("dt")
        dlg_id = msg.get("dlg_id")

        return LMessage(dlg_id, dt, text, tg_id)

    def __get_local_dialog__(self, dlg: dict) -> LDialog:
        name = dlg.get("name")
        tg_id = dlg.get("tg_id")
        max_msg_dt = dlg.get("max_msg_dt")
        dlg_type = dlg.get("dlg_type")

        return LDialog(name=name,tg_id=tg_id,dlg_type=dlg_type, max_msg_dt=max_msg_dt)

    def get_book_by_name(self, fileName: str) -> LBook:
        book = self.books.find_one(filter={"fileName": fileName})
        if book is None:
            return None
        return self.__get_local_book__(book)

    def get_book_by_hash(self, bhash: str) -> LBook:
        book = self.books.find_one(filter={"hash": bhash})
        if book is None:
            return None
        return self.__get_local_book__(book)


    def update_by_name(self, fileName: str, book: LBook):
        return self.books.update_one(filter={"fileName": fileName}, update={"$set": {
            "tg_id": book.tg_id,
            "dlg_id": book.dlg_id,
            "msg_id": book.msg_id,
            "size": book.size,
            "doctype": book.doctype
        }})

    def update_keys(self, file_id, keys):
        return self.books.update_one(filter={"tg_id": file_id}, update={"$set": {
            "keys": keys
        }})

    def update_hash(self, file_id, hash):
        return self.books.update_one(filter={"tg_id": file_id}, update={"$set": {
            "hash": hash
        }})

    # def delete_all (self):
    #    self.books.drop()

    def delete_dummy(self, file_id=None):
        self.books.delete_many(filter={"tg_id": file_id})

    def get_dialog_by_id(self, id: int):
        return self.dialogs.find_one(filter={"tg_id": id})

    def add_dialog(self, dlg: LDialog) -> LDialog:
        try:
            dialog = self.get_dialog_by_id(dlg.tg_id)

            if dialog is None:
                self.dialogs.insert_one({"name": dlg.name, "tg_id": dlg.tg_id})
                return dlg
            else:
                return self.__get_local_dialog__(dialog)
        except Exception as ex:
            logging.exception(ex)
            raise ex

    def update_dialog(self, dlg:LDialog):
        if dlg is not None:
            self.dialogs.update_one(filter={"tg_id": dlg.tg_id},update={"$set":{"name":dlg.name, "tg_id":dlg.tg_id, "max_msg_dt":dlg.max_msg_dt}})

    def get_message_by_id(self, dlg_id: int, tg_id: str):
        return self.messages.find_one(filter={"tg_id": tg_id, "dlg_id": dlg_id })

    def add_message_if_not_exists(self, msg: LMessage) -> None:
        if self.get_message_by_id(tg_id=msg.tg_id, dlg_id=msg.dlg_id) is None:
            self.messages.insert_one(
                {
                    "dlg_id": msg.dlg_id,
                    "text": msg.text,
                    "tg_id": msg.tg_id,
                    "dt": msg.dt
                }
            )

    def add_book(self, file: LBook):
        self.books.insert_one({"fileName": file.filename, "tg_id": file.tg_id,
                               "doctype": file.doctype, "size": file.size,
                               "dlg_id": file.dlg_id, "msg_id": file.msg_id})

    def add_tag(self, tg_id, tags):
        self.books.update_one(
            filter={"tg_id": tg_id}, update={"$set": {
            tags: tags}
        })


    def add_key(self, key):
        self.keys.insert_one({"key": key})



    # def add_book(self, fileName: str, docType: str):
    #    self.books.insert_one({"fileName": fileName, "docType": docType})

    def get_books(self) -> [LBook]:
        cursor: Cursor = self.books.find()
        r = [self.__get_local_book__(c) for c in cursor]
        cursor.close()
        return r

    def search_book(self, filename=None, hash=None, publisher=None, like=False) -> [LBook]:
        bfilter = {}
        if filename:
            bfilter["filename"] = filename
        if hash:
            bfilter["hash"] = hash
        if publisher:
            bfilter["books_meta.Publisher"] = publisher

        cursor: Cursor = self.books.find(
                filter=bfilter
        )
        r = [self.__get_local_book__(c) for c in cursor]
        cursor.close()
        return r

    def get_books_empty_keys(self) -> [LBook]:
        cursor: Cursor = self.books.find(filter={"keys": None})
        r = [self.__get_local_book__(c) for c in cursor]
        cursor.close()
        return r

    def get_books_empty_hash(self) -> [LBook]:
        cursor: Cursor = self.books.find(filter={"hash": None})
        r = [self.__get_local_book__(c) for c in cursor]
        cursor.close()
        return r

    def get_books_by_doctype(self, docType: str) -> [LBook]:
        cursor: Cursor = self.books.find(filter={"docType": docType})
        r = [self.__get_local_book__(c) for c in cursor]
        cursor.close()
        return r

    def __del__(self):
        self.client.close()

    def get_max_message_dt(self, dlg_id):
        cursor: Cursor = self.messages.find(filter={"dlg_id":dlg_id}, sort=[("dt", -1)], limit=1)
        c = cursor.next()
        if c is None:
            return None

        r = self.__get_local_message__(c)
        return r

    def get_publisher_by_id (self, publisher):
       return self.publishers.find_one(filter={"name": publisher}, limit=1)

    def get_publishers (self):
       cursor = self.publishers.find()
       r = [c["name"] for c in cursor]
       cursor.close()
       return r

    def add_publisher(self, publisher):
        if self.get_publisher_by_id(publisher) is None:
            self.publishers.insert_one(
                {
                    "name": publisher
              }
            )

    def get_keys(self):
        cursor = self.keys.find()
        r = [c["key"] for c in cursor]
        cursor.close()
        return r

    def update_books_meta (self, book: LBook, meta):
        if meta:
            logging.info (f"updating meta book: {book.tg_id} meta:{meta}")
            return self.books.update_one(filter={"tg_id": book.tg_id}, update={"$set": {
                "books_meta": meta
            }})


class MongoTest(unittest.TestCase):
    def setUp(self) -> None:
        self.dao = BooksStorage("books_test")

    def test_delete_dummy(self):
        self.dao.delete_dummy()


    def test0(self):
        doc = self.dao.get_book_by_name("doc1.pdf")
        print(doc)

    def test1(self):
        name = "doc3.pdf"
        docType = "pdf"

        if self.dao.get_book_by_name(name) is None:
            self.dao.add_book(name, docType)

    def test2(self):
        r = self.dao.get_books_by_doctype("pdf")
        print(r)

    def test_max_message(self):
        r = self.dao.get_max_message_dt(-1001075040616)
        print (r)

    def test_delete2(self):
        #self.dao.books.delete_many(filter={"dlg_id": {"$lt": 0}})
        pass

    def test_delete3(self):
        #self.dao.messages.drop()
        pass

    def test_delete4(self):
        #self.dao.books.delete_many(filter={"dlg_id": {"$lt": 0}})
        pass


    def test_publisher(self):
        name = "test1"
        self.dao.publishers.delete_many(filter= {})
        self.dao.add_publisher(name)

        p = self.dao.get_publisher_by_id(name)
        self.assertTrue(p is not None)
        self.assertTrue(p["name"] == name)

        self.dao.add_publisher(name+"1")
        self.dao.add_publisher(name+"2")
        self.dao.add_publisher(name+"1")

        count = self.dao.publishers.count_documents(filter = {})
        self.assertTrue(count == 3)

    def test_add_meta(self):
        pass

    def test_get_books(self):
        books = self.dao.get_books()
        print(books[0])



if __name__ == "__main__":
    unittest.main()
