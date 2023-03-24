import pymongo as pm
import logging, unittest
from pymongo.cursor import Cursor
from local_types import *


class BooksDAO:

    def __init__(self):
        self.client = pm.mongo_client.MongoClient()
        self.db = self.client.get_database("books")
#        self.db = self.client.get_database("books_test")


        self.books = self.db.get_collection("books")
        self.dialogs = self.db.get_collection("dialogs")
        self.messages = self.db.get_collection("messages")

    def __get_local_book__(self, book: dict) -> LBook:
        filename = book.get("fileName")
        tg_id = book.get("tg_id")
        msg_id = book.get("msg_id")
        size = book.get("size")
        doctype = book.get("doctype")
        dlg_id = book.get("dlg_id")

        return LBook(filename=filename, tg_id=tg_id, msg_id=msg_id, dlg_id=dlg_id, size=size, doctype=doctype)

    def __get_local_message__(self, msg: dict) -> LMessage:
        text = msg.get("text")
        tg_id = msg.get("tg_id")
        dt = msg.get("dt")
        dlg_id = msg.get("dlg_id")

        return LMessage(dlg_id,dt,text,tg_id)

    def __get_local_dialog__(self, dlg: dict) -> LDialog:
        name = dlg.get("name")
        tg_id = dlg.get("tg_id")
        max_msg_dt = dlg.get("max_msg_dt")
        dlg_type = dlg.get("dlg_type")

        return LDialog(name=name,tg_id=tg_id,dlg_type=dlg_type, max_msg_dt=max_msg_dt)


    def get_book_by_name(self, fileName: str):
        book = self.books.find_one(filter={"fileName": fileName})
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


    # def delete_all (self):
    #    self.books.drop()

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

    def get_message_by_id(self, id: int):
        return self.messages.find_one(filter={"tg_id": id})

    def add_message(self, msg: LMessage) -> None:
        if self.get_message_by_id(msg.tg_id) is None:
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

    # def add_book(self, fileName: str, docType: str):
    #    self.books.insert_one({"fileName": fileName, "docType": docType})

    def get_books(self) -> [LBook]:
        cursor: Cursor = self.books.find()
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


class MongoTest(unittest.TestCase):
    def setUp(self) -> None:
        self.dao = BooksDAO()

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


if __name__ == "__main__":
    unittest.main()
