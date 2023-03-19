import pymongo as pm
import logging, unittest
from pymongo.cursor import Cursor
from local_types import *


class BooksDAO:

    def __init__(self):
        self.client = pm.mongo_client.MongoClient()
        self.db = self.client.get_database("books")
        self.books = self.db.get_collection("books")

    def get_book_by_name(self, fileName: str):
        return self.books.find_one(filter={"fileName": fileName})

    def delete_all (self):
        self.books.drop()

    def add_dialog (self, dlg:Dialog):
        pass

    def add_msg (self, msg: Msg):
        pass
    def add_book(self, file: Book):
        pass


    def add_book(self, fileName: str, docType: str):
        self.books.insert_one({"fileName": fileName, "docType": docType})

    def get_books_by_doctype(self, docType: str):
        cursor:Cursor = self.books.find(filter={"docType": docType})
        r = [c for c in cursor]
        cursor.close()
        return r

    def __del__(self):
        self.client.close()


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

    def test_delete(self):
        self.dao.delete_all()

if __name__ == "__main__":
    unittest.main()
