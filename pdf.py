# importing required modules
import PyPDF2
import re
import unittest
import pdftotext
import isbnlib
import isbn


# ISBN-10
# ISBN-13


class PdfReader:

    def __init__(self, file):
        self.file = file

    def get_pages(self) -> iter:
        with open(self.file, "rb") as f:
            pdf = pdftotext.PDF(f)

        return pdf

    def extract_isbn2(self,text: str):
        return isbn.IsbnExtractor().get_isbn(text)


def __extract_isbn(text: str):

    pos = text.find("ISBN-13")
    if pos > 0:
        pos += len("ISBN-13")

    if pos < 0:
        pos = text.find("ISBN-10")
        if pos > 0:
            pos += len ("ISBN-10")

    if pos < 0:
        pos = text.find("ISBN")
        if pos > 0:
            pos += len("ISBN")



    if pos >= 0:
        text = text[pos:pos + 25]
        #   splits = text.split()

        #                isbn_cand = splits[1]
        isbn_cand = text.replace(" ", "")
        match_result = re.match(r"\d*-\d*-\d*-\d*-\d*", isbn_cand)
        if match_result is not None:
            return match_result.group()
        else:
            match_result = re.match(r"\d*", isbn_cand)
            if match_result is not None:
                return match_result.group()
    return None


def get_isbn(file: str):
    reader = PdfReader(file)

    for p in reader.get_pages():
        try:
            text = p
            isbn = reader.extract_isbn2(text)
            if len(isbn)>0:
                return isbn

        except BaseException as ex:
            print(ex)


class PdfTest(unittest.TestCase):

    def test1(self):
        import os

        dir = "/Volumes/SSD/download"
        for f in os.listdir("/Volumes/SSD/download"):
            try:
                if f.endswith(".pdf"):
                    isbn = get_isbn(dir + "/" + f)
                    print(f"file = {f} isbn={isbn}")
            except Exception as ex:
                print(ex)

    def test2(self):
        import os

        f = "/Volumes/SSD/download/Practical Shader Development Kyle Halladay.pdf"
        f = "/Volumes/SSD/download/Геймдизайн.pdf"
        f = "/Volumes/SSD/download/Высоконагруженные приложения.pdf"

        isbn_list = get_isbn(f)
        for isbn in isbn_list:
            print (isbnlib.meta(isbn))
        print(f"file = {f} isbn={isbn}")


