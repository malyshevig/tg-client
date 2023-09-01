
import isbnlib
import logging
import unittest
import re


class IsbnExtractor:
    def __init__(self):
        pass

    def get_isbn(self,text):
        lines = text.split("\n")

        isbn_list = []
        for text in lines:

            pos = text.find("ISBN-13")
            if pos > 0:
                pos += len("ISBN-13")

            if pos < 0:
                pos = text.find("ISBN-10")
                if pos > 0:
                    pos += len("ISBN-10")

            if pos < 0:
                pos = text.find("ISBN")
                if pos > 0:
                    pos += len("ISBN")

            if pos >= 0:
                t = text[pos:]
                words = re.findall(r'\b\w+\b', t)
                s = ""
                for w in words:
                    s += w + " "

                w = isbnlib.get_isbnlike(t)
                if len(w)>0:
                    isbn_list.extend(w)

        return isbn_list

class IsbnTest(unittest.TestCase):

    def test1(self):
        t = IsbnExtractor().get_isbn("ISBN 978-5-0056-1871-9,sfsf\n ssffsfsfs:sdssfdsf\n")
        print (t)