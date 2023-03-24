from local_types import *
import logging
from dao import BooksDAO
import re
import nltk
import pymorphy2 as pm

anal_uk = pm.MorphAnalyzer(lang="uk")
anal_ru = pm.MorphAnalyzer(lang="ru")

def norm(s):
    if ord(s[0])<255:
        return anal_uk.parse(s)[0].normal_form
    else:
        return anal_ru.parse(s)[0].normal_form




def get_keys(name:str):
    idx = name.rfind(".",)
    s = name[:idx]

    keys = re.split(r"[-;,._\s]\s*",s)
    keys = [norm(k) for k in keys if k != ""]
    return keys


def index():
    dao = BooksDAO()
    books = dao.get_books()
    print(len(books))
    for b in books:
        keys = get_keys(b.filename)
        dao.update_keys(b.tg_id,keys)
        print(f"update {b.tg_id} {keys}")




index()


