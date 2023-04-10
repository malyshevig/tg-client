import logging
import unittest
from olclient.openlibrary import OpenLibrary
import olclient.common as common

ol = OpenLibrary(base_url=u'http://localhost:8080')
#author_olid = ol.Author.get_olid_by_name('Dan Brown')
#author_obj = ol.get(author_olid)

ol.

book = common.Book(
    title="Warlight: A novel",
    authors=[common.Author(name="Michael Ondaatje")],
    publisher="Deckle Edge",
    publish_date="2018",
)
book.add_id('isbn_10', '0525521194')
book.add_id('isbn_13', '978-0525521198')

# Create a new book
new_book = ol.create_book(book)


