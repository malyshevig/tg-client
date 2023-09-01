
from flask import Flask, jsonify, request
from books_storage.storage import BooksStorage

app = Flask("books")

dao = BooksStorage()

@app.route('/')
@app.route('/test')
def test():
    return jsonify({"a":1})


@app.route("/books")
def books():
    books = dao.get_books()
    return jsonify(books)

@app.route("/publishers")
def publishers():
    publishers = dao.get_publishers()
    return jsonify(sorted(publishers))


@app.route("/book/hash/<hash>")
def book_by_hash(hash):
    books = dao.search_book(hash=hash)
    return jsonify(books)

@app.route("/book/file/<name>")
def book_by_name(name):
    books = dao.search_book(filename=name)
    return jsonify(books)

@app.route("/book/publisher/<name>")
def book_by_publisher(name):
    books = dao.search_book(publisher=name)
    return jsonify(books)

@app.route("/book/publisher/<name>")
def book_like_publisher(name):
    books = dao.search_book(publisher=name)
    return jsonify(books)

if __name__ == "__main__":
    app.run(host="127.0.0.1", port=8081, debug=True)