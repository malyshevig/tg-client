import hashlib


def hash_file(path:str) -> str:
    res = None

    with open(path, "rb") as f:
        md5 = hashlib.md5()

        while data := f.read(1024):
            md5.update(data)

        res = md5.hexdigest()

    return res
