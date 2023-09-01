
from hash import book_hash
import os

#dir = "/Users/im/Downloads"
dir = "/Volumes/SSD/zip"

ext = [".torrent", ".pdf", "pub", ".PDF", "pptx", "jpg", "zip", "rar"]
passed = {}

def filter(fname:str) -> bool:
    if fname is None:
        return False
    fname=fname.lower()

    if fname.startswith('.'):
        return False

    for e in ext:
        if fname.endswith(e):
            return True
    return False

print (filter ("addsdsf.epub"))

for f in os.listdir(dir):
    fd = os.path.join(dir,f)
    is_dir = os.path.isdir(fd)
    tm = os.path.getctime (fd)

    if os.path.isfile(fd) and filter(f):
        #print (f)
        bhash = book_hash.hash_file(fd)

        ctime = os.path.getctime(fd)
        src_file_meta = passed.get(bhash)

        if src_file_meta:
            src_fd, src_ctime = src_file_meta
            to_remove = fd
            if src_ctime > ctime:
                passed [bhash] = (fd,ctime)
                to_remove = src_fd

            print (f"to be removed {to_remove} src_file={ passed[bhash][0]} ")
            os.remove(to_remove)
        else:
            passed[bhash]=(fd, ctime)