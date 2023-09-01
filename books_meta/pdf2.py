# some python file
import textract, os


def gettext(file: str) -> str:
    text = textract.process(file)
    return text

dir = "/Volumes/SSD/download"
for f in os.listdir("/Volumes/SSD/download")[:5]:
    try:
        if f.endswith(".pdf"):
            text = gettext(dir+"/"+f)
            print(f"file = {f}  text={text}")

    except Exception as ex:
        print(ex)
