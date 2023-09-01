import pdftotext
import os

def print_pdf(file):
    # Load your PDF
    with open(file, "rb") as f:
        pdf = pdftotext.PDF(f)

    # If it's password-protected
    with open(file, "rb") as f:
        pdf = pdftotext.PDF(f, "secret")

    # How many pages?
    print(len(pdf))

    # Iterate over all the pages
    for page in pdf:
        print(page)

    # Read some individual pages
    print(pdf[0])
    print(pdf[1])

    # Read all the text into one string
    print("\n\n".join(pdf))


dir = "/Volumes/SSD/download"
for f in os.listdir("/Volumes/SSD/download")[:10]:
    try:
        if f.endswith(".pdf"):
            text = print_pdf(dir+"/"+f)
            print(f"file = {f}  text={text}")
            break
    except Exception as ex:
        print(ex)