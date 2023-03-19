from telethon import TelegramClient, events
import asyncio
import logging
import dao
import sys

logging.basicConfig(encoding='utf-8', format='%(asctime)s %(levelname)s %(message)s',  level=logging.ERROR)

api_id = 29093757
api_hash = "0d64c0d51b514dd14b4efad572cbb5db"

dao = dao.BooksDAO()



async def main(client):
    fd = open("/tmp/out.txt", "wt")
    #fd = sys.stdout

    try:
        dlgs = await client.get_dialogs()
        for d in dlgs:
            name:str = d.name
            if name.startswith("IT Books"):
                async for msg in client.iter_messages(d):
                    print ("------- begin -----\n", file=fd)
                    #print(msg,file=fd)
                    print (d,file=fd)
                    print (msg, file =fd)

                    client.iter_download()

                    print("------- end -----\n", file=fd)
                    file = msg.file
                    if file is not None and file.name is not None:
                        print ("file name:"+file.name, file = fd)
                        name:str = file.name
                        doc_type = "other"
                        if name.endswith(".pdf"):
                            doc_type = "pdf"
                        elif name.endswith(".epub"):
                            doc_type = "epub"
                        if dao.get_book_by_name(name) is None:
                            path = await msg.download_media("/Volumes/SSD/download")
                            dao.add_book(name, doc_type, msg.date)

                            print (path, file=fd)

                    print("------- end -----\n", file=fd)




        #msgs = await client.get_messages("me")

        #print(msgs)
        await client.send_message('Ilya Malyshev', 'Hello, myself!')
        r = await client.get_me()
        print(r)
    except Exception as e:
        logging.error(e)
        return
    finally:
        fd.close()

client = TelegramClient("im", api_id, api_hash)
with client:
    client.loop.run_until_complete(main(client))


