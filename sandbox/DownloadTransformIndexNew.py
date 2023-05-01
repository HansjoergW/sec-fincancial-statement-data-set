"""
Prompt:
ich benötige effiziente logik in python, welche folgende Aufgabe erfüllt. Als erstes müssen von einer WebSeite ca. 50 zip files heruntergeladen werden. Jedes Zip file ist um die 50MB gross. In dieser Zip Datei befinden sich je 3 CSV Dateien, die mit Pandas gelesen werden müssen und anschliessend als parquet gespeichert werden sollen. Wie mache ich das am effizientesten? mit Threads, multprocessing, der joblib?

"""
import logging
import time

from secfsdstools.a_utils.downloadutils import UrlDownloader
from secfsdstools.c_download.secdownloading import SecZipDownloader

if len(logging.root.handlers) == 0:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(module)s  %(message)s",
        handlers=[
            logging.StreamHandler()
        ]
    )
downloader = SecZipDownloader(zip_dir="./zips/",
                              urldownloader=UrlDownloader(user_agent='me@host.com'))

start = time.time()
downloader.download()
print(time.time() - start)




#mit Threadvariatne: Problem: 12 Dateien haben keine Daten, d.h. Download ist fehlgeschlagenm, aber keine log eintrag

# mit Threadvariante 2 processes / chunksize 2 -> 104 Sekunden
# mit Thread 3 processes / 3 chcunksize -> 83 sek / 2. Lauf 89
# mit Thread 4 processes / 4 chunksize -> 88 sek
# mit Thread 6 processes / 6 chunksize ->  sek 104

# mit serial Mode -> Total 110 sekunden

# import pandas as pd
# from joblib import Parallel, delayed
#
# zip_files = ['file1.zip', 'file2.zip', ...] # Liste der Zip-Dateien, die gelesen werden sollen
#
# def read_csv(file):
#     # Lesen der CSV-Dateien und Speichern als Parquet
#     df = pd.read_csv(file)
#     df.to_parquet(file[:-4] + '.parquet')
#
# # Lesen der CSV-Dateien parallel ausführen
# num_cores = multiprocessing.cpu_count()
# Parallel(n_jobs=num_cores)(delayed(read_csv)(file) for file in zip_files)
