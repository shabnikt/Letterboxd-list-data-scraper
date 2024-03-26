import os
from os.path import join, dirname, isfile
from dotenv import load_dotenv

from src.database.database import DB
from src.csv.csv_processor import CsvProcessor
from src.csv.download_csv import download_csv


class App:
    boxd_db: DB

    def __init__(self):
        self.boxd_db = DB()
        self.get_ignore_file()

    def get_ignore_file(self):
        ignore_file = join(dirname(dirname(__file__)), "ignore_list.txt")
        if isfile(ignore_file):
            with open(ignore_file, 'r') as file:
                self.ignore_list = [line.strip() for line in file.readlines()]
        else:
            self.ignore_list = []

    def open_files(self):
        folder_path = join((dirname(dirname(__file__))), 'letterboxd-data\\lists')

        csv_files = os.listdir(folder_path)
        csv_files = [file for file in csv_files if os.path.splitext(file)[0] not in self.ignore_list]
        CsvProcessor(csv_files, self.boxd_db)


if __name__ == "__main__":
    dotenv_path = join(dirname(dirname(__file__)), 'letterboxd.env')
    load_dotenv(dotenv_path)
    if os.getenv("RUN_DOWNLOAD"):
        download_csv()
    if not os.getenv("RUN_DOWNLOAD") or not os.getenv("CREATE_LIST_IGNORE"):
        app = App()
        app.open_files()
