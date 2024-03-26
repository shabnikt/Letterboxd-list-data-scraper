from os.path import join, dirname
from pandas import DataFrame, concat
from uuid import uuid4
import re

from src.parsers.film_streaming_parser import HdRezkaScrapper
from src.parsers.film_image_parser import ImageParser
from src.help_lib.logfile import log


class CsvProcessor:
    prepared_film_df: DataFrame
    prepared_list_df: DataFrame
    prepared_film_list_df: DataFrame
    list_table_from_db: DataFrame
    sql_statements: list

    def __init__(self, csv_files, boxd_db):
        self.boxd_db = boxd_db
        self.prepared_df = DataFrame(columns=['film_name', 'film_page', 'list_name'])
        self.extra_lists = DataFrame(columns=['film_name', 'list_name'])

        if csv_files:
            self.process_files(csv_files)

        self.update_watched_films()

    def process_files(self, csv_files):
        processed_films = self.boxd_db.get_col_from_db(col='film_name', table='film')["film_name"].to_list()
        for file in csv_files:
            log.debug(file)
            film_df = self.create_dataframe(file)

            new_film_list = film_df.loc[:, ['film_name', 'list_name']]
            self.extra_lists = concat([self.extra_lists, new_film_list], ignore_index=True)

            film_df = film_df.loc[~film_df['film_name'].isin(processed_films)]
            processed_films += film_df['film_name'].tolist()

            self.prepared_df = concat([self.prepared_df, film_df], ignore_index=True)

        self.add_film_imgs()
        self.add_film_streaming()

        self.prepare_film_df_to_db()
        self.prepare_list_df_to_db()
        self.prepare_film_list_df_to_db()

        self.create_sql_statements()

        self.boxd_db.exec(self.sql_statements)

    def create_dataframe(self, file_name, watched=False):
        if watched:
            file_path = join(dirname(dirname(dirname(__file__))), f'letterboxd-data\\{file_name}')
        else:
            file_path = join(dirname(dirname(dirname(__file__))), f'letterboxd-data\\lists\\{file_name}')

        file_rows = [b_row.decode('utf-8').replace('\r\n', '').rstrip(',') for b_row in open(file_path, "rb").readlines()]
        if 'Letterboxd list export' in file_rows[0]:
            list_name = file_rows[2].split(',')[1]
            df_start_idx = file_rows.index('Position,Name,Year,URL,Description')
            film_df = self.df_from_list(file_rows[df_start_idx:], list_name)
        elif 'Date,Name,Year,Letterboxd URI' in file_rows[0]:
            film_df = self.df_from_list(file_rows[0:])
        else:
            raise 'Wrong file.'
        return film_df

    @staticmethod
    def df_from_list(file_rows, list_name=''):
        rows_list = list()
        for row in file_rows:
            separated_row = row.replace('"', "").replace(", ", "change_it").split(',')
            separated_row[1] = separated_row[1].replace("change_it", ", ")
            cleaned_row = [f"{separated_row[1]}, {separated_row[2]}", separated_row[3]]
            rows_list.append(cleaned_row)
        film_df = DataFrame(rows_list[1:], columns=['film_name', 'film_page'])
        if list_name:
            film_df['list_name'] = list_name
        return film_df

    def add_film_imgs(self):
        log.debug('Start parse images.')
        img_parser = ImageParser()
        img_parser.parse_list(self.prepared_df['film_page'].to_list(), 'Parse film images:')
        film_imgs = img_parser.get_film_imgs()
        self.prepared_df = self.prepared_df.assign(film_img_url=film_imgs)

    def add_film_streaming(self):
        log.debug('Start parse streaming links.')
        hd_rezka = HdRezkaScrapper(self.prepared_df['film_name'].to_list())
        film_streaming_links = hd_rezka.get_links()
        self.prepared_df = self.prepared_df.assign(film_streaming=film_streaming_links)

    def prepare_film_df_to_db(self):
        log.debug('Preparing films to db.')
        pk_film = [str(uuid4()) for idx_row in range(self.prepared_df.shape[0])]

        self.prepared_film_df = self.prepared_df
        self.prepared_film_df.insert(0, "pk_film", pk_film, True)
        self.prepared_film_df = self.prepared_df.drop("list_name", axis=1)
        self.prepared_film_df["film_watched"] = "False"
        self.prepared_film_df["film_use"] = "True"

    def prepare_list_df_to_db(self):
        log.debug('Preparing lists to db.')
        self.list_table_from_db = self.boxd_db.get_col_from_db(col=['pk_list', 'list_name'], table='list')
        self.list_table_from_db['pk_list'] = self.list_table_from_db['pk_list'].apply(str)
        list_name_from_db = self.list_table_from_db['list_name'].to_list()
        all_new_list_name: list = self.extra_lists['list_name'].to_list()

        unic_new_list_name = list(set(all_new_list_name).difference(set(list_name_from_db)))
        unic_new_pk_film = [str(uuid4()) for new_list_name in unic_new_list_name]
        unic_new_list_address = [self.create_list_address(new_list_name) for new_list_name in unic_new_list_name]

        self.prepared_list_df = DataFrame({'pk_list': unic_new_pk_film, 'list_name': unic_new_list_name,
                                           'list_address': unic_new_list_address})
        self.list_table_from_db = concat([self.list_table_from_db, self.prepared_list_df[['pk_list', 'list_name']]],
                                         ignore_index=True)
        self.prepared_list_df['list_use'] = "True"

    def prepare_film_list_df_to_db(self):
        log.debug('Preparing list_content to db.')
        list_table_from_db = self.boxd_db.get_col_from_db(col=['fk_film', 'fk_list'], table='list_content')
        list_table_from_db['fk_film'] = list_table_from_db['fk_film'].apply(str)
        list_table_from_db['fk_list'] = list_table_from_db['fk_list'].apply(str)

        check_films = self.boxd_db.get_col_from_db(col=['pk_film', 'film_name'], table='film')
        check_films['pk_film'] = check_films['pk_film'].apply(str)
        check_films = concat([check_films, self.prepared_film_df[['pk_film', 'film_name']]], ignore_index=True)
        check_films.drop_duplicates(subset=['pk_film'])

        films_for_list_content = self.extra_lists['film_name'].to_list()
        lists_for_list_content = self.extra_lists['list_name'].to_list()

        new_fk_film = list()
        for film in films_for_list_content:
            fk_list = check_films[check_films['film_name'] == film]['pk_film'].to_list()
            if len(fk_list) != 0:
                new_fk = fk_list[0]
                new_fk_film.append(new_fk)
        new_fk_list = list()
        for film in lists_for_list_content:
            fk_list = self.list_table_from_db[self.list_table_from_db['list_name'] == film]['pk_list'].to_list()
            if len(fk_list) != 0:
                new_fk = fk_list[0]
                new_fk_list.append(new_fk)
        self.prepared_film_list_df = DataFrame({'fk_film': new_fk_film, 'fk_list': new_fk_list})
        self.drop_film_list_duplicates(list_table_from_db)

    @staticmethod
    def create_list_address(list_name):
        cleaned_list_name = re.sub(r'[а-яА-Я]|[^\w\-–_ ]', '', list_name).strip().lower()
        list_address = cleaned_list_name.replace(' – ', '_').replace(' - ', '_')\
                                        .replace('–', '_').replace('-', '_').replace(' ', '_')
        return list_address

    def drop_film_list_duplicates(self, list_table_from_db):
        list_table_from_db['is_new'] = False
        self.prepared_film_list_df['is_new'] = True
        temp_film_list = concat([list_table_from_db, self.prepared_film_list_df], ignore_index=True)
        temp_film_list = temp_film_list.drop_duplicates(subset=['fk_film', 'fk_list'])
        self.prepared_film_list_df = temp_film_list[temp_film_list['is_new'] == True].reset_index(drop=True)
        self.prepared_film_list_df = self.prepared_film_list_df.drop('is_new', axis=1)

    def create_sql_statements(self):
        self.sql_statements = list()
        for statement_case in (('film', self.prepared_film_df), ("list", self.prepared_list_df), ("list_content", self.prepared_film_list_df)):
            if statement_case[1].shape[0] != 0:
                self.sql_statements.append(self.boxd_db.create_insert_statement(table=statement_case[0], df=statement_case[1]))

    def update_watched_films(self):
        watched_films = self.create_dataframe('watched.csv', watched=True)["film_name"].tolist()
        # watched_films.remove('"Are You There God?'+" It's Me, "+'Margaret.", 2023')
        # watched_films.remove('"Luxembourg, Luxembourg", 2022')
        # watched_films.remove('"Synecdoche, New York", 2008')
        self.boxd_db.update_watched_films(watched_films)
