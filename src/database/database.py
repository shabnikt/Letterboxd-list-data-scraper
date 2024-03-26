from os import getenv

from sqlalchemy import create_engine
from sqlalchemy.sql import text
from sqlalchemy.orm import Session
from pandas import read_sql

from src.help_lib.logfile import log


class DB:
    def __init__(self):

        self.info = {'username': getenv('CONNECTOR_USER', None),
                     'password': getenv('CONNECTOR_PW', None),
                     'host': getenv('CONNECTOR_HOST', None),
                     'port': getenv('CONNECTOR_PORT', None),
                     'dbname': getenv('CONNECTOR_DBNAME', None),
                     }
        self.log_info()

        self.get_engine()

    def log_info(self):
        info_values = ''
        for line, value in self.info.items():
            if line != 'password':
                info_values += f"\n{line.upper().ljust(10)}{value}"

        log.debug(info_values)

    def get_engine(self):
        uri = 'postgresql://' + self.info['username'] + ':' + self.info['password'] + \
              '@' + self.info['host'] + ':' + self.info['port'] + '/' + self.info['dbname']
        self.engine = create_engine(uri)
        self.conn = self.engine.connect()

    def exec(self, sql_statements):
        if type(sql_statements) == str:
            sql_statements = [sql_statements,]
        with Session(self.conn) as session:
            session.begin()
            for statement in sql_statements:
                log.debug(statement)
                session.execute(self.format_sql(statement))
            session.execute(self.format_sql('commit'))
            log.debug('sql statements have been executed.\n')

    def get_data(self, sql_statement):
        """
        Returns result of sql statement
        :param sql_statement:
        :return array of data:
        """
        log.debug(sql_statement)
        with Session(self.conn) as session:
            session.begin()
            data = session.execute(self.format_sql(sql_statement)).fetchall()
            session.execute(self.format_sql('commit'))
            return data

    def get_data_as_df(self, sql_statement):
        log.debug(sql_statement)
        df = read_sql(sql_statement, self.conn)
        return df

    def get_col_from_db(self, col, table):
        if type(col) == list:
            col = ", ".join(col)
        sql_statement = f'select {col} from letterboxd.{table};'
        col_from_db = self.get_data_as_df(sql_statement)
        return col_from_db

    @staticmethod
    def create_insert_statement(table, df):
        columns = ', '.join(df.columns.tolist())
        sql_statement = f'INSERT INTO letterboxd.{table} ({columns}) VALUES '

        for idx_row in range(df.shape[0]):
            try:
                row_values = ', '.join(["'" + i.replace("'", "''") + "'" for i in df.loc[idx_row].to_list()])
            except:
                print()
            sql_statement += f'\n({row_values}), '
        sql_statement = sql_statement[:-2] + ';'
        log.info(f'Add {df.shape[0]} rows to {table} table.')
        return sql_statement

    def update_watched_films(self, watched_list):
        films = "', '".join([film.replace("'", "''") for film in watched_list])
        sql_statement = "UPDATE letterboxd.film " \
                        "SET film_watched = TRUE, film_use = FALSE " \
                        f"WHERE film_name IN ('{films}');"

        log.debug(f'Add {watched_list} to watched films.')
        self.exec(sql_statement)

    def get_joined_tables(self):
        sql_statement = 'SELECT ' \
                          'letterboxd.list.list_name, ' \
                          'letterboxd.list.list_use, ' \
                          'letterboxd.film.film_name, ' \
                          'letterboxd.film.film_page, ' \
                          'letterboxd.film.film_img_url, ' \
                          'letterboxd.film.film_use ' \
                        'FROM ' \
                          'letterboxd.film ' \
                        'INNER JOIN ' \
                          'letterboxd.list_content ' \
                        'ON ' \
                          'letterboxd.film.pk_film = letterboxd.list_content.fk_film ' \
                        'INNER JOIN ' \
                          'letterboxd.list ' \
                        'ON ' \
                          'letterboxd.list_content.fk_list = letterboxd.list.pk_list;'
        joined_tables = self.get_data_as_df(sql_statement)

        return joined_tables

    @staticmethod
    def format_sql(sql_query):
        return text(sql_query)
