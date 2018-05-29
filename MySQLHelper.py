from mysql.connector.connection import MySQLConnection
from mysql.connector import pooling
from typing import Optional


class MySQLHelper(object):

    __instance = None
    __db_config = {
            'user': 'root',
            'password': '123qwe',
            'host': '127.0.0.1',
            'database': 'nix_ru_scrap'
        }

    @staticmethod
    def get_instance(**kwargs):
        if MySQLHelper.__instance is None:
            MySQLHelper.__instance = MySQLHelper(**kwargs)
        return MySQLHelper.__instance

    @staticmethod
    def connection_inst(**kwargs) -> MySQLConnection:
        return MySQLHelper.get_instance(**kwargs).get_connection()

    def __init__(self, **kwargs):
        self._init_kwargs(**kwargs)
        self._pool = pooling.MySQLConnectionPool(pool_size=31, **self.__db_config)

        if MySQLHelper.__instance is not None:
            raise Exception('Not none singleton instance')
        else:
            MySQLHelper.__instance = self

    def get_connection(self) -> Optional[MySQLConnection]:
        if self._pool is None:
            return None
        return self._pool.get_connection()

    def _init_kwargs(self, **kwargs):
        if kwargs is None or len(kwargs) == 0:
            return
        if 'user' in kwargs:
            self.__db_config['user'] = kwargs['user'] or 'root'
        if 'password' in kwargs:
            self.__db_config['password'] = kwargs['password'] or '123qwe'
        if 'host' in kwargs:
            self.__db_config['host'] = kwargs['host'] or '127.0.0.1'
        if 'database' in kwargs:
            self.__db_config['database'] = kwargs['database'] or 'nix_ru_scrap'
