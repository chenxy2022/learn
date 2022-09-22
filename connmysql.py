"""
需要mysql服务器配置autocommit为True
查看配置语句：.cursor.execute('show variables like "%autocommit%"')
"""
import pandas as pd
import pymysql
from sqlalchemy import create_engine
from urllib.parse import quote_plus as urlquote  # 为了解决密码含有@字符导致engine无法连接数据库
# import xlwings as xw


class QQ_Sql(object):
    def __init__(self):
        para = dict(
            user='Numpy_Pandas_admin',
            password='Npandas123!@#',
            host='39.101.131.154',
            database='Python_Numpy_Pandas_QQ',
            charset='utf8', )
        self._conn = pymysql.Connect(**para)
        self.cursor = self._conn.cursor()
        para['password'] = urlquote(para['password'])
        _db_str = 'mysql+pymysql://{user}:{password}@{host}:3306/{database}?charset={charset}'.format(
            **para)
        self.conn = create_engine(_db_str)

    def _get_df(self, data):
        col = self.cursor.description
        fields = [x[0] for x in col]
        return pd.DataFrame(data, columns=fields)

    def execute(self, *arg):
        self.cursor.execute(*arg)
        if results := self.cursor.fetchall():
            return self._get_df(results)

    def close(self):
        self.cursor.close()
        self._conn.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, exc_trance):
        self.close()


if __name__ == '__main__':
    # df =xw.Range('a1').current_region.options('df',index=0,numbers=int).value
    with QQ_Sql() as q:
        sql = 'select * from ayitemp '
        df = q.execute(sql)
        # df.to_sql('ayitemp',con=q.conn,index=False,if_exists='replace')
        df1 = pd.read_sql(sql, con=q.conn)

    print(df)
    print(df1)
