# -*- coding:utf-8 -*-

# @Time : 21/11/03 PM 08:49
# @Author : LINZIJIE (linzijie1998@126.com)
# @Filename : __init__.py.py
# @Version: v0.1
# @License: GNU GENERAL PUBLIC LICENSE Version 3
# @Description: 连接MongoDB数据库，并且读取数据

import pymongo


class MyMongodb:
    def __init__(self, ip_address, port=27017):
        """
        初始化设置

        :param ip_address:  MongoDB数据库服务器IP地址
        :param port:        MongoDB数据库服务器端口，默认为27017
        """
        self.link = 'mongodb://' + ip_address + ':' + str(port) + '/'
        self.client = pymongo.MongoClient(self.link)

    def get_collection_data(self, database_name, collection_name, username=None, pwd=None):
        """
        获取MongoDB数据库指定集合的数据, 默认不进行验证.
        连接成功时返回集合数据, 该数据可通过for循环迭代, 也可使用list()转换为list类型

        :param database_name:   需要连接的MongoDB数据库名称
        :param collection_name: 需要连接的MongoDB数据集合
        :param username:        登录验证的用户名，默认为空
        :param pwd:             登录验证的密码，默认为空
        :return:                连接成功时返回MongoDB数据集合, 类型为pymongo.cursor.Cursor, 连接失败时抛出异常
        """
        # noinspection PyBroadException
        try:
            database = self.client[database_name]
            database.authenticate(username, pwd)
            collection = database[collection_name]
            if collection:
                print(f'Mongodb collection connect SUCCESS: \n'
                      f'\tdatabase_name: {database_name}\n\tcollection_name: {collection_name}')
                if collection_name == 'MRD_VQ':
                    return collection.find().sort('Datetime_301')
                else:
                    return collection.find().sort('Datetime')
        except Exception as e:
            raise Exception("Mongodb collection connect ERROR : " + str(e))

    def close_mongodb_client(self):
        """
        关闭MongoDB数据库连接
        """
        self.client.close()
