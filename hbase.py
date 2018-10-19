#!/usr/bin/python
# coding:utf-8

from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol

from hbase import Hbase
from hbase.ttypes import ColumnDescriptor, Mutation,BatchMutation

class HbaseClient(object):
    def __init__(self, host='10.255.*.*', port=9090):
        # 为了提高性能，使用Buffer封装原生的socket连接
        transport = TTransport.TBufferedTransport(TSocket.TSocket(host, port))
        protocol = TBinaryProtocol.TBinaryProtocol(transport)
        self.client = Hbase.Client(protocol)
        transport.open()

    def get_tables(self):
        """
        获取所有表
        """
        return self.client.getTableNames()

    def create_table(self, table, *columns):
        """
        创建表
        """
        self.client.createTable(table, map(lambda column: ColumnDescriptor(column), columns))

    def enable_Table(self,table):
        """
        启用表,如果之前表未关闭，会引发IOError错误
        """
        return self.client.enableTable(table)

    def disable_Table(self,table):
        """
        关闭表
        """
        return self.client.disableTable(table)

    def isTableEnabled(self,table):
        """
        验证表是否被启用，返回布尔
        """
        return self.client.isTableEnabled(table)

    def getTableRegions(self,table):
        """
        获取所有与表关联的regions，返回一个TRegionInfo对象列表
        """
        return self.client.getTableRegions(table)

    def deletetable(self,table):
        """
        删除表,表不存在或者表未被禁用，会引发IOError错误
        """
        self.client.disableTable(table)
        self.client.deleteTable(table)

    def put(self, table, row, columns, attributes=None):
        """
        添加记录
        """
        self.client.mutateRow(table, row, map(lambda (k,v): Mutation(column=k, value=v), columns.items()), attributes)

    def get(self,table,row,columns,attributes=None):
        """
        获取数据列表
        """
        return self.client.get(table,row,columns,attributes)

    def get_new_timestamp(self,table,row,attributes=None):
        """
        获取表中指定行,所有列族最新更新的时间戳
        """
        timestamp = 0
        try:
            result = self.client.getRow(table,row,attributes)[0].columns.values()
            for ele in result:
                if ele.timestamp > timestamp:
                    timestamp = ele.timestamp
        except:
            return None
        return timestamp

    def getRowWithColumns(self,table,row,columns,attributes=None):
        """
        获取表中指定行与指定列在最新时间戳上的数据,返回一个hbase.ttypes.TRowResult对象列表
        """
        return self.client.getRowWithColumns(table, row, columns, attributes)

    def getVer(self,table,row,columns,numVersions=1,attributes=None):
        """
        获取数据列表,可以获取多个版本，numVersions为要检索的版本数量
        """
        return self.client.getVer(table,row,columns,numVersions,attributes)

    def deleteAll(self,table,row,columns,attributes=None):
        """
        删除指定表指定行与指定列的所有数据
        """
        return self.client.deleteAll(table,row,columns,attributes)

    def deleteAllTs(self,table,row,columns,timestamp,attributes=None):
        """
        删除指定表指定行与指定列中，小于等于指定时间戳的所有数据
        """
        return self.client.deleteAllTs(table,row,columns,timestamp,attributes)

    def deleteAllRow(self,table,row,attributes=None):
        """
        删除整行数据
        """
        return self.client.deleteAllRow(table,row,attributes)

    def deleteAllRowTs(self,table,row,timestamp,attributes=None):
        """
        删除整行数据
        """
        return self.client.deleteAllRowTs(table,row,timestamp,attributes)

    def scan(self, table, start_row="", columns=None, attributes=None):
        """
        获取记录,指定开始行到最后。
        """
        scanner = self.client.scannerOpen(table, start_row, columns, attributes)
        while True:
            r = self.client.scannerGet(scanner)
            if not r:
                break
            yield dict(map(lambda (k, v): (k, v.value),r[0].columns.items())),r[0].row

    def scannerOpenWithStop(self, table, start_row="", end_row="",columns=None, attributes=None):
        """
        获取记录,指定开始行到最后。
        """
        scanner = self.client.scannerOpenWithStop(table, start_row, end_row,columns, attributes)
        while True:
            r = self.client.scannerGet(scanner)
            if not r:
                break
            yield dict(map(lambda (k, v): (k, v.value),r[0].columns.items()))

    def scannerOpenWithPrefix(self, table, start_prefix="",columns=None, attributes=None):
        """
        在指定表中，扫描具有指定前缀的行，扫描指定列的数据
        """
        scanner = self.client.scannerOpenWithPrefix(table, start_prefix,columns, attributes)
        while True:
            r = self.client.scannerGet(scanner)
            if not r:
                break
            yield dict(map(lambda (k, v): (k, v.value),r[0].columns.items()))

    def getColumnDescriptors(self,table):
        '''
        列的描述,获取所有列族信息，返回字典
        '''
        return self.client.getColumnDescriptors(table)

if __name__ == "__main__":
    client = HbaseClient("10.255.*.*", 9090)
    # client.create_table("raw_info", "pre","avl","yara","vector")
    # client.create_table("test_test","name")
    # client.create_table("test", "time","pe","mail","basic","detect","office","sign")
    # client.deletetable("test")
    print client.get_tables()
    # print client.get('test_test',"1","name")

    # print client.getColumnDescriptors("test")
    # client.enable_Table("test")
    # print client.isTableEnabled("test")
    # print client.get("test","1","data:age")[0].value
    # print client.getVer("test","1","data:age",1)[0].value
    # print client.get_new_timestamp("test","2")
    # print client.getRowWithColumns("test","1",["data:age","name:"])
    # client.deleteAllRow("test","1")
    # print client.getTableRegions("raw_info")

    # client.put("test_test", "1", {"name:":"zhangsan"})
    # client.put("test_test", "2", {"name:":"lisi"})
    # client.put("test", "3", {"name:": "wangwu", "data:age": "55", "data:city": "beijing"})
    # client.put("test", "4", {"name:": "chenliu", "data:age": "66", "data:city": "beijing"})
    # client.put("test", "5", {"name:": "qi", "data:age": "77", "data:city": "haerbin"})

    for k,v in client.scan("test_test",columns=["name"]):
       print k,v