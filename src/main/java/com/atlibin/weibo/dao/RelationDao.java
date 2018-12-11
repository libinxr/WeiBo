package com.atlibin.weibo.dao;

import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by 11610 on 2018/12/2.
 */
public class RelationDao {
    public void createTable(Connection connection) throws IOException {
        Admin admin = connection.getAdmin();
        TableName tableName = TableName.valueOf("atweibo:relation");
        //判断表是否存在
        if(admin.tableExists(tableName)){
            return;
        }
        //表的描述器
        HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);
        //列族描述器
        //创建关注的人的列族
        HColumnDescriptor hColumnDescriptorAttend = new HColumnDescriptor("attend");
        //设置块缓存
        hColumnDescriptorAttend.setBlockCacheEnabled(true);
        //设置块缓存大小
        hColumnDescriptorAttend.setBlocksize(2*1024*1024);
        //设置版本边界
        hColumnDescriptorAttend.setMinVersions(1);
        hColumnDescriptorAttend.setMaxVersions(1);

        tableDescriptor.addFamily(hColumnDescriptorAttend);

        //创建粉丝的列族
        HColumnDescriptor hColumnDescriptorFans = new HColumnDescriptor("fans");

        //设置块缓存
        hColumnDescriptorFans.setBlockCacheEnabled(true);
        //设置块缓存大小
        hColumnDescriptorFans.setBlocksize(2*1024*1024);
        hColumnDescriptorFans.setMaxVersions(1);
        hColumnDescriptorFans.setMinVersions(1);

        tableDescriptor.addFamily(hColumnDescriptorFans);
        admin.createTable(tableDescriptor);
        admin.close();
    }

    public List<String> getFans(Connection connection, String userId) throws IOException {
        TableName tableName = TableName.valueOf("atweibo:relation");
        Table table = connection.getTable(tableName);
        Get get = new Get(Bytes.toBytes(userId));
        get.addFamily(Bytes.toBytes("fans"));
        Result result = table.get(get);
        List<String> fans=new ArrayList<>();
        for (Cell cell : result.rawCells()) {
            fans.add(String.valueOf(CellUtil.cloneQualifier(cell)));
        }
        return fans;
    }

    public void insertAttendUser(Connection connection, String attendId, String userId) throws IOException {
        TableName tableName = TableName.valueOf("atweibo:relation");
        Table table = connection.getTable(tableName);
        Put put = new Put(Bytes.toBytes(userId));
        put.addColumn(Bytes.toBytes("attend"),
                Bytes.toBytes(attendId),
                Bytes.toBytes(attendId));
        table.put(put);

        Put put1 = new Put(Bytes.toBytes(attendId));
        put1.addColumn(Bytes.toBytes("fans"),
                Bytes.toBytes(userId),
                Bytes.toBytes(userId));
        table.put(put1);
        table.close();
    }

    public void deleteAttendUser(Connection connection, String attendId, String userId) throws IOException {
        TableName tableName = TableName.valueOf("atweibo:relation");
        Table table = connection.getTable(tableName);
        Delete delete = new Delete(Bytes.toBytes(userId));
        delete.addColumn(Bytes.toBytes("attend"),
                Bytes.toBytes(attendId));
        table.delete(delete);
        Delete delete1 = new Delete(Bytes.toBytes(attendId));
        delete1.addColumn(Bytes.toBytes("fans"),
                Bytes.toBytes(userId));
        table.delete(delete1);
        table.close();
    }

}
