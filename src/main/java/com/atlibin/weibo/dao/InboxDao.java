package com.atlibin.weibo.dao;

import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by 11610 on 2018/12/2.
 */
public class InboxDao {
    public void createTable(Connection connection) throws IOException {
        Admin admin = connection.getAdmin();
        TableName tableName = TableName.valueOf("atweibo:inbox");
        //判断表是否存在
        if(admin.tableExists(tableName)){
            return;
        }
        //表的描述器
        HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);
        //列族描述器
        HColumnDescriptor hColumnDescriptor = new HColumnDescriptor("info");
        //设置块缓存
        hColumnDescriptor.setBlockCacheEnabled(true);
        //设置块缓存大小
        hColumnDescriptor.setBlocksize(10*1024*1024);

        tableDescriptor.addFamily(hColumnDescriptor);
        hColumnDescriptor.setMinVersions(5);
        hColumnDescriptor.setMaxVersions(5);
        admin.createTable(tableDescriptor);
        admin.close();
    }

    /**
     * 发布新微博时向粉丝的收件箱中添加消息
     * @param connection
     * @param fans
     * @param userId
     * @param timeStamp
     * @param rowKey
     * @throws IOException
     */
    public void insertDatas(Connection connection, List<String> fans, String userId, String timeStamp, String rowKey) throws IOException {
        TableName tableName = TableName.valueOf("atweibo:inbox");
        Table table = connection.getTable(tableName);
        List<Put> puts = new ArrayList<Put>();
        for (String fanId : fans) {
            Put put = new Put(Bytes.toBytes(fanId));
            put.addColumn(Bytes.toBytes("info"),
                    Bytes.toBytes(userId),
                    Long.valueOf(timeStamp),
                    Bytes.toBytes(rowKey));
            puts.add(put);
        }
        table.put(puts);
        table.close();
    }

    public void insertDatas(Connection connection, String userId, String attendId, List<byte[]> weibos) throws IOException {
        TableName tableName = TableName.valueOf("atweibo:inbox");
        Table table = connection.getTable(tableName);
        Put put = new Put(Bytes.toBytes(userId));
        for (byte[] weibo : weibos) {
            String rowKey = Bytes.toString(weibo);
            String[] strings = rowKey.split("_");
            String timeStamp = strings[1];
            put.addColumn(Bytes.toBytes("info"),
                    Bytes.toBytes(attendId),
                    Long.valueOf(timeStamp),
                    Bytes.toBytes(rowKey));
        }
        table.put(put);
        table.close();
    }

   /* public void insertDatas(Connection connection, String userId, String attendId, String timeStamp, String rowKey) throws IOException {
        TableName tableName = TableName.valueOf("atweibo:inbox");
        Table table = connection.getTable(tableName);
        Put put = new Put(Bytes.toBytes(userId));
        put.addColumn(Bytes.toBytes("info"),
                Bytes.toBytes(attendId),
                Long.valueOf(timeStamp),
                Bytes.toBytes(rowKey));
        table.put(put);
        table.close();
    }*/

    public void deleteAttendMessage(Connection connection, String attendId, String userId) throws IOException {
        TableName tableName = TableName.valueOf("atweibo:inbox");
        Table table = connection.getTable(tableName);
        Delete delete=new Delete(Bytes.toBytes(userId));
        delete.addColumns(Bytes.toBytes("info"),
                Bytes.toBytes(attendId));
        table.delete(delete);
        table.close();
    }

    public List<byte[]> getMessages(Connection connection,String userId) throws IOException {
        TableName tableName = TableName.valueOf("atweibo:inbox");
        Table table = connection.getTable(tableName);
        Get get = new Get(Bytes.toBytes(userId));
        get.setMaxVersions(4);
        get.addFamily(Bytes.toBytes("info"));
        Result result = table.get(get);
        //遍历结果
        List<byte[]> list=new ArrayList<>();
        for (Cell cell : result.rawCells()) {
            //将每个列族中rowKey取出4个
            list.add(CellUtil.cloneValue(cell));
        }
        table.close();
        return list;
    }


}
