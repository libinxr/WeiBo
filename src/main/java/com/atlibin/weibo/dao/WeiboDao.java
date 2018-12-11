package com.atlibin.weibo.dao;

import com.atlibin.weibo.pojo.ContentMessage;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.util.Bytes;
import org.mortbay.util.StringUtil;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Created by 11610 on 2018/12/2.
 */
public class WeiboDao {
    public void createTable(Connection connection) throws IOException {
        Admin admin = connection.getAdmin();
        TableName tableName = TableName.valueOf("atweibo:weibo");
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
        //设置版本边界
        hColumnDescriptor.setMinVersions(1);
        hColumnDescriptor.setMaxVersions(1);
        tableDescriptor.addFamily(hColumnDescriptor);
        admin.createTable(tableDescriptor);
        admin.close();
    }


    public String insertData(Connection connection, String userId, String content) throws IOException {
        TableName tableName = TableName.valueOf("atweibo:weibo");
        Table table = connection.getTable(tableName);
        String rowKey =userId+"_"+System.currentTimeMillis();
        Put put=new Put(Bytes.toBytes(rowKey));
        put.addColumn(Bytes.toBytes("info"),
                Bytes.toBytes("content"),Bytes.toBytes(content));
        table.put(put);
        table.close();
        return rowKey;
    }

    public List<byte[]> getWeibos(Connection connection, String attendId) throws IOException {
        TableName tableName = TableName.valueOf("atweibo:weibo");
        Table table = connection.getTable(tableName);
        Scan scan = new Scan();
        //存放扫描出来的微博
        List<byte[]> weibos=new ArrayList<>();
        //创建过滤器，得到符合条件的微博
        RowFilter rowFilter=new RowFilter(CompareFilter.CompareOp.EQUAL,new SubstringComparator(attendId+"_"));
        scan.setFilter(rowFilter);
        //通过scan获得扫描结果
        ResultScanner scanner = table.getScanner(scan);
        Iterator<Result> iterator = scanner.iterator();
        int i=0;
        while (iterator.hasNext()){
            weibos.add(iterator.next().getRow());
            i++;
            if(i==4)
                break;
        }
        return weibos;
    }

    public List<ContentMessage> getContent(Connection connection, List<byte[]> rowKeyList) throws IOException {
        TableName tableName = TableName.valueOf("atweibo:weibo");
        Table table = connection.getTable(tableName);
        List<ContentMessage> contentList=new ArrayList<>();
        for (byte[] rowKey : rowKeyList) {
            ContentMessage contentMessage=new ContentMessage();
            Get get = new Get(rowKey);
            get.addFamily(Bytes.toBytes("info"));
            Result result = table.get(get);
            String row=Bytes.toString(result.getRow());
            String[] strings = row.split("_");
            contentMessage.setUserId(strings[0]);
            contentMessage.setTimeStamp(Long.valueOf(strings[1]));
            for (Cell cell : result.rawCells()) {
                contentMessage.setContent(Bytes.toString(CellUtil.cloneValue(cell)));
                contentList.add(contentMessage);
            }
        }
        return contentList;
    }
}
