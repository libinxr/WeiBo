package com.atlibin.weibo.dao;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;

/**
 * Created by 11610 on 2018/12/2.
 */
public class HbaseDao {
    public Connection createConnection() throws IOException {
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "192.168.25.101");
        configuration.set("hbase.zookeeper.property.clientPort", "2181");
        Connection connection = ConnectionFactory.createConnection(configuration);
        return connection;
    }

    public void createNameSpace(Connection connection) throws IOException {
        Admin admin = connection.getAdmin();
        try {
            //先判断命名空间是否存在
            NamespaceDescriptor namespaceDescriptor = admin.getNamespaceDescriptor("atweibo");
            admin.close();
        }catch (Exception e){
            NamespaceDescriptor namespaceDescriptor1= NamespaceDescriptor.create("atweibo")
                    .addConfiguration("creator","libin")
                    .addConfiguration("time",String.valueOf(System.currentTimeMillis()))
                    .build();

            admin.createNamespace(namespaceDescriptor1);
            admin.close();
        }
    }


}
