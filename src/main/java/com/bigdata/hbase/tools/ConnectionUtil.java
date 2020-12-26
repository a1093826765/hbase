package com.bigdata.hbase.tools;

import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;

/**
 * Created by IntelliJ IDEA.
 * 1。创建和关闭Connection对象
 *
 * 2。如何在HBase中创建一个Configuration对象
 *      可以使用HBaseConfig.create() 返回Configuration
 *      这个既包含hadoop的8个配置文件参数，又包含hbase-default.xml和hbase-site.xml中所有参数的配置
 *      （需要把配置文件放进resources中，hbase-default.xml可以不用放）
 * @Author: november
 * Date: 2020/12/14
 */
public class ConnectionUtil {

    /**
     * 创建一个Connection对象
     * 创建连接
     * @return
     * @throws IOException
     */
    public static Connection getConn() throws IOException {
        return ConnectionFactory.createConnection();
    }

    /**
     * 关闭连接
     * @param connection
     * @throws IOException
     */
    public static void close(Connection connection) throws IOException {
        if(connection!=null){
            connection.close();
        }
    }
}
