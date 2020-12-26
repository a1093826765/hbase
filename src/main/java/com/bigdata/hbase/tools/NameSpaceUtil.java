package com.bigdata.hbase.tools;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * 1。创建/删除/查询/判断是否存在 名称空间（库）
 * HBase shell : 开启一个客户端对象
 * HBase shell : create_namepace
 * <p>
 * 2。Admin : HBase
 * 例如：创建，删除查询表等
 * 可以使用Connection getAdmin()来获取一个Admin的一个实例
 * 使用完成后，调用close关闭
 * <p>
 * 3。Connection : Connection代表客户端和集群的一个连接
 * 这个连接包含对master连接和zookeeper连接
 * 可以使用ConnectionFactory来创建
 * 重量级，因此建议一个应用只创建一个Connection对象
 * Connection是线程安全的,可在多个线程中共享一个Connection实例
 * Connection的生命周期是用户自己控制的
 * <p>
 * 从Connection中获取Table和Admin对象的实例
 * Table和Admin对象的创建是轻量级的，且不是线程安全的
 * 因此，不建议池化或缓存Table和Admin对象的实例，每个线程自己的Table和Admin对象的实例
 *
 * @author november
 */
public class NameSpaceUtil {

    private static Logger logger = LoggerFactory.getLogger(NameSpaceUtil.class);

    /**
     * 查询所有的名称空间
     *
     * @return
     */
    public static List<String> listNameSpace(Connection conn) throws Exception {
        List<String> nameSpaceList = new ArrayList<>();

        //提供一个Admin
        Admin admin = conn.getAdmin();

        //查询所有的库
        NamespaceDescriptor[] namespaceDescriptors = admin.listNamespaceDescriptors();

        for (NamespaceDescriptor namespaceDescriptor : namespaceDescriptors) {
            //取出每个库描述中的库名称
            nameSpaceList.add(namespaceDescriptor.getName());
        }

        //关闭admin
        admin.close();

        return nameSpaceList;
    }

    /**
     * 判断名称空间是否存在
     *
     * @param conn
     * @param nameSpace
     * @return
     * @throws Exception
     */
    public static boolean isNameSpaceExists(Connection conn, String nameSpace) throws Exception {
        //库名校验
        if (StringUtils.isBlank(nameSpace)) {
            logger.error("库名参数非法");
            //库名非法
            return false;
        }

        //提供一个Admin
        Admin admin = conn.getAdmin();

        try {
            //判断名称空间是否存在，如果找不到就抛异常
            if (admin.getNamespaceDescriptor(nameSpace) != null) {
                return true;
            }
        } catch (Exception e) {
            //名称空间不存在
            return false;
        } finally {
            //关闭admin
            admin.close();
        }
        return false;
    }

    /**
     * 新建名称空间（库）
     * @param conn
     * @param nameSpace
     * @return
     * @throws Exception
     */
    public static boolean creatNameSpace(Connection conn, String nameSpace) throws Exception {
        //库名校验
        if (StringUtils.isBlank(nameSpace)) {
            logger.error("库名参数非法");
            //库名非法
            return false;
        }

        //提供一个Admin
        Admin admin = conn.getAdmin();

        try {
            //先创建名称空间（库）的描述
            NamespaceDescriptor namespaceDescriptor=NamespaceDescriptor.create(nameSpace).build();
            //新建名称空间（库）
            admin.createNamespace(namespaceDescriptor);
            return true;
        } catch (Exception e) {
            //名称空间已存在
            logger.error("名称空间已经存在");
            return false;
        } finally {
            //关闭admin
            admin.close();
        }
    }

    /**
     * 删除名称空间（库）
     * @param conn
     * @param nameSpace
     * @return
     * @throws Exception
     */
    public static boolean deleteNameSpace(Connection conn, String nameSpace) throws Exception {
        //库名校验
        if (StringUtils.isBlank(nameSpace)) {
            logger.error("库名参数非法");
            //库名非法
            return false;
        }

        //提供一个Admin
        Admin admin = conn.getAdmin();

        try {
            //判断库是否为空
            if(TableUtil.listTable(conn,nameSpace).size()==0){
                //删除名称空间（库）,只能删除空库，删除前先判断库是否为空
                admin.deleteNamespace(nameSpace);
                return true;
            }
            logger.error("当前库不为空");
            return false;
        } catch (Exception e) {
            return false;
        } finally {
            //关闭admin
            admin.close();
        }
    }
}
