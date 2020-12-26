package com.bigdata.hbase.tools;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * 1。创建表和删除表，查询表
 *
 * 2。TableName：代表 表名
 *              调用valueof(库名，表名)，返回表名
 *              如果库名为null,则会使用default作为库名
 *
 * 3。TableDescriptor：代表 表的细节（描述），包含表中列族的描述
 * @author november
 */
public class TableUtil {
    private static Logger logger = LoggerFactory.getLogger(TableUtil.class);

    /**
     * 验证表名和库名是否合法
     * @param nameSpace
     * @param tableName
     * @return
     */
    public static boolean checkTableName(String nameSpace,String tableName){
        //库名校验
        if (StringUtils.isBlank(nameSpace)) {
            logger.error("库名参数非法");
            //库名非法
            return false;
        }
        //表名校验
        if (StringUtils.isBlank(tableName)) {
            logger.error("表名参数非法");
            //表名非法
            return false;
        }
        return true;
    }

    /**
     * 查询当前库的所有表
     * @param conn
     * @param nameSpace
     * @return
     */
    public static List<String> listTable(Connection conn,String nameSpace) throws Exception {
        //库名校验
        if (StringUtils.isBlank(nameSpace)) {
            logger.error("库名参数非法");
            //库名非法
            return null;
        }

        List<String> tableList=new ArrayList<>();

        //提供一个admin
        Admin admin=conn.getAdmin();

        //查询当前库的所有表
        List<TableDescriptor> tableDescriptors = admin.listTableDescriptorsByNamespace(nameSpace.getBytes());
        for(TableDescriptor tableDescriptor:tableDescriptors){
            //取出每个表的表描述
            tableList.add(tableDescriptor.getTableName().toString());
        }

        //关闭admin
        admin.close();
        return tableList;
    }

    /**
     * 判断表是否存在
     * @param conn
     * @param tableName
     * @param nameSpace
     * @return
     * @throws Exception
     */
    public static boolean isTableExists(Connection conn,String nameSpace,String tableName) throws Exception{

        //校验表名和库名
        if(checkTableName(nameSpace,tableName)) {
            //提供一个admin
            Admin admin = conn.getAdmin();

            //判断表是否存在
            boolean tableExists = admin.tableExists(TableName.valueOf(nameSpace, tableName));

            //关闭admin
            admin.close();

            return tableExists;
        }
        return false;
    }

    /**
     * 创建表
     * @param conn
     * @param nameSpace
     * @param tableName
     * @param familyNames
     * @return
     * @throws Exception
     */
    public static boolean createTable(Connection conn,String nameSpace,String tableName,String...familyNames)throws Exception{
        //String...cfs 可变参数
        //校验表名和库名
        if(checkTableName(nameSpace,tableName)) {
            if(familyNames.length<1){
                logger.error("至少需要指定一个列族");
                return false;
            }

            //提供一个admin
            Admin admin = conn.getAdmin();

            // 创建集合用于存放ColumnFamilyDescriptor对象，存放列族描述
            List<ColumnFamilyDescriptor> families = new ArrayList<>();

            // 将每个familyName对应的ColumnFamilyDescriptor对象添加到families集合中保存
            for (String familyName : familyNames) {
                ColumnFamilyDescriptorBuilder columnFamilyDescriptorBuilder = ColumnFamilyDescriptorBuilder.newBuilder(familyName.getBytes());

                //添加列族设置
                columnFamilyDescriptorBuilder.setMinVersions(3);
                columnFamilyDescriptorBuilder.setMaxVersions(10);

                families.add(columnFamilyDescriptorBuilder.build());
            }

            // 构建TableDescriptor对象，以保存tableName与familyNames
            TableDescriptor tableDescriptor = TableDescriptorBuilder.newBuilder(TableName.valueOf(nameSpace,tableName)).setColumnFamilies(families).build();

            // 有了表描述器便可以创建表了
            admin.createTable(tableDescriptor);

            //关闭admin
            admin.close();

            return true;

        }
        return false;
    }

    /**
     * 删除表
     * @param conn
     * @param nameSpace
     * @param tableName
     * @return
     * @throws Exception
     */
    public static boolean deleteTable(Connection conn,String nameSpace,String tableName)throws Exception{
        //校验表名和库名
        if(checkTableName(nameSpace,tableName)) {

            //提供一个admin
            Admin admin = conn.getAdmin();

            TableName name = TableName.valueOf(nameSpace, tableName);

            //禁用表
            admin.disableTable(name);

            //删除表，删除表之前要把表禁用
            admin.deleteTable(name);

            //关闭admin
            admin.close();

            return true;

        }
        return false;

    }
}
