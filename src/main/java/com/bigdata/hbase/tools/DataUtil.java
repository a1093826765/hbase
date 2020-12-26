package com.bigdata.hbase.tools;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * 1。数据的增删改查
 *    需要使用Table对象
 *
 * 2。put:代表对单行数据的put操作
 *
 * 3。在hbase中，操作的数据都是以byte[]形式存在，需要把常用的数据类型转为byte[]
 *    hbase提供了工具类 Bytes
 *    Bytes.toBytes(x)：基本数据类型转byte[]
 *    Bytes.toXxx(X)：从byte[]转基本数据类型
 *
 * 4。Result：scan或get的单行的所有记录
 * @Author: november
 * Date: 2020/12/14
 */
public class DataUtil {

    /**
     * 获取表的table对象
     * @param conn
     * @param nameSpace
     * @param tableName
     * @return
     */
    public static Table getTable(Connection conn,String nameSpace,String tableName) throws Exception{
        //校验表名和库名是否合法
        if(TableUtil.checkTableName(nameSpace,tableName)){
           return conn.getTable(TableName.valueOf(nameSpace,tableName));
        }
        return null;
    }

    /**
     * 添加/修改数据（put）
     * @param conn
     * @param nameSpace
     * @param tableName
     * @param rowKey
     * @param value
     * @param cf 列族
     * @param cq 列名
     */
    public static void put(Connection conn,String nameSpace,String tableName,String rowKey,String value,String cf,String cq)throws Exception{
        Table table = getTable(conn, nameSpace, tableName);
        if(table==null){
            return;
        }

        //创建一个put对象
        Put put = new Put(Bytes.toBytes(rowKey));

        //向put中设置cell的细节信息
        put.addColumn(Bytes.toBytes(cf),Bytes.toBytes(cq),Bytes.toBytes(value));


        //提交数据
        table.put(put);

        //关闭table
        table.close();
    }

    /**
     * 查询数据
     * @param conn
     * @param tableName
     * @param nameSpace
     * @param rowKey
     * @return
     * @throws Exception
     */
    public static Result get(Connection conn,String nameSpace,String tableName,String rowKey)throws Exception{
        Table table = getTable(conn, nameSpace, tableName);
        if(table==null){
            return null;
        }

        //创建一个get对象
        Get get=new Get(Bytes.toBytes(rowKey));

        //设置但航查询的详细信息
        //设置查对应的列
//        get.addColumn();
        //设置查对应的列族
//        get.addFamily();
        //设置查对应的时间戳
//        get.setTimeRange();//指定某个时间段
//        get.setTimestamp();//指定时间戳
        //设置返回的versions(版本)
//        get.readVersions();//指定返回对应的版本
//        get.readAllVersions();//指定返回所有版本

        //获取数据
        Result result = table.get(get);

        //关闭table
        table.close();

        return result;
    }

    /**
     * 遍历Result
     * @param result
     */
    public static void parseResult(Result result){
        if(result==null){
            return;
        }
        //返回原始数据
//        Cell[] cells = result.rawCells();

        //返回已经排序的数据
        List<Cell> cells = result.listCells();

        //遍历数据
        for(Cell cell:cells){
            System.out.println("列族："+Bytes.toString(CellUtil.cloneFamily(cell)));
            System.out.println("列名："+Bytes.toString(CellUtil.cloneQualifier(cell)));
            System.out.println("keyRow："+Bytes.toString(CellUtil.cloneRow(cell)));
            System.out.println("值："+Bytes.toString(CellUtil.cloneValue(cell)));

        }
    }
}
