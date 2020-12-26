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
 *
 * 5。Cell：代表一个单元格，hbase提供cellUtil.clonexxx(cell) 来获取cell中的列族，列名，值等属性
 *
 * 6。ResultScanner：代表多行Result对象的集合
 *
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
     * 查询数据（get）
     * @param conn
     * @param tableName
     * @param nameSpace
     * @param rowKey
     * @return
     * @throws Exception
     */
    public static void get(Connection conn,String nameSpace,String tableName,String rowKey)throws Exception{
        Table table = getTable(conn, nameSpace, tableName);
        if(table==null){
            return ;
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

        parseResult(result);

        //关闭table
        table.close();

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
            System.out.println("================================");
        }
    }

    /**
     * 查询数据（scan）
     * 可选参数：STARTROW STOPROW LIMIT
     * @param conn
     * @param nameSpace
     * @param tableName
     * @throws Exception
     */
    public static void scan(Connection conn,String nameSpace,String tableName) throws Exception{
        Table table = getTable(conn, nameSpace, tableName);
        if(table==null){
            return;
        }

        //构建scan对象
        Scan scan = new Scan();

        //设置scan参数
        //设置扫描的起始行
//        scan.withStartRow();
        //设置扫描的结束行
//        scan.withStopRow();

        //scan查询数据,返回是一个结果集扫描器
        ResultScanner scanner = table.getScanner(scan);

        //遍历ResultScanner
        for(Result result:scanner){
            parseResult(result);
        }

        //关闭table
        table.close();
    }

    /**
     * 删除表数据
     * @param conn
     * @param nameSpace
     * @param tableName
     * @param rowKey
     * @throws Exception
     */
    public static void delete(Connection conn,String nameSpace,String tableName,String rowKey) throws Exception{
        Table table = getTable(conn, nameSpace, tableName);
        if(table==null){
            return;
        }

        //构建delete对象
        Delete delete = new Delete(Bytes.toBytes(rowKey));

        //设置delete参数
        //删除某个具体的列(最新)（为此列最新的cell，添加一条type=Delete的标记，只能删除最新的记录，无法删除历史版本的记录）
//        delete.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("cq1"));
        //删除某个具体的列(所有版本) （添加一条type=DeleteColumn的标记，删除所有历史版本记录）
//        delete.addColumns(Bytes.toBytes("cf1"), Bytes.toBytes("cq1"));
        //删除整个列族（添加一条type=DeleteFamily的标记，删除整个列族）
//        delete.addFamily(Bytes.toBytes("cf1"));

        table.delete(delete);

        //关闭table
        table.close();

    }
}
