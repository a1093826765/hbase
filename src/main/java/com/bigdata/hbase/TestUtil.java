package com.bigdata.hbase;

import com.bigdata.hbase.tools.ConnectionUtil;
import com.bigdata.hbase.tools.DataUtil;
import com.bigdata.hbase.tools.NameSpaceUtil;
import com.bigdata.hbase.tools.TableUtil;
import org.apache.hadoop.hbase.client.Connection;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;


/**
 * Created by IntelliJ IDEA.
 *
 * @Author: november
 * Date: 2020/12/23 9:43 上午
 */

public class TestUtil {
    private Connection conn;

    @Before
    public void init() throws Exception{
        conn=ConnectionUtil.getConn();
    }

    @After
    public void close() throws Exception{
        ConnectionUtil.close(conn);
    }

    @Test
    public void test() throws Exception {
        DataUtil.delete(conn,"test1","table1","key1");
    }
}
