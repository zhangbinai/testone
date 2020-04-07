package com.marin.bigdata.hbase;

import com.marin.bigdata.hbase.util.HBaseUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * 测试hbase的api
 */
public class TestHbaseAPI_3 {
    public static void main(String[] args) throws IOException {
        //创建连接
        HBaseUtil.makeHbaseConnection();

        //增加数据
        HBaseUtil.insertData("marin:student","1002","info","name","lisi");

        //关闭连接
        HBaseUtil.close();

    }
}
