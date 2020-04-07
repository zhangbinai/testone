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
public class TestHbaseAPI_4 {
    public static void main(String[] args) throws IOException {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum","hadoop100:2181,hadoop101:2181,hadoop103:2181");

        Connection connection = ConnectionFactory.createConnection(conf);
        TableName tableName = TableName.valueOf("marin:student");

        //扫面数据
        Table table = connection.getTable(tableName);
        Scan scan = new Scan();
        ResultScanner scanner = table.getScanner(scan);
        for (Result result : scanner) {
            for (Cell cell : result.rawCells()) {
                System.out.println("value = "+ Bytes.toString(CellUtil.cloneValue(cell)));
                System.out.println("rowkey = "+CellUtil.cloneRow(cell));
                System.out.println("family = "+CellUtil.cloneFamily(cell));
                System.out.println("collumn = "+CellUtil.cloneQualifier(cell));
            }
        }

        table.close();
        connection.close();
    }
}
