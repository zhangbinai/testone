package com.marin.bigdata.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * 功能：测试hbase的api
 */
public class TestHbaseAPI_5 {
    public static void main(String[] args) throws IOException {

        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum","hadoop100:2181,hadoop101:2181,hadoop103:2181");

        Connection connection = ConnectionFactory.createConnection(conf);
        TableName tableName = TableName.valueOf("student");

      //删除表

        Admin admin = connection.getAdmin();
        if(admin.tableExists(tableName)){
            //禁用表disable
            admin.disableTable(tableName);
            //删除表
            admin.deleteTable(tableName);
            System.out.println("表删除成功");

        }

        /*
        //删除数据
        Table table = connection.getTable(tableName);
        String rowkey = "1001";
        Delete delete = new Delete(Bytes.toBytes(rowkey));
        table.delete(delete);
//        table.delete();里面传一个list可以删除多行数据
        System.out.println("删除数据。。。");
        */
        //扫面数据
        Table table = connection.getTable(tableName);
        Scan scan = new Scan();
//        scan.addFamily(Bytes.toBytes("info"));
        BinaryComparator bc = new BinaryComparator(Bytes.toBytes("1002"));
        RegexStringComparator rsc = new RegexStringComparator("^\\d{3}$");
        Filter f = new RowFilter(CompareFilter.CompareOp.EQUAL,rsc);

//        MUST_PASS_ALL:等同于and
//        MUST_PASS_ONE：等同于or
        FilterList list = new FilterList(FilterList.Operator.MUST_PASS_ONE);
        RowFilter rf = new RowFilter(CompareFilter.CompareOp.EQUAL,bc);
        list.addFilter(f);
        list.addFilter(rf);
        //扫描时增加过滤器
        //所谓的过滤，其实每条数据都会筛选过滤，性能较低
        /*
        Table table = connection.getTable(tableName);
        scan.setFilter(list);
        ResultScanner scanner = table.getScanner(scan);
        for (Result result : scanner) {
            for (Cell cell : result.rawCells()) {
                System.out.println("value = "+Bytes.toString(CellUtil.cloneValue(cell)));
                System.out.println("rowkey = "+Bytes.toString(CellUtil.cloneRow(cell)));
                System.out.println("family = "+Bytes.toString(CellUtil.cloneFamily(cell)));
                System.out.println("collumn = "+Bytes.toString(CellUtil.cloneQualifier(cell)));
            }
        }

         */

    }

}
