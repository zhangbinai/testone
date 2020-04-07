package com.marin.bigdata.hbase;

import com.google.inject.internal.cglib.core.$ClassEmitter;
import com.marin.bigdata.hbase.util.HBaseUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * 测试hbase的api
 */
public class TestHbaseAPI_6 {
    public static void main(String[] args) throws IOException {

        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum","hadoop100:2181,hadoop101:2181,hadoop103:2181");
        //1.获取hbase的连接对象
        Connection connection = ConnectionFactory.createConnection(conf);
        /*
        Admin admin = connection.getAdmin();
            HTableDescriptor td = new HTableDescriptor(TableName.valueOf("emp1"));
            HColumnDescriptor cd = new HColumnDescriptor("info");
            td.addFamily(cd);

            byte[][] bs = new byte[2][];

            bs[0] = Bytes.toBytes("0|");
            bs[1] = Bytes.toBytes("1|");

            //创建表的同时，增加与分区
            admin.createTable(td, bs);
            System.out.println("表格创建成功！！！");
         */
        //增加数据
        Table emp1 = connection.getTable(TableName.valueOf("emp1"));
        String rowkey = "zhangsan";
        //hashmap
        //将rowkey均匀的分配到不同的分区中，效果和hashmap数据存储的规则是一样的
        //hashmap
        rowkey = HBaseUtil.genRegionNum(rowkey,3);
        Put put = new Put(Bytes.toBytes(rowkey));
        put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("age"),Bytes.toBytes("20"));
        emp1.put(put);
    }
}
