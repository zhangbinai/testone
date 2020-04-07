package com.marin.bigdata.hbase.util;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class HBaseUtil {
    //ThreadLocal
    private static ThreadLocal<Connection> connHolder = new ThreadLocal<Connection>();

    //    private static Connection conn = null;
    private HBaseUtil() {

    }

    /**
     * 获取连接对象
     */
    public static void makeHbaseConnection() throws IOException {
//        Configuration conf = HBaseConfiguration.create();
//        conn = ConnectionFactory.createConnection(conf);
//        return conn;
//        return ConnectionFactory.createConnection(conf);
        Connection conn = connHolder.get();
        if (conn == null) {
            Configuration conf = HBaseConfiguration.create();
            conf.set("hbase.zookeeper.quorum", "hadoop100:2181,hadoop101:2181,hadoop103:2181");
            conn = ConnectionFactory.createConnection(conf);
            connHolder.set(conn);
        }
    }

    public static void main(String[] args) {
        //String zhangsan = genRegionNum("lisi", 3);
        //System.out.println(zhangsan);
        byte[][] bytes = genRegionKeys(6);
        for (byte[] aByte : bytes) {
            System.out.println(Bytes.toString(aByte));
        }
    }

    /**
     * 生成分区建
     * @param regionCount
     * @return
     */
    public static byte[][] genRegionKeys(int regionCount) {
        byte[][] bs = new byte[regionCount - 1][];
        for (int i = 0; i < regionCount - 1; i++) {
            bs[i] = Bytes.toBytes(i+"|");
        }
        return bs;
    }

    /**
     * 生成分区号
     *
     * @return
     */
    public static String genRegionNum(String rowkey, int regionCount) {
        int regionNum;
        int hash = rowkey.hashCode();

        if (regionCount > 0 && (regionCount & (regionCount - 1)) == 0) {
            //2的n次方
            regionNum = hash & (regionCount - 1);
        } else {
            regionNum = hash % (regionCount);
        }
        return regionNum + "_" + rowkey;
    }

    /**
     * @param tableName
     * @param rowkey
     * @param family
     * @param column
     * @param value
     * @throws IOException
     */
    public static void insertData(String tableName, String rowkey, String family, String column, String value) throws IOException {
        Connection conn = connHolder.get();
        Table table = conn.getTable(TableName.valueOf(tableName));
        Put put = new Put(Bytes.toBytes(rowkey));
        put.addColumn(Bytes.toBytes(family), Bytes.toBytes(column), Bytes.toBytes(value));
        table.put(put);
        table.close();
    }

    public static void close() throws IOException {
        Connection conn = connHolder.get();
        if (conn != null) {
            conn.close();
            connHolder.remove();
        }
    }
}
