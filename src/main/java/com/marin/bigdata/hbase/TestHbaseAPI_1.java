package com.marin.bigdata.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * 测试hbase的api
 */
public class TestHbaseAPI_1 {
    public static void main(String[] args) throws IOException {
        //通过Java代码访问mysql数据库jdbc
        //1.加载数据库驱动
        //2.获取数据库的链接（url,user,pwd）
        //3获取数据库操作对象
        //sql
        //5执行数据库操作
        //6获取查询结果ResultSet
        //同通过Java代码访问hbase数据库
        //0创建配置对象，获取hbase的连接
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum","hadoop100:2181,hadoop101:2181,hadoop103:2181");
        //1.获取hbase的连接对象
        Connection connection = ConnectionFactory.createConnection(conf);
//        System.out.println(connection);
        //2获取操作对象
//        new HBaseAdmin(connection); 此方法不推荐使用
        Admin admin = connection.getAdmin();
        //3操作数据库:判断hbase中有没有某张表
        //3-1判断命名空间
//        NamespaceDescriptor namespace = admin.getNamespaceDescriptor("marin");
        try {
            admin.getNamespaceDescriptor("marin");
        }catch (NamespaceNotFoundException e){
            //创建表空间
            NamespaceDescriptor nd =
                    NamespaceDescriptor.create("marin").build();
            admin.createNamespace(nd);
        }

        TableName tableName = TableName.valueOf("student");
        boolean flg = admin.tableExists(tableName);
//        System.out.println(flg);
        if(flg){
            //获取指定表对象
            Table table = connection.getTable(tableName);
            //查询数据
            //DDL(create, drop),DML(update,insert,delete),DQL(select)
            String rowkey = "1001";
            //string ==> byte[]
            //字符编码
            Get get = new Get(Bytes.toBytes(rowkey));
            Result result = table.get(get);
            boolean empty = result.isEmpty();
            System.out.println("1001数据是否存在="+!empty);
            if(empty){
                //新增数据
                Put put = new Put(Bytes.toBytes(rowkey));
                String family = "info";
                String column = "name";
                String val = "zhangsan";
                put.addColumn(Bytes.toBytes(family),Bytes.toBytes(column),Bytes.toBytes(val));
                table.put(put);
                System.out.println("增加数据。。。");
            }else {
                for (Cell cell : result.rawCells()) {
//                    cell.
                    System.out.println("value = "+Bytes.toString(CellUtil.cloneValue(cell)));
                    System.out.println("rowkey = "+CellUtil.cloneRow(cell));
                    System.out.println("family = "+CellUtil.cloneFamily(cell));
                    System.out.println("collumn = "+CellUtil.cloneQualifier(cell));
                }
            }

        }else{
            //删除表
            //创建表
            //创建表描述对象
            HTableDescriptor td = new HTableDescriptor(tableName);
            //增加协处理器
            td.addCoprocessor("com.marin.bigdata.hbase.coprocesser.InserMarinStudentCoprocesser");
            //增加列族
            HColumnDescriptor cd = new HColumnDescriptor("info");
            td.addFamily(cd);
            admin.createTable(td);
            System.out.println("表格创建成功！！！");

        }
        //4获取操作的结果
        //5关闭数据库的连接
    }
}
