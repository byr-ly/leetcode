package com.eb.bi.rs.mras.realcollection.userbehavior.Topo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * Created by LiMingji on 2015/10/8.
 */
public class PutSample {
    public static void main(String[] args) throws IOException {
        Configuration conf = HBaseConfiguration.create();
//        conf.set("hbase.zookeeper.quorum", "ZJHZ-RTS-NN1,ZJHZ-RTS-NN2,ZJHZ-RTS-DN1,ZJHZ-RTS-DN2,ZJHZ-RTS-DN3");
        conf.set("hbase.zookeeper.quorum", "10.1.69.170");
        conf.set("hbase.zookeeper.property.clientPort", "2182");

        HTable table = new HTable(conf, Bytes.toBytes("limingji"));

        System.out.println("tag1");

        Put put = new Put(Bytes.toBytes("1"));
        put.add(Bytes.toBytes("cf"), Bytes.toBytes("qul1"), Bytes.toBytes("val1"));
        System.out.println("tag2");

        table.put(put);
        System.out.println("tag3");

    }
}
