import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * Created by LiMingji on 2015/9/6.
 */
public class Insert2HBase {
    public static void main(String[] args) throws IOException {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "eb179,eb178,eb177");
        conf.set("hbase.zookeeper.property.clientPort", "2181");

        HTable table = new HTable(conf, "limingji");

        table.setAutoFlush(false);

        Put put = new Put(Bytes.toBytes("row1"));
        put.add(Bytes.toBytes("cf"), Bytes.toBytes("qul1"), Bytes.toBytes("val1"));

        table.put(put);
    }
}
