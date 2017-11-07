package com.eb.bi.rs.mras2.classifyweight.hadoop.A.historyWeight.updateClassWeight;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.text.DecimalFormat;

/**将结果写入hbase
 * 输入格式： 用户_分类  A|行为|权重|最近行为时间
 *           用户_分类  B|历史权重
 * Created by linwanying on 2017/6/12.
 */
public class putWeightReducer extends TableReducer<Text,Text,ImmutableBytesWritable> {
    private static Logger log = Logger.getLogger("updateHbaseDriver");
    private String exciWeightTable;
    private String classWeightTable;
    @Override
    public void setup(Context context) throws IOException, InterruptedException{
        exciWeightTable = context.getConfiguration().get("excitation_weight_table");
        classWeightTable = context.getConfiguration().get("class_weight_table");
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        String historyweight = "", classweight = "0", action = "", latest_time = "";
        for (Text value : values) {
            String[] fields = value.toString().split("\\|");
            if (fields[0].equals("A")) {
                historyweight = fields[2];
                action = fields[1];
                latest_time = fields[3];
            }
            if (fields[0].equals("B")) {
                classweight = fields[1];
            }
//            log.info("reducer-value:"+value);
//            log.info("reduce-action:"+action);
//            log.info("reduce-historyweight:"+historyweight);
//            log.info("reduce-classweight:"+classweight);
        }
        ImmutableBytesWritable ib2 = new ImmutableBytesWritable();
        ib2.set(Bytes.toBytes(classWeightTable));
        double newweight = Double.parseDouble(classweight);
        Put put2 = new Put(Bytes.toBytes(key.toString()));
        if (!historyweight.equals("")) {
            ImmutableBytesWritable ib = new ImmutableBytesWritable();
            ib.set(Bytes.toBytes(exciWeightTable));
            Put put = new Put(Bytes.toBytes(key.toString()));
            double weight = Double.parseDouble(historyweight);
            if (action.equals("1")) {
                newweight = Math.max(weight, newweight);
                if (weight > 0) {
                    put.add(Bytes.toBytes("cf"), Bytes.toBytes("weight"),
                            Bytes.toBytes(String.valueOf(weight)));
                    put.add(Bytes.toBytes("cf"), Bytes.toBytes("action"),
                            Bytes.toBytes(String.valueOf(action)));
                    put.add(Bytes.toBytes("cf"), Bytes.toBytes("latest_time"),
                            Bytes.toBytes(String.valueOf(latest_time)));
                    log.info("exciTable_put:"+key.toString()+"|"+weight + "|" + action + "|" + latest_time);
                    context.write(ib, put);
                }
            } else {
                newweight = Math.min(weight, newweight);
                if (weight < 0.5) {
                    put.add(Bytes.toBytes("cf"), Bytes.toBytes("weight"),
                            Bytes.toBytes(String.valueOf(weight)));
                    put.add(Bytes.toBytes("cf"), Bytes.toBytes("action"),
                            Bytes.toBytes(String.valueOf(action)));
                    put.add(Bytes.toBytes("cf"), Bytes.toBytes("latest_time"),
                            Bytes.toBytes(String.valueOf(latest_time)));
                    log.info("exciTable_put:"+key.toString()+"|"+weight);
                    context.write(ib, put);
                }
            }
        }
        put2.add(Bytes.toBytes("cf"), Bytes.toBytes("weight"),
                Bytes.toBytes(String.valueOf(new DecimalFormat("0.00").format(newweight))));
        log.info("classWeightTable_put:"+key.toString()+"|"+new DecimalFormat("0.00").format(newweight));
        context.write(ib2, put2);
    }
}
