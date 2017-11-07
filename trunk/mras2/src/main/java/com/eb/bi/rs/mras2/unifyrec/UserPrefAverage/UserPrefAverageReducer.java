package com.eb.bi.rs.mras2.unifyrec.UserPrefAverage;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.Iterator;

/**
 * Created by LiMingji on 15/10/27.
 */
public class UserPrefAverageReducer extends Reducer<Text, Text, Text, Text> {

    private double new_values_sum = 0;
    private double serialize_values_sum = 0;
    private double famous_values_sum = 0;
    private double sales_values_sum = 0;
    private double pack_values_sunm = 0;
    private double whol_values_sum = 0;
    private double chpt_values_sum = 0;
    private double man_values_sum = 0;
    private double female_values_sum=0;
    private double low_values_sum = 0;
    private double high_values_sum = 0;
    private double hot_values_sum = 0;

    private int count = 0;

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Iterator<Text> it = values.iterator();
        while (it.hasNext()) {
            String[] fields = it.next().toString().split("\\|", -1);
            count += 1;
            new_values_sum += getDouble(fields[0]);
            serialize_values_sum += getDouble(fields[1]);
            famous_values_sum += getDouble(fields[2]);
            sales_values_sum += getDouble(fields[3]);
            pack_values_sunm += getDouble(fields[4]);
            whol_values_sum += getDouble(fields[5]);
            chpt_values_sum += getDouble(fields[6]);
            man_values_sum += getDouble(fields[7]);
            female_values_sum += getDouble(fields[8]);
            low_values_sum += getDouble(fields[9]);
            high_values_sum += getDouble(fields[10]);
            hot_values_sum += getDouble(fields[11]);
        }

        double new_values_avg = new_values_sum / count;
        double serialize_values_avg = serialize_values_sum / count;
        double famous_values_avg = famous_values_sum / count;
        double sales_values_avg = sales_values_sum / count;
        double pack_values_avg = pack_values_sunm / count;
        double whol_values_avg = whol_values_sum / count;
        double chpt_values_avg = chpt_values_sum / count;
        double man_values_avg = man_values_sum /count;
        double femal_values_avg = female_values_sum /count;
        double low_values_avg = low_values_sum / count;
        double hight_values_avg = high_values_sum / count;
        double hot_valus_avg = hot_values_sum / count;

        StringBuffer sb = new StringBuffer();
        sb.append(formatDouble(new_values_avg) + "|");
        sb.append(formatDouble(serialize_values_avg) + "|");
        sb.append(formatDouble(famous_values_avg) + "|");
        sb.append(formatDouble(sales_values_avg) + "|");
        sb.append(formatDouble(pack_values_avg) + "|");
        sb.append(formatDouble(whol_values_avg) + "|");
        sb.append(formatDouble(chpt_values_avg) + "|");
        sb.append(formatDouble(man_values_avg) + "|");
        sb.append(formatDouble(femal_values_avg) + "|");
        sb.append(formatDouble(low_values_avg) + "|");
        sb.append(formatDouble(hight_values_avg) + "|");
        sb.append(formatDouble(hot_valus_avg));
        context.write(new Text(sb.toString()), new Text(""));
    }

    private Double getDouble(String input) {
        try {
            return Double.parseDouble(input.trim());
        } catch (NumberFormatException e) {
            return 0.0;
        }
    }

    private String formatDouble(Double input) {
        DecimalFormat dcmFmt = new DecimalFormat("0.0000");
        return dcmFmt.format(input);
    }
}