package com.eb.bi.rs.mras.bookrec.channelrec;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ChannelScoreMapper extends Mapper<Object, Text, Text, Text> {

	private Map<String , String> parameterToChannel;

	@Override
	protected void map(Object key, Text value,Context context) throws IOException, InterruptedException {
		
		String[] fields = value.toString().split("\\|");
		
		if (fields.length == 3 ) {//字段：msisdn| sale_parameter|record_day
			if (parameterToChannel.keySet().contains(fields[1])) {
				SimpleDateFormat sdf =   new SimpleDateFormat( "yyyyMMdd" );
				try {
					Long visitTime = sdf.parse(fields[2]).getTime();
					Long limitTime = 6*30*24*60*60*1000L;
					if (System.currentTimeMillis() - visitTime < limitTime) {
						context.write(new Text(fields[0]), new Text("0" + "|" + parameterToChannel.get(fields[1])));
					}
				} catch (ParseException e) {
					e.printStackTrace();
				}
			}
		}
	}

	@Override
	protected void setup(Context context)
			throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		parameterToChannel = new HashMap<String , String>();
		String[] channelTypeAndSaleParameters = conf.get("channelTypeAndSaleParameters").split(";");
		for (String string : channelTypeAndSaleParameters) {
			String[] fields = string.split("\\|");
			if (fields.length == 2) {
				String[] sale_parameters = fields[1].split(",");
				for (String sale_parameter : sale_parameters) {
					parameterToChannel.put(sale_parameter , fields[0]);					
				}
			}
		}

	}

}
