package org.frame.recframe.resultcal.offline.datareduction;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class DataReductionMapper extends Mapper<Object, Text, Text, Text> {

	private String fieldDelimiter;
	private String fieldDelimiter1;
	private int outputDataNum;
	private String dataPosEdit;

	protected void setup(Context context) {
		Configuration conf = context.getConfiguration();
		fieldDelimiter = conf.get("field.delimiter", "\\|");
		fieldDelimiter1 = conf.get("field.delimiter.1", ",");
		outputDataNum = Integer.parseInt(conf.get("output.data.num", "0"));
		dataPosEdit = conf.get("data.pos.edit", "");
	}

	protected void map(Object text, Text value, Context context) throws IOException, InterruptedException {

		if (outputDataNum != 0 && !dataPosEdit.equals("") && !value.toString().equals("")) { // 处理数据的个数不为0，且所需字段位置信息不为空
			
			String[] fields = value.toString().split(fieldDelimiter);
			String[] dataPosEdits = dataPosEdit.toString().split(fieldDelimiter1);

			// 输入数据字段数大于等于输出数据字段,且输出字段与截取字段个数相等
			if (fields.length >= outputDataNum && outputDataNum == dataPosEdits.length) { 

				String output = "";
				for (int i = 0; i < dataPosEdits.length; i++) { // 循环获取所需的字段数
					if (Integer.parseInt(dataPosEdits[i]) < fields.length) { // 获取的字段位置必须在输入数据内
						output += fields[Integer.parseInt(dataPosEdits[i])-1] + "|";
					} else {
						output = "";
						break;
					}
				}

				if (!output.equals("")) { // 输出数据不为空，则数据整理完成
					context.write(new Text(), new Text(output));
				}
			}
		}
	}
}
