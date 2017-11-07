package com.eb.bi.rs.andedu.synthesize;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 
 * 输入数据格式：
 *  resource_id资源ID|resource_score品质分|resource_type资源类型|content_type内容类型|phase学段|subject学科|version版本|grade年级|term分册|content目录（目录id1，目录id2，…）| 
 *  
 * 输出数据格式：
 * key:属性组合1
 * value:资源A|品质分
 * 
 */
public class SynthesizeSortMapper extends Mapper<Object, Text, Text, Text> {

	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {

		String[] fields = value.toString().split("\\|", -1);

		if (fields.length >= 10) {

			String[] a = { fields[3], "all" };
			String[] b = { fields[4], "all" };
			String[] c = { fields[5], "all" };
			String[] d = { fields[6], "all" };
			String[] e = { fields[7], "all" };
			String[] f = { fields[8], "all" };
			String[] g = { fields[9], "all" };

			for (int i = 0; i < a.length; i++) {
				if (StringUtils.isBlank(a[i]))
					continue;
				for (int j = 0; j < b.length; j++) {
					if (StringUtils.isBlank(b[j]))
						continue;
					for (int k = 0; k < c.length; k++) {
						if (StringUtils.isBlank(c[k]))
							continue;
						for (int l = 0; l < d.length; l++) {
							if (StringUtils.isBlank(d[l]))
								continue;
							for (int m = 0; m < e.length; m++) {
								if (StringUtils.isBlank(e[m]))
									continue;
								for (int n = 0; n < f.length; n++) {
									if (StringUtils.isBlank(f[n]))
										continue;
									for (int o = 0; o < g.length; o++) {
										if (StringUtils.isBlank(g[o]))
											continue;
										if (o == 0) {
											String[] split = g[o].split(",");
											for (int p = 0; p < split.length; p++) {
												context.write(new Text(a[i] + "+"
														+ b[j] + "+" + c[k] + "+"
														+ d[l] + "+" + e[m] + "+"
														+ f[n] + "+" + split[p]),
														new Text(fields[0] + "|"
																+ fields[1]));
											}
										} else {
											context.write(new Text(a[i] + "+"
													+ b[j] + "+" + c[k] + "+"
													+ d[l] + "+" + e[m] + "+"
													+ f[n] + "+" + g[o]),
													new Text(fields[0] + "|"
															+ fields[1]));
										}
									}
								}
							}
						}
					}
				}
			}
		}
	}
}
