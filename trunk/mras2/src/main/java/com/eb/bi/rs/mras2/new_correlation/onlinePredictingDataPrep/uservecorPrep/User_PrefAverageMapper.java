package com.eb.bi.rs.mras2.new_correlation.onlinePredictingDataPrep.uservecorPrep;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by LiMingji on 15/10/27.
 */
public class User_PrefAverageMapper extends Mapper<Object, Text, Text, Text> {
    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split("\\|", -1);
        if (fields.length >= 20) {
            String groupType = fields[1];// '分群ID 1：新用户；2：深度用户；3：中度用户；4、浅度用户；5、沉默用户',
            if (Integer.parseInt(groupType) != 2) {
                return;
            }
            /**
             *格式：msisdn用户号码|group_type分群ID|class1_id第一分类偏好|class1_value第一分类偏好强度|
             *class2_id第二分类偏好|class2_value第二分类偏好强度|class3_id第三分类偏好|class3_value第三分类偏好强度|
             *label1_name第一标签偏好|label1_value第一标签偏好强度|label2_name第二标签偏好|label2_value第二标签偏好强度|
             *label3_name第三标签偏好|label3_value第三标签偏好强度|new_value新书偏好强度|serialize_value连载书偏好强度|
             *famous_values名家偏好强度|sales_values促销书偏好强度|pack_values包月书偏好强度|whol_values按本书偏好强度|
             *chpt_values按章书偏好强度|free_values免费书偏好强度|man_value男性偏好强度|female_value女性偏好强度|
             *low_value俗偏好强度|high_value雅偏好强度|hot_value流行书偏好强度|record_day日期）
             */
            String new_values = fields[14];// '新书偏好强度',
            String serialize_values = fields[15];// '连载书偏好强度 ',
            String famous_values = fields[16];// '名家偏好强度',
            String sales_values = fields[17]; // '促销书偏好强度 ',
            String pack_values = fields[18]; // '包月书偏好强度',
            String whol_values = fields[19]; // '按本书偏好强度',
            String chpt_values = fields[20]; // '按章书偏好强度',
            String man_values = fields[22]; // 男性偏好
            String femal_values = fields[23]; // 女性偏好
            String low_values = fields[24]; //俗偏好
            String high_values = fields[25];//雅偏好
            String hot_values = fields[26]; // 流行书偏好

            StringBuffer sb = new StringBuffer();
            sb.append(new_values + "|");
            sb.append(serialize_values + "|");
            sb.append(famous_values + "|");
            sb.append(sales_values + "|");
            sb.append(pack_values + "|");
            sb.append(whol_values + "|");
            sb.append(chpt_values +"|");
            sb.append(man_values + "|");
            sb.append(femal_values + "|");
            sb.append(low_values + "|");
            sb.append(high_values + "|");
            sb.append(hot_values);

            context.write(new Text("limingji"), new Text(sb.toString()));
        }
    }
}
