package com.eb.bi.rs.mras2.unifyrec.UserPrefAverage;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by LiMingji on 15/10/27.
 */
public class UserPrefAverageMapper extends Mapper<Object, Text, Text, Text> {
    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split("\\|", -1);
        if (fields.length >= 20) {
            String groupType = fields[1];// '分群ID 1：新用户；2：深度用户；3：中度用户；4、浅度用户；5、沉默用户',
            if (Integer.parseInt(groupType) != 2) {
                return;
            }
            /**
             *用户历史偏好表：DMN.IRECM_US_PREF_ALL
             字段：用户msisdn|分群group_type|第一分类偏好class1_id|强度class1_value|第二分类偏好class2_id|
             强度class2_value|第三分类偏好class3_id|强度class3_value|新书偏好new_value|
             连载书偏好serialize_value|名家偏好famous_values|促销偏好sales_values|包月偏好pack_values|
             按本偏好whol_values|按章偏好chpt_values|男性偏好man_value|女性偏好female_value|
             俗偏好low_value|雅偏好high_value|流行书偏好hot_value|日期record_day

             */
            String new_values = fields[8];// '新书偏好强度',
            String serialize_values = fields[9];// '连载书偏好强度 ',
            String famous_values = fields[10];// '名家偏好强度',
            String sales_values = fields[11]; // '促销书偏好强度 ',
            String pack_values = fields[12]; // '包月书偏好强度',
            String whol_values = fields[13]; // '按本书偏好强度',
            String chpt_values = fields[14]; // '按章书偏好强度',
            String man_values = fields[15]; // 男性偏好
            String femal_values = fields[16]; // 女性偏好
            String low_values = fields[17]; //俗偏好
            String high_values = fields[18];//雅偏好
            String hot_values = fields[19]; // 流行书偏好

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
