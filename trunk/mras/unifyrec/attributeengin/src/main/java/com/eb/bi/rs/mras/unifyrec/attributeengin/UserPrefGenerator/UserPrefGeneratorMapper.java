package com.eb.bi.rs.mras.unifyrec.attributeengin.UserPrefGenerator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by LiMingji on 2015/10/16.
 */
public class UserPrefGeneratorMapper extends Mapper<Object, Text, Text, UserPrefVector> {
    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split("\\|", -1);
        if (fields.length >= 20) {
            /**
             * 用户msisdn|分群group_type|第一分类偏好class1_id|强度class1_value|
             * 第二分类偏好class2_id|强度class2_value|第三分类偏好class3_id|强度class3_value|
             * 新书偏好new_value|连载书偏好serialize_value|名家偏好famous_values|促销偏好sales_values|
             * 包月偏好pack_values|按本偏好whol_values|按章偏好chpt_values|日期record_day
             *
             * */
            String msisdn = fields[0];
            String groupType = fields[1];// '分群ID 1：新用户；2：深度用户；3：中度用户；4、浅度用户；5、沉默用户',
            String class1_id = fields[2];// '第一分类偏好',
            String class1_value = fields[3];// '第一分类偏好强度',
            String class2_id = fields[4]; // '第二分类偏好'
            String class2_value = fields[5]; // '第二分类偏好强度',
            String class3_id = fields[6]; // '第三分类偏好'
            String class3_value = fields[7]; // '第三分类偏好强度',
            String new_values = fields[8];// '新书偏好强度',
            String serialize_values = fields[9];// '连载书偏好强度 ',
            String famous_values = fields[10];// '名家偏好强度',
            String sales_values = fields[11]; // '促销书偏好强度 ',
            String pack_values = fields[12]; // '包月书偏好强度',
            String whol_values = fields[13]; // '按本书偏好强度',
            String chpt_values = fields[14]; // '按章书偏好强度',
            String man_values = fields[15];  //男性偏好
            String female_values = fields[16]; //女性偏好
            String low_values = fields[17]; // 俗偏好
            String high_values = fields[18]; // 雅偏好
            String hot_values = fields[19]; // 热书偏好

            UserPrefVector personPref = new UserPrefVector(msisdn, groupType, class1_id, class1_value, class2_id, class2_value,
                    class3_id, class3_value, new_values, serialize_values, famous_values, sales_values, pack_values,
                    whol_values, chpt_values, man_values, female_values, low_values, high_values, hot_values);

            context.write(new Text(msisdn), personPref);
        }
    }
}
