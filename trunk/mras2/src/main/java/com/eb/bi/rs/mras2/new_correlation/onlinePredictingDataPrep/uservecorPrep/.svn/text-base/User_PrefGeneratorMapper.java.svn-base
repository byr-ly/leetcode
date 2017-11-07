package com.eb.bi.rs.mras2.new_correlation.onlinePredictingDataPrep.uservecorPrep;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.eb.bi.rs.mras2.unifyrec.UserPrefGenerator.UserPrefVector;

import java.io.IOException;

/**
 * Created by LiMingji on 2015/10/16.
 */
public class User_PrefGeneratorMapper extends Mapper<Object, Text, Text, UserPrefVector> {
    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split("\\|", -1);
        if (fields.length >= 20) {
            /**
             * 格式：msisdn用户号码|group_type分群ID|class1_id第一分类偏好|class1_value第一分类偏好强度|
             *class2_id第二分类偏好|class2_value第二分类偏好强度|class3_id第三分类偏好|class3_value第三分类偏好强度|
             *label1_name第一标签偏好|label1_value第一标签偏好强度|label2_name第二标签偏好|label2_value第二标签偏好强度|
             *label3_name第三标签偏好|label3_value第三标签偏好强度|new_value新书偏好强度|serialize_value连载书偏好强度|
             *famous_values名家偏好强度|sales_values促销书偏好强度|pack_values包月书偏好强度|whol_values按本书偏好强度|
             *chpt_values按章书偏好强度|free_values免费书偏好强度|man_value男性偏好强度|female_value女性偏好强度|
             *low_value俗偏好强度|high_value雅偏好强度|hot_value流行书偏好强度|record_day日期）
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
            String new_values = fields[14];// '新书偏好强度',
            String serialize_values = fields[15];// '连载书偏好强度 ',
            String famous_values = fields[16];// '名家偏好强度',
            String sales_values = fields[17]; // '促销书偏好强度 ',
            String pack_values = fields[18]; // '包月书偏好强度',
            String whol_values = fields[19]; // '按本书偏好强度',
            String chpt_values = fields[20]; // '按章书偏好强度',
            String man_values = fields[22];  //男性偏好
            String female_values = fields[23]; //女性偏好
            String low_values = fields[24]; // 俗偏好
            String high_values = fields[25]; // 雅偏好
            String hot_values = fields[26]; // 热书偏好

            UserPrefVector personPref = new UserPrefVector(msisdn, groupType, class1_id, class1_value, class2_id, class2_value,
                    class3_id, class3_value, new_values, serialize_values, famous_values, sales_values, pack_values,
                    whol_values, chpt_values, man_values, female_values, low_values, high_values, hot_values);

            context.write(new Text(msisdn), personPref);
        }
    }
}
