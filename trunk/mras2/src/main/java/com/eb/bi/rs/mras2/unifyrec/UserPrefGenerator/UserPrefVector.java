package com.eb.bi.rs.mras2.unifyrec.UserPrefGenerator;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * Created by LiMingji on 2015/10/16.
 */
public class UserPrefVector implements WritableComparable<UserPrefVector> {
    public String msisdn = null;
    private Double class_weight = null; //分类偏好强度
    public String groupType = null; // '分群ID 1：新用户; 2：深度用户；3：中度用户；4、浅度用户；5、沉默用户',
    public String class1_id = null; // '第一分类偏好',
    public String class1_value = null;// '第一分类偏好强度',
    public String class2_id = null; // '第二分类偏好'
    public String class2_value = null;// '第二分类偏好强度',
    public String class3_id = null;// '第三分类偏好'
    public String class3_value = null; // '第三分类偏好强度',
    public String new_values = null;// '新书偏好强度',
    public String serialize_values = null;// '连载书偏好强度 ',
    public String famous_values = null;// '名家偏好强度',
    public String sales_values = null; // '促销书偏好强度 ',
    public String pack_values = null; // '包月书偏好强度',
    public String whol_values = null; // '按本书偏好强度',
    public String chpt_values = null; // '按章书偏好强度',
    public String man_values= null; // 男性偏好
    public String female_values = null; // 女性偏好
    public String low_values = null;  // 俗偏好
    public String high_values = null; // 雅偏好
    public String hot_values = null;   // 流行偏好
    public Integer stubborn_weight = null; // 用户偏执度


    public UserPrefVector(String msisdn, String groupType, String class1_id, String class1_value, String class2_id,
                          String class2_value, String class3_id, String class3_value, String new_values, String serialize_values,
                          String famous_values, String sales_values, String pack_values, String whol_values, String chpt_values,
                          String man_values, String female_values, String low_values, String high_values, String hot_values) {
        this.msisdn = msisdn;
        this.groupType = groupType;
        this.class1_id = class1_id;
        this.class1_value = class1_value;
        this.class2_id = class2_id;
        this.class2_value = class2_value;
        this.class3_id = class3_id;
        this.class3_value = class3_value;
        this.new_values = new_values;
        this.serialize_values = serialize_values;
        this.famous_values = famous_values;
        this.sales_values = sales_values;
        this.pack_values = pack_values;
        this.whol_values = whol_values;
        this.chpt_values = chpt_values;
        this.man_values = man_values;
        this.female_values = female_values;
        this.low_values = low_values;
        this.high_values = high_values;
        this.hot_values = hot_values;
    }

    public UserPrefVector() {
        this(null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
                null, null, null, null, null);
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof UserPrefVector)) {
            return false;
        }
        UserPrefVector other = (UserPrefVector) obj;
        return this.msisdn.equals(other.msisdn) && this.groupType.equals(other.groupType)
                && this.class1_id.equals(other.class1_id) && this.class1_value.equals(other.class1_value)
                && this.class2_id.equals(other.class2_id) && this.class2_value.equals(other.class2_value)
                && this.class3_id.equals(other.class3_id) && this.class3_value.equals(other.class3_value)
                && this.new_values.equals(other.new_values) && this.serialize_values.equals(other.serialize_values)
                && this.famous_values.equals(other.famous_values) && this.sales_values.equals(other.sales_values)
                && this.pack_values.equals(other.pack_values) && this.whol_values.equals(other.whol_values)
                && this.chpt_values.equals(other.chpt_values) && this.man_values.equals(other.man_values)
                && this.female_values.equals(other.female_values) && this.low_values.equals(other.low_values)
                && this.high_values.equals(other.high_values) && this.hot_values.equals(other.hot_values);
    }

    /**
     * 这里不需要对结果有什么排序。所以不实现该方法。
     *
     * @return
     */
    @Override
    public int compareTo(UserPrefVector o) {
        return 0;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(msisdn);
        dataOutput.writeUTF(groupType);
        dataOutput.writeUTF(class1_id);
        dataOutput.writeUTF(class1_value);
        dataOutput.writeUTF(class2_id);
        dataOutput.writeUTF(class2_value);
        dataOutput.writeUTF(class3_id);
        dataOutput.writeUTF(class3_value);
        dataOutput.writeUTF(new_values);
        dataOutput.writeUTF(serialize_values);
        dataOutput.writeUTF(famous_values);
        dataOutput.writeUTF(sales_values);
        dataOutput.writeUTF(pack_values);
        dataOutput.writeUTF(whol_values);
        dataOutput.writeUTF(chpt_values);
        dataOutput.writeUTF(man_values);
        dataOutput.writeUTF(female_values);
        dataOutput.writeUTF(low_values);
        dataOutput.writeUTF(high_values);
        dataOutput.writeUTF(hot_values);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.msisdn = dataInput.readUTF();
        this.groupType = dataInput.readUTF();
        this.class1_id = dataInput.readUTF();
        this.class1_value = dataInput.readUTF();
        this.class2_id = dataInput.readUTF();
        this.class2_value = dataInput.readUTF();
        this.class3_id = dataInput.readUTF();
        this.class3_value = dataInput.readUTF();
        this.new_values = dataInput.readUTF();
        this.serialize_values = dataInput.readUTF();
        this.famous_values = dataInput.readUTF();
        this.sales_values = dataInput.readUTF();
        this.pack_values = dataInput.readUTF();
        this.whol_values = dataInput.readUTF();
        this.chpt_values = dataInput.readUTF();
        this.man_values = dataInput.readUTF();
        this.female_values = dataInput.readUTF();
        this.low_values = dataInput.readUTF();
        this.high_values = dataInput.readUTF();
        this.hot_values = dataInput.readUTF();
    }

    public String getWeight(Double new_values_avg, Double serialize_values_avg, Double famous_values_avg,
                            Double sales_values_avg, Double pack_values_avg, Double whol_values_avg, Double chpt_values_avg,
                            Double man_values_avg, Double female_values_avg, Double low_values_avg, Double high_values_avg,
                            Double hot_values_avg, HashMap<String, ArrayList<String>> simClass) {
        /**
         用户msisdn|分群group_type|分类属性权重class_weight|新书属性权重new_weight |
         名家属性权重fame_weight|流行书属性权重popular_weight|性别属性权重sex_weight|
         雅俗属性权重classifier_weight|连载属性权重serialize_weight|付费方式属性权重charge_weight|
         促销属性权重sale_weight|包月属性权重pack_weight
         */
        StringBuffer sb = new StringBuffer("");
        //用户msisdn
        sb.append(this.msisdn + "|");
        //用户分群
        sb.append(groupType+"|");
        //分类权重
        sb.append(this.getClassWeight() + "|");
        //新书属性权重
        sb.append(this.getValue(this.new_values, new_values_avg) + "|");
        //名家属性权重
        sb.append(this.getValue(this.famous_values, famous_values_avg) + "|");
        //连载属性权重
        sb.append(this.getValue(this.serialize_values, serialize_values_avg) + "|");
        //付费方式属性权重
        sb.append(this.getChargeWeight(this.serialize_values, serialize_values_avg, whol_values_avg, chpt_values_avg) + "|");
        //促销属性权重
        sb.append(this.getValue(this.sales_values, sales_values_avg) + "|");
        //包月属性权重
        sb.append(this.getValue(this.pack_values, pack_values_avg) + "|");
        //男偏好权重
        sb.append(this.getValue(this.man_values, man_values_avg) + "|");
        //女偏好权重
        sb.append(this.getValue(this.female_values, female_values_avg) + "|");
        //俗偏好权重
        sb.append(this.getValue(this.low_values, low_values_avg) + "|");
        //雅偏好权重
        sb.append(this.getValue(this.high_values, high_values_avg) + "|");
        //热书偏好权重
        sb.append(this.getValue(this.hot_values, hot_values_avg) + "|");
        /**
         * 这里补上前三个分类偏好的ID在计算用户图书向量的时候使用。
         */
        //第一分类偏好
        sb.append(this.class1_id + "|");
        //第二分类偏好
        sb.append(this.class2_id + "|");
        //第三分类偏好
        sb.append(this.class3_id + "|");
        //用户偏执类型
        sb.append(this.stubborn_weight + "|");

        //用户喜好分类的相似分类。取Top2
        ArrayList<String> list = simClass.get(class1_id);
        for (int i = 0, curSize = 0; list != null && i < list.size() && curSize < 2; i++) {
            if (!list.get(i).equals(class1_id) && !list.get(i).equals(class2_id) &&
                    !list.get(i).equals(class3_id)) {
                sb.append(list.get(i) + "|");
                curSize +=1;
            }
        }
        list = simClass.get(class2_id);
        for (int i = 0, curSize = 0; list != null && i < list.size() && curSize < 2; i++) {
            if (!list.get(i).equals(class1_id) && !list.get(i).equals(class2_id) &&
                    !list.get(i).equals(class3_id)) {
                sb.append(list.get(i) + "|");
                curSize +=1;
            }
        }
        list = simClass.get(class3_id);
        for (int i = 0, curSize = 0; list != null && i < list.size() && curSize < 2; i++) {
            if (!list.get(i).equals(class1_id) && !list.get(i).equals(class3_id) &&
                    !list.get(i).equals(class3_id)) {
                sb.append(list.get(i) + "|");
                curSize += 1;
            }
        }
        return sb.toString();
    }

    /**
     * 计算各个偏好的用户权重
     *
     * @param value     用户权重
     * @param avg_value 深度用户的偏好权重均值
     * @return
     */
    private String getValue(String value, Double avg_value) {
        //低于平均值的权重为0
        DecimalFormat dcmFmt = new DecimalFormat("0.0000");
        double currentValue = 0.0;
        try {
             currentValue = Double.parseDouble(value) - avg_value;
        } catch (NumberFormatException e) {
            currentValue = 0.0;
        }
        if (currentValue < 0.00001) {
            currentValue = 0.0;
        }
        return dcmFmt.format(currentValue);
    }

    /**
     * 计算付费方式偏好。参见文档：BI推荐系统之新版统一推荐图书引擎说明书
     *
     * @return
     */
    private String getChargeWeight(String current_serialize, double serialize_avg, double whol_values_avg, double chpt_values_avg) {
        DecimalFormat dcmFmt = new DecimalFormat("0.0000");
        double current_whol_values = Double.parseDouble(this.whol_values);
        double current_chpt_values = Double.parseDouble(this.chpt_values);
        double current_serialize_values = Double.parseDouble(current_serialize);
//        System.out.println("current_serialize: " + (current_serialize_values - serialize_avg));
        if (current_serialize_values - serialize_avg > 0) {
            return dcmFmt.format(0.0);
        }
        if (current_whol_values >= whol_values_avg) {
            return dcmFmt.format(-(current_whol_values - whol_values_avg));
        } else if (current_whol_values < whol_values_avg &&
                current_chpt_values >= chpt_values_avg) {
            return dcmFmt.format(current_chpt_values - chpt_values_avg);
        } else if (current_whol_values < whol_values_avg &&
                current_chpt_values < chpt_values_avg) {
            return dcmFmt.format(0.0);
        }
        return dcmFmt.format(0.0);
    }

    /**
     * 计算用户分类权重。参见文档：BI推荐系统之新版统一推荐图书引擎说明书
     *
     * @return
     */
    private String getClassWeight() {
        //分群1：新用户； 2：深度用户；3：中度用户；4、浅度用户；5、沉默用户',
        Double class1_weight = 0.0;
        try {
            class1_weight = Double.parseDouble(this.class1_value);
        } catch (NumberFormatException e) {
            class1_weight = 0.0;
        }
        Double class2_weight = 0.0;
        try {
            class2_weight = Double.parseDouble(this.class2_value);
        } catch (NumberFormatException e) {
            class2_weight = 0.0;
        }
        Double class3_weight = 0.0;
        try {
            class3_weight = Double.parseDouble(this.class3_value);
        } catch (NumberFormatException e) {
            class3_weight = 0.0;
        }

        //默认为弱偏执
        this.stubborn_weight = 1;
        Integer groupType = Integer.parseInt(this.groupType);
        this.class_weight = 0.1;
        if (class1_weight + class2_weight + class3_weight >= 0.9) {
            if (groupType == 2) {
                this.class_weight = 0.5;
            } else if (groupType == 3) {
                this.class_weight = 0.4;
            } else if (groupType == 4) {
                this.class_weight = 0.3;
            }
            //强偏执
            stubborn_weight = 3;
        } else if (class1_weight + class2_weight + class3_weight < 0.9 &&
                class1_weight + class2_weight + class3_weight >= 0.5) {
            if (groupType == 2) {
                this.class_weight = 0.4;
                //中偏执
                stubborn_weight = 2;
            } else if (groupType == 3) {
                this.class_weight = 0.3;
                //中偏执
                stubborn_weight = 2;
            } else if (groupType == 4) {
                //弱偏执
                stubborn_weight = 1;
            }
        } else if (class1_weight + class2_weight + class3_weight < 0.5 &&
                class1_weight + class2_weight + class3_weight > 0) {
            if (groupType == 2) {
                this.class_weight = 0.3;
            } else if (groupType == 3) {
                this.class_weight = 0.2;
            }
            //弱偏执
            stubborn_weight = 1;
        }
        return this.class_weight + "";
    }

    @Override
    public String toString() {
        return msisdn + "|" + groupType + "|" + class1_id + "|" + class1_value + "|" + class2_id + "|" + class2_value
                + "|" + class3_id + "|" + class3_value + "|" + new_values + "|" + serialize_values + "|" + famous_values
                + "|" + sales_values + "|" + pack_values + "|" + whol_values + "|" + chpt_values + "|" + man_values
                + "|" + female_values + "|" + low_values + "|" + high_values + "|" + hot_values;
    }
}
