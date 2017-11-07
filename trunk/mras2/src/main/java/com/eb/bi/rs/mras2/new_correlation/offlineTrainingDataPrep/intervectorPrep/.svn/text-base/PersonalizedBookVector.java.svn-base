package com.eb.bi.rs.mras2.new_correlation.offlineTrainingDataPrep.intervectorPrep;

import com.eb.bi.rs.mras2.unifyrec.SeparatePrefRecResult.SeparatePrefRecResultReducer;
import com.eb.bi.rs.mras2.unifyrec.UserBookScoreTools.UserVector;

import java.util.HashMap;
import java.util.HashSet;

/**
 * Created by LiMingji on 2015/11/10.
 * Revised by LiuJie on 2016/04/10.
 */
public class PersonalizedBookVector {
    public String bookID;
    public String classId;
    public Double class_weight;
    public Integer new_weight;

    public Integer sex_weight;
    public Integer classifier_weight;
    public Integer serialize_weight;
    public Integer charge_weight;

    public PersonalizedBookVector(HashMap<String, Double> bookScores, String bookID, String classId, String ifNew, 
               String chargeType, String ifFinish, String sexId) {
        this.bookID = bookID;
        this.classId = classId;

        new_weight = this.getIntegerValue(ifNew) == 1 ? 1 : 0;
        int chargeTypeInt = this.getIntegerValue(chargeType);
        if (chargeTypeInt == 2) {
            charge_weight = 1;
        } else if (chargeTypeInt == 1) {
            charge_weight = -1;
        } else {
            charge_weight = 0;
        }
        serialize_weight = this.getIntegerValue(ifFinish) == 0 ? 1 : 0;
        int sexIdNum = this.getIntegerValue(sexId);
        if (sexIdNum == 1) {
            sex_weight = 1;
        } else if (sexIdNum == 3) {
            sex_weight = 0;
        } else if (sexIdNum == 2) {
            sex_weight = -1;
        } else {
            sex_weight = 438;
        }

        if (!bookScores.containsKey(bookID)) {
            classifier_weight = 438;
        } else {
            double score = bookScores.get(bookID);
            if (score >= 90) {
                this.classifier_weight = 1;
            } else if (score < 10) {
                this.classifier_weight = -1;
            } else {
                this.classifier_weight = 0;
            }
        }
    }

    /**
     * 类型转换函数。如果产生异常则为0.否则则为String中的值。
     *
     * @param value
     * @return
     */
    private double getDoubleValue(String value) {
        if (value == null) {
            return 0;
        }
        try {
            return Double.parseDouble(value.trim());
        } catch (NumberFormatException e) {
            return 0;
        }
    }

    private int getIntegerValue(String value) {
        if (value == null) {
            return 0;
        }
        try {
            return Integer.parseInt(value.trim());
        } catch (NumberFormatException e) {
            return 0;
        }
    }

    @Override
    public String toString() {
        return "图书向量: " + this.bookID + "|" + classId + "|" + new_weight + "|" +
                sex_weight + "|" + +serialize_weight + "|" +
                charge_weight ;
    }
    public String getInterVector(UserVector user, HashSet<String> classSet){
    	/**
         * 计算用户分类权重的值。
         * 强偏执3 中偏执2 弱偏执1
         */

        int userStubbornWeight = getIntegerValue(user.stubborn_weight);
        if (this.classId == null) {
            this.class_weight = 0.0;
        } else {
            if (SeparatePrefRecResultReducer.debug) {
                System.out.println("图书分类: " + this.classId + " stubborn_weight " + userStubbornWeight);
                System.out.println("用户喜好分类的相似分类: " + user.simClass);
                System.out.println("用户阅读过的分类: " + classSet);
            }

            //属于用户喜欢的前三个分类
            if (this.classId.equals(user.class1_id) || this.classId.equals(user.class2_id) ||
                    this.classId.equals(user.class3_id)) {
                this.class_weight = 1.0;
                if(SeparatePrefRecResultReducer.debug) {
                    System.out.println("属于用户喜欢的前三个分类");
                }
            }
            //不属于用户喜欢的三个分类和用户喜欢分类的相似分类但是是用户阅读过的分类。
            else if ((user.simClass != null && !user.simClass.contains(this.classId))&&(classSet != null && classSet.contains(this.classId))) {
            	 //强分类偏执
                if (userStubbornWeight == 3) {
                    this.class_weight = 0.1;
                } else if (userStubbornWeight == 2) {
                    this.class_weight = 0.3;
                } else {
                    this.class_weight = 0.5;
                }
                if (SeparatePrefRecResultReducer.debug) {
                    System.out.println("属于用户阅读过的分类");
                }
            } else {
                if (userStubbornWeight == 3) {
                    this.class_weight = -1.0;
                } else if (userStubbornWeight == 2) {
                    this.class_weight = -0.5;
                } else {
                    this.class_weight = 0.0;
                }
                if (SeparatePrefRecResultReducer.debug) {
                    System.out.println("用户没有接触过的分类");
                }
            }
        }
        /**
         * 性别偏好计算
         */
        double sex_weight_values = 0.0;
        if (sex_weight == 1) {
            sex_weight_values = getDoubleValue(user.man_weight) - getDoubleValue(user.female_weight);
        } else if (sex_weight == -1) {
            sex_weight_values = getDoubleValue(user.female_weight) - getDoubleValue(user.man_weight);
        } else if (sex_weight == 0) {
            sex_weight_values = getDoubleValue(user.man_weight) + getDoubleValue(user.female_weight);
        } else if (sex_weight == 438) {
            sex_weight_values = 0.0;
        }

        /**
         * 雅俗偏好计算
         */
        double classifier_weight_values = 0.0;
        if (classifier_weight == 1) {
            classifier_weight_values = getDoubleValue(user.high_weight) - getDoubleValue(user.low_weight);
        } else if (classifier_weight == -1) {
            classifier_weight_values = getDoubleValue(user.low_weight) - getDoubleValue(user.high_weight);
        } else if (classifier_weight == 0) {
            classifier_weight_values = getDoubleValue(user.high_weight) + getDoubleValue(user.low_weight);
        } else if (classifier_weight == 438) {
            classifier_weight_values = 0.0;
        }

        /**
         * 对于有付费方式偏好的用户，如果其有按本偏好，则将按章付费的图书付费方式属性值改为’0’；
         * 如果其有按章偏好，则将按本付费的图书付费方式属性值改为’0’。
         * 2是按章，用户是正。 1是按本。 用户是负的。
         *
         * ps: 也就是说结果是正的，则保留，是负的。取0.
         */
        double charge_weight_values = getDoubleValue(user.charge_weight) * this.charge_weight;
        charge_weight_values = charge_weight_values > 0 ? charge_weight_values : 0;

        /**
         * 用户向量和图书向量对应字段进行相乘。结果为用户和图书的交互的向量结果。用于关联推荐个性化推荐的训练数据。
         */
        String inter_vector =
                getDoubleValue(user.class_weight) * this.class_weight +"|"
         +      0.2*getDoubleValue(user.new_weight) * this.new_weight+ "|"
         +      0.2*sex_weight_values +"|"
         +      0.2*classifier_weight_values+"|"
         +      0.2*getDoubleValue(user.serialize_weight) * this.serialize_weight+"|"
         +      0.2*charge_weight_values;

        return inter_vector;
    	
    }
}
