package com.eb.bi.rs.mras2.unifyrec.ForceRecClassPref;

import com.eb.bi.rs.mras2.unifyrec.UserBookScoreTools.UserVector;

import java.util.HashSet;

/**
 * Created by LiuJie on 2016/04/10.
 */
public class ForceRecBookVector {
    public String bookID;
    public String classId;
    public String pageId;
	private double class_weight;
 
    public ForceRecBookVector(String bookID, String classId, String pageId) {
        this.bookID = bookID;
        this.classId = classId;
        this.pageId = pageId;
    }

    public double getClassWeight(UserVector user, HashSet<String> classSet) {
        /**
         * 计算用户分类权重的值。
         * 强偏执3 中偏执2 弱偏执1
         */

        int userStubbornWeight = getIntegerValue(user.stubborn_weight);
        if (this.classId == null) {
            this.class_weight = 0.0;
        } else {
            
            //属于用户喜欢的前三个分类
            if (this.classId.equals(user.class1_id) || this.classId.equals(user.class2_id) ||
                    this.classId.equals(user.class3_id)) {
                this.class_weight = 1.0;
            }
            //不属于用户喜欢的三个分类，但是用户喜欢分类的相似分类。
            else if (user.simClass != null && user.simClass.contains(this.classId)) {
                if (userStubbornWeight == 3) {
                    this.class_weight = 0.8;
                } else {
                    this.class_weight = 1.0;
                }                
            }
            //属于用户阅读过的分类
            else if (classSet != null && classSet.contains(this.classId)) {
                //强分类偏执
                if (userStubbornWeight == 3) {
                    this.class_weight = 0.0;
                } else if (userStubbornWeight == 2) {
                    this.class_weight = 0.3;
                } else {
                    this.class_weight = 0.5;
                }               
            } else {
                if (userStubbornWeight == 3) {
                    this.class_weight = -1.0;
                } else if (userStubbornWeight == 2) {
                    this.class_weight = -0.5;
                } else {
                    this.class_weight = 0.0;
                }                
            }
        }

        return this.class_weight;
    }

    /**
     * 类型转换函数。如果产生异常则为0.否则则为String中的值。
     *
     * @param value
     * @return
     */
   
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
        return  this.bookID + "|" + pageId + "|" + Double.toString(this.class_weight);
    }
}
