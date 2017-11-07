package com.eb.bi.rs.mrasstorm.correrecrealtimefilter;

import java.util.HashSet;

/**
 * Created by cyx on 2017/08/04.
 */
public class UserVector {

    public String msisdn;
    public String class_weight;
    public String new_weight;
    public String famous_weight;
    public String serialize_weight;
    public String charge_weight;
    public String sale_weight;
    public String pack_weight;
    public String man_weight;
    public String female_weight;
    public String low_weight;
    public String high_weight;
    public String hot_weight;
    public String class1_id;
    public String class2_id;
    public String class3_id;
    public String stubborn_weight;

    public HashSet<String> simClass;

    public UserVector(String msisdn, String class_weight, String new_weight, String famous_weight,
               String serialize_weight, String charge_weight, String sale_weight, String pack_weight,
               String man_weight, String female_weight, String low_weight, String high_weight, String hot_weight,
               String class1_id, String class2_id, String class3_id, String stubborn_weight, HashSet<String> simClass) {
        this.msisdn = msisdn;
        this.class_weight = class_weight;
        this.new_weight = new_weight;
        this.famous_weight = famous_weight;
        this.serialize_weight = serialize_weight;
        this.charge_weight = charge_weight;
        this.sale_weight = sale_weight;
        this.pack_weight = pack_weight;
        this.man_weight = man_weight;
        this.female_weight = female_weight;
        this.low_weight = low_weight;
        this.high_weight = high_weight;
        this.hot_weight = hot_weight;
        this.class1_id = class1_id;
        this.class2_id = class2_id;
        this.class3_id = class3_id;
        this.stubborn_weight = stubborn_weight;

        this.simClass = simClass;
    }

    @Override
    public String toString() {
        return msisdn + "|" + class_weight + "|" + new_weight + "|" + famous_weight + "|" + hot_weight + "|"
                + serialize_weight + "|" + charge_weight + "|" + sale_weight + "|" + pack_weight + "|"
                + man_weight + "|" + female_weight + "|" + high_weight + "|" + low_weight + "|" + class1_id + "|" +
                class2_id + "|" + class3_id + "|" + stubborn_weight;
    }
}
