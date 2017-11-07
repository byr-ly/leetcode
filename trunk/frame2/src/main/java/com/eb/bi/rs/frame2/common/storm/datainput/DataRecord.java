package com.eb.bi.rs.frame2.common.storm.datainput;


import com.eb.bi.rs.frame2.common.storm.config.ConfRecord;
import com.eb.bi.rs.frame2.common.storm.config.ConfRecordField;

import java.util.Vector;


public class DataRecord {

    public Vector<DataField> m_fields = new Vector<DataField>();
    private ConfRecord m_rConf = null;

    public DataRecord(ConfRecord rConf) {
        m_rConf = rConf;
    }

    //tangkun+
    public ConfRecord getConfRecord() {
        return m_rConf;
    }

    public int size() {
        return m_fields.size();
    }

    public DataField getField(String name) {
        ConfRecordField tmp = m_rConf.getField(name);
        if (tmp == null) {
            return null;
        }
        int index = tmp.getIndex();
        return getField(index);
    }

    public DataField getField(int index) {//��0��ʼ

        //System.out.println("size is:" + m_fields.size());

        if (index >= m_fields.size()) {
            return null;
        }
        return m_fields.elementAt(index);
    }

    public void addField(DataField field) {
        m_fields.add(field);
    }

}
