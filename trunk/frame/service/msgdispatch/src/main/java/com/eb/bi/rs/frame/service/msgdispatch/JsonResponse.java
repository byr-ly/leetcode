package com.eb.bi.rs.frame.service.msgdispatch;

import javax.xml.bind.annotation.XmlRootElement;

/**
 * Created by lenovo on 14-3-10.
 */
@XmlRootElement
public class JsonResponse {
    private String resCode;
    private String resDesc;


    public JsonResponse() {
    }

    public JsonResponse(String resCode,String resDesc) {
       this.resDesc = resDesc;
       this.resCode = resCode;
    }


    public String getResCode() {
        return resCode;
    }

    public void setResCode(String resCode) {
        this.resCode = resCode;
    }

    public String getResDesc() {
        return resDesc;
    }
    public void setResDesc(String resDesc) {
        this.resDesc = resDesc;
    }

}
