package com.eb.bi.rs.qhll.userapprec.userapppref;




public class UserAppInfo{
	
	private String msisdn;
	private String serviceCode;
	private String serviceType;
	private double[] indicators;
	private int indicatorCnt;
	
    private double preferScore;
    
    public UserAppInfo(){    	
    }
    
    public String getMSISDN(){
    	return msisdn;
    }
    public void setMSISDN(String msisdn){
    	this.msisdn = msisdn;
    }
    public String getServiceCode(){
    	return serviceCode;
    }
    public void setServiceCode(String serviceCode) {
    	this.serviceCode = serviceCode;
    }
    
    public String getServiceType(){
    	return serviceType;
    } 
    public void setServiceType(String serviceType){
    	this.serviceType = serviceType;
    }
    
    public void setIndicators(double[] indicators){
    	this.indicators = indicators;
    }
    public double[] getIndicators(){
    	return indicators;
    }
    public void setIndicatorCnt(int indicatorCnt){
    	this.indicatorCnt = indicatorCnt;
    }
    
    public int getIndicatorCnt() {
    	return indicatorCnt;
    }

    public double getPreferScore(){
    	return preferScore;
    }   
    public void setPreferScore(double preferScore){
    	this.preferScore = preferScore;
    }
    @Override
    public String toString(){
    	String result = msisdn + "|" + serviceCode + "|" + serviceType + "|";
    	for(int idx = 0; idx < indicatorCnt; ++idx)
    	{
    		result += indicators[idx] + "|";
    	}
		return result.substring(0, result.length() - 1);    	
    }
}