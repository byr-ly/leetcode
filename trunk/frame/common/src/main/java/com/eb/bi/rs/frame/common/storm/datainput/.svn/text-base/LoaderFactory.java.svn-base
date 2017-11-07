package com.eb.bi.rs.frame.common.storm.datainput;

public class LoaderFactory {
	public static LoaderBase getLoader(String name){
		LoaderBase loader = null;
		try{
			loader = (LoaderBase)Class.forName("com.eb.bi.rs.frame.common.storm.datainput."+name+"Loader").newInstance();
		}
		catch(ClassNotFoundException e) {
			e.printStackTrace();
		} catch (InstantiationException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		}	
		return loader;
	}
	private LoaderFactory(){}
}
