package com.eb.bi.rs.frame.service.dataload.file2redis;


public class LoaderFactory {	
	
	public static LoaderBase getLoader(String type) {
		LoaderBase loader = null;
		
		try {
			loader = Class.forName("com.eb.bi.rs.frame.service.dataload.file2redis." + type + "Loader").asSubclass(LoaderBase.class).newInstance();
		} catch (InstantiationException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
		return loader;
	}
	
	private LoaderFactory() {}
	

}



