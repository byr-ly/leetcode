package com.eb.bi.rs.frame2.common.storm.datainput;

public class ParserFactory {
    public static ParserBase getParse(String name) {
        ParserBase parser = null;

        try {
            parser = (ParserBase) Class.forName("com.eb.bi.rs.frame2.common.storm.datainput." + name + "Parser").newInstance();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (InstantiationException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }
        return parser;
    }

    private ParserFactory() {
    }

//	public static void main(String[] args){
//		String name = "XmlParser";
//		ParserBase parser = ParserFactory.getParse(name);
//		parser.print();
//	}
}
