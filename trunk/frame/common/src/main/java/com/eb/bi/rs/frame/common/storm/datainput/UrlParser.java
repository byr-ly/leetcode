package com.eb.bi.rs.frame.common.storm.datainput;
import java.io.UnsupportedEncodingException;
import java.util.Map;

import com.eb.bi.rs.frame.common.storm.config.ConfRecord;



public class UrlParser extends ParserBase {
	public UrlParser(){}
	public UrlParser(ConfRecord meta){
		super(meta);
	}
	public boolean doParse(String text, DataRecord record){
		try {
			parseParameters(text,"utf-8",record);
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return true;
	}
	public void print(){
		System.out.println("i am UrlParser");
	}
	
	private void parseParameters(String data, String encoding, DataRecord record) 
			throws UnsupportedEncodingException {
		if ((data == null) || (data.length() <= 0)) {
			return;
		}
		byte[] bytes = null;
		try {
			if (encoding == null)
				bytes = data.getBytes();
			else
				bytes = data.getBytes(encoding);
		}
		catch (UnsupportedEncodingException uee){
		}
		parseParameters(bytes, encoding, record);
	}
	
	private void parseParameters(byte[] data, String encoding, DataRecord record) throws UnsupportedEncodingException
	  {
	    if ((data != null) && (data.length > 0)) {
	      int ix = 0;
	      int ox = 0;
	      String key = null;
	      String value = null;
	      while (ix < data.length) {
	        byte c = data[(ix++)];
	        switch ((char)c)
	        {
	        case '&':
	          value = new String(data, 0, ox, encoding);
	          if (key != null) {
	            putMapEntry(key, value,record);
	            key = null;
	          }
	          ox = 0;
	          break;
	        case '=':
	          if (key == null) {
	            key = new String(data, 0, ox, encoding);
	            ox = 0;
	          } else {
	            data[(ox++)] = c;
	          }
	          break;
	        case '+':
	          data[(ox++)] = 32;
	          break;
	        case '%':
	          data[(ox++)] = (byte)((convertHexDigit(data[(ix++)]) << 4) + convertHexDigit(data[(ix++)]));

	          break;
	        default:
	          data[(ox++)] = c;
	        }
	      }

	      if (key != null) {
	        value = new String(data, 0, ox, encoding);
	        putMapEntry(key, value, record);
	      }
	    }
	  }
	private  void putMapEntry(String name, String value, DataRecord record) {
		/*    
			String[] newValues = null;
		    String[] oldValues = (String[])(String[])map.get(name);
		    if (oldValues == null) {
		      newValues = new String[1];
		      newValues[0] = value;
		    } else {
		      newValues = new String[oldValues.length + 1];
		      System.arraycopy(oldValues, 0, newValues, 0, oldValues.length);
		      newValues[oldValues.length] = value;
		    }
		    map.put(name, newValues);*/
		DataField dataField= new DataField(value);
		record.addField(dataField);
	}
		  private  byte convertHexDigit(byte b)
		  {
		    if ((b >= 48) && (b <= 57)) return (byte)(b - 48);
		    if ((b >= 97) && (b <= 102)) return (byte)(b - 97 + 10);
		    if ((b >= 65) && (b <= 70)) return (byte)(b - 65 + 10);
		    return 0;
		  }
	
}
