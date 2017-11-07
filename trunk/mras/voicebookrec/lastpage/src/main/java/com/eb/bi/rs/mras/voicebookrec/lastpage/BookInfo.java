package com.eb.bi.rs.mras.voicebookrec.lastpage;


/*图书ID|系列ID|系列顺序|栏目ID*/
public class BookInfo {
	private String bookId;
	private String serialId;
	private int sequence;
	private String columnId;
	
	
	public BookInfo(){		
	}
	
	public BookInfo(String line) {
		String[] fields = line.split("\\|", -1);
		if(fields.length == 4){
			this.bookId = fields[0];
			this.serialId = fields[1];
			if("".equals(fields[2]))
			{	
				this.sequence = 0;
			}else {
				this.sequence = Integer.parseInt(fields[2]);
			}
			this.columnId = fields[3];
		}
	}
	
	public BookInfo(String bookId, String serialId, int sequence,String columnId) {
		this.bookId = bookId;
		this.serialId = serialId;
		this.sequence = sequence;
		this.columnId = columnId;
	}
	
	
	public String getBookId() {
		return bookId;
	}
	public void setBookId(String bookId) {
		this.bookId = bookId;
	}
	public String getSerialId() {
		return serialId;
	}
	public void setSerialId(String serialId) {
		this.serialId = serialId;
	}
	public int getSequence() {
		return sequence;
	}
	
	public void setSequence(int sequence) {
		this.sequence = sequence;
	}
	public String getColumnId() {
		return columnId;
	}
	public void setColumnId(String columnId) {
		this.columnId = columnId;
	}
	
	@Override
	public String toString() {	
		return bookId+"|" + serialId + "|" + sequence + "|" + columnId;
	}
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((bookId == null) ? 0 : bookId.hashCode());
		result = prime * result
				+ ((columnId == null) ? 0 : columnId.hashCode());
		result = prime * result + sequence;
		result = prime * result
				+ ((serialId == null) ? 0 : serialId.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if(obj instanceof BookInfo){
			BookInfo other = (BookInfo)obj;
			return bookId.equals(other.bookId) && serialId.equals(other.serialId) && sequence == other.sequence && columnId.equals(other.columnId) ;
		}
		return super.equals(obj);

	}

	
	

}
