package com.eb.bi.rs.mras2.bookrec.guessyoulike;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class BookInfo implements WritableComparable<BookInfo> {
	
	private String bookId;
	private double confidence;
	private short classType;
	
	public BookInfo() {
		
	}
	
	public BookInfo(BookInfo rhs) {
		set(rhs.bookId, rhs.confidence, rhs.classType);		
	}
	
	public BookInfo(String bookId, double confidence, short classType) {
		set(bookId, confidence, classType);
	}	
	
	public void set(String bookId, double confidence, short classType) {
		this.bookId = bookId;
		this.confidence = confidence;
		this.classType = classType;	
	}
	
	public String getBookId() {
		return bookId;
	}

	public void setBookId(String bookId) {
		this.bookId = bookId;
	}

	public double getConfidence() {
		return confidence;
	}

	public void setConfidence(double confidence) {
		this.confidence = confidence;
	}

	public short getClassType() {
		return classType;
	}

	public void setClassType(short classType) {
		this.classType = classType;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(bookId);
		out.writeDouble(confidence);
		out.writeShort(classType);
		
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.bookId = in.readUTF();
		this.confidence = in.readDouble();
		this.classType = in.readShort();	
	}

	@Override
	public int compareTo(BookInfo o) {
		if (this.classType < o.classType) {
			return 1;
		} 
		if (this.classType > o.classType) {
			return -1;
		} 
		if (this.confidence < o.confidence) {
			return -1;			
		} 
		if (this.confidence > o.confidence) {
			return 1;			
		}		
		return this.bookId.compareTo(o.bookId);
	}
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((bookId == null) ? 0 : bookId.hashCode());
		result = prime * result + classType;
		long temp;
		temp = Double.doubleToLongBits(confidence);
		result = prime * result + (int) (temp ^ (temp >>> 32));
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		BookInfo other = (BookInfo) obj;
		if (bookId == null) {
			if (other.bookId != null)
				return false;
		} else if (!bookId.equals(other.bookId))
			return false;
		if (classType != other.classType)
			return false;
		if (Double.doubleToLongBits(confidence) != Double
				.doubleToLongBits(other.confidence))
			return false;
		return true;
	}
	
	@Override
	public String toString() {
		return bookId + "|" + confidence + "|" + classType;
	}

}
