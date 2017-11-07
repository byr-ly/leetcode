package com.eb.bi.rs.mras.seachrec.keyseach.online;

public class BookPrefData implements Comparable {
	private String m_name;
	private double m_number;
	
	public BookPrefData(String name, double number){
		m_name = name;
		m_number = number;
	}
	
	public int compareTo(Object o) {
        if (o instanceof BookPrefData) {
            int cmp = Double.compare(m_number, ((BookPrefData) o).m_number);
            if (cmp != 0) {
                return cmp;
            }
            return m_name.compareTo(((BookPrefData) o).m_name);
        }

        throw new ClassCastException("Cannot compare Pair with "
                + o.getClass().getName());

    }
	
	public String getbookId(){
		return m_name;
	}
	
	public String toString() {
        return m_name + ' ' + m_number;
    }
}
