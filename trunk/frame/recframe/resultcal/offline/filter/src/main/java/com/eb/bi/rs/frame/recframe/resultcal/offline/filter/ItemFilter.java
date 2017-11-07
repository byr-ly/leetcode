package com.eb.bi.rs.frame.recframe.resultcal.offline.filter;

import org.apache.hadoop.util.Tool;

import com.eb.bi.rs.frame.recframe.resultcal.offline.filter.mr.ItemFilterDriver;

public class ItemFilter extends FilterBase{

	@Override
	public Tool getTool() {
		return new ItemFilterDriver();
	}

}
