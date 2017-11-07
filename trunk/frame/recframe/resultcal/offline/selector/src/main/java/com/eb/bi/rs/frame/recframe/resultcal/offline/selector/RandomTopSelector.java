package com.eb.bi.rs.frame.recframe.resultcal.offline.selector;

import org.apache.hadoop.util.Tool;

import com.eb.bi.rs.frame.recframe.resultcal.offline.selector.mr.RandomTopSelectorDriver;

public class RandomTopSelector extends SelectorBase{

	@Override
	public Tool getTool() {		
		return new RandomTopSelectorDriver();
	}
	

}
