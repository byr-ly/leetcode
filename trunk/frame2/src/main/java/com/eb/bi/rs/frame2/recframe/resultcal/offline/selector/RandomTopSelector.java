package com.eb.bi.rs.frame2.recframe.resultcal.offline.selector;

import com.eb.bi.rs.frame2.recframe.resultcal.offline.selector.mr.RandomTopSelectorDriver;
import org.apache.hadoop.util.Tool;

public class RandomTopSelector extends SelectorBase{

	@Override
	public Tool getTool() {		
		return new RandomTopSelectorDriver();
	}
	
}
