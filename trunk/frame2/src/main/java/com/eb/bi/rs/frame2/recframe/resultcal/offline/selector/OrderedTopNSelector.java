package com.eb.bi.rs.frame2.recframe.resultcal.offline.selector;

import com.eb.bi.rs.frame2.recframe.resultcal.offline.selector.mr.OrderedTopNSelectorDriver;
import org.apache.hadoop.util.Tool;

public class OrderedTopNSelector extends SelectorBase{

	@Override
	public Tool getTool() {
		return new OrderedTopNSelectorDriver();
	}

}
