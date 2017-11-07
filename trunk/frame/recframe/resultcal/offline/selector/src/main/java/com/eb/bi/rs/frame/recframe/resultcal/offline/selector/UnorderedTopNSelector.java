package com.eb.bi.rs.frame.recframe.resultcal.offline.selector;

import org.apache.hadoop.util.Tool;
import com.eb.bi.rs.frame.recframe.resultcal.offline.selector.mr.UnorderedTopNSelectorDriver;

public class UnorderedTopNSelector extends SelectorBase {
	@Override
	public Tool getTool() {
		return new UnorderedTopNSelectorDriver();
	}

}
