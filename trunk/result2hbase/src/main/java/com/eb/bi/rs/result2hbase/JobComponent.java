package com.eb.bi.rs.result2hbase;


/**
 * 各类Job的基类，组合模式中的Component
 */
public abstract class JobComponent {

	private String name;

	public JobComponent(String name) {
		this.name = name;
	}

	/**
	 * Composite应该实现此方法
	 */
	public void add(JobComponent jobComponent) {
		throw new UnsupportedOperationException();
	}

	/**
	 * Composite应该实现此方法
	 */
	public void remove(JobComponent jobComponent) {
		throw new UnsupportedOperationException();
	}

	public abstract int run(String[] args) throws Exception;

	public String getName() {
		return name;
	}
}
