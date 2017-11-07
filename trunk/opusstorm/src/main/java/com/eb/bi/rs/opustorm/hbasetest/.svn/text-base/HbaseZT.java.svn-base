package com.eb.bi.rs.opustorm.hbasetest;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.security.UserGroupInformation;

public class HbaseZT {
	static Configuration config = null;
	static String tableName = "hbase_rec:opus_test";

	static {
		System.setProperty("java.security.krb5.kdc", "fjxm-dm-dell-ztenn-01");
		System.setProperty("java.security.krb5.realm", "NBDP.COM");
		config = HBaseConfiguration.create();
		// Configuration config = new Configuration();
		config.set("hbase.zookeeper.quorum", "fjxm-dm-dell-ztenn-01");
		config.set("hbase.zookeeper.property.clientPort", "2181");
		config.set("hadoop.security.authentication", "kerberos");
		config.set("hbase.security.authorization", "true");
		config.set("hbase.master.kerberos.principal", "hbase/_HOST@NBDP.COM");
		config.set("hbase.regionserver.kerberos.principal",
				"hbase/_HOST@NBDP.COM");
		config.set("hbase.thrift.kerberos.principal", "hbase/_HOST@NBDP.COM");
		config.set("hbase.zookeeper.client.kerberos.principal",
				"zookeeper/_HOST@NBDP.COM");
		UserGroupInformation.setConfiguration(config);
		UserGroupInformation userGroupInformation;
		try {
			userGroupInformation = UserGroupInformation
					.loginUserFromKeytabAndReturnUGI("eb@NBDP.COM",
							"/home/eb/idox/repository/job0002/meta_data/eb.keytab");
			UserGroupInformation.setLoginUser(userGroupInformation);
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	public static void main(String[] args) throws IOException {
		HTable table = new HTable(config, tableName);
		Scan scan = new Scan();
		Filter filter1 = new RowFilter(CompareFilter.CompareOp.LESS_OR_EQUAL,
				new BinaryComparator("row010".getBytes()));
		scan.setFilter(filter1);
		ResultScanner scanner1 = table.getScanner(scan);
		for (Result res : scanner1) {
			System.out.println(res);
		}
		scanner1.close();
		
	}
}
