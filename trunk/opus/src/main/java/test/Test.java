package test;

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

public class Test {
	static String tableName = "hbase_dwa:test";

	
	static Configuration config = null;
	
	static {
		System.setProperty("java.security.krb5.kdc", "kdc主机名，形如host63");
		System.setProperty("java.security.krb5.realm", "kerberos域名，形如COOLTAN.COM");
		config = HBaseConfiguration.create();
		// Configuration config = new Configuration();
		config.set("hbase.zookeeper.quorum", "zookeeper主机名");
		config.set("hbase.zookeeper.property.clientPort", "端口");
		config.set("hadoop.security.authentication", "kerberos");
		config.set("hbase.security.authorization", "true");
		config.set("hbase.master.kerberos.principal", "hbase服务主体，形如hbase/_HOST@COOLTAN.COM");
		config.set("hbase.regionserver.kerberos.principal",
				"hbase服务主体，形如hbase/_HOST@COOLTAN.COM");
		config.set("hbase.thrift.kerberos.principal", "hbase服务主体，形如hbase/_HOST@COOLTAN.COM");
		config.set("hbase.zookeeper.client.kerberos.principal",
				"zookeeper服务主体，形如zookeeper/_HOST@COOLTAN.COM");
		UserGroupInformation.setConfiguration(config);
		UserGroupInformation userGroupInformation;
		try {
			userGroupInformation = UserGroupInformation
					.loginUserFromKeytabAndReturnUGI("租户主体，形如crm@COOLTAN.COM",
							"密钥文件路径，形如/home/keytab/crm/crm.keytab");
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
