package test;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.security.UserGroupInformation;

public class Hbase1 {
	static Configuration config = null;
	static String tableName = "hbase_rec:opus_opus_info";

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
							"/home/eb/recsys/eb.keytab");
			UserGroupInformation.setLoginUser(userGroupInformation);
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	public static void main(String[] args) throws IOException {
		HTable table = new HTable(config, tableName);
		System.out.println("step1");
		Put put = new Put(Bytes.toBytes("123"));
		put.add(Bytes.toBytes("cf"), Bytes.toBytes("test"),
				Bytes.toBytes("val1"));
		System.out.println("step2");
		table.put(put);
		System.out.println("step3");
	}
}
