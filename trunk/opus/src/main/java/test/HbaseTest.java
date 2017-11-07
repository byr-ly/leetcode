package test;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.security.UserGroupInformation;

public class HbaseTest {
	
	static Configuration config = null;
	
	static{
		System.setProperty("java.security.krb5.kdc","fjxm-dm-dell-ztenn-01");
		System.setProperty("java.security.krb5.realm", "NBDP.COM");
		config = HBaseConfiguration.create();
		//Configuration config = new Configuration();
		config.set("hbase.zookeeper.quorum", "fjxm-dm-dell-ztenn-01");
		config.set("hbase.zookeeper.property.clientPort", "2181");
		config.set("hadoop.security.authentication", "kerberos");
		config.set("hbase.security.authorization", "true");
		config.set("hbase.master.kerberos.principal", "hbase/_HOST@NBDP.COM");
		config.set("hbase.regionserver.kerberos.principal","hbase/_HOST@NBDP.COM");
		config.set("hbase.thrift.kerberos.principal", "hbase/_HOST@NBDP.COM");
		config.set("hbase.zookeeper.client.kerberos.principal", "zookeeper/_HOST@NBDP.COM");
		UserGroupInformation.setConfiguration(config);
		UserGroupInformation userGroupInformation;
		try {
			userGroupInformation = UserGroupInformation.
					loginUserFromKeytabAndReturnUGI("eb@NBDP.COM", "/home/eb/recsys/eb.keytab");
			UserGroupInformation.setLoginUser(userGroupInformation);
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}
	
	public static void main(String[] args) throws IOException {
		
		HBaseAdmin admin =new HBaseAdmin(config);
		System.out.println("hbase_dwa:test");
		
		if (admin.tableExists("hbase_dwa:test")) { 
			System.out.println("存在");
		}else{
			System.out.println("不存在");
		}
		
	}
}
