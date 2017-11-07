package test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;

public class HiveTest {	

	public static void main(String[] args) throws Exception {
		System.setProperty("java.security.krb5.kdc","fjxm-dm-dell-ztenn-01");
		System.setProperty("java.security.krb5.realm", "NBDP.COM");
		Configuration conf = new Configuration();
		conf.set("hadoop.security.authentication", "Kerberos");
		UserGroupInformation.setConfiguration(conf);
		UserGroupInformation userGroupInformation = UserGroupInformation.
				loginUserFromKeytabAndReturnUGI("eb@NBDP.COM", "/home/eb/recsys/eb.keytab");
		UserGroupInformation.setLoginUser(userGroupInformation);
		Class.forName("org.apache.hive.jdbc.HiveDriver");
		Connection conn = DriverManager.getConnection(
				"jdbc:hive2://fjxm-dm-dell-ztenn-02:10000/hive_dwa;principal=hive/fjxm-dm-dell-ztenn-02@NBDP.COM",
				"", "");
		String hsql = "select * from test3";
		Statement stmt = conn.createStatement();
		ResultSet rs = stmt.executeQuery(hsql);
		stmt.equals(hsql);
		while(rs.next()){
			System.out.println(rs.getString(1));
		}
		rs.close();
		stmt.close();
		conn.close();
	}

}
