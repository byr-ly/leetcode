package test;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;

public class Hdfs {
	public static void main(String[] args) throws IOException {
		Configuration conf = new Configuration();
		conf.set("fs.default.name", "hdfs://fjxm-dm-dell-ztenn-01:8020");
		conf.set("hadoop.security.authentication", "Kerberos");
		conf.set("dfs.namenode.kerberos.principal","hdfs/_HOST@NNBDP.COM");
		System.setProperty("java.security.krb5.kdc","fjxm-dm-dell-ztenn-01"); 
		System.setProperty("java.security.krb5.realm", "NBDP.COM");
		UserGroupInformation.setConfiguration(conf);
		UserGroupInformation.loginUserFromKeytab("eb@NBDP.COM", "/home/eb/recsys/eb.keytab");
		
		FileSystem fs = FileSystem.get(URI.create("hdfs://fjxm-dm-dell-ztenn-01:8020/user/hive/warehouse/test.txt"), conf);
		FSDataInputStream hdfsInStream = fs.open(new Path("hdfs://fjxm-dm-dell-ztenn-01:8020/user/hive/warehouse/test.txt"));
		OutputStream out = new FileOutputStream("D:/qq-hdfs.txt"); 
		
		byte[] ioBuffer = new byte[1024];
		int readLen = hdfsInStream.read(ioBuffer);
		
		 while(-1 != readLen){
			  out.write(ioBuffer, 0, readLen);  
			  readLen = hdfsInStream.read(ioBuffer);
		}
		 out.close();
		  hdfsInStream.close();
		  fs.close();
	}
}
