package com.eb.bi.rs.opus.hdfs2hbase.java2hbase;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.Date;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.security.UserGroupInformation;
import com.eb.bi.rs.frame2.common.pluginutil.PluginConfig;
import com.eb.bi.rs.frame2.common.pluginutil.PluginExitCode;
import com.eb.bi.rs.frame2.common.pluginutil.PluginResult;
import com.eb.bi.rs.frame2.common.pluginutil.PluginUtil;

public class Main{
	static String hdfsName = null;
	static String hdfsPath = null;
	static String tableName = null;
	static String dataSplit = null;
	static String dataExpress = null;
	static String[] columns = null;
	static Configuration config = null;
	static HTable table = null;
	
	private static void initHbase() {
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
	
	public static void main(String[] args) throws Exception {

		PluginUtil plugin = PluginUtil.getInstance();
		plugin.init(args);
		PluginConfig configFile = plugin.getConfig();
		long startTime = System.currentTimeMillis();
		long endTime   = 0;

		hdfsName = configFile.getParam("hdfs_name", null);
		hdfsPath = configFile.getParam("hdfs_path", null);
		tableName = configFile.getParam("hbase_table", null);
		dataSplit = configFile.getParam("data_split", null);
		dataExpress = configFile.getParam("data_express", null);
		columns = dataExpress.split(dataSplit, -1);
		// 初始化hbase
		initHbase();
		// 创建hbase
		table = new HTable(config, tableName);
		// 写入Hbase
		readFileAndputToHbase();
		
		endTime = System.currentTimeMillis();
		Date dateEnd = new Date();
		SimpleDateFormat format	 = new SimpleDateFormat("yyyyMMddHHmmss");
		String strEndTime = format.format(dateEnd);		

		PluginResult result = plugin.getResult();
		result.setParam("endTime", strEndTime);
		result.setParam("timeCosts", (endTime - startTime)/1000);
		result.setParam("exitCode", PluginExitCode.PE_SUCC);
		result.setParam("exitDesc", "run successfully");
		result.save();
		System.exit(0);
	}

	private static void readFileAndputToHbase() throws Exception {
        Configuration conf = new Configuration();
        //conf.set("fs.default.name", hdfsName);
        FileSystem hdfs = FileSystem.get(conf);

        try {
            FileStatus[] inputFiles = hdfs.globStatus(new Path(hdfsPath));
            System.out.println("Hdfs file Number : " + inputFiles.length);
            //FSDataOutputStream out = local.create(new Path(localFile));

            for (int i = 0; i < inputFiles.length; i++) {
                System.out.println("read file:" + inputFiles[i].getPath().getName());
                FSDataInputStream in = hdfs.open(inputFiles[i].getPath());
                BufferedReader bf = new BufferedReader(new InputStreamReader(in));
                String line;
                while ((line = bf.readLine()) != null) {
					putToHbase(line);
				}
                bf.close();
                in.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
	}

	private static void putToHbase(String line) throws Exception {
		Put put = null;
		String[] fields = line.split(dataSplit, -1);
		for (int i=0; i<fields.length && i<columns.length; i++) {
			if (columns[i].equals("rowkey")) {
				put = new Put(Bytes.toBytes(fields[i]));
			}
		}
		if (put == null) return;
		for (int i=0; i<fields.length && i<columns.length; i++) {
			if (!columns[i].equals("rowkey")) {
				put.add(Bytes.toBytes((columns[i].split(":", -1))[0]), 
						Bytes.toBytes((columns[i].split(":", -1))[1]),
						Bytes.toBytes(fields[i]));
			}
		}
		table.put(put);
	}

}