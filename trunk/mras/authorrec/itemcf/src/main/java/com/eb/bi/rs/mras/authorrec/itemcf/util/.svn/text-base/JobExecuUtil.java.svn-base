package com.eb.bi.rs.mras.authorrec.itemcf.util;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;

import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 * @modify LiMingji
 * @date 2015-12-17
 */

public class JobExecuUtil {
    public void checkOutputPath(String fileName) {
        LogUtil logUtil = LogUtil.getInstance();
        try {
            FileSystem fs = FileSystem.get(URI.create(fileName), new Configuration());
            Path f = new Path(fileName);
            boolean isExists = fs.exists(f);
            if (isExists) {    //if exists, delete
                boolean isDel = fs.delete(f, true);
                logUtil.getLogger().info(fileName + "  delete?\t" + isDel);
            } else {
                logUtil.getLogger().info(fileName + "  exist?\t" + isExists);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void deleteOutputPath(String fileName) {
        LogUtil logUtil = LogUtil.getInstance();
        try {
            FileSystem fs = FileSystem.get(URI.create(fileName), new Configuration());
            Path f = new Path(fileName);
            boolean isExists = fs.exists(f);
            if (isExists) {    //if exists, delete
                boolean isDel = fs.delete(f, true);
                logUtil.getLogger().info(fileName + "  delete?\t" + isDel);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public String getMROutSliptFiles(String dir) {
        String path = null;
        String temp = "part-*";
        if (dir.endsWith("/") || dir.endsWith("\\")) {
            path = String.format("%s%s", dir, temp);
        } else {
            path = String.format("%s/%s", dir, temp);
        }
        return path;
    }

    public String getPartsMROutSliptFiles(String dir) {
        String path = null;
        String temp = "turn*/part-*";
        if (dir.endsWith("/") || dir.endsWith("\\")) {
            path = String.format("%s%s", dir, temp);
        } else {
            path = String.format("%s/%s", dir, temp);
        }
        return path;
    }

    public String getJobExecuteLogstr(long start, String str, boolean bsuccess) {
        long interval = System.currentTimeMillis() - start;
        String timeStr = String.format("time cost: %d(s)", interval / 1000);
        String retStr = "complete";
        if (!bsuccess) {
            retStr = "failed";
        }
        String logStr = String.format("%s %s. %s", str, retStr, timeStr);
        return logStr;
    }

    public String beginJobLogStr(String str) {
        String logStr = String.format("Begin: %s", str);
        return logStr;
    }

    public Job newJobAndAddCacheFile(Configuration conf, Map<String, PluginUtil.WorkPath> keyPaths) throws IOException {
        PluginUtil pluginUtil = PluginUtil.getInstance();
        Job job = null;
        if (keyPaths != null && keyPaths.size() > 1) {
            for (Entry<String, PluginUtil.WorkPath> entry : keyPaths.entrySet()) {
                conf.set(entry.getKey(), entry.getValue().getPathValue());
            }
        }
        if (pluginUtil.isNewHadoop()) {
            job = Job.getInstance(conf);
            if (keyPaths != null) {
                for (PluginUtil.WorkPath workPath : keyPaths.values()) {
                    if (workPath.getIsVirtual()) {
                        continue;
                    }
                    FileSystem fs = FileSystem.get(conf);
                    String path = workPath.getPathValue();
                    FileStatus[] status;
                    System.out.println("JobExecuUtil: "+workPath.getIsParts() + " " + workPath.getPathValue());
                    if (workPath.getIsParts()) {
                        path = getMROutSliptFiles(path);
                        status = fs.globStatus(new Path(path));
                    } else {
                        status = fs.listStatus(new Path(path));
                    }
                    for (FileStatus st : status) {
                        job.addCacheFile(URI.create(st.getPath().toString()));
                        System.out.println(st.getPath().toString() + " has been add into distributedCache");
                    }
                }
            }
        } else {
            if (keyPaths != null) {
                for (PluginUtil.WorkPath workPath : keyPaths.values()) {
                    if (workPath.getIsVirtual()) {
                        continue;
                    }
                    FileSystem fs = FileSystem.get(conf);
                    String path = workPath.getPathValue();
                    if (workPath.getIsParts()) {
                        path = getMROutSliptFiles(path);
                    }
                    FileStatus[] status = fs.globStatus(new Path(path));
                    for (FileStatus st : status) {
                        DistributedCache.addCacheFile(URI.create(st.getPath().toString()), conf);
                        System.out.println(st.getPath().toString() + " has been add into distributedCache");
                    }
                }
            }
            job = new Job(conf);
        }
        return job;
    }

    public Job newJobAndAddCacheFile(Configuration conf, Map<String, PluginUtil.WorkPath> keyPaths,
                                     List<String> pathes) throws IOException {

        if (keyPaths != null && keyPaths.size() > 1) {
            for (Entry<String, PluginUtil.WorkPath> entry : keyPaths.entrySet()) {
                conf.set(entry.getKey(), entry.getValue().getPathValue());
            }
        }
        PluginUtil pluginUtil = PluginUtil.getInstance();
        Job job = null;
        if (pluginUtil.isNewHadoop()) {
            job = Job.getInstance(conf);
            if (keyPaths != null) {
                for (PluginUtil.WorkPath workPath : keyPaths.values()) {
                    if (workPath.getIsVirtual()) {
                        continue;
                    }
                    FileSystem fs = FileSystem.get(conf);
                    String path = workPath.getPathValue();
                    if (workPath.getIsParts()) {
                        path = getMROutSliptFiles(path);
                    }
                    FileStatus[] status = fs.globStatus(new Path(path));
                    for (FileStatus st : status) {
                        job.addCacheFile(URI.create(st.getPath().toString()));
                    }
                }
            }
            if (pathes != null) {
                for (String path : pathes) {
                    job.addCacheFile(URI.create(path.toString()));
                }
            }
        } else {
            if (keyPaths != null) {
                for (PluginUtil.WorkPath workPath : keyPaths.values()) {
                    if (workPath.getIsVirtual()) {
                        continue;
                    }
                    FileSystem fs = FileSystem.get(conf);
                    String path = workPath.getPathValue();
                    if (workPath.getIsParts()) {
                        path = getMROutSliptFiles(path);
                    }
                    FileStatus[] status = fs.globStatus(new Path(path));
                    for (FileStatus st : status) {
                        DistributedCache.addCacheFile(
                                URI.create(st.getPath().toString()), conf);
                    }
                }
            }
            if (pathes != null) {
                for (String path : pathes) {
                    DistributedCache.addCacheFile(URI.create(path), conf);
                }
            }
            job = new Job(conf);
        }
        return job;
    }

    public String getFileName(String path) {
        path = path.replace('\\', '/');
        if (path.endsWith("/")) {
            path = path.substring(0, path.length() - 1);
        }
        int pos = path.lastIndexOf('/');
        if (pos < 0) {
            return path;
        }
        String ending = path.substring(pos + 1);
        if (ending.startsWith("part-") || ending.startsWith("*")
                || ending.startsWith("record_day")
                || ending.startsWith("turn")
                || ending.startsWith("201")) {
            path = path.substring(0, pos);
            return getFileName(path);
        }
        return ending;
    }

    /**
     * 根据Hadoop不同版本或取DistributedCache中的文件。
     *
     * @param context Reduce端
     * @return
     * @throws IOException
     */
    public URI[] getCacheFiles(Reducer.Context context)
            throws IOException {
        boolean isNewHadoop = context.getConfiguration().getBoolean(PluginUtil.USE_NEW_HADOOP_KEY, false);
        if (isNewHadoop) {
            return context.getCacheFiles();
        } else {
            Configuration conf = context.getConfiguration();
            Path[] localFiles = DistributedCache.getLocalCacheFiles(conf);
            if (localFiles != null) {
                URI[] temps = new URI[localFiles.length];
                for (int i = 0; i < localFiles.length; i++) {
                    temps[i] = localFiles[i].toUri();
                }
                return temps;
            }
        }
        return null;
    }

    /**
     * 同上
     *
     * @param context Map端
     * @return
     * @throws IOException
     */
    public URI[] getCacheFiles(Mapper.Context context)
            throws IOException {
        boolean isNewHadoop = context.getConfiguration().getBoolean(PluginUtil.USE_NEW_HADOOP_KEY, false);
        if (isNewHadoop) {
            return context.getCacheFiles();
        } else {
            Configuration conf = context.getConfiguration();
            Path[] localFiles = DistributedCache.getLocalCacheFiles(conf);
            if (localFiles != null) {
                URI[] temps = new URI[localFiles.length];
                for (int i = 0; i < localFiles.length; i++) {
                    temps[i] = localFiles[i].toUri();
                }
                return temps;
            }
        }
        return null;
    }

    public boolean isBadUser(String msisdn) {
        if (msisdn == null) {
            return true;
        }
        if (msisdn.length() != 11) {
            return true;
        }
        String temp = msisdn.substring(8);
        try {
            Integer.parseInt(temp);
        } catch (Exception e) {
            return true;
        }
        return false;
    }

    public boolean isPhoneNum(String msisdn) {
        if (isBadUser(msisdn)) {
            return false;
        }
        if (msisdn.startsWith("1")) {
            return true;
        }
        return false;
    }

    public String getWriteOutputForTurns(int turn, String path) {
        if (turn < 0) {
            return path;
        }
        if (path.endsWith("/") || path.endsWith("\\")) {
            path = String.format("%sturn%d", path, turn);
        } else {
            path = String.format("%s/turn%d", path, turn);
        }
        return path;
    }
}
