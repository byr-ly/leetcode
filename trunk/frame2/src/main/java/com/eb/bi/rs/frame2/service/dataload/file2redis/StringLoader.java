package com.eb.bi.rs.frame2.service.dataload.file2redis;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;

import java.io.*;


public class StringLoader extends LoaderBase {

    @Override
    public boolean prepare() throws IOException {
        BufferedReader br = null;
        BufferedWriter bw = null;

        int idx = 0;
        cnt.setRecordcnt(0);
        try {
            for (File file : files) {
                br = new BufferedReader(new FileReader(file));
                File tmp = new File(file.getParent() + "/_" + file.getName());
                bw = new BufferedWriter(new FileWriter(tmp));
                tmpFiles[idx++] = tmp;
                String line = null;
                long records = 0;
                while ((line = br.readLine()) != null) {
                    String sKey = LineParser.getKey(line, key);
                    String sValue = LineParser.getValue(line, value);
                    bw.append(sKey + "|" + sValue + "\n");
                    records++;
                }
                log.info("file " + file + " contains " + records + " records");
                log.info("record count before:" + cnt.getRecordcnt());
                cnt.setRecordcnt(cnt.getRecordcnt() + records);
                log.info("record count now:" + cnt.getRecordcnt());
                bw.flush();
                bw.close();
            }
        } finally {
            if (br != null) {
                try {
                    br.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            if (bw != null) {
                try {
                    bw.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }

            }
        }
        return true;

    }

    @Override
    public boolean loadCore() throws IOException {

        Jedis jedis = null;
        BufferedReader br = null;
        cnt.setRecordcnt(0);
        try {
            jedis = new Jedis(redisIp, redisPort);
            for (File file : tmpFiles) {
                br = new BufferedReader(new FileReader(file));
                String line = null;
                long records = 0;
                Pipeline pipeline = jedis.pipelined();
                while ((line = br.readLine()) != null) {
                    String[] pairs = line.split("\\|", 2);
                    pipeline.set(pairs[0], pairs[1]);
                    records++;
                }
                log.info("tmp file " + file + " contains " + records + " key value pair");
                if (records > 0) {
                    pipeline.sync();
                }
                log.info("key value pair count before:" + cnt.getRecordcnt());
                cnt.setRecordcnt(cnt.getRecordcnt() + records);
                log.info("key value pair count now:" + cnt.getRecordcnt());
                file.delete();
            }
        } finally {
            if (br != null) {
                try {
                    br.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }

            }
            if (jedis != null) {
                jedis.close();
            }
        }
        return true;
    }
}


