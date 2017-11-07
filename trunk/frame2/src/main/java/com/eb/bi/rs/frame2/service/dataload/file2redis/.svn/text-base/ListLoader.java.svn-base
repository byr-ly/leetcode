package com.eb.bi.rs.frame2.service.dataload.file2redis;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;

import java.io.*;
import java.util.ArrayList;
import java.util.List;


public class ListLoader extends LoaderBase {

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
                if (!multiGrp) {
                    while ((line = br.readLine()) != null) {
                        String lKey = LineParser.getKey(line, key);
                        String lValue = LineParser.getValue(line, value);
                        bw.append(lKey + "|" + lValue + "\n");
                        records++;
                    }
                } else {
                    int grpBegIdx = Integer.parseInt(value);
                    while ((line = br.readLine()) != null) {
                        String lKey = LineParser.getKey(line, key);
                        int grpLen = config.getParam("groupLength", 1);
                        ArrayList<Integer> valLocInGrp = new ArrayList<Integer>();
                        String[] indexes = config.getParam("valueLocInGroup", "0").split(",");
                        for (String index : indexes) {
                            valLocInGrp.add(Integer.parseInt(index));
                        }

                        String delimiter = config.getParam("delimiterInValue", ":");
                        List<String> values = LineParser.getValues(line, grpBegIdx, grpLen, valLocInGrp, delimiter);
                        for (String lValue : values) {
                            bw.append(lKey + "|" + lValue + "\n");
                        }
                        records++;
                    }
                }
                bw.flush();
                bw.close();
                log.info("file " + file + " contains " + records + " records");
                log.info("record count before:" + cnt.getRecordcnt());
                cnt.setRecordcnt(cnt.getRecordcnt() + records);
                log.info("record count now:" + cnt.getRecordcnt());
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

        String command = config.getParam("pushCommand", "rpush");
        int batch = config.getParam("batchKeyNumber", 10000);

        Jedis jedis = null;
        BufferedReader br = null;
        cnt.setRecordcnt(0);
        try {
            jedis = new Jedis(redisIp, redisPort);
            for (File file : tmpFiles) {
                log.info("begin to process file " + file);
                br = new BufferedReader(new FileReader(file));
                String line = null;
                long records = 0;
                int count = 0;
                Pipeline pipeline = jedis.pipelined();
                String currentKey = "";
                if ("lpush".equalsIgnoreCase(command)) {
                    while ((line = br.readLine()) != null) {
                        String[] pairs = line.split("\\|", 2);
                        if (pairs.length == 2) {
                            String key = pairs[0];
                            if (!currentKey.equals(key)) {
                                pipeline.del(key);
                                currentKey = key;
                                ++count;
                                if (count % batch == 0) {
                                    pipeline.sync();
                                    log.info("sync...");
                                }
                            }
                            pipeline.lpush(key, pairs[1]);
                            records++;
                        }
                    }
                } else {
                    while ((line = br.readLine()) != null) {
                        String[] pairs = line.split("\\|", 2);
                        if (pairs.length == 2) {
                            String key = pairs[0];
                            if (!currentKey.equals(key)) {
                                pipeline.del(key);
                                currentKey = key;
                                ++count;
                                if (count % batch == 0) {
                                    pipeline.sync();
                                    log.info("sync...");
                                }
                            }
                            pipeline.rpush(key, pairs[1]);
                            records++;
                        }
                    }
                }
                //
                try {
                    pipeline.sync();
                } catch (NullPointerException e) {
                    e.printStackTrace();
                    log.info("there is no command in pipeline");
                }

                log.info("sync...");
                log.info("tmp file " + file + " contains " + count + " key");
                log.info("tmp file " + file + " contains " + records + " key value pair");
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





