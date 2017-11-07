package com.eb.bi.rs.mras.authorrec.itemcf.driver;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.eb.bi.rs.mras.authorrec.itemcf.util.PluginUtil;
import org.apache.hadoop.conf.Configuration;


public class CommRecommDriver extends WrapDriver {

    protected String initUserPath = null;
    protected String recommOutPath = null;
    protected String userPrefFilterKey
            = PluginUtil.TEMP_TORECOMM_USER_PREF_FILTER_KEY;
    protected PluginUtil.WorkPath userPrefFilterPath = null;
    protected int currTurn = -1;
    protected List<String> partFilePathes = null;
    protected List<String> readDepthPartPathes = null;
    protected List<String> noFillerPathes = null;

    public CommRecommDriver(Configuration cf) {
        super(cf);
        // TODO Auto-generated constructor stub
    }

    public CommRecommDriver(Configuration cf, String initPath) {
        this(cf);
        this.initUserPath = initPath;
        userPrefFilterPath = pluginUtil.getPath(userPrefFilterKey);
    }

    public CommRecommDriver(Configuration cf, String initPath,
                            int turn, List<String> initParts) {
        this(cf, initPath);
        this.currTurn = turn;
        this.partFilePathes = new ArrayList<String>();
        this.partFilePathes.addAll(initParts);
    }

    public void setReadDepthParts(List<String> paths) {
        this.readDepthPartPathes = paths;
    }

    public void setNoFillerUserPath(List<String> paths) {
        this.noFillerPathes = paths;
    }

    public void setTurn(int turn) {
        this.currTurn = turn;
    }

    public void setInitFilePathes(List<String> parts) {
        partFilePathes = new ArrayList<String>();
        partFilePathes.addAll(parts);
    }

    public int recommendJob()
            throws ClassNotFoundException, IOException, InterruptedException {
        int filterAuthorStatu = toRecommUserFilterJob();
        if (filterAuthorStatu != 0) {
            return filterAuthorStatu;
        }
        int filterReadDepthStatu = toRecommBooksFilterJob();
        if (filterReadDepthStatu != 0) {
            return filterReadDepthStatu;
        }
        int recommStatu = orderRecommJob();
        if (recommStatu != 0) {
            return recommStatu;
        }
        return 0;
    }

    protected int toRecommUserFilterJob()
            throws IOException, ClassNotFoundException, InterruptedException {
        return 0;
    }

    protected int orderRecommJob()
            throws IOException, ClassNotFoundException, InterruptedException {
        return 0;
    }

    protected int toRecommBooksFilterJob()
            throws IOException, ClassNotFoundException, InterruptedException {
        return 0;
    }
}
