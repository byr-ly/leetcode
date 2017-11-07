package com.eb.bi.rs.mras2.consonance.manager;

import com.eb.bi.rs.frame2.common.pluginutil.PluginConfig;
import com.eb.bi.rs.frame2.common.pluginutil.PluginExitCode;
import com.eb.bi.rs.frame2.common.pluginutil.PluginResult;
import com.eb.bi.rs.frame2.common.pluginutil.PluginUtil;
import com.eb.bi.rs.frame2.recframe.base.ComponentHelper;
import com.eb.bi.rs.frame2.recframe.base.JobComponent;
import org.apache.log4j.Logger;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by houmaozheng on 2017/6/12.
 */
public class ManagerMain {
    public static void main(String[] args) throws Exception {
        PluginUtil pluginUtil = PluginUtil.getInstance();
        pluginUtil.init(args);
        Logger log = pluginUtil.getLogger();

        log.info("ManagerMain start!!!!!!!!!!!!!!!!");

        PluginConfig pluginConfig = pluginUtil.getConfig();
        JobComponent root = ComponentHelper.createComposite(pluginConfig.getElement("composite"));

        Date begin = new Date();

        int ret = root.run(args);

        Date end = new Date();
        SimpleDateFormat format = new SimpleDateFormat("yyyyMMddHHmmss");
        String endTime = format.format(end);
        long timeCost = end.getTime() - begin.getTime();

        PluginResult result = pluginUtil.getResult();
        result.setParam("endTime", endTime);
        result.setParam("timeCosts", timeCost);
        result.setParam("exitCode", ret == 0 ? PluginExitCode.PE_SUCC : PluginExitCode.PE_LOGIC_ERR);
        result.setParam("exitDesc", ret == 0 ? "run successfully" : "run failed.");
        result.save();

        log.info("time cost in total(s): " + (timeCost / 1000.0));
        System.exit(ret);
    }
}
