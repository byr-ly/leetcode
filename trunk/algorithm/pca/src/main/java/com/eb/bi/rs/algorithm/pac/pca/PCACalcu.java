package com.eb.bi.rs.algorithm.pac.pca;

import Jama.Matrix;
import com.eb.bi.rs.algorithm.pac.Util.LogUtil;
import com.eb.bi.rs.algorithm.pac.Util.ObjectWriter;
import com.eb.bi.rs.algorithm.pac.Util.PluginUtil;
import com.eb.bi.rs.algorithm.pac.data.PCAData;
import com.eb.bi.rs.algorithm.pac.data.PCADataGroup;
import com.eb.bi.rs.algorithm.pac.data.PCADataRow;
import com.eb.bi.rs.algorithm.pac.data.PCADataRowComparator;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Map.Entry;


/**
 * Modified by LiMingji on 2015/11/1.
 */
public class PCACalcu {
    private PCAData pcaData = PCAData.getInstance();

    public static void main(String[] args) {
        if (args.length == 0) {
            System.err.println("Usage: pca.PCACalcu conf");
            return;
        }
        String conf = args[0];
        PluginUtil pluginUtil = PluginUtil.getInstance();
        pluginUtil.load(conf);
        PCACalcu calcu = new PCACalcu();
        calcu.initFromFile(pluginUtil.getMagicCubeBookPath());
        List cmreadBooks = calcu.calcu();
        calcu.output(cmreadBooks);
    }

    public void initFromFile(String path) {
        this.pcaData.initFromFile(path);
    }

    public void init(List<String> linesData) {
        this.pcaData.init(linesData);
    }

    private List<PCADataRow> calcu() {
        Map groups = this.pcaData.getCalcuDataGroups();
        List dataRows = new ArrayList();
        LogUtil logUtil = LogUtil.getInstance();
        PCADataRowComparator comparator = new PCADataRowComparator();
        for (Iterator localIterator = groups.entrySet().iterator(); localIterator.hasNext(); ) {
            Entry entry = (Entry) localIterator.next();

            String key = (String) entry.getKey();
            String infoStr = String.format("begin calcu. group key: %s", new Object[]{key});
            logUtil.getLogger().info(infoStr);
            PCADataGroup group = (PCADataGroup) entry.getValue();
            List groupRows = group.getDataRows();
            double[] scores = calcuPCA(group.getMatrix());
            if (scores.length != groupRows.size()) {
                String errStr = String.format("wrong!!!!group key: %s", new Object[]{key});
                logUtil.getLogger().error(errStr);
                break;
            }
            for (int i = 0; i < groupRows.size(); ++i) {
                ((PCADataRow) groupRows.get(i)).setPCAScore(scores[i]);
            }
            Collections.sort(groupRows, comparator);
            for (int i = 0; i < groupRows.size(); ++i) {
                ((PCADataRow) groupRows.get(i)).setSortPos(i + 1);
            }
            dataRows.addAll(groupRows);
            infoStr = String.format("end calcu. group key: %s", new Object[]{key});
            logUtil.getLogger().info(infoStr);
        }
        return dataRows;
    }

    private double[] calcuPCA(Matrix matrix) {
        PCA pca = new PCA();
        pca.prepare(matrix.getArray());
        pca.principalComponentSelect();
        pca.principalLoading();
        PluginUtil pluginUtil = PluginUtil.getInstance();
        boolean bRotate = pluginUtil.isRotate();
        if (bRotate) {
            pca.kaiserRotation(pluginUtil.getRotateTryTimes());
        }
        double[] result = pca.compositeScoreCalcu2(bRotate);
        return result;
    }

    private void output(List<PCADataRow> cmreadBooks) {
        if (cmreadBooks == null) {
            return;
        }
        Collections.sort(cmreadBooks);
        PluginUtil pluginUtil = PluginUtil.getInstance();
        String outputDir = pluginUtil.getOutputDir();
        SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd");
        String dateStr = format.format(new Date());
        outputDir = String.format("%s/%s", new Object[]{outputDir, dateStr});
        File outputFile = new File(outputDir);
        if (!(outputFile.exists())) {
            outputFile.mkdirs();
        }
        String filePath = String.format("%s/pcascore%s.txt", new Object[]{outputDir, dateStr});
        try {
            ObjectWriter writer = new ObjectWriter(filePath);
            writer.write(cmreadBooks);
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}