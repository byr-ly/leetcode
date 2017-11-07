package com.eb.bi.rs.algorithm.pac.data;

import Jama.Matrix;
import com.eb.bi.rs.algorithm.pac.Util.FileUtil;
import com.eb.bi.rs.algorithm.pac.Util.PluginUtil;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class PCAData {
    private List<PCADataRow> dataRows = new ArrayList();
    private static PCAData instance = null;

    public static PCAData getInstance() {
        if (instance == null) {
            instance = new PCAData();
        }
        return instance;
    }

    public Matrix getDataMatrix() {
        Matrix dataMatrix = null;
        int row = this.dataRows.size();
        if (row <= 0) {
            return null;
        }
        int col = ((PCADataRow) this.dataRows.get(0)).getItemData().length;
        double[][] matrixValues = new double[row][col];
        for (int i = 0; i < this.dataRows.size(); ++i) {
            matrixValues[i] = ((PCADataRow) this.dataRows.get(i)).getItemData();
        }
        dataMatrix = new Matrix(matrixValues);
        return dataMatrix;
    }

    public Map<String, PCADataGroup> getCalcuDataGroups() {
        Map dataGroups = new HashMap();
        PluginUtil pluginUtil = PluginUtil.getInstance();
        int groupCol = pluginUtil.getGroupColIndex();
        if (groupCol < 0) {
            PCADataGroup group = new PCADataGroup();
            group.addRows(this.dataRows);
            dataGroups.put("all", group);
        } else {
            for (Iterator localIterator = this.dataRows.iterator(); localIterator.hasNext(); ) {
                PCADataRow row = (PCADataRow) localIterator.next();

                String groupKey = row.getGroupColValue();
                PCADataGroup group = (PCADataGroup) dataGroups.get(groupKey);
                if (group == null) {
                    group = new PCADataGroup();
                    dataGroups.put(groupKey, group);
                }
                group.addRow(row);
            }
        }
        System.out.println("dataGroup:" + dataGroups);
        return dataGroups;
    }

    public void initFromFile(String path) {
        File file = new File(path);
        try {
            List lines = FileUtil.getFileLines(file, "gbk");
            init(lines);
            System.out.println("current rows " + dataRows.size());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void init(List<String> bookLines) {
        PluginUtil pluginUtil = PluginUtil.getInstance();
        int groupCol = pluginUtil.getGroupColIndex();
        int startCol = pluginUtil.getStartCol();
        int endCol = pluginUtil.getEndCol();
        int currRowNo = 0;
        int colNum = -1;
        for (Iterator localIterator = bookLines.iterator(); localIterator.hasNext(); ) {
            String line = (String) localIterator.next();
            if (line.trim().length() == 0) {
                continue;
            }
            if (line.startsWith("BOOK_")) {
                continue;
            }
            if (colNum < 0) {
                String[] strs = line.split("\t|\",\"|\\|");
                if (endCol < 0) {
                    endCol = strs.length - 1;
                }
                colNum = endCol - startCol + 1;
            }
            String[] strs = line.split("\t|\\|");
            PCADataRow dataRow = null;
            if (startCol > 1) {
                String bookId = strs[0];
                String bookName = strs[1];

                CmreadBook book = new CmreadBook(bookId, bookName);
                dataRow = new CmreadBookRow(book);
                dataRow.setIndex(currRowNo++);
            } else {
                dataRow = new PCADataRow(currRowNo++);
            }
            if ((groupCol >= 0) && (strs.length > groupCol)) {
                String gkey = strs[groupCol];
                dataRow.setGroupColValue(gkey);
                List cols = pluginUtil.getFilterIndexes(gkey);
                if (cols != null) {
                    addFilterCols(cols, dataRow, startCol, endCol, strs);
                }
            } else {
                List cols = pluginUtil.getFilterIndexes("all");
                if (cols != null) {
                    addFilterCols(cols, dataRow, startCol, endCol, strs);
                }
            }
            if (dataRow.getItemData() == null) {
                double[] rowValues = new double[colNum];
                int i = startCol;
                for (int j = 0; i <= endCol; ) {
                    String s = strs[i];
                    rowValues[j] = Double.parseDouble(s);

                    ++i;
                    ++j;
                }
                dataRow.setItemData(rowValues);
            }
            System.out.println("dataRow : " + dataRow);
            this.dataRows.add(dataRow);
        }
    }

    private void addFilterCols(List<Integer> cols, PCADataRow dataRow, int startCol, int endCol, String[] strs) {
        double[] rowValues = new double[cols.size()];
        int i = startCol;
        for (int j = 0; i <= endCol; ++i) {
            if (!(cols.contains(Integer.valueOf(i)))) {
                continue;
            }
            String s = strs[i];
            rowValues[(j++)] = Double.parseDouble(s);
        }
//        每行数据抽取的字段有：深度阅读率，信息费，订购率，访问用户 4个字段。
//        System.out.println("current row Values: ");
//        for (int m = 0; m < rowValues.length; m++) {
//            System.out.printf(rowValues[m] + " ");
//        }
//        System.out.println();
        dataRow.setItemData(rowValues);
    }
}