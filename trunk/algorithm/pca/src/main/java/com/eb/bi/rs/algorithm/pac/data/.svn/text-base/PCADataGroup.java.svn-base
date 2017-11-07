package com.eb.bi.rs.algorithm.pac.data;

import Jama.Matrix;

import java.util.ArrayList;
import java.util.List;

public class PCADataGroup {
    private List<PCADataRow> dataRows = null;

    public void addRow(PCADataRow row) {
        if (this.dataRows == null) {
            this.dataRows = new ArrayList();
        }
        this.dataRows.add(row);
    }

    public void addRows(List<PCADataRow> rows) {
        this.dataRows = rows;
    }

    public List<PCADataRow> getDataRows() {
        return this.dataRows;
    }

    public Matrix getMatrix() {
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
}