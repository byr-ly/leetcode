package com.eb.bi.rs.algorithm.pac.pca;

import Jama.EigenvalueDecomposition;
import Jama.Matrix;
import Jama.SingularValueDecomposition;
import com.eb.bi.rs.algorithm.pac.Util.ArrayCalcuUtil;
import com.eb.bi.rs.algorithm.pac.Util.PluginUtil;
import com.eb.bi.rs.algorithm.pac.Util.PrintUtil;

/**
 * Modified by LiMingji on 2015/11/1.
 */
public class PCA {
    private double[][] standardDataMatrix = null;
    private double[] eigenValues = null;
    private double[][] eigenVectors = null;
    private double eigenValueSum = 0D;
    private double[] selectedEigenValues = null;
    private double[][] selectedEigenVectors = null;
    private double[][] principalLoading = null;
    private double[][] rotateLoading = null;
    private double[][] principalCoefficient = null;
    private boolean bDebug = false;

    public PCA() {
        PluginUtil util = PluginUtil.getInstance();
        this.bDebug = util.isDebug();
    }

    public void prepare(double[][] origDataMatrix) {
        standardlizer(origDataMatrix);
        double[][] covMatrix = covarianceCalcu(this.standardDataMatrix);
        eigenValueVectorCalcu(covMatrix);
    }

    private void standardlizer(double[][] origDataMatrix) {
        int i;
        if (origDataMatrix.length <= 0) {
            return;
        }
        int rowNum = origDataMatrix.length;
        int colNum = origDataMatrix[0].length;
        double[] average = new double[colNum];
        this.standardDataMatrix = new double[rowNum][colNum];
        double[] stdev = new double[colNum];

        //求每一列的均值。
        for (int k = 0; k < colNum; ++k) {
            double colSum = 0D;
            for (i = 0; i < rowNum; ++i) {
                colSum += origDataMatrix[i][k];
            }
            average[k] = (colSum / rowNum);
        }

        for (int k = 0; k < colNum; ++k) {
            double varSum = 0D;
            for (i = 0; i < rowNum; ++i) {
                double temp = origDataMatrix[i][k] - average[k];
                varSum += Math.pow(temp, 2.0D);
            }
            stdev[k] = Math.sqrt(varSum / (rowNum - 1));
        }

        for (i = 0; i < rowNum; ++i) {
            for (int j = 0; j < colNum; ++j) {
                this.standardDataMatrix[i][j] = ((origDataMatrix[i][j] - average[j]) / stdev[j]);
            }
        }
        origDataMatrix = null;
    }

    private double[][] covarianceCalcu(double[][] normalizeMatrix) {
        int rowNum = normalizeMatrix.length;
        int colNum = normalizeMatrix[0].length;
        double[][] result = new double[colNum][colNum];
        for (int i = 0; i < colNum; ++i) {
            for (int j = 0; j < colNum; ++j) {
                double temp = 0D;
                for (int k = 0; k < rowNum; ++k) {
                    temp += normalizeMatrix[k][i] * normalizeMatrix[k][j];
                }
                result[i][j] = (temp / (rowNum - 1));
            }
        }
        if (this.bDebug) {
            PrintUtil.printArray(result, "协方差阵");
        }
        return result;
    }

    private void eigenValueVectorCalcu(double[][] covMatrix) {
        Matrix matrix = new Matrix(covMatrix);
        EigenvalueDecomposition decomposition = matrix.eig();
        Matrix valueMatrix = decomposition.getD();
        Matrix vectorMatrix = decomposition.getV();
        this.eigenVectors = vectorMatrix.getArray();
        double[][] eigens = valueMatrix.getArray();
        this.eigenValues = new double[eigens.length];
        for (int i = 0; i < eigens.length; ++i) {
            this.eigenValues[i] = eigens[i][i];
        }
        if (this.bDebug) {
            PrintUtil.printArray(this.eigenValues, "特征值", false);
            PrintUtil.printArray(this.eigenVectors, "特征向量");
        }
    }

    public void principalComponentSelect() {
        int size = this.eigenValues.length;
        double[] sortedEigenValues = new double[size];
        int[] orderPos = ArrayCalcuUtil.sortArray(this.eigenValues, sortedEigenValues);
        for (int i = 0; i < size; ++i) {
            this.eigenValueSum += sortedEigenValues[i];
        }
        double currSum = 0D;
        PluginUtil pluginUtil = PluginUtil.getInstance();
        double threhold = pluginUtil.getEigThreshold();
        int selectCount = 0;
        for (int i = 0; i < size; ++i) {
            currSum += sortedEigenValues[i];
            ++selectCount;
            double temp = 1D * currSum / this.eigenValueSum;
            if (temp > threhold) {
                break;
            }
        }
        this.selectedEigenValues = new double[selectCount];
        for (int i = 0; i < selectCount; ++i) {
            this.selectedEigenValues[i] = sortedEigenValues[i];
        }
        this.selectedEigenVectors = new double[size][selectCount];
        for (int j = 0; j < selectCount; ++j) {
            int colNum = orderPos[j];
            for (int i = 0; i < size; ++i) {
                this.selectedEigenVectors[i][j] = this.eigenVectors[i][colNum];
            }
        }
        if (this.bDebug) {
            PrintUtil.printArray(this.selectedEigenValues, "筛选后特征值", false);
            PrintUtil.printArray(this.selectedEigenVectors, "筛选后特征向量");
        }
    }

    private void adjustEigenVectors() {
        int row = this.selectedEigenVectors.length;
        int col = this.selectedEigenVectors[0].length;
        for (int j = 0; j < col; ++j) {
            int positive = 0;
            double maxAbsValue = 0D;
            boolean bmaxAbsValuePositive = true;
            for (int i = 0; i < row; ++i) {
                double temp = this.selectedEigenVectors[i][j];
                if (temp > 0D) {
                    ++positive;
                }
                if (Math.abs(temp) > maxAbsValue) {
                    maxAbsValue = Math.abs(temp);
                    if (temp < 0D) {
                        bmaxAbsValuePositive = false;
                    } else
                        bmaxAbsValuePositive = true;
                }
            }

            if ((positive * 2 < row) || (
                    (positive * 2 == row) && (!(bmaxAbsValuePositive)))) {
                for (int i = 0; i < row; ++i) {
                    this.selectedEigenVectors[i][j] = (0D - this.selectedEigenVectors[i][j]);
                }
            }
        }
    }

    private void adjustRotateLoading() {
        int row = this.rotateLoading.length;
        int col = this.rotateLoading[0].length;
        for (int j = 0; j < col; ++j) {
            double maxAbsValue = 0D;
            boolean bmaxAbsValuePositive = true;
            for (int i = 0; i < row; ++i) {
                double temp = this.rotateLoading[i][j];
                if (Math.abs(temp) > maxAbsValue) {
                    maxAbsValue = Math.abs(temp);
                    if (temp < 0D) {
                        bmaxAbsValuePositive = false;
                    } else
                        bmaxAbsValuePositive = true;
                }
            }

            if (!(bmaxAbsValuePositive)) {
                for (int i = 0; i < row; ++i) {
                    this.rotateLoading[i][j] = (0D - this.rotateLoading[i][j]);
                }
            }
        }
    }

    public void principalLoading() {
        int row = this.selectedEigenVectors.length;
        int col = this.selectedEigenVectors[0].length;
        this.principalLoading = new double[row][col];
        for (int i = 0; i < row; ++i) {
            for (int j = 0; j < col; ++j) {
                this.principalLoading[i][j] =
                        (Math.sqrt(this.selectedEigenValues[j]) *
                                this.selectedEigenVectors[i][j]);
            }
        }
        if (this.bDebug) {
            PrintUtil.printArray(this.principalLoading, "成份矩阵（主成分载荷）");
        }
    }

    public void principalCoeffiMatrix(boolean bRotate) {
        double[][] compentsMatrix = null;
        if (bRotate) {
            compentsMatrix = this.rotateLoading;
        } else
            compentsMatrix = this.principalLoading;

        double[] eigs = this.selectedEigenValues;
        int rowNum = compentsMatrix.length;
        int colNum = compentsMatrix[0].length;
        this.principalCoefficient = new double[rowNum][colNum];
        for (int j = 0; j < colNum; ++j) {
            for (int i = 0; i < rowNum; ++i) {
                this.principalCoefficient[i][j] = (compentsMatrix[i][j] / eigs[j]);
            }
        }
        if (this.bDebug) {
            PrintUtil.printArray(this.principalCoefficient, "成分得分系数矩阵");
        }
    }

    private double[][] principalScoreCalcu() {
        Matrix dataMatrix = new Matrix(this.standardDataMatrix);
        Matrix coffMatrix = new Matrix(this.principalCoefficient);
        Matrix scoreMatrix = dataMatrix.times(coffMatrix);
        double[][] principalScore = scoreMatrix.getArray();
        if (this.bDebug) {
            PrintUtil.printArray(principalScore, "主成分得分");
        }
        return principalScore;
    }

    public double[] compositeScoreCalcu(boolean bRotate) {
        principalCoeffiMatrix(bRotate);
        double[][] principalScores = principalScoreCalcu();
        int rowNum = principalScores.length;
        int colNum = principalScores[0].length;

        double[] result = new double[rowNum];
        for (int i = 0; i < rowNum; ++i) {
            double score = 0D;
            double eigContribuSum = 0D;
            for (int j = 0; j < colNum; ++j) {
                double temp = this.selectedEigenValues[j] / this.eigenValueSum;
                eigContribuSum += temp;
                score += principalScores[i][j] * temp;
            }
            score /= eigContribuSum;
            result[i] = score;
        }
        if (this.bDebug) {
            PrintUtil.printArray(result, "综合得分", true);
        }
        return result;
    }

    public double[] compositeScoreCalcu2(boolean bRotate) {
        double[][] loading = null;
        if (!(bRotate)) {
            loading = this.principalLoading;
        } else
            loading = this.rotateLoading;

        Matrix product = new Matrix(this.standardDataMatrix).times(new Matrix(loading));
        int rowNum = product.getArray().length;
        int colNum = product.getArray()[0].length;
        double selectEigValSum = 0D;
        for (int i = 0; i < this.selectedEigenValues.length; ++i) {
            selectEigValSum += this.selectedEigenValues[i];
        }
        double[] result = new double[rowNum];
        for (int i = 0; i < rowNum; ++i) {
            double score = 0D;
            for (int j = 0; j < colNum; ++j) {
                score += product.getArray()[i][j];
            }
            score /= selectEigValSum;
            result[i] = score;
        }

        return result;
    }

    public void kaiserRotation(int tryTimes) {
        Matrix beforeRotate = new Matrix(this.principalLoading);
        int row = this.principalLoading.length;
        int col = this.principalLoading[0].length;
        int lamda = 1;
        double trace = 0D;
        double lastTrace = 0D;
        double[][] initBasis = new double[col][col];
        for (int i = 0; i < col; ++i) {
            for (int j = 0; j < col; ++j) {
                if (i == j) {
                    initBasis[i][j] = 1D;
                } else
                    initBasis[i][j] = 0D;
            }
        }

        double tol = 0.0001D;
        Matrix rotateBasis = new Matrix(initBasis);
        Matrix grammaMatrix = null;
        int times = 0;
        for (times = 0; times < tryTimes; ++times) {
            lastTrace = trace;
            grammaMatrix = beforeRotate.times(rotateBasis);
            Matrix hadamardProduct = grammaMatrix.arrayTimes(grammaMatrix).arrayTimes
                    (grammaMatrix);
            Matrix tempProduct = grammaMatrix.transpose().times(grammaMatrix);
            double[][] diag = new double[col][col];
            for (int i = 0; i < tempProduct.getArray().length; ++i) {
                for (int j = 0; j < tempProduct.getArray()[0].length; ++j) {
                    if (i == j) {
                        diag[i][j] = tempProduct.getArray()[i][j];
                    } else
                        diag[i][j] = 0D;
                }
            }

            Matrix sub = grammaMatrix.times(new Matrix(diag)).times(lamda / row);
            Matrix afterSub = hadamardProduct.minus(sub);
            Matrix svdMatrix = beforeRotate.transpose().times(afterSub);
            SingularValueDecomposition decomp = svdMatrix.svd();
            Matrix u = decomp.getU();
            Matrix v = decomp.getV();
            Matrix s = decomp.getS();
            rotateBasis = u.times(v.transpose());
            trace = s.trace();
            if ((lastTrace != 0D) && (trace - lastTrace < tol)) {
                break;
            }
        }
        grammaMatrix = beforeRotate.times(rotateBasis);
        this.rotateLoading = grammaMatrix.getArray();
        if (this.bDebug) {
            PrintUtil.printLog(String.format("旋转%d次后收敛", new Object[]{Integer.valueOf(times + 1)}));
            PrintUtil.printArray(rotateBasis.getArray(), "成份转换矩阵");
            PrintUtil.printArray(this.rotateLoading, "旋转成份矩阵");
        }
        PluginUtil pluginUtil = PluginUtil.getInstance();
        if (pluginUtil.isNeedAdjust()) {
            adjustRotateLoading();
            if (this.bDebug) {
                PrintUtil.printArray(this.rotateLoading, "调整后旋转成份矩阵");
            }
        }
    }
}