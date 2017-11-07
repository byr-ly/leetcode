package com.eb.bi.rs.mras2.new_correlation.util;

/**node entity
 * Created by lenovo on 2017/7/12.
 */
public class Node {
    private String treeId;
    private String nodeId;
    private double predict;
    private boolean isLeaf;
    private int feature;
    private double threshold;
    private String leftNodeId;
    private String rightNodeId;

    public Node(String treeId, String nodeId, double predict, boolean isLeaf) {
        this.treeId = treeId;
        this.nodeId = nodeId;
        this.predict = predict;
        this.isLeaf = isLeaf;
    }

    public Node(String treeId, String nodeId, double predict, boolean isLeaf, int feature, double threshold, String leftNodeId, String rightNodeId) {
        this.treeId = treeId;
        this.nodeId = nodeId;
        this.predict = predict;
        this.isLeaf = isLeaf;
        this.feature = feature;
        this.threshold = threshold;
        this.leftNodeId = leftNodeId;
        this.rightNodeId = rightNodeId;
    }

    public String getTreeId() {
        return treeId;
    }

    public void setTreeId(String treeId) {
        this.treeId = treeId;
    }

    public String getNodeId() {
        return nodeId;
    }

    public void setNodeId(String nodeId) {
        this.nodeId = nodeId;
    }

    public double getPredict() {
        return predict;
    }

    public void setPredict(double predict) {
        this.predict = predict;
    }

    public boolean isLeaf() {
        return isLeaf;
    }

    public void setLeaf(boolean leaf) {
        isLeaf = leaf;
    }

    public int getFeature() {
        return feature;
    }

    public void setFeature(int feature) {
        this.feature = feature;
    }

    public double getThreshold() {
        return threshold;
    }

    public void setThreshold(double threshold) {
        this.threshold = threshold;
    }

    public String getLeftNodeId() {
        return leftNodeId;
    }

    public void setLeftNodeId(String leftNodeId) {
        this.leftNodeId = leftNodeId;
    }

    public String getRightNodeId() {
        return rightNodeId;
    }

    public void setRightNodeId(String rightNodeId) {
        this.rightNodeId = rightNodeId;
    }
}
