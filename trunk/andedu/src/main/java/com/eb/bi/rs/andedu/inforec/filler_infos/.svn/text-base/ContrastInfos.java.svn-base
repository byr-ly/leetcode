package com.eb.bi.rs.andedu.inforec.filler_infos;

public class ContrastInfos implements Comparable<ContrastInfos> {

    public String infosID;
    public Double visits;

    public ContrastInfos(String infosID, double visits) {
        this.infosID = infosID;
        this.visits = visits;
    }

    public ContrastInfos(String infosID, String visitsStr) {
        this.infosID = infosID;
        Double visits = Double.parseDouble(visitsStr);
        this.visits = visits;
    }

    @Override
    public int hashCode() {
        return infosID.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        ContrastInfos objt = (ContrastInfos) obj;
        return this.infosID.equals(objt.infosID);
    }

    @Override
    public int compareTo(ContrastInfos o) {
        if (this.infosID.equals(o.infosID)) {
            return 0;
        } else {
            int ret = this.visits.compareTo(o.visits);
            return ret > 0 ? -1 : 1;
        }
    }

    @Override
    public String toString() {
        return infosID + "|" + visits;
    }
}
