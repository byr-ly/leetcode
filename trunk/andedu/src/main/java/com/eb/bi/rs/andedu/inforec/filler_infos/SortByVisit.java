package com.eb.bi.rs.andedu.inforec.filler_infos;

import java.util.Comparator;

public class SortByVisit implements Comparator{
	public int compare(Object o1, Object o2) {
		ContrastInfos InfosOne = (ContrastInfos) o1;
		ContrastInfos InfosTwo = (ContrastInfos) o2;
        if(InfosOne.infosID.equals(InfosTwo.infosID)){
            return 0;
        }
        else{
            if(InfosOne.visits.compareTo(InfosTwo.visits) == 0){
                return 0;
            }
            else{
                return InfosOne.visits.compareTo(InfosTwo.visits) > 0 ? -1 : 1;
            }
        }
    }
}
