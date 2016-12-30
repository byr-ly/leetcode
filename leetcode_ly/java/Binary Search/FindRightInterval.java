/**
 * Definition for an interval.
 * public class Interval {
 *     int start;
 *     int end;
 *     Interval() { start = 0; end = 0; }
 *     Interval(int s, int e) { start = s; end = e; }
 * }
 */
public class Solution {
    public int[] findRightInterval(Interval[] intervals) {
        int[] res = new int[intervals.length];
        TreeMap<Integer,Integer> map = new TreeMap<Integer,Integer>();
        
        for(int i = 0; i < intervals.length; i++){
            map.put(intervals[i].start,i);
        }
        
        //ceilingEntry返回比目标key大的entry，只返回最近的那个
        for(int i = 0; i < res.length; i++){
            Map.Entry<Integer,Integer> entry = map.ceilingEntry(intervals[i].end);
            res[i] = (entry == null) ? -1 : entry.getValue();
        }
        return res;
    }
}