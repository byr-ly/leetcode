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
    public List<Interval> insert(List<Interval> intervals, Interval newInterval) {
        List<Interval> res = new ArrayList<Interval>();
        intervals.add(newInterval);
        Collections.sort(intervals,new Comparator<Interval>(){
            @Override
            public int compare(Interval a,Interval b){
                return a.start - b.start;
            }
        });
        int begin = intervals.get(0).start;
        int end = intervals.get(0).end;
        for(Interval i : intervals){
            if(i.start <= end){
                end = Math.max(end,i.end);
            }
            else{
                res.add(new Interval(begin,end));
                begin = i.start;
                end = i.end;
            }
        }
        res.add(new Interval(begin,end));
        return res;
    }
}