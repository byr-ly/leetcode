/**
 * Definition for a point.
 * class Point {
 *     int x;
 *     int y;
 *     Point() { x = 0; y = 0; }
 *     Point(int a, int b) { x = a; y = b; }
 * }
 */
public class Solution {
    public int maxPoints(Point[] points) {
        if(points.length <= 2) return points.length;
        int res = 0;
        for(int i = 0; i < points.length; i++){
            HashMap<Double,Integer> map = new HashMap<Double,Integer>();
            int count = 0;
            int samePoint = 1;
            int infinite = 0;
            for(int j = 0; j < points.length; j++){
                if(i != j){
                    if(points[i].x == points[j].x && points[i].y == points[j].y) samePoint++;
                    else if(points[i].x == points[j].x) infinite++;
                    else{
                        double dist = ((double)points[j].y - points[i].y) / (points[j].x - points[i].x);
                        if(map.containsKey(dist)) map.put(dist,map.get(dist) + 1);
                        else map.put(dist,1);
                    }
                }
            }
            map.put((double)Integer.MAX_VALUE,infinite);
            for(Integer val : map.values()){
                count = Math.max(count,val + samePoint);
            }
            res = Math.max(res,count);
        }
        return res;
    }
}