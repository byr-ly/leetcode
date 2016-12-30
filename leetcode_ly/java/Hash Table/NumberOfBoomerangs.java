public class Solution {
    public int numberOfBoomerangs(int[][] points) {
        int res = 0;
        HashMap<Integer,Integer> map = new HashMap<Integer,Integer>();
        for(int i = 0; i < points.length; i++){
            for(int j = 0; j < points.length; j++){
                if(i == j) continue;
                else {
                    int d = getDistance(points[i],points[j]);
                    if(map.containsKey(d)) map.put(d,map.get(d) + 1);
                    else map.put(d,1);
                }
            }
            for(int val : map.values()){
                res = res + (val - 1) * val;
            }
            map.clear();
        }
        return res;
    }
    
    public int getDistance(int[] x,int[] y){
        int a = x[0] - y[0];
        int b = x[1] - y[1];
        return a * a + b * b;
    }
}