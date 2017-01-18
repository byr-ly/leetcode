public class Solution {
    public int findMinArrowShots(int[][] points) {
        if(points.length == 0) return 0;
        Arrays.sort(points,new Comparator<int[]>(){
            @Override
            public int compare(int[] a,int[] b){
                return a[0] - b[0];
            }
        });
        
        int res = 1;
        int end = points[0][1];
        for(int i = 1; i < points.length; i++){
            int[] point = points[i];
            if(point[0] <= end){
                end = Math.min(end,point[1]);
            }
            else{
                res++;
                end = point[1];
            }
        }
        return res;
    }
}