public class Solution {
        public int minimumTotal(List<List<Integer>> triangle) {
        int m = triangle.size();
        for(int i = 1; i < m; i++){
            //triangle.get(i).get(0) += triangle.get(i - 1).get(0);  不能这样赋值！！！
            int element = triangle.get(i-1).get(0) + triangle.get(i).get(0);
            triangle.get(i).set(0,element);
            int newElement = triangle.get(i-1).get(i-1) + triangle.get(i).get(i);
            triangle.get(i).set(i,newElement);
            //triangle.get(i).get(i) += triangle.get(i - 1).get(i - 1);
        }
        for(int i = 2; i < m; i++){
            for(int j = 1; j < i; j++){
                //triangle.get(i).get(j) += Math.min(triangle.get(i - 1).get(j - 1),triangle.get(i - 1).get(j));
                int newElement = Math.min(triangle.get(i-1).get(j),triangle.get(i-1).get(j-1)) + triangle.get(i).get(j);
                triangle.get(i).set(j,newElement);
            }
        }
        int min = Integer.MAX_VALUE;
        for(int i = 0; i < m; i++){
            min = Math.min(min,triangle.get(m - 1).get(i));
        }
        return min;
    }
}