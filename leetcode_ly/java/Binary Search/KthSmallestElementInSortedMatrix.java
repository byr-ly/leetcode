public class Solution {
    public int kthSmallest(int[][] matrix, int k) {
        int m = matrix.length;
        if(m == 0) return -1;
        int n = matrix[0].length;
        if(k > m * n) return -1;
        
        PriorityQueue<Integer> q = new PriorityQueue<Integer>(
            new Comparator<Integer>(){
                @Override
                public int compare(Integer a,Integer b){
                    return a - b;
                }
            }    
        );
        for(int i = 0; i < m; i++){
            for(int j = 0; j < n; j++){
                q.add(matrix[i][j]);
            }
        }
        while(!q.isEmpty() && k != 1){
            q.poll();
            k--;
        }
        return Integer.valueOf(q.poll());
    }
}