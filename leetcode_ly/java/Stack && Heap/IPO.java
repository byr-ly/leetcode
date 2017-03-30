public class Solution {
    public int findMaximizedCapital(int k, int W, int[] Profits, int[] Capital) {
        PriorityQueue<int[]> cap = new PriorityQueue<int[]>(
            new Comparator<int[]>(){
                @Override
                public int compare(int[] a,int[] b){
                    return a[0] - b[0];
                }
            }    
        );
        PriorityQueue<int[]> pro = new PriorityQueue<int[]>(
            new Comparator<int[]>(){
                @Override
                public int compare(int[] a,int[] b){
                    return b[1] - a[1];
                }
            }  
        );
        for(int i = 0; i < Profits.length; i++){
            cap.add(new int[]{Capital[i],Profits[i]});
        }
        for(int i = 0; i < k; i++){
            while(!cap.isEmpty() && W >= cap.peek()[0]){
                pro.add(cap.poll());
            }
            if(pro.isEmpty()) break;
            W += pro.poll()[1];
        }
        return W;
    }
}