public class Solution {
    public int[][] reconstructQueue(int[][] people) {
        if(people.length == 0) return new int[0][0];
        Arrays.sort(people,new Comparator<int[]>(){
            @Override
            public int compare(int[] a,int[] b){
                if(a[0] == b[0]){
                    return a[1] - b[1]; 
                }
                return b[0] - a[0];
            }
        });
        
        //排完序后用插入法
        List<int[]> temp = new ArrayList<int[]>();
        for(int i = 0; i < people.length; i++){
            int[] ans = people[i];
            temp.add(ans[1],new int[]{ans[0],ans[1]});
        }
        
        int[][] res = new int[people.length][people[0].length];
        int i = 0;
        for(int[] k : temp){
            res[i][0] = k[0];
            res[i][1] = k[1];
            i++;
        }
        return res;
    }
}