public class Solution {
    public int hIndex(int[] citations) {
        Arrays.sort(citations);
        int res = 0;
        for(int i = citations.length - 1; i >= 0; i--){
            if(citations[i] >= citations.length - i) res = Math.max(citations.length - i,res);
        }
        return res;
    }
}