public class Solution {
    public int findPoisonedDuration(int[] timeSeries, int duration) {
        int res = 0;
        if(timeSeries == null || timeSeries.length == 0) return res;
        for(int i = 1; i < timeSeries.length; i++){
            if(timeSeries[i - 1] + duration <= timeSeries[i]) res += duration;
            else res += (timeSeries[i] - timeSeries[i - 1]);
        }
        res += duration;
        return res;
    }
}