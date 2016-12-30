public class Solution {
    public int findRadius(int[] houses, int[] heaters) {
        Arrays.sort(houses);
        Arrays.sort(heaters);
        
        int j = 0;
        int res = 0;
        for(int i = 0; i < houses.length; i++){
            //找里house[i]最近的heater
            while(j < heaters.length - 1 && heaters[j] + heaters[j + 1] <= 2 * houses[i]){
                j++;
            }
            res = Math.max(res,Math.abs(heaters[j] - houses[i]));
        }
        return res;
    }
}