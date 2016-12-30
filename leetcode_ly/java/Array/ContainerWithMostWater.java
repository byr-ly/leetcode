public class Solution {
    public int maxArea(int[] height) {
        int i = 0;
        int j = height.length - 1;
        int res = Integer.MIN_VALUE;
        while(i < j){
            res = Math.max(res,Math.min(height[i],height[j]) * (j - i));
            if(height[i] < height[j]) i++;
            else j--;
        }
        return res;
    }
}