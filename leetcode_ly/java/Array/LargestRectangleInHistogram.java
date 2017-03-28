public class Solution {
    public int largestRectangleArea(int[] heights) {
        if(heights == null || heights.length == 0) return 0;
        int max = 0;
        Stack<Integer> s = new Stack<>();
        for(int i = 0; i <= heights.length; i++){
            int cur = (i == heights.length) ? 0 : heights[i];
            if(s.isEmpty() || cur >= heights[s.peek()]){
                s.push(i);
            }
            else{
                //每次pop计算以pop出来的bar高度为最小高度的面积
                int h = heights[s.pop()];
                max = Math.max(max,h * (s.isEmpty() ? i : i - s.peek() - 1));
                i--;
            }
        }
        return max;
    }
}