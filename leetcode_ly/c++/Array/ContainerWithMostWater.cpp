class Solution {
public:
    int maxArea(vector<int>& height) {
        int begin = 0;
        int end = height.size() - 1;
        int area = 0;
        int max = INT_MIN;
        
        while(begin <= end){
            while(height[begin] <= height[end] && begin <= end){
                area = (end - begin) * height[begin];
                max = (max > area) ? max : area;
                begin++;
            }
            while(height[begin] > height[end] && begin <= end){
                area = (end - begin) * height[end];
                max = (max > area) ? max : area;
                end--;
            }
        }
        return (max == INT_MIN) ? 0 : max;
    }
};