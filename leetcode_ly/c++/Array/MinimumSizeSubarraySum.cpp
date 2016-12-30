class Solution {
public:
    int minSubArrayLen(int s, vector<int>& nums) {
        int start = 0;
        int end = 0;
        int sum = 0;
        int minLen = 2147483647;
        
        while(start < nums.size() && end < nums.size()){
            while(sum < s && end < nums.size()){
                sum += nums[end];
                end++;
            }
            while(sum >= s && start <= end){
                sum -= nums[start];
                start++;
                int len = end - start + 1;
                minLen = (len < minLen) ? len : minLen;
            }
        }
        return (minLen == 2147483647) ? 0 : minLen;
    }
};