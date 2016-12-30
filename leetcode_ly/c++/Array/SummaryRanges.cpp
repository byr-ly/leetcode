class Solution {
public:
    vector<string> summaryRanges(vector<int>& nums) {
        vector<string> res;
        if(nums.size() == 0) return res;
        if(nums.size() == 1) {
            res.push_back(to_string(nums[0]));
            return res;
        }
        int begin = 0;
        int end = 1;
        int index = 0;
        while(index < nums.size()){
            while(nums[end] - nums[begin] == 1 && end < nums.size()){
                begin++;
                end++;
            }
            if(end - index == 1) res.push_back(to_string(nums[index]));
            else{
                string result = to_string(nums[index]) + "->" + to_string(nums[end - 1]);
                res.push_back(result);
            }
            index = end;
            begin = index;
            end = begin + 1;
        }
        return res;
    }
};