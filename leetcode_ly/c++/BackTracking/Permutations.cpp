class Solution {
public:
    vector<vector<int>> permute(vector<int>& nums) {
        vector<int> elements;
        vector<vector<int>> result;
        if(nums.size() == 0) return result;
        getResult(nums,result,elements);
        return result;
    }
    
    void getResult(vector<int>& nums,vector<vector<int>>& result,vector<int>& elements){
        if(elements.size() == nums.size()){
            result.push_back(elements);
            return;
        }
        for(int i = 0; i < nums.size(); i++){
            if(find(elements.begin(),elements.end(),nums[i]) != elements.end()) continue;
            elements.push_back(nums[i]);
            getResult(nums,result,elements);
            elements.pop_back();
        }
    }
};