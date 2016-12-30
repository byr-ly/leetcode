class Solution {
public:
    bool containsDuplicate(vector<int>& nums) {
        int len = nums.size();
        map<int,int> myMap;
        map<int,int>::iterator it;
        for(int i = 0; i < len; i++){
            if((it = myMap.find(nums[i])) == myMap.end()){
                myMap.insert(pair<int,int>(nums[i],i));
            }
            else{
                return true;
            }
        }
        return false;
    }
};