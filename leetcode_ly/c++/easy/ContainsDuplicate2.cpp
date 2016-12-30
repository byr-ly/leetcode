class Solution {
public:
    bool containsNearbyDuplicate(vector<int>& nums, int k) {
        int len = nums.size();
        map<int,int> myMap;
        map<int,int>::iterator it;
        for(int i = 0; i < len; i++){
            if((it = myMap.find(nums[i])) == myMap.end()){
                myMap.insert(pair<int,int>(nums[i],i));
            }
            else{
                if(abs(i - it->second) <= k){
                    return true;
                }
                it->second = i;
            }
        }
        return false;
    }
};