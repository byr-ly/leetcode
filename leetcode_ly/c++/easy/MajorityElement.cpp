class Solution {
public:
    int majorityElement(vector<int>& nums) {
        int len = nums.size();
        map<int,int> myMap;
        map<int,int>::iterator it;
        for(int i = 0 ; i < len; i++){
            if((it = myMap.find(nums[i])) == myMap.end()){
                myMap.insert(pair<int,int>(nums[i],1));
            }
            else{
                it->second++;
            }
        }
        for(map<int,int>::iterator rt = myMap.begin(); rt != myMap.end(); rt++){
            if(rt->second > len / 2){
                return rt->first;
            }
            else{
                continue;
            }
        }
    }
};