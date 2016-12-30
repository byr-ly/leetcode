class Solution {
public:
    vector<int> intersection(vector<int>& nums1, vector<int>& nums2) {
        vector<int> result;
        map<int,bool> resMap;
        for(int i = 0; i < nums1.size(); i++){
            resMap.insert(pair<int,bool>(nums1[i],true));
        }
        for(int j = 0; j < nums2.size(); j++){
            if(resMap[nums2[j]]){
                resMap[nums2[j]] = false;
                result.push_back(nums2[j]);
            }
        }
        return result;
    }
};