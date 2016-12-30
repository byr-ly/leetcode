class Solution {
public:
    vector<int> intersect(vector<int>& nums1, vector<int>& nums2) {
        vector<int> result;
        map<int,int> myMap;
        for(int i = 0; i < nums1.size(); i++){
            if(myMap.find(nums1[i]) == myMap.end()){
                myMap.insert(pair<int,bool>(nums1[i],1)); 
            }
            else{
                myMap[nums1[i]]++;
            }
        }
        for(int j = 0; j < nums2.size(); j++){
            if(myMap.find(nums2[j]) != myMap.end()){
                myMap[nums2[j]]--;
                if(myMap[nums2[j]] >= 0){
                    result.push_back(nums2[j]);
                }
            }
        }
        return result;
    }
};