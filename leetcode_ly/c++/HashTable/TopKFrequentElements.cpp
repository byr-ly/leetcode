class Solution {
public:
    vector<int> topKFrequent(vector<int>& nums, int k) {
        map<int,int> myMap;
        priority_queue<pair<int,int>> heap;
        vector<int> ret;
        
        for(int i = 0; i < nums.size(); i++){
            myMap[nums[i]]++;
        }
        
        map<int,int>::iterator it;
        for(it = myMap.begin(); it != myMap.end(); it++){
            heap.push(make_pair(it->second,it->first));
        }
        
        for(int j = 0; j < k; j++){
            ret.push_back(heap.top().second);
            heap.pop();
        }
        return ret;
    }
};