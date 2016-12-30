class Solution {
public:
    struct cmp{
        bool operator() (pair<int,int>& a,pair<int,int>& b){
            return a.first + a.second > b.first + b.second; 
        }
    };

    vector<pair<int, int>> kSmallestPairs(vector<int>& nums1, vector<int>& nums2, int k) {
        int l1 = nums1.size();
        int l2 = nums2.size();
        int row = (k < l1) ? k : l1;
        int col = (k < l2) ? k : l2;
        priority_queue<pair<int,int>,vector<pair<int,int>>,cmp> q;
        vector<pair<int,int>> result;
        
        for(int i = 0; i < row; i++){
            for(int j = 0; j < col; j++){
                q.push(make_pair(nums1[i],nums2[j]));
            }
        }
        
        int i = 0;
        while(!q.empty() && i < k){
            result.push_back(q.top());
            q.pop();
            i++;
        }
        return result;
    }
};