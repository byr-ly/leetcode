class Solution {
public:
    vector<int> majorityElement(vector<int>& nums) {
        vector<int> val;
        vector<int> cnt;
        vector<int> res;
        if(nums.size() == 0) return res;
        for(int i = 0; i < nums.size(); i++){
            if(val.size() == 0){
                val.push_back(nums[i]);
                cnt.push_back(1);
            }
            else if(val.size() == 1){
                if(val[0] == nums[i]) cnt[0]++;
                else{
                    val.push_back(nums[i]);
                    cnt.push_back(1);
                }
            }
            else{
                if(val[0] == nums[i]) cnt[0]++;
                else if(val[1] == nums[i]) cnt[1]++;
                else{
                    cnt[0]--;
                    cnt[1]--;
                    if(cnt[1] == 0){
                        val.pop_back();
                        cnt.pop_back();
                    } 
                    if(cnt[0] == 0){
                        val.erase(val.begin());
                        cnt.erase(cnt.begin());
                    }
                }
            }
        }
        
        for(int i = 0; i < val.size(); i++){
            int temp = 0;
            for(int j = 0; j < nums.size(); j++){
                if(val[i] == nums[j]) temp++;
            }
            if(temp > nums.size() / 3) res.push_back(val[i]);
        }
        return res;
    }
};