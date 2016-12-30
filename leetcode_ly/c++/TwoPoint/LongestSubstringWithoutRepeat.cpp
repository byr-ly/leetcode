class Solution {
public:
    int lengthOfLongestSubstring(string s) {
        vector<int> flag(256,0);
        int start = 0;
        int count = 0;
        int max = 0;
        
        for(int i = 0; i < s.size(); i++){
            if(!flag[s[i]]){
                flag[s[i]] = 1;
                count++;
            }
            else{
                max = (max > count) ? max : count;
                while(s[start] != s[i]){
                    flag[s[start]] = 0;
                    start++;
                    count--;
                }
                start++;
            }
        }
        max = (max > count) ? max : count;
        return max;
    }
};