class Solution {
public:
    int firstUniqChar(string s) {
        vector<int> count(26);
        for(int i = 0; i < s.size(); i++){
            count[s[i] - 'a']++;
        }
        for(int i = 0; i < s.size(); i++){
            if(count[s[i] - 'a'] == 1) return i;
            else continue;
        }
        return -1;
    }
};