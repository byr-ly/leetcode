class Solution {
public:
    string longestCommonPrefix(vector<string>& strs) {
        if(strs.size() == 0) return "";
        int i;
        string res = strs[0];
        for(i = 1; i < strs.size(); i++){
            res = compare(res,strs[i]);
        }
        return res;
    }
    
    string compare(string a, string b){
        int j = 0;
        string s = "";
        int lenA = a.size();
        int lenB = b.size();
        while(j < lenA && j < lenB){
            if(a[j] == b[j]){
                s += a[j];
                j++;
            }
            else break;
        }
        return s;
    }
};