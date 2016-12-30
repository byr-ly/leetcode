class Solution {
public:
    int lengthOfLastWord(string s) {
        if(s == "") return 0;
        vector<string> result = getWord(s);
        int len = result[result.size() - 1].size();
        return len;
    }
    
    vector<string> getWord(string s){
        int i = 0;
        string str = "";
        vector<string> res;
        while(i < s.size()){
            while(i < s.size() && s[i] != ' '){
                str = str + s[i];
                i++;
            }
            i++;
            while(i < s.size() && s[i] == ' '){
                i++;
            }
            res.push_back(str);
            str = "";
        }
        return res;
    }
};