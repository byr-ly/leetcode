class Solution {
public:
    string decodeString(string s) {
        if(s.size() == 0) return "";
        stack<char> s1;
        for(int i = 0; i < s.size(); i++){
            if(s[i] != ']'){
                s1.push(s[i]);
            }
            else{
                string res = "";
                while(s1.top() != '['){
                    res += s1.top();
                    s1.pop();
                }
                reverse(res.begin(),res.end());
                s1.pop();
                string num = "";
                while(!s1.empty() && s1.top() >= '0' && s1.top() <= '9'){
                    num += s1.top();
                    s1.pop();
                }
                reverse(num.begin(),num.end());
                int cnt = atoi(num.c_str());
                string temp = res;
                for(int j = 0; j < cnt - 1; j++){
                    res += temp;;
                }
                //s1.pop();
                for(int k = 0; k < res.size(); k++){
                    s1.push(res[k]);
                }
            }
        }
        string ans = "";
        while(!s1.empty()){
            ans += s1.top();
            s1.pop();
        }
        reverse(ans.begin(),ans.end());
        return ans;
    }
};