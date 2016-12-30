class Solution {
public:
    vector<string> generateParenthesis(int n) {
        string s = "";
        vector<string> result;
        if(n == 0) return result;
        getResult(s,result,n,n);
        return result;
    }
    
    void getResult(string s,vector<string>& result,int left,int right){
        if(left == 0 && right == 0){
            result.push_back(s);
            return;
        }
        
        if(left > 0){
            getResult(s + '(',result,left - 1,right);
            s.substr(0,s.size() - 1);
        }
        
        if(left < right){
            getResult(s + ')',result,left,right - 1);
            s.substr(0,s.size() - 1);
        }
    }
};