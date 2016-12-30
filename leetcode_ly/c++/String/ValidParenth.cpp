class Solution {
public:
    bool isValid(string s) {
        int i = 0;
        vector<char> path;
        if(s[0] == ')' || s[0] == ']' || s[0] == '}') return false;
        while(i < s.size()){
            if(s[i] == '(' || s[i] == '[' || s[i] == '{'){
                path.push_back(s[i]);
            }
            else{
                switch(s[i]){
                    case ')':{
                        if(path[path.size() - 1] != '(') return false;
                        else path.pop_back();
                        break;
                    }
                    case ']':{
                        if(path[path.size() - 1] != '[') return false;
                        else path.pop_back();
                        break;
                    }
                    case '}':{
                        if(path[path.size() - 1] != '{') return false;
                        else path.pop_back();
                        break;
                    }
                    default:
                        break;
                }
            }
            i++;
        }
        if(path.size() == 0){
            return true;
        }
        else{
            return false;
        }
    }
};