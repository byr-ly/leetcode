class Solution {
public:
    bool isValidSerialization(string preorder) {
        if(preorder.size() == 0) return false;
        stack<string> s;
        vector<string> s1;
        int start = 0;
        int count = 0;
        int i;
        for(i = 0; i < preorder.size(); i++){
            if(preorder[i] != ','){
                count++;
            }
            else{
                s1.push_back(preorder.substr(start,count));
                start = i + 1;
                count = 0;
            }
        }
        if(preorder[i - 1] != ',') s1.push_back(preorder.substr(start,count));
        
        s.push(s1[0]);
        int k = 1;
        while(!s.empty() && k < s1.size()){
            if(s1[k][0] == '#'){
                string temp = s.top();
                while(temp[0] == '#' && s.size() >= 2){
                    s.pop();
                    string ss = s.top();
                    if(ss[0] == '#'){
                        return false;
                    }
                    s.pop();
                    if(!s.empty()) temp = s.top();
                }
                s.push("#");
            }
            else{
                s.push(s1[k]);
            }
            k++;
        }
        if(s.size() == 1 && s.top() == "#") return true;
        else return false;
    }
};