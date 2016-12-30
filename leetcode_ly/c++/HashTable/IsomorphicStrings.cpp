class Solution {
public:
    bool isIsomorphic(string s, string t) {
        map<char,char> myMap;
        map<char,char>::iterator iter;
        int len = s.size();
        for(int i = 0; i < len; i++){
            iter = myMap.find(s[i]);
            if(iter == myMap.end()){
                myMap.insert(pair<char,char>(s[i],t[i]));
            }
            else{
                if(iter->second != t[i]){
                    return false;
                }
            }
        }
        
        map<char,char> tMap;
        map<char,char>::iterator titer;
        int tlen = t.size();
        for(int i = 0; i < tlen; i++){
            titer = tMap.find(t[i]);
            if(titer == tMap.end()){
                tMap.insert(pair<char,char>(t[i],s[i]));
            }
            else{
                if(titer->second != s[i]){
                    return false;
                }
            }
        }
        if(myMap.size() == tMap.size()){
            return true;
        }
        return false;
    }
};