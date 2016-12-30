class Solution {
public:
    bool wordPattern(string pattern, string str) {
        map<char,string> sMap;
        map<char,string>::iterator ster;
        map<string,char> tMap;
        map<string,char>::iterator tter;
        vector<string> result;
        int pos = 0;
        int count = 0;
        while(pos < str.size()){
            int k = pos;
            while(str[k] != ' ' && k < str.size()){
                k++;
                count++;
            }
            string s = str.substr(pos,count);
            count = 0;
            pos = k + 1;
            result.push_back(s);
        }
        int slen = pattern.size();
        int tlen = result.size();
        if(slen != tlen){
            return false;
        }
        else{
            for(int i = 0; i < slen; i++){
                ster = sMap.find(pattern[i]);
                if(ster == sMap.end()){
                    sMap.insert(pair<char,string>(pattern[i],result[i]));
                }
                else{
                    if(ster->second != result[i]){
                        return false;
                    }
                }
            }
            
            for(int j = 0; j < tlen; j++){
                tter = tMap.find(result[j]);
                if(tter == tMap.end()){
                    tMap.insert(pair<string,char>(result[j],pattern[j]));
                }
                else{
                    if(tter->second != pattern[j]){
                        return false;
                    }
                }
            }
            if(sMap.size() == tMap.size()){
                return true;
            }
            return false;
        }
    }
};