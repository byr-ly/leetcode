class Solution {
public:
    int compareVersion(string version1, string version2) {
        vector<int> v1 = getDigit(version1);
        vector<int> v2 = getDigit(version2);
        int len = (v1.size() <= v2.size()) ? v1.size() : v2.size();
        for(int i = 0; i < len;){
            int r = compare(v1[i],v2[i]);
            if(r != 0) return r;
            else i++;
        }
        if(len == v1.size() && len != v2.size()){
            for(int m = v1.size(); m < v2.size(); m++){
                if(v2[m] != 0) return -1;
                else continue;
            }
            return 0;
        }
        else if(len == v2.size() && len != v1.size()){
            for(int n = v2.size(); n < v1.size(); n++){
                if(v1[n] != 0) return 1;
                else continue;
            }
            return 0;
        }
        else return 0;
    }
    
    vector<int> getDigit(string s){
        int i = 0;
        int result = 0;
        vector<int> res;
        while(i < s.size()){
            while(i < s.size() && s[i] != '.'){
                result = result * 10 + (s[i] - '0');
                i++;
            }
            i++;
            res.push_back(result);
            result = 0;
        }
        return res;
    }
    
    int compare(int v1, int v2){
        if(v1 < v2) return -1;
        else if(v1 > v2) return 1;
        else return 0;
    }
};