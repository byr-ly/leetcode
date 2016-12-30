class Solution {
public:
    vector<string> findRepeatedDnaSequences(string s) {
        vector<string> result;
        if(s.size() <= 10) return result;
        map<string,int> myMap;
        string s1;
        for(int i = 0; i <= s.size() - 10; i++){
            s1 = s.substr(i,10);
            myMap[s1]++;
            if(myMap[s1] == 2) result.push_back(s1);
        }
        return result;
    }
};