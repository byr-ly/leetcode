class Solution {
public:
    char findTheDifference(string s, string t) {
        map<int,int> myMap;
        for(int i = 0; i < s.size(); i++){
            myMap[s[i] - 'a']++;
        }
        for(int i = 0; i < t.size(); i++){
        	myMap[t[i] - 'a']--;
        }
        map<int,int>::iterator it;
        for(it = myMap.begin(); it != myMap.end(); it++){
            if(it->second) return 'a' + it->first;
        }
        return t[0];
    }
};