class Solution {
public:
    vector<string> letterCombinations(string digits) {
        vector<string> result;
        if(digits.size() == 0) return result;
        map<char,string> myMap;
        myMap.insert(pair<char,string>('2',"abc"));
        myMap.insert(pair<char,string>('3',"def"));
        myMap.insert(pair<char,string>('4',"ghi"));
        myMap.insert(pair<char,string>('5',"jkl"));
        myMap.insert(pair<char,string>('6',"mno"));
        myMap.insert(pair<char,string>('7',"pqrs"));
        myMap.insert(pair<char,string>('8',"tuv"));
        myMap.insert(pair<char,string>('9',"wxyz"));
        getResult(digits,result,"",myMap,0);
        return result;
    }
    
    void getResult(string digits,vector<string>& result,string elements,map<char,string> myMap,int start){
        if(elements.size() == digits.size()){
            result.push_back(elements);
            return;
        }
        
        for(int i = start; i < digits.size(); i++){
            string s = myMap[digits[i]];
            for(int j = 0; j < s.size(); j++){
                elements += s[j];
                getResult(digits,result,elements,myMap,i+1);
                elements = elements.substr(0,elements.size() - 1);
            }
        }
    }
};