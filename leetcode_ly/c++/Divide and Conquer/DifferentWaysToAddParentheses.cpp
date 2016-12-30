class Solution {
public:
    vector<int> diffWaysToCompute(string input) {
        vector<int> result;
        
        for(int i = 0; i < input.size(); i++){
            if(input[i] == '+' || input[i] == '-' || input[i] == '*'){
                vector<int> left = diffWaysToCompute(input.substr(0,i));
                vector<int> right = diffWaysToCompute(input.substr(i + 1));
                
                for(int j = 0; j < left.size(); j++){
                    for(int k = 0; k < right.size(); k++){
                        if(input[i] == '+'){
                            result.push_back(left[j] + right[k]);
                        }
                        else if(input[i] == '-'){
                            result.push_back(left[j] - right[k]);
                        }
                        else if(input[i] == '*'){
                            result.push_back(left[j] * right[k]);
                        }
                    }
                }
            }
        }
        
        if(result.empty()){
            result.push_back(atoi(input.c_str()));
        }
        return result;
    }
};