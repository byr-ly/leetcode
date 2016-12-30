class Solution {
public:
    vector<int> getRow(int rowIndex) {
        vector<vector<int>> result;
        for(int i = 0; i <= rowIndex; i++){
            vector<int> ret(i + 1,1);
            result.push_back(ret);
        }
        
        for(int i = 2; i <= rowIndex; i++){
            for(int j = 1; j < i; j++){
                result[i][j] = result[i - 1][j] + result[i - 1][j - 1];
            }
        }
        return result[rowIndex];
    }
};