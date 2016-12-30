class Solution {
public:
    int minimumTotal(vector<vector<int>>& triangle) {
        int m = triangle.size();
        if(m == 0) return 0;
        
        for(int i = 1; i < m; i++){
            for(int j = 0; j < triangle[i].size(); j++){
                if(j == 0) triangle[i][j] += triangle[i - 1][j];
                else if(j == triangle[i].size() - 1) triangle[i][j] += triangle[i - 1][triangle[i - 1].size() - 1];
                else{
                    int left = triangle[i][j] + triangle[i - 1][j - 1];
                    int right = triangle[i][j] + triangle[i - 1][j];
                    triangle[i][j] = (left < right) ? left : right;   
                }
            }
        }
        
        int min = triangle[m - 1][0];
        for(int i = 1; i < triangle[m - 1].size(); i++){
            min = (triangle[m - 1][i] < min) ? triangle[m - 1][i] : min;
        }
        return min;
    }
};