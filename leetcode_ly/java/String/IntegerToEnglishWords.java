public class Solution {
    private final String[] belowTen = new String[] {"","One","Two","Three","Four","Five","Six","Seven","Eight","Nine"};
    private final String[] belowTwenty = new String[] {"Ten", "Eleven", "Twelve", "Thirteen", "Fourteen", "Fifteen", "Sixteen", "Seventeen", "Eighteen", "Nineteen"};
    private final String[] belowHundred = new String[] {"","Ten", "Twenty", "Thirty", "Forty", "Fifty", "Sixty", "Seventy", "Eighty", "Ninety"};
    
    public String numberToWords(int num) {
        if(num == 0) return "Zero";
        return dfs(num);
    }
    
    private String dfs(int num){
        String result = new String();
        if(num < 10) result = belowTen[num];
        else if(num < 20) result = belowTwenty[num % 10];
        else if(num < 100) result = belowHundred[num / 10] + " " + dfs(num % 10);
        else if(num < 1000) result = dfs(num / 100) + " Hundred " + dfs(num % 100);
        else if(num < 1000000) result = dfs(num / 1000) + " Thousand " + dfs(num % 1000);
        else if(num < 1000000000) result = dfs(num / 1000000) + " Million " + dfs(num % 1000000);
        else result = dfs(num / 1000000000) + " Billion " + dfs(num % 1000000000);
        return result.trim();
    }
}