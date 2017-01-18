public class Solution {
    public String largestNumber(int[] nums) {
        if(nums == null || nums.length == 0) return "";
        String[] str = new String[nums.length];
        for(int i = 0; i < nums.length; i++){
            str[i] = String.valueOf(nums[i]);
        }
        
        Arrays.sort(str,new Comparator<String>(){
            @Override
            public int compare(String a,String b){
                return (b + a).compareTo(a + b);
            }
        });
        
        //开头为0则后面一定全为0
        if(str[0].charAt(0) == '0') return "0";
        StringBuffer s = new StringBuffer();
        for(int i = 0; i < str.length; i++){
            s = s.append(str[i]);
        }
        return s.toString();
    }
}