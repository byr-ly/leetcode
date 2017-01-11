public class Solution {
    public List<String> readBinaryWatch(int num) {
        List<String> res = new ArrayList<String>();
        int[] hour = {8,4,2,1};
        int[] minute = {32,16,8,4,2,1};
        
        for(int i = 0; i <= num; i++){
            //i表示时和分各有几个灯亮，产生所有可能的值
            List<Integer> list1 = generateDigit(hour,i);
            List<Integer> list2 = generateDigit(minute,num - i);
            
            for(Integer num1 : list1){
                if(num1 >= 12) continue;
                for(Integer num2 : list2){
                    if(num2 >= 60) continue;
                    res.add(num1 + ":" + (num2 < 10 ? "0" + num2 : num2));
                }
            }
        }
        return res;
    }
    
    public List<Integer> generateDigit(int[] nums,int count){
        List<Integer> res = new ArrayList<Integer>();
        getResult(res,nums,count,0,0);
        return res;
    }
    
    public void getResult(List<Integer> res,int[] nums,int count,int start,int sum){
        if(count == 0){
            res.add(sum);
            return;
        }
        
        for(int i = start; i < nums.length; i++){
            getResult(res,nums,count - 1,i + 1,sum + nums[i]);
        }
    }
}