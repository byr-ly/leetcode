public class RandomizedSet {

    /** Initialize your data structure here. */
    public RandomizedSet() {
        //list是由数组实现的，因此增加删除的复杂度为O(n),需要用空间复杂度换取时间复杂度
        list = new ArrayList<Integer>();
        map = new HashMap<Integer,Integer>();
    }
    
    /** Inserts a value to the set. Returns true if the set did not already contain the specified element. */
    public boolean insert(int val) {
        if(map.containsKey(val)) return false;
        else{
            map.put(val,list.size());
            list.add(val);
        }
        return true;
    }
    
    /** Removes a value from the set. Returns true if the set contained the specified element. */
    public boolean remove(int val) {
        if(!map.containsKey(val)) return false;
        else{
            int loc = map.get(val);
            if(loc < list.size() - 1){
                int value = list.get(list.size() - 1);
                list.set(loc,value);
                map.put(value,loc);
            }
            map.remove(val);
            list.remove(list.size() - 1);
        }
        return true;
    }
    
    /** Get a random element from the set. */
    public int getRandom() {
        Random rand = new Random();
        return list.get(rand.nextInt(list.size()));
    }
    
    private ArrayList<Integer> list;
    private HashMap<Integer,Integer> map;
}

/**
 * Your RandomizedSet object will be instantiated and called as such:
 * RandomizedSet obj = new RandomizedSet();
 * boolean param_1 = obj.insert(val);
 * boolean param_2 = obj.remove(val);
 * int param_3 = obj.getRandom();
 */