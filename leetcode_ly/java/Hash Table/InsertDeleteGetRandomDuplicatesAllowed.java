public class RandomizedCollection {

    /** Initialize your data structure here. */
    public RandomizedCollection() {
        //ArrayList是用数组实现的，LinkedHashSet继承HashSet，是由map实现的，remove操作时间复杂度为O(1),而且能根据添加顺序遍历
        list = new ArrayList<Integer>();
        map = new HashMap<Integer,LinkedHashSet<Integer>>();
    }
    
    /** Inserts a value to the collection. Returns true if the collection did not already contain the specified element. */
    public boolean insert(int val) {
        if(map.containsKey(val)){
            map.get(val).add(list.size());
            list.add(val);
            return false;
        }
        else{
            map.put(val,new LinkedHashSet<Integer>());
            map.get(val).add(list.size());
            list.add(val);
        }
        return true;
    }
    
    /** Removes a value from the collection. Returns true if the collection contained the specified element. */
    public boolean remove(int val) {
        if(!map.containsKey(val)) return false;
        else{
            int loc = map.get(val).iterator().next();
            map.get(val).remove(loc);
            if(loc < list.size() - 1){
                int value = list.get(list.size() - 1);
                list.set(loc,value);
                map.get(value).add(loc);
                map.get(value).remove(list.size() - 1);
            }
            list.remove(list.size() - 1);
            if(map.get(val).isEmpty()) map.remove(val);
        }
        return true;
    }
    
    /** Get a random element from the collection. */
    public int getRandom() {
        Random rand = new Random();
        return list.get(rand.nextInt(list.size()));
    }
    
    private ArrayList<Integer> list;
    private HashMap<Integer,LinkedHashSet<Integer>> map;
}

/**
 * Your RandomizedCollection object will be instantiated and called as such:
 * RandomizedCollection obj = new RandomizedCollection();
 * boolean param_1 = obj.insert(val);
 * boolean param_2 = obj.remove(val);
 * int param_3 = obj.getRandom();
 */