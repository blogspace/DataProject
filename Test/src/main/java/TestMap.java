
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
public class TestMap {
     public static void main(String[] args) {
     Map<String, Object> map = new HashMap<String, Object>();
 map.put("aaa", 111);
  map.put("bbb", 222);
  map.put("ccc", 333);
 map.put("ddd", 444);
  //Map集合循环遍历方式一  
 System.out.println("第一种：通过Map.keySet()遍历key和value：");
  for(String key:map.keySet()){//keySet获取map集合key的集合  然后在遍历key即可
  String value = map.get(key).toString();//
  System.out.println("key:"+key+" vlaue:"+value);
  }

 //Map集合循环遍历二  通过迭代器的方式
 System.out.println("第二种：通过Map.entrySet使用iterator遍历key和value：");
 Iterator<Entry<String, Object>> it = map.entrySet().iterator();
 while(it.hasNext()){
  Entry<String, Object> entry = it.next();
 System.out.println("key:"+entry.getKey()+"  key:"+entry.getValue());
 }

 // Map集合循环遍历方式三 推荐，尤其是容量大时
 System.out.println("第三种：通过Map.entrySet遍历key和value");
 for (Map.Entry<String, Object> m : map.entrySet()) {
System.out.println("key:" + m.getKey() + " value:" + m.getValue());
}

// 第四种：
System.out.println("第四种：通过Map.values()遍历所有的value，但不能遍历key");
for(Object m:map.values()){
System.out.println(m);
 }
}
}
