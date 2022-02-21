import java.util.*;

/**
 * @author Yuh Z
 * @date 2/18/22
 */
public class testStringJoin {
    public static void main(String[] args) {
        List<String> deque = new LinkedList<>();
        deque.add("hello");
        deque.add("world");
        System.out.println(String.join(" ",deque));
    }
}
