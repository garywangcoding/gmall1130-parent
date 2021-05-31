import java.lang.reflect.Field;

/**
 * @Author lizhenchao@atguigu.cn
 * @Date 2021/5/31 10:29
 */
public class ClassDemo {
    public static void main(String[] args) throws IllegalAccessException, InstantiationException, NoSuchFieldException {
        Class<User> c = User.class;
//        Field[] fields = c.getFields();
        /*Field[] fields = c.getDeclaredFields();
        for (Field field : fields) {
            String fieldName = field.getName();
            System.out.println(fieldName);
    
        }*/
    
        User user = c.newInstance();
    
        Field ageF = c.getDeclaredField("age");
        ageF.setAccessible(true);
        ageF.set(user, 10);
        Object o = ageF.get(user);
        System.out.println(o);
    
    }
}

class User{
    private int age;
    public String name;
}
