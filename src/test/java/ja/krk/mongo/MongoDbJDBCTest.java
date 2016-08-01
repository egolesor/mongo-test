package ja.krk.mongo;

import ja.krk.mongo.mongo.JasDBTest;
import org.testng.annotations.Test;

/**
 * Unit test for simple App.
 */
public class MongoDbJDBCTest{

    //operation:get	100	2	0:00:02.916
    @Test
    public void oneCollection100TimeTest(){
        JasDBTest jasDBTest = new JasDBTest(100);

        jasDBTest.methodTest("member1=member.json");
    }

    @Test
    public void twoCollection100TimeTest(){
        JasDBTest jasDBTest = new JasDBTest(100);

        jasDBTest.methodTest("member1=member.json", "member2=member.json");
    }

    @Test
    public void oneCollection1000TimeTest(){
        JasDBTest jasDBTest = new JasDBTest(1000);

        jasDBTest.methodTest("member1=member.json");
    }

}
