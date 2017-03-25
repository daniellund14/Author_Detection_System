package Tests;

import org.junit.Test;
import pkg.AuthorCosSim;

import java.util.ArrayList;
import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Created by DanielLund on 3/25/17.
 * Colorado State University
 * CS435
 */
public class AuthorCosSimTests {

    @Test
    public void testAutorCosSimConstructor(){
        String line = "testAuthor 1.0";
        AuthorCosSim one = new AuthorCosSim(line);
        assertEquals(one.getTerm(), "testAuthor");
        assertEquals((double)one.getCosSim(), 1.0, 0);
    }

    @Test public void testAuthorCosSimCompareTo(){
        String line = "testAuthor 1.0";
        AuthorCosSim one = new AuthorCosSim(line);
        String line2 = "testAuthor2 2.0";
        AuthorCosSim two = new AuthorCosSim(line2);
        assertTrue(one.compareTo(two) ==  -1);
    }

    @Test public void testTopTen(){
        ArrayList<AuthorCosSim> cosSims = new ArrayList<>();
        String authors = "author1 1.0,author2 1.1,author3 1.11,author4 1.111,author5 1.1111,author6 1.11111,author7 1.111111,author8 1.1111111,author9 1.11111111,author10 1.111111111";
        String[] authorCos = authors.split(",");
        for(String line: authorCos){
            cosSims.add(new AuthorCosSim(line));
        }
        Collections.sort(cosSims,Collections.reverseOrder());
        System.out.println(cosSims.toString());
        AuthorCosSim previous = null;
        for(AuthorCosSim sim: cosSims){
            if(previous == null){
                previous = sim;
                continue;
            }else{
                assertTrue(previous.getCosSim().toString() + " >= " + sim.getCosSim().toString(), previous.getCosSim() >= sim.getCosSim());
            }
        }
    }
}
