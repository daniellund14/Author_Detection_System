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
        String authors = "Shedd 2.9878223965646594E-5, Various 2.9878223965646445E-5, Burroughs 2.9878223965646397E-5, Morley 2.9878223965646377E-5, Sinclair 2.987822396564633E-5, Stewart 2.9878223965646306E-5, Madison 2.9878223965646282E-5, Stevenson 2.9878223965646238E-5, Doyle 2.9878223965646167E-5, Maugham 2.987822396564616E-5, Dickens 2.9878223965646133E-5, Balzac 2.9878223965645977E-5, Hamilton 2.987822396564597E-5, Haultain 2.987822396564597E-5, Grimalkin 2.987822396564595E-5, Twain 2.9878223965645947E-5, Austen 2.9878223965645913E-5, Hardy 2.987822396564587E-5, Eliot 2.9878223965645852E-5, Woolf 2.9878223965645808E-5, Atherton 2.9878223965645692E-5";
        String[] authorCos = authors.split(", ");
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
