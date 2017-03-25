package pkg;

/**
 * Created by DanielLund on 3/21/17.
 * Colorado State University
 * CS435
 */
public class AuthorCosSim implements Comparable{
    private String author;
    private Double cosSim;

    public AuthorCosSim(String line){
        initIdfTerm(line);
    }

    private void initIdfTerm(String line){
        // Expects string line to be of for
        // term \t <cosSim value>
        String[] authorCosSim = line.split("\\s+");
        this.author = authorCosSim[0];
        this.cosSim = new Double(authorCosSim[1]);
    }

    public String getTerm() {
        return author;
    }

    public Double getCosSim() {
        return cosSim;
    }

    @Override
    public int compareTo(Object o){
        AuthorCosSim two = (AuthorCosSim) o;
        if (this.getCosSim() > two.getCosSim()) return 1;
        else if (this.getCosSim() < two.getCosSim()) return -1;
        else return 0;
    }

    @Override
    public String toString() {
        return author + " " + cosSim.toString();
    }
}
