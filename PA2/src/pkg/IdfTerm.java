package pkg;

/**
 * Created by DanielLund on 3/21/17.
 * Colorado State University
 * CS435
 */
public class IdfTerm {
    private String term;
    private Double idf;

    public IdfTerm(String line){
        initIdfTerm(line);
    }

    private void initIdfTerm(String line){
        // Expects string line to be of for
        // term \t <idf value>
        String[] termIDF = line.split("\\s+");
        this.term = termIDF[0];
        this.idf = new Double(termIDF[1]);
    }

    public String getTerm() {
        return term;
    }

    public Double getIdf() {
        return idf;
    }

}
