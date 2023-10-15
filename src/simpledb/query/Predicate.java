package simpledb.query;

import lombok.NoArgsConstructor;
import simpledb.plan.Plan;
import simpledb.record.Schema;

import java.util.ArrayList;
import java.util.List;
import java.util.Iterator;


@NoArgsConstructor
public class Predicate {
    private final List<Term> terms = new ArrayList<>();

    public Predicate(Term t){
        terms.add(t);
    }

    public void conjoinWith(Predicate pred){
        terms.addAll(pred.terms);
    }
    public boolean isSatisfied(Scan s) {
        for (Term t : terms){
            if (!t.isSatisfied(s)){
                return false;
            }
        }
        return true;
    }

    public int reductionFactor(Plan p){
        int factor = 1;
        for (Term t : terms){
            factor *= t.reductionFactor(p);
        }
        return factor;
    }

    public Predicate selectSubPredicate(Schema sch){
        Predicate result = new Predicate();
        for (Term t : terms){
            if (t.appliesTo(sch)){
                result.terms.add(t);
            }
        }
        if(result.terms.isEmpty()){
            return null;
        }
        return result;
    }

    public Predicate joinSubPredicate(Schema sch1, Schema sch2){
        Predicate result = new Predicate();
        Schema newSchema = new Schema();
        newSchema.addAll(sch1);
        newSchema.addAll(sch2);
        for (Term t : terms){
            if (!t.appliesTo(sch1) && !t.appliesTo(sch2) && t.appliesTo(newSchema)){
                result.terms.add(t);
            }
        }
        if(result.terms.isEmpty()){
            return null;
        }
        return result;
    }

    public Constant equatesWithConstant(String fldname){
        for(Term t : terms){
            Constant val = t.equatesWithConstant(fldname);
            if (val != null){
                return val;
            }
        }
        return null;
    }

    public String toString(){
        Iterator<Term> iter = terms.iterator();
        if (!iter.hasNext()){
            return "";
        }
        StringBuilder result = new StringBuilder(iter.next().toString());
        while (iter.hasNext()){
            result.append(" and ").append(iter.next().toString());
        }
        return result.toString();
    }

    public String equatesWithField(String fieldName) {
        for(Term t : terms){
            String val = t.equatesWithField(fieldName);
            if (val != null){
                return val;
            }
        }
        return null;
    }
}
