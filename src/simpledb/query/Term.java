package simpledb.query;

import lombok.AllArgsConstructor;
import simpledb.plan.Plan;
import simpledb.record.Schema;

@AllArgsConstructor
public class Term {
    private final Expression lhs;
    private final Expression rhs;

    public boolean isSatisfied(Scan s) {
        Constant lhsVal = lhs.evaluate(s);
        Constant rhsVal = rhs.evaluate(s);
        return lhsVal.equals(rhsVal);
    }


    /*
     * この項はクエリが出力するレコードの数をどの程度減らすかを計算する
     * 例えば、もし減少率が 2 であれば、項は出力のサイズを半分にする
     * @param p クエリのプラン
     * @return 整数の減少率
     */
    public int reductionFactor(Plan p) {
        String lhsName, rhsName;
        if (lhs.isFieldName() && rhs.isFieldName()) {
            lhsName = lhs.asFieldName();
            rhsName = rhs.asFieldName();
            return Math.max(p.distinctValues(lhsName), p.distinctValues(rhsName));
        } else if (lhs.isFieldName()) {
            lhsName = lhs.asFieldName();
            return p.distinctValues(lhsName);
        } else if (rhs.isFieldName()) {
            rhsName = rhs.asFieldName();
            return p.distinctValues(rhsName);
        } else if (lhs.asConstant().equals(rhs.asConstant())) {
            return 1;
        } else {
            return Integer.MAX_VALUE;
        }
    }

    /*
     * この項は "F=c" の形式であるかどうかを判断する
     * ここで F は指定されたフィールドであり c は定数である
     * もしそうであれば、メソッドはその定数を返す
     * そうでなければ、メソッドは null を返す
     * @param fldname フィールドの名前
     * @return 定数または null
     */
    public Constant equatesWithConstant(String fldname) {
        if (lhs.isFieldName() && !rhs.isFieldName() && lhs.asFieldName().equals(fldname)) {
            return rhs.asConstant();
        } else if (rhs.isFieldName() && !lhs.isFieldName() && rhs.asFieldName().equals(fldname)) {
            return lhs.asConstant();
        } else {
            return null;
        }
    }


    /*
     * この項は "F1=F2" の形式であるかどうかを判断する
     * ここで F1 は指定されたフィールドであり F2 は別のフィールドである
     * もしそうであれば、メソッドはそのフィールドの名前を返す
     * そうでなければ、メソッドは null を返す
     * @param fldname フィールドの名前
     * @return 他のフィールドの名前または null
     */
    public String equatesWithField(String fldname) {
        if (lhs.isFieldName() && !rhs.isFieldName() && lhs.asFieldName().equals(fldname)) {
            return rhs.asFieldName();
        } else if (rhs.isFieldName() && !lhs.isFieldName() && rhs.asFieldName().equals(fldname)) {
            return lhs.asFieldName();
        } else {
            return null;
        }
    }

    public boolean appliesTo(Schema sch) {
        return lhs.appliesTo(sch) && rhs.appliesTo(sch);
    }

    public String toString() {
        return lhs.toString() + "=" + rhs.toString();
    }
}
