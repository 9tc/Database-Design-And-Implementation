package simpledb.jdbc;

import lombok.AllArgsConstructor;
import simpledb.plan.Plan;
import simpledb.plan.Planner;
import simpledb.transaction.Transaction;

import java.sql.SQLException;

@AllArgsConstructor
public class EmbeddedStatement extends StatementAdapter{
    private EmbeddedConnection connection;
    private Planner planner;

    public EmbeddedResultSet executeQuery(String query) throws SQLException{
        try{
            Transaction transaction = connection.getTransaction();
            Plan plan = planner.createQueryPlan(query, transaction);
            return new EmbeddedResultSet(plan, connection);
        }catch(RuntimeException e){
            connection.rollback();
            throw new SQLException(e);
        }
    }

    public int executeUpdate(String command) throws SQLException{
        try{
            Transaction transaction = connection.getTransaction();
            int result = planner.executeUpdate(command, transaction);
            connection.commit();
            return result;
        }catch (RuntimeException e){
            connection.rollback();
            throw new SQLException(e);
        }
    }

    public void close() throws SQLException{
        // do nothing
    }
}
