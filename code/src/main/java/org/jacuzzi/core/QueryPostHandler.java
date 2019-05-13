package org.jacuzzi.core;

import java.sql.PreparedStatement;

/**
 * @author kuviman (kuviman@gmail.com)
 */
public interface QueryPostHandler {
    /**
     * Called after a successfully executed query
     *
     * This method may be called concurrently, thus should be thread safe
     *
     * @param query Successfully executed query
     */
    void handle(Query query);

    class Query {
        private String query;
        private Object[] args;
        private PreparedStatement statement;
        private long executionTimeMillis;

        public Query(String query, Object[] args, PreparedStatement statement, long executionTimeMillis) {
            this.query = query;
            this.args = args;
            this.statement = statement;
            this.executionTimeMillis = executionTimeMillis;
        }

        public String getQuery() {
            return query;
        }

        public Object[] getArgs() {
            return args;
        }

        public PreparedStatement getStatement() {
            return statement;
        }

        public long getExecutionTimeMillis() {
            return executionTimeMillis;
        }
    }
}
