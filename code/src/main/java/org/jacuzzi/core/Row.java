package org.jacuzzi.core;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

/** @author Mike Mirzayanov */
public class Row extends HashMap<String, Object> {
    /**
     * Extracts all rows from the result set and return them as List.
     *
     * @param resultSet JDBC result set to be read.
     * @return List<Row> Rows in result set.
     */
    static List<Row> readFromResultSet(ResultSet resultSet) {
        List<Row> result = new LinkedList<Row>();
        try {
            ResultSetMetaData metaData = resultSet.getMetaData();

            while (resultSet.next()) {
                addRowFromResultSet(resultSet, result, metaData);
            }

            resultSet.close();
        } catch (SQLException e) {
            throw new DatabaseException("Can't read the list of rows from the result set.", e);
        }
        return result;
    }

    /**
     * Extracts the first row from the result set.
     *
     * @param resultSet JDBC result set to be read.
     * @return Row The first row from the result set.
     */
    static Row readFirstFromResultSet(ResultSet resultSet) {
        List<Row> result = new LinkedList<Row>();
        ResultSetMetaData metaData;
        try {
            metaData = resultSet.getMetaData();
            if (resultSet.next()) {
                addRowFromResultSet(resultSet, result, metaData);
            }
            resultSet.close();
        } catch (SQLException e) {
            throw new DatabaseException("Can't read the first row from the result set.", e);
        }
        return result.size() == 1 ? result.get(0) : null;
    }

    /**
     * Reads the current row from result set and adds it to the list.
     *
     * @param resultSet Result set to be read.
     * @param result    List to be appended by new row.
     * @param metaData  JDBC meta data.
     */
    private static void addRowFromResultSet(ResultSet resultSet, List<Row> result, ResultSetMetaData metaData) {
        Row row = new Row();
        try {
            for (int i = 1; i <= metaData.getColumnCount(); i++) {
                row.put(metaData.getColumnLabel(i), resultSet.getObject(i));
            }
        } catch (SQLException e) {
            throw new DatabaseException("Can't add row from the result set.", e);
        }
        result.add(row);
    }
}
