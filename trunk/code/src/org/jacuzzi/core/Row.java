package org.jacuzzi.core;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

/**
 * @author: Mike Mirzayanov
 */
public class Row extends HashMap<String, Object> {
    static List<Row> readFromResultSet(ResultSet resultSet) throws SQLException {
        List<Row> result = new LinkedList<Row>();
        ResultSetMetaData metaData = resultSet.getMetaData();

        while (resultSet.next()) {
            addRowFromResultSet(resultSet, result, metaData);
        }

        resultSet.close();
        return result;
    }

    public static Row readFirstFromResultSet(ResultSet resultSet) throws SQLException {
        List<Row> result = new LinkedList<Row>();
        ResultSetMetaData metaData = resultSet.getMetaData();

        if (resultSet.next()) {
            addRowFromResultSet(resultSet, result, metaData);
        }

        resultSet.close();

        return result.size() == 1 ? result.get(0) : null;
    }

    private static void addRowFromResultSet(ResultSet resultSet, List<Row> result, ResultSetMetaData metaData) throws SQLException {
        Row row = new Row();
        for (int i = 1; i <= metaData.getColumnCount(); i++) {
            row.put(metaData.getColumnName(i), resultSet.getObject(i));
        }
        result.add(row);
    }
}
