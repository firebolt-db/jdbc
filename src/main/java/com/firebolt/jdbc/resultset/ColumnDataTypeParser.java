package com.firebolt.jdbc.resultset;

import com.firebolt.jdbc.resultset.column.Column;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.commons.text.StringEscapeUtils;

import static com.firebolt.jdbc.util.StringUtil.splitAll;

/**
 * Parser that knows to convert the data type result set header into column information
 */
public class ColumnDataTypeParser {

    public List<Column> getColumns(String[] columnNames, String columnTypes) {
        String[] types = splitAll(columnTypes, '\t');

        return IntStream.range(0, types.length)
                .mapToObj(i -> Column.of(types[i], StringEscapeUtils.unescapeJava(columnNames[i])))
                .collect(Collectors.toList());
    }

}
