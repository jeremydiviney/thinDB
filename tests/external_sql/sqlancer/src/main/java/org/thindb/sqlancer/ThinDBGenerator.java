package org.thindb.sqlancer;

import java.util.ArrayList;
import java.util.List;

import sqlancer.Randomly;
import org.thindb.sqlancer.ThinDBProvider.Column;
import org.thindb.sqlancer.ThinDBProvider.Table;
import org.thindb.sqlancer.ThinDBProvider.Type;

final class ThinDBGenerator {
    private final Randomly random;
    private final Table table;

    ThinDBGenerator(Randomly random, Table table) {
        this.random = random;
        this.table = table;
    }

    static String literal(String value) {
        return "'" + value.replace("'", "''") + "'";
    }

    static String text(Randomly random) {
        return List.of("", " ", "  a", "a  ", "a", "A", "NULL", "a'b", "a_b", "a%b", "two words")
                .get(random.getInteger(0, 11));
    }

    private String column(Type type) {
        List<Column> columns = table.getColumns().stream().filter(c -> c.getType() == type).toList();
        return columns.get(random.getInteger(0, columns.size())).getName();
    }

    private String integer() {
        String column = column(Type.INTEGER);
        int value = random.getInteger(-10, 11);
        return switch (random.getInteger(0, 5)) {
            case 0 -> Integer.toString(value);
            case 1 -> "(" + column + " + " + value + ")";
            case 2 -> "COALESCE(" + column + ", " + value + ")";
            case 3 -> "(-" + column + ")";
            default -> column;
        };
    }

    private String string() {
        String column = column(Type.TEXT);
        return switch (random.getInteger(0, 3)) {
            case 0 -> literal(text(random));
            case 1 -> "COALESCE(" + column + ", " + literal(text(random)) + ")";
            default -> column;
        };
    }

    String predicate(int depth) {
        if (depth > 0) {
            switch (random.getInteger(0, 5)) {
                case 0: return "(" + predicate(depth - 1) + " AND " + predicate(depth - 1) + ")";
                case 1: return "(" + predicate(depth - 1) + " OR " + predicate(depth - 1) + ")";
                case 2: return "(NOT " + predicate(depth - 1) + ")";
                default: break;
            }
        }
        return switch (random.getInteger(0, 7)) {
            case 0 -> "(" + integer() + " " + List.of("=", "<>", "<", "<=", ">", ">=")
                    .get(random.getInteger(0, 6)) + " " + integer() + ")";
            case 1 -> "(" + string() + (random.getInteger(0, 2) == 0 ? " = " : " <> ") + string() + ")";
            case 2 -> "(" + column(Type.TEXT) + " LIKE " + literal(List.of("%", "a%", "%a", "_", "a_b")
                    .get(random.getInteger(0, 5))) + ")";
            case 3 -> "(" + integer() + " IS NULL)";
            case 4 -> "(" + string() + " IS NOT NULL)";
            case 5 -> "(" + integer() + " IN (-1, 0, 1, NULL))";
            default -> "(" + integer() + " BETWEEN -5 AND 5)";
        };
    }

    List<String> queries() {
        List<String> projection = new ArrayList<>();
        for (Column column : table.getColumns()) {
            if (random.getInteger(0, 2) == 0) {
                projection.add(column.getName());
            }
        }
        if (projection.isEmpty()) {
            projection.add(table.getColumns().getFirst().getName());
        }
        if (random.getInteger(0, 2) == 0) {
            projection.add(integer());
        }
        String predicate = predicate(3);
        String base = "SELECT " + String.join(", ", projection) + " FROM " + table.getName();
        return List.of(base, base + " WHERE (" + predicate + ")", base + " WHERE NOT (" + predicate + ")",
                base + " WHERE (" + predicate + ") IS NULL");
    }
}
