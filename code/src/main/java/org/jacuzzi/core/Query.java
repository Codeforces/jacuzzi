package org.jacuzzi.core;

/**
 * @author Mike Mirzayanov
 */
class Query {
    /**
     * Use following jokers:
     * ?f: field name
     * ?t: table name
     *
     * @param query String with jokers
     * @param args  Parameters to be substituted instead of jokers
     * @return {@code query} with substitutions.
     */
    @SuppressWarnings("AccessOfSystemProperties")
    public static String format(String query, Object... args) {
        boolean tableQuotation = Boolean.parseBoolean(System.getProperty("jacuzzi.tableQuotation"));
        boolean fieldQuotation = Boolean.parseBoolean(System.getProperty("jacuzzi.fieldQuotation"));

        StringBuilder result = new StringBuilder(query.length() + 32);
        int index = 0;

        for (int i = 0; i < query.length(); ++i) {
            // Starts as joker?
            if (i + 1 < query.length() && query.charAt(i) == '?') {
                // Replace field?
                if (query.charAt(i + 1) == 'f') {
                    if (fieldQuotation) {
                        result.append('`');
                    }
                    result.append(args[index++]);
                    if (fieldQuotation) {
                        result.append('`');
                    }

                    ++i;
                    continue;
                }

                // Replace table?
                if (query.charAt(i + 1) == 't') {
                    if (tableQuotation) {
                        result.append('`');
                    }
                    result.append(args[index++]);
                    if (tableQuotation) {
                        result.append('`');
                    }

                    ++i;
                    continue;
                }
            }

            result.append(query.charAt(i));
        }

        if (index != args.length) {
            throw new IllegalArgumentException("There are not enough jokers in '" + query + "'.");
        }

        return result.toString();
    }
}
