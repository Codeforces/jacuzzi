package org.jacuzzi.core;

/**
 * @author: Mike Mirzayanov
 */
class Query {
    /**
     * Use following jokers:
     *  ?f: field name
     *  ?t: table name
     *
     * @param query String with jokers
     * @param args Parametersto be substituted instead of jokers
     * @return {@code query} with substitutions.
     */
    public static String format(String query, Object ... args) {
        StringBuffer result = new StringBuffer(query.length() + 32);
        int index = 0;

        for (int i = 0; i < query.length(); i++) {
            // Starts as joker?
            if (i + 1 < query.length() && query.charAt(i) == '?') {
                // Replace field?
                if (query.charAt(i + 1) == 'f') {
                    result.append('`');
                    result.append(args[index++]);
                    result.append('`');

                    i++;
                    continue;
                }

                // Replace table?
                if (query.charAt(i + 1) == 't') {
                    result.append('`');
                    result.append(args[index++]);
                    result.append('`');

                    i++;
                    continue;
                }
            }

            result.append(query.charAt(i));
        }

        if (index != args.length) {
            throw new IllegalArgumentException("There are not enought jokers in '" + query + "'.");
        }

        return result.toString();
    }
}
