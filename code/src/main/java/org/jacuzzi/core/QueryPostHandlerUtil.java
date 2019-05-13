package org.jacuzzi.core;

import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * @author kuviman (kuviman@gmail.com)
 */
class QueryPostHandlerUtil {
    private static final List<QueryPostHandler> handlers = new ArrayList<>();

    static {
        try {
            Properties properties = new Properties();
            properties.load(QueryPostHandler.class.getResourceAsStream("/jacuzzi.properties"));
            String classes = properties.getProperty("queryPostHandlerClasses");
            if (classes != null) {
                for (String clazz : classes.split(";")) {
                    clazz = StringUtils.trimToNull(clazz);
                    if (clazz != null) {
                        Object instance = Class.forName(clazz).newInstance();
                        if (instance instanceof QueryPostHandler) {
                            handlers.add((QueryPostHandler) instance);
                        } else {
                            throw new RuntimeException(clazz + " does not implement " + QueryPostHandler.class.getSimpleName());
                        }
                    }
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    static void handle(QueryPostHandler.Query query) {
        for (QueryPostHandler handler : handlers) {
            handler.handle(query);
        }
    }
}
