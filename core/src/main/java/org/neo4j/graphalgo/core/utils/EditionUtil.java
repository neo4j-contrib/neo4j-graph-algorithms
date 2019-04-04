package org.neo4j.graphalgo.core.utils;

import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.kernel.impl.factory.DatabaseInfo;
import org.neo4j.kernel.impl.factory.Edition;
import org.neo4j.kernel.impl.util.UnsatisfiedDependencyException;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.udc.UsageData;
import org.neo4j.udc.UsageDataKeys;

import java.util.Iterator;

public final class EditionUtil {

    private EditionUtil() {}

    public static boolean isOnEnterprise(final GraphDatabaseAPI db) {
        if (db == null) {
            return false;
        }
        return isOnEnterprise(db.getDependencyResolver());
    }

    public static boolean isOnEnterprise(final DependencyResolver db) {
        if (db == null) {
            return false;
        }
        return edition(db) == Edition.enterprise;
    }

    private static Edition edition(final DependencyResolver dependencyResolver) {

        DatabaseInfo databaseInfo = null;
        try {
            databaseInfo = dependencyResolver.resolveDependency(DatabaseInfo.class, OPTIONAL);
        } catch (UnsatisfiedDependencyException ignored) {
        }
        if (databaseInfo != null) {
            Edition edition = databaseInfo.edition;
            if (edition != Edition.unknown) {
                return edition;
            }
        }

        UsageData usageData = null;
        try {
            usageData = dependencyResolver.resolveDependency(UsageData.class, OPTIONAL);
        } catch (UnsatisfiedDependencyException ignored) {
        }
        if (usageData != null) {
            Edition edition = usageData.get(UsageDataKeys.edition);
            if (edition != null && edition != Edition.unknown) {
                return edition;
            }
        }

        return Edition.unknown;
    }


    private static final DependencyResolver.SelectionStrategy OPTIONAL = new DependencyResolver.SelectionStrategy() {
        @Override
        public <T> T select(final Class<T> type, final Iterable<? extends T> candidates) {
            Iterator<? extends T> iterator = candidates.iterator();
            if (!iterator.hasNext()) {
                return null;
            }
            return iterator.next();
        }
    };

}
