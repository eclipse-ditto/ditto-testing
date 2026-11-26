/*
 * Copyright (c) 2026 Contributors to the Eclipse Foundation
 *
 * See the NOTICE file(s) distributed with this work for additional
 * information regarding copyright ownership.
 *
 * This program and the accompanying materials are made available under the
 * terms of the Eclipse Public License 2.0 which is available at
 * http://www.eclipse.org/legal/epl-2.0
 *
 * SPDX-License-Identifier: EPL-2.0
 */
package org.eclipse.ditto.testing.system.persistence;

import java.util.List;

import org.bson.BsonDocument;
import org.eclipse.ditto.json.JsonArray;
import org.eclipse.ditto.json.JsonCollectors;
import org.eclipse.ditto.json.JsonValue;

import com.mongodb.ReadPreference;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;

/**
 * {@link PersistenceInspector} for the MongoDB persistence backend. Counts journal and snapshot documents in the
 * {@code things}/{@code policies}/{@code connectivity} databases.
 */
public final class MongoPersistenceInspector implements PersistenceInspector {

    private static final String CONNECTION_PREFIX = "connection:";

    private final MongoClient mongoClient;

    /**
     * Constructs a new instance connecting to the given MongoDB URI.
     *
     * @param mongoDbUri the MongoDB connection string.
     */
    public MongoPersistenceInspector(final String mongoDbUri) {
        mongoClient = MongoClients.create(mongoDbUri);
    }

    @Override
    public long countThingEvents(final List<String> thingIds) {
        return countDocuments(thingIds, "things", "things_journal", "thing:", false);
    }

    @Override
    public long countThingSnaps(final List<String> thingIds) {
        return countDocuments(thingIds, "things", "things_snaps", "thing:", false);
    }

    @Override
    public long countPolicyEvents(final List<String> policyIds) {
        return countDocuments(policyIds, "policies", "policies_journal", "policy:", false);
    }

    @Override
    public long countPolicySnaps(final List<String> policyIds) {
        return countDocuments(policyIds, "policies", "policies_snaps", "policy:", false);
    }

    @Override
    public long countConnectionEvents(final List<String> connIds, final boolean onlyConnectivityEvents) {
        return countDocuments(connIds, "connectivity", "connection_journal", CONNECTION_PREFIX,
                onlyConnectivityEvents);
    }

    @Override
    public long countConnectionSnaps(final List<String> connIds) {
        return countDocuments(connIds, "connectivity", "connection_snaps", CONNECTION_PREFIX, false);
    }

    private long countDocuments(final List<? extends CharSequence> ids,
            final String database, final String collection,
            final String prefix,
            final boolean filterConnectivityEvents) {

        final JsonArray idsJson = ids.stream()
                .map(id -> prefix + id)
                .map(JsonValue::of)
                .collect(JsonCollectors.valuesToArray());

        final BsonDocument bsonDocument;
        if (CONNECTION_PREFIX.equals(prefix) && filterConnectivityEvents) {
            // filter out empty-events
            bsonDocument = BsonDocument.parse(String.format("{$and:[{\"pid\":{\"$in\":%s}}," +
                    "{'events.p.type': { $regex: \"connectivity.events:\"}}]}", idsJson));
        } else {
            bsonDocument = BsonDocument.parse(String.format("{\"pid\":{\"$in\":%s}}", idsJson));
        }

        return mongoClient.getDatabase(database)
                .getCollection(collection)
                .withReadPreference(ReadPreference.primary())
                .countDocuments(bsonDocument);
    }

    @Override
    public void close() {
        mongoClient.close();
    }

}
