/*
 * Copyright (c) 2023 Contributors to the Eclipse Foundation
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
package org.eclipse.ditto.testing.system.search.things;


import static org.eclipse.ditto.testing.common.matcher.search.SearchProperties.attribute;
import static org.eclipse.ditto.testing.common.matcher.search.SearchResponseMatchers.isEmpty;
import static org.eclipse.ditto.testing.common.matcher.search.SearchResponseMatchers.isEqualTo;
import static org.eclipse.ditto.thingsearch.model.SearchModelFactory.and;
import static org.eclipse.ditto.thingsearch.model.SearchModelFactory.not;
import static org.eclipse.ditto.thingsearch.model.SearchModelFactory.newSortOption;

import java.util.List;

import org.eclipse.ditto.base.model.json.JsonSchemaVersion;
import org.eclipse.ditto.json.JsonFactory;
import org.eclipse.ditto.json.JsonValue;
import org.eclipse.ditto.testing.common.VersionedSearchIntegrationTest;
import org.eclipse.ditto.testing.common.matcher.search.SearchMatcher;
import org.eclipse.ditto.testing.common.matcher.search.SearchProperties;
import org.eclipse.ditto.testing.common.matcher.search.SortProperties;
import org.eclipse.ditto.things.model.Attributes;
import org.eclipse.ditto.things.model.Thing;
import org.eclipse.ditto.things.model.ThingId;
import org.eclipse.ditto.thingsearch.model.SearchFilter;
import org.eclipse.ditto.thingsearch.model.SearchProperty;
import org.eclipse.ditto.thingsearch.model.SortOptionEntry;
import org.junit.Test;

/**
 *
 */
public class QueryThingsWithFilterByAttributesIT extends VersionedSearchIntegrationTest {

    private static final String NUMBER_ATTR = "numberAttr";
    private static final String STRING_ATTR = "stringAttr";
    private static final String REGEX_ATTR = "stringRegex";
    private static final String BOOL_ATTR = "boolAttr";
    private static final String NULL_ATTR = "nullAttr";

    private static final String THING1_STR_ATTR_VALUE = "a";
    private static final String THING1_REGEX_VALUE = "Das ist ein X beliebiger String";
    private static final String THING1_REGEX_STARTS_WITH = "Das*";

    private static final String THING2_STR_ATTR_VALUE = "b";
    private static final int THING2_NUMBER_ATTR_VALUE = 2;
    private static final boolean THING2_BOOL_ATTR_VALUE = true;
    private static final String THING2_REGEX_VALUE = "Der zweite String";
    private static final String THING2_REGEX_CONTAINS = "*zweite*";

    private static final double THING3_NUMBER_ATTR_VALUE = 3.1;
    private static final boolean THING3_BOOL_ATTR_VALUE = false;
    private static final String THING3_REGEX_VALUE = "Teststring nummer drei";
    // wildcard "*" in the middle of a value
    private static final String THING3_WILDCARD_ASTERISK_MIDDLE_VALUE = "Teststr*ng nummer drei";
    private static final String THING3_WILDCARD_QUESTION_MARK_VALUE = "Teststr?ng nummer drei";
    private static final String THING3_WILDCARD_DOT_VALUE = "Teststr.ng nummer drei";
    private static final String THING3_WILDCARD_BOL_VALUE = "^Teststring nummer drei";
    private static final String THING3_WILDCARD_EOL_VALUE = "Teststring nummer drei$";

    private static final boolean THING4_BOOL_ATTR_VALUE = THING3_BOOL_ATTR_VALUE;
    private static final String THING4_REGEX_VALUE = "Der vierte und letzte Teststring";
    private static final String THING4_REGEX_ENDS_WITH = "*Teststring";
    private static final String THING4_REGEX_REPEATING_WILDCARD = "****vierte***letzte*****";

    private static final SearchFilter THING_ID_FILTER =
            SearchProperties.thingId().like("*QueryThingsWithFilterByAttributesIT*");
    // in() operands of the null-operand tests; "non-null" is thing2's NULL_ATTR value (see createThing2)
    private static final List<JsonValue> NON_NULL_OR_NULL =
            List.of(JsonFactory.newValue("non-null"), JsonFactory.nullLiteral());
    private static final List<JsonValue> ONLY_NULL = List.of(JsonFactory.nullLiteral());

    private static ThingId thing1Id;
    private static ThingId thing2Id;
    private static ThingId thing3Id;
    private static ThingId thing4Id;

    protected void createTestData() {
        thing1Id = persistThingAndWaitTillAvailable(createThing1());
        thing2Id = persistThingAndWaitTillAvailable(createThing2());
        thing3Id = persistThingAndWaitTillAvailable(createThing3());
        thing4Id = persistThingAndWaitTillAvailable(createThing4());
    }

    private Thing createThing1() {
        final Attributes attrs = Attributes.newBuilder()
                .set(STRING_ATTR, THING1_STR_ATTR_VALUE)
                .set(NUMBER_ATTR, 1)
                .set(BOOL_ATTR, true)
                .set(NULL_ATTR, JsonFactory.nullLiteral())
                .set(REGEX_ATTR, THING1_REGEX_VALUE)
                .build();
        return Thing.newBuilder()
                .setId(createThingId(1))
                .setAttributes(attrs)
                .build();
    }

    private Thing createThing2() {
        final Attributes attrs = Attributes.newBuilder()
                .set(STRING_ATTR, THING2_STR_ATTR_VALUE)
                .set(NUMBER_ATTR, THING2_NUMBER_ATTR_VALUE)
                .set(BOOL_ATTR, THING2_BOOL_ATTR_VALUE)
                .set(NULL_ATTR, "non-null")
                .set(REGEX_ATTR, THING2_REGEX_VALUE)
                .build();
        return Thing.newBuilder()
                .setId(createThingId(2))
                .setAttributes(attrs)
                .build();
    }

    private Thing createThing3() {
        final Attributes attrs = Attributes.newBuilder()
                .set(STRING_ATTR, "c")
                .set(NUMBER_ATTR, THING3_NUMBER_ATTR_VALUE)
                .set(BOOL_ATTR, THING3_BOOL_ATTR_VALUE)
                .set(REGEX_ATTR, THING3_REGEX_VALUE)
                .build();
        return Thing.newBuilder()
                .setId(createThingId(3))
                .setAttributes(attrs)
                .build();
    }

    private Thing createThing4() {
        final Attributes attrs = Attributes.newBuilder()
                .set(STRING_ATTR, "d")
                .set(NUMBER_ATTR, 4)
                .set(BOOL_ATTR, THING4_BOOL_ATTR_VALUE)
                .set(REGEX_ATTR, THING4_REGEX_VALUE)
                .build();
        return Thing.newBuilder()
                .setId(createThingId(4))
                .setAttributes(attrs)
                .build();
    }

    private static ThingId createThingId(final int i) {
        return ThingId.of(idGenerator()
                .withPrefixedRandomName(QueryThingsWithFilterByAttributesIT.class.getSimpleName(), String.valueOf(i)));
    }

    @Test
    public void queryByEqNumber() {
        search(attribute(NUMBER_ATTR).eq(THING2_NUMBER_ATTR_VALUE))
                .expectingBody(isEqualTo(toThingResult(thing2Id)))
                .fire();
    }

    @Test
    public void queryByEqBoolean() {
        search(attribute(BOOL_ATTR).eq(THING3_BOOL_ATTR_VALUE))
                .expectingBody(isEqualTo(toThingResult(thing3Id, thing4Id)))
                .fire();
    }

    @Test
    public void queryByEqString() {
        search(attribute(STRING_ATTR).eq(THING1_STR_ATTR_VALUE))
                .expectingBody(isEqualTo(toThingResult(thing1Id)))
                .fire();
    }

    @Test
    public void queryByEqNull() {
        search(attribute(NULL_ATTR).eq(null))
                .expectingBody(isEqualTo(toThingResult(thing1Id)))
                .fire();
    }

    @Test
    public void queryByNeString() {
        search(attribute(STRING_ATTR).ne(THING1_STR_ATTR_VALUE))
                .expectingBody(isEqualTo(toThingResult(thing2Id, thing3Id, thing4Id)))
                .fire();
    }

    @Test
    public void queryByNeNumber() {
        search(attribute(NUMBER_ATTR).ne(THING2_NUMBER_ATTR_VALUE))
                .expectingBody(isEqualTo(toThingResult(thing1Id, thing3Id, thing4Id)))
                .fire();
    }

    @Test
    public void queryByNeBoolean() {
        search(attribute(BOOL_ATTR).ne(THING3_BOOL_ATTR_VALUE))
                .expectingBody(isEqualTo(toThingResult(thing1Id, thing2Id)))
                .fire();
    }

    @Test
    public void queryByNeNull() {
        search(attribute(NULL_ATTR).ne(null))
                .expectingBody(isEqualTo(toThingResult(thing2Id)))
                .fire();
    }

    @Test
    public void queryByGtString() {
        search(attribute(STRING_ATTR).gt(THING2_STR_ATTR_VALUE))
                .expectingBody(isEqualTo(toThingResult(thing3Id, thing4Id)))
                .fire();
    }

    @Test
    public void queryByGtNumber() {
        search(attribute(NUMBER_ATTR).gt(THING3_NUMBER_ATTR_VALUE))
                .expectingBody(isEqualTo(toThingResult(thing4Id)))
                .fire();
    }

    @Test
    public void queryByGtBoolean() {
        search(attribute(BOOL_ATTR).gt(THING3_BOOL_ATTR_VALUE))
                .expectingBody(isEqualTo(toThingResult(thing1Id, thing2Id)))
                .fire();
    }

    @Test
    public void queryByGeString() {
        search(attribute(STRING_ATTR).ge(THING2_STR_ATTR_VALUE))
                .expectingBody(isEqualTo(toThingResult(thing2Id, thing3Id, thing4Id)))
                .fire();
    }

    @Test
    public void queryByGeNumber() {
        search(attribute(NUMBER_ATTR).ge(THING3_NUMBER_ATTR_VALUE))
                .expectingBody(isEqualTo(toThingResult(thing3Id, thing4Id)))
                .fire();
    }

    @Test
    public void queryByGeBoolean() {
        search(attribute(BOOL_ATTR).ge(THING3_BOOL_ATTR_VALUE))
                .expectingBody(isEqualTo(toThingResult(thing1Id, thing2Id, thing3Id, thing4Id)))
                .fire();
    }

    @Test
    public void queryByLtString() {
        search(attribute(STRING_ATTR).lt(THING2_STR_ATTR_VALUE))
                .expectingBody(isEqualTo(toThingResult(thing1Id)))
                .fire();
    }

    @Test
    public void queryByLtNumber() {
        search(attribute(NUMBER_ATTR).lt(THING3_NUMBER_ATTR_VALUE))
                .expectingBody(isEqualTo(toThingResult(thing1Id, thing2Id)))
                .fire();
    }

    @Test
    public void queryByLtBoolean() {
        search(attribute(BOOL_ATTR).lt(THING2_BOOL_ATTR_VALUE))
                .expectingBody(isEqualTo(toThingResult(thing3Id, thing4Id)))
                .fire();
    }

    @Test
    public void queryByLeString() {
        search(attribute(STRING_ATTR).le(THING2_STR_ATTR_VALUE))
                .expectingBody(isEqualTo(toThingResult(thing1Id, thing2Id)))
                .fire();
    }

    @Test
    public void queryByLeNumber() {
        search(attribute(NUMBER_ATTR).le(THING3_NUMBER_ATTR_VALUE))
                .expectingBody(isEqualTo(toThingResult(thing1Id, thing2Id, thing3Id)))
                .fire();
    }

    @Test
    public void queryByLeBoolean() {
        search(attribute(BOOL_ATTR).le(THING2_BOOL_ATTR_VALUE))
                .expectingBody(isEqualTo(toThingResult(thing1Id, thing2Id, thing3Id, thing4Id)))
                .fire();
    }

    @Test
    public void queryByInNumber() {
        search(attribute(NUMBER_ATTR).in(THING2_NUMBER_ATTR_VALUE, THING3_NUMBER_ATTR_VALUE))
                .expectingBody(isEqualTo(toThingResult(thing2Id, thing3Id)))
                .fire();
    }

    @Test
    public void queryByInBoolean() {
        search(attribute(BOOL_ATTR).in(THING2_BOOL_ATTR_VALUE, THING3_BOOL_ATTR_VALUE))
                .expectingBody(isEqualTo(toThingResult(thing1Id, thing2Id, thing3Id, thing4Id)))
                .fire();
    }

    @Test
    public void queryByInString() {
        search(attribute(STRING_ATTR).in(THING1_STR_ATTR_VALUE, THING2_STR_ATTR_VALUE))
                .expectingBody(isEqualTo(toThingResult(thing1Id, thing2Id)))
                .fire();
    }

    /**
     * Test to check starts with functionality
     */
    @Test
    public void queryByLikeString1() {
        search(attribute(REGEX_ATTR).like(THING1_REGEX_STARTS_WITH))
                .expectingBody(isEqualTo(toThingResult(thing1Id)))
                .fire();
    }

    /**
     * Test to check starts with functionality, ignoring case sensitivity.
     */
    @Test
    public void queryByILikeString1() {
        search(attribute(REGEX_ATTR).ilike(THING1_REGEX_STARTS_WITH.toLowerCase()))
                .expectingBody(isEqualTo(toThingResult(thing1Id)))
                .fire();
    }

    /**
     * Test to check contains functionality
     */
    @Test
    public void queryByLikeString2() {
        search(attribute(REGEX_ATTR).like(THING2_REGEX_CONTAINS))
                .expectingBody(isEqualTo(toThingResult(thing2Id)))
                .fire();
    }

    /**
     * Test to check contains functionality, ignoring case sensitivity.
     */
    @Test
    public void queryByILikeString2() {
        search(attribute(REGEX_ATTR).ilike(THING2_REGEX_CONTAINS.toUpperCase()))
                .expectingBody(isEqualTo(toThingResult(thing2Id)))
                .fire();
    }

    /**
     * Make sure that all supported wildcards can be used.
     */
    @Test
    public void queryByLikeString3WithSupportedWildcards() {
        search(attribute(REGEX_ATTR).like(THING3_WILDCARD_QUESTION_MARK_VALUE))
                .expectingBody(isEqualTo(toThingResult(thing3Id)))
                .fire();

        search(attribute(REGEX_ATTR).like(THING3_WILDCARD_ASTERISK_MIDDLE_VALUE))
                .expectingBody(isEqualTo(toThingResult(thing3Id)))
                .fire();
    }

    /**
     * Make sure that not supported wildcards can't be used.
     */
    @Test
    public void queryByLikeString3WithNotSupportedWildcards() {
        search(attribute(REGEX_ATTR).like(THING3_WILDCARD_DOT_VALUE))
                .expectingBody(isEmpty())
                .fire();

        search(attribute(REGEX_ATTR).like(THING3_WILDCARD_BOL_VALUE))
                .expectingBody(isEmpty())
                .fire();

        search(attribute(REGEX_ATTR).like(THING3_WILDCARD_EOL_VALUE))
                .expectingBody(isEmpty())
                .fire();
    }

    /**
     * Test to check ends with functionality
     */
    @Test
    public void queryByLikeString4() {
        search(attribute(REGEX_ATTR).like(THING4_REGEX_ENDS_WITH))
                .expectingBody(isEqualTo(toThingResult(thing4Id)))
                .fire();
    }

    /**
     * Test to check regular expression repeating wildcards
     */
    @Test
    public void queryByLikeWithRepeatingWildcard() {
        search(attribute(REGEX_ATTR).like(THING4_REGEX_REPEATING_WILDCARD))
                .expectingBody(isEqualTo(toThingResult(thing4Id)))
                .fire();
    }

    @Test
    public void queryByEmptyNullAttribute() {
        search(attribute(NULL_ATTR).empty())
                .expectingBody(isEqualTo(toThingResult(thing1Id, thing3Id, thing4Id)))
                .fire();
    }

    @Test
    public void queryByNotEmptyNullAttribute() {
        search(not(attribute(NULL_ATTR).empty()))
                .expectingBody(isEqualTo(toThingResult(thing2Id)))
                .fire();
    }

    @Test
    public void queryByEmptyStringAttributeNoMatch() {
        search(attribute(STRING_ATTR).empty())
                .expectingBody(isEmpty())
                .fire();
    }

    @Test
    public void queryByNotEmptyStringAttribute() {
        search(not(attribute(STRING_ATTR).empty()))
                .expectingBody(isEqualTo(toThingResult(thing1Id, thing2Id, thing3Id, thing4Id)))
                .fire();
    }

    @Test
    public void queryByEmptyAbsentAttribute() {
        search(attribute("missingAttr").empty())
                .expectingBody(isEqualTo(toThingResult(thing1Id, thing2Id, thing3Id, thing4Id)))
                .fire();
    }

    /*
     * Null operands of in()/ge/gt/le/lt. The search contract: a null operand of in(), ge and le matches a null value
     * and a missing attribute; gt and lt with a null operand match nothing; only eq(null) and ne require the
     * attribute to exist. This is the behaviour of the MongoDB-backed search the contract comes from. Fixture:
     * thing1 nullAttr = null, thing2 "non-null", thing3/thing4 without nullAttr.
     */

    @Test
    public void queryByInStringAndNull() {
        search(attribute(NULL_ATTR).in(NON_NULL_OR_NULL))
                .expectingBody(isEqualTo(toThingResult(thing1Id, thing2Id, thing3Id, thing4Id)))
                .fire();
    }

    @Test
    public void queryByInNull() {
        search(attribute(NULL_ATTR).in(ONLY_NULL))
                .expectingBody(isEqualTo(toThingResult(thing1Id, thing3Id, thing4Id)))
                .fire();
    }

    /**
     * The null-or-missing leg of in() combined with a point predicate on another attribute.
     */
    @Test
    public void queryByInStringAndNullCombinedWithEqString() {
        search(and(attribute(NULL_ATTR).in(NON_NULL_OR_NULL), attribute(STRING_ATTR).eq("c")))
                .expectingBody(isEqualTo(toThingResult(thing3Id)))
                .fire();
    }

    /**
     * Negated in() with a value and a null operand: every thing holds the value, null or no attribute at all, so
     * nothing is left.
     */
    @Test
    public void queryByNotInStringAndNull() {
        search(not(attribute(NULL_ATTR).in(NON_NULL_OR_NULL)))
                .expectingBody(isEmpty())
                .fire();
    }

    @Test
    public void queryByGeNull() {
        search(nullComparison("ge", attribute(NULL_ATTR)))
                .expectingBody(isEqualTo(toThingResult(thing1Id, thing3Id, thing4Id)))
                .fire();
    }

    @Test
    public void queryByLeNull() {
        search(nullComparison("le", attribute(NULL_ATTR)))
                .expectingBody(isEqualTo(toThingResult(thing1Id, thing3Id, thing4Id)))
                .fire();
    }

    @Test
    public void queryByGtNull() {
        search(nullComparison("gt", attribute(NULL_ATTR)))
                .expectingBody(isEmpty())
                .fire();
    }

    @Test
    public void queryByLtNull() {
        search(nullComparison("lt", attribute(NULL_ATTR)))
                .expectingBody(isEmpty())
                .fire();
    }

    @Test
    public void queryByNotGtNull() {
        search("not(" + nullComparison("gt", attribute(NULL_ATTR)) + ")")
                .expectingBody(isEqualTo(toThingResult(thing1Id, thing2Id, thing3Id, thing4Id)))
                .fire();
    }

    @Test
    public void queryByNotLtNull() {
        search("not(" + nullComparison("lt", attribute(NULL_ATTR)) + ")")
                .expectingBody(isEqualTo(toThingResult(thing1Id, thing2Id, thing3Id, thing4Id)))
                .fire();
    }

    /**
     * Renders {@code <operator>(<path>,null)} as raw RQL: {@link SearchProperty#ge(String)}, {@code gt}, {@code le}
     * and {@code lt} reject a {@code null} value, so the filter cannot be built with the search model.
     */
    private static String nullComparison(final String operator, final SearchProperty property) {
        return operator + "(" + property.getPath() + ",null)";
    }

    private SearchMatcher search(final SearchFilter searchFilter) {
        return search(searchFilter, apiVersion);
    }

    private SearchMatcher search(final String rqlFilter) {
        return searchThings(apiVersion)
                .filter("and(" + rqlFilter + "," + THING_ID_FILTER + ")")
                .option(newSortOption(SortProperties.thingId(), SortOptionEntry.SortOrder.ASC));
    }

    private static SearchMatcher search(final SearchFilter searchFilter, final JsonSchemaVersion apiVersion) {
        return searchThings(apiVersion)
                .filter(and(searchFilter, THING_ID_FILTER))
                .option(newSortOption(SortProperties.thingId(), SortOptionEntry.SortOrder.ASC));
    }

}
