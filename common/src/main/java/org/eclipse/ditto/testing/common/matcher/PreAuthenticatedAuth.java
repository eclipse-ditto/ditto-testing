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
package org.eclipse.ditto.testing.common.matcher;

import static java.util.Objects.requireNonNull;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.concurrent.Immutable;

import org.eclipse.ditto.policies.model.Subject;

/**
 * A simple holder for pre-authenticated Authentication Subjects.
 */
@Immutable
public final class PreAuthenticatedAuth {

    private final Set<String> authorizationSubjectIds;

    private PreAuthenticatedAuth(final Collection<String> authorizationSubjectIds) {
        this.authorizationSubjectIds = Set.copyOf(requireNonNull(authorizationSubjectIds));
    }

    public static PreAuthenticatedAuth fromSubjectIds(final Collection<String> authorizationSubjectIds) {
        return new PreAuthenticatedAuth(authorizationSubjectIds);
    }

    public static PreAuthenticatedAuth fromSubjects(final Collection<Subject> authorizationSubjects) {
        requireNonNull(authorizationSubjects);

        final Set<String> subjectIds =  authorizationSubjects.stream()
                .map(subject -> subject.getId().toString())
                .collect(Collectors.toSet());

        return new PreAuthenticatedAuth(subjectIds);
    }

    public static PreAuthenticatedAuth fromSubjects(final Subject firstSubject, final Subject... furtherSubjects) {
        requireNonNull(firstSubject);

        final Set<Subject> subjects =  new HashSet<>();
        subjects.add(firstSubject);
        subjects.addAll(Arrays.asList(furtherSubjects));

        return fromSubjects(subjects);
    }

    public String toHeaderValue() {
        return String.join(",", authorizationSubjectIds);
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final PreAuthenticatedAuth that = (PreAuthenticatedAuth) o;
        return Objects.equals(authorizationSubjectIds, that.authorizationSubjectIds);
    }

    public Set<String> getAuthorizationSubjectIds() {
        return authorizationSubjectIds;
    }

    @Override
    public int hashCode() {
        return Objects.hash(authorizationSubjectIds);
    }

    @Override
    public String toString() {
        return toHeaderValue();
    }

}
