package ru.aasmc.tracing.collector.migrator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

import java.sql.SQLException;
import org.assertj.core.api.ThrowableAssert.ThrowingCallable;
import org.springframework.core.NestedExceptionUtils;

/** The SQLSTATE with which PostgreSQL rejected a statement, e.g. {@code 42501} (permission denied). */
final class SqlStates {

    static final String PERMISSION_DENIED = "42501";
    static final String CHECK_VIOLATION = "23514";

    private SqlStates() {}

    /** Runs {@code statement}, which must fail with an {@link SQLException}, and returns its SQLSTATE. */
    static String sqlStateOf(ThrowingCallable statement) {
        Throwable failure = catchThrowable(statement);
        assertThat(failure).as("the statement must fail").isNotNull();
        Throwable cause = NestedExceptionUtils.getMostSpecificCause(failure);
        assertThat(cause).isInstanceOf(SQLException.class);
        return ((SQLException) cause).getSQLState();
    }
}
