package com.firebolt.jdbc.client.query;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

import java.util.Collections;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.firebolt.jdbc.connection.settings.FireboltProperties;
import com.firebolt.jdbc.statement.StatementInfoWrapper;

@ExtendWith(MockitoExtension.class)
public class QueryLabelResolverTest {

    @Mock
    private FireboltProperties mockFireboltProperties;

    @Mock
    private StatementInfoWrapper mockStatementInfoWrapper;

    @BeforeEach
    public void setupMethod() {
        lenient().when(mockStatementInfoWrapper.getLabel()).thenReturn("label from statement");
    }

    @Test
    public void willResolveQueryFromSessionWhenPresent() {
        when(mockFireboltProperties.getRuntimeAdditionalProperties()).thenReturn(Map.of("query_label", "label from connection"));
        assertEquals("label from connection", QueryLabelResolver.getQueryLabel(mockFireboltProperties, mockStatementInfoWrapper));
        verify(mockStatementInfoWrapper, never()).getLabel();
    }

    @Test
    public void willResolveQueryFromStatementWhenNotPresentOnSessionProperties() {
        when(mockFireboltProperties.getRuntimeAdditionalProperties()).thenReturn(Collections.emptyMap());
        assertEquals("label from statement", QueryLabelResolver.getQueryLabel(mockFireboltProperties, mockStatementInfoWrapper));
    }

}
