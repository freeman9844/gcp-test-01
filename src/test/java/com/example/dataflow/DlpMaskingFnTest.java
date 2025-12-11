package com.example.dataflow;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import com.google.api.services.bigquery.model.TableRow;
import com.google.privacy.dlp.v2.ContentItem;
import com.google.privacy.dlp.v2.DeidentifyContentRequest;
import com.google.privacy.dlp.v2.DeidentifyContentResponse;
import com.google.privacy.dlp.v2.FieldId;
import com.google.privacy.dlp.v2.Table;
import com.google.privacy.dlp.v2.Value;

public class DlpMaskingFnTest {

    private DlpMaskingFn dlpMaskingFn;
    private DoFn.ProcessContext mockContext;
    private TupleTag<TableRow> successTag;
    private TupleTag<TableRow> deadLetterTag;

    @BeforeEach
    public void setup() {
        successTag = new TupleTag<>() {
        };
        deadLetterTag = new TupleTag<>() {
        };

        // Subclass to override the actual API call
        dlpMaskingFn = spy(new DlpMaskingFn("test-project", null, null, successTag, deadLetterTag));
        mockContext = mock(DoFn.ProcessContext.class);
    }

    @Test
    public void testProcessElement_masksData() throws IOException {
        // Input data
        TableRow row1 = new TableRow().set("name", "John").set("rrn", "123456-1234567");
        List<TableRow> rows = Collections.singletonList(row1);
        KV<String, Iterable<TableRow>> input = KV.of("key", rows);

        when(mockContext.element()).thenReturn(input);

        // Mock DLP response
        Table maskedTable = Table.newBuilder()
                .addHeaders(FieldId.newBuilder().setName("name").build())
                .addHeaders(FieldId.newBuilder().setName("rrn").build())
                .addRows(Table.Row.newBuilder()
                        .addValues(Value.newBuilder().setStringValue("John").build())
                        .addValues(Value.newBuilder().setStringValue("123456-*******").build())
                        .build())
                .build();
        DeidentifyContentResponse response = DeidentifyContentResponse.newBuilder()
                .setItem(ContentItem.newBuilder().setTable(maskedTable).build())
                .build();

        doReturn(response).when(dlpMaskingFn).deidentifyContent(any(DeidentifyContentRequest.class));

        // Run
        dlpMaskingFn.processElement(mockContext);

        // Verify
        ArgumentCaptor<TableRow> captor = ArgumentCaptor.forClass(TableRow.class);
        verify(mockContext).output(any(TupleTag.class), captor.capture());
        TableRow outputRow = captor.getValue();

        assertEquals("John", outputRow.get("name"));
        assertEquals("123456-*******", outputRow.get("rrn"));
    }

    @Test
    public void testProcessElement_masksPhoneNumber() throws IOException {
        // Input data
        TableRow row1 = new TableRow()
                .set("name", "Alice")
                .set("phone", "010-1234-5678");
        List<TableRow> rows = Collections.singletonList(row1);
        KV<String, Iterable<TableRow>> input = KV.of("key", rows);

        when(mockContext.element()).thenReturn(input);

        // Mock DLP response
        Table maskedTable = Table.newBuilder()
                .addHeaders(FieldId.newBuilder().setName("name").build())
                .addHeaders(FieldId.newBuilder().setName("phone").build())
                .addRows(Table.Row.newBuilder()
                        .addValues(Value.newBuilder().setStringValue("Alice").build())
                        .addValues(Value.newBuilder().setStringValue("010-1234-****").build()) // Masked last 4 digits
                        .build())
                .build();
        DeidentifyContentResponse response = DeidentifyContentResponse.newBuilder()
                .setItem(ContentItem.newBuilder().setTable(maskedTable).build())
                .build();

        doReturn(response).when(dlpMaskingFn).deidentifyContent(any(DeidentifyContentRequest.class));

        // Run
        dlpMaskingFn.processElement(mockContext);

        // Verify
        ArgumentCaptor<TableRow> captor = ArgumentCaptor.forClass(TableRow.class);
        verify(mockContext).output(any(TupleTag.class), captor.capture());
        TableRow outputRow = captor.getValue();

        assertEquals("Alice", outputRow.get("name"));
        assertEquals("010-1234-****", outputRow.get("phone"));
    }

    @Test
    public void testProcessElement_mixedTextdetection() throws IOException {
        // Input data from user issue
        String originalText = "일반적인 고객 등록 110204-4109716 이며 전화번호는 010-4993-4108 입니다.";
        TableRow row1 = new TableRow().set("description", originalText);
        List<TableRow> rows = Collections.singletonList(row1);
        KV<String, Iterable<TableRow>> input = KV.of("key", rows);

        when(mockContext.element()).thenReturn(input);

        // Mock DLP response
        Table maskedTable = Table.newBuilder()
                .addHeaders(FieldId.newBuilder().setName("description").build())
                .addRows(Table.Row.newBuilder()
                        .addValues(Value.newBuilder()
                                .setStringValue("일반적인 고객 등록 110204-******* 이며 전화번호는 010-4993-**** 입니다.").build())
                        .build())
                .build();
        DeidentifyContentResponse response = DeidentifyContentResponse.newBuilder()
                .setItem(ContentItem.newBuilder().setTable(maskedTable).build())
                .build();

        doReturn(response).when(dlpMaskingFn).deidentifyContent(any(DeidentifyContentRequest.class));

        // Run
        dlpMaskingFn.processElement(mockContext);

        // Verify
        ArgumentCaptor<TableRow> captor = ArgumentCaptor.forClass(TableRow.class);
        verify(mockContext).output(any(TupleTag.class), captor.capture());
        TableRow outputRow = captor.getValue();

        assertEquals("일반적인 고객 등록 110204-******* 이며 전화번호는 010-4993-**** 입니다.", outputRow.get("description"));
    }
}
