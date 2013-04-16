package org.elasticsearch.river.couchdb.kernel.index;

import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.BDDMockito.given;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.river.couchdb.IndexConfig;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

@RunWith(MockitoJUnitRunner.class)
public class ChangeCollectorTest {

    @Mock
    private BlockingQueue<String> changesStream;
    @Mock
    private ChangeProcessor changeProcessor;
    @Mock
    private IndexConfig indexConfig;

    @InjectMocks
    private ChangeCollector collector;

    @Mock
    private BulkRequestBuilder bulk;

    @Test
    public void shouldWaitForMoreChangesAndReturnSeqOfTheLastNonNullOne() throws Exception {
        // given 3 enqueued changes
        String change1 = "irrelevant #1", change2 = "irrelevant #2", change3 = "irrelevant #3";
        given(changesStream.take()).willReturn(change1);
        given(changesStream.poll(anyLong(), any(TimeUnit.class))).willReturn(change2, change3, null);

        // and given processing of those changes yields the following seq numbers
        String seq1 = "1", seq2 = null, seq3 = "3";
        given(changeProcessor.processChange(anyString(), any(BulkRequestBuilder.class))).willReturn(seq1, seq2, seq3);

        givenBulkSizeNotExceeded();

        // when
        Object seq = collector.collectAndProcessChanges(bulk);

        // then
        assertThat(seq).isEqualTo(seq3);
    }

    private void givenBulkSizeNotExceeded() {
        given(indexConfig.getBulkTimeout()).willReturn(mock(TimeValue.class));

        given(indexConfig.getBulkSize()).willReturn(100);
        given(bulk.numberOfActions()).willReturn(0);
    }
}
