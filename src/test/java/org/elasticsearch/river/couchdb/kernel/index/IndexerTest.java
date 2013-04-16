package org.elasticsearch.river.couchdb.kernel.index;

import static org.elasticsearch.common.unit.TimeValue.timeValueMillis;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.BDDMockito.given;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import org.elasticsearch.action.ListenableActionFuture;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.base.Optional;
import org.elasticsearch.river.couchdb.IndexConfig;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import java.util.concurrent.BlockingQueue;

@RunWith(MockitoJUnitRunner.class)
public class IndexerTest {

    @Mock
    private BlockingQueue<String> changesStream;
    @Mock
    private Client client;
    @Mock
    private LastSeqFormatter lastSeqFormatter;
    @Mock
    private ChangeProcessor changeProcessor;
    @Mock
    private RequestFactory requestFactory;
    @Mock
    private IndexConfig indexConfig;
    @Mock
    private BulkRequestBuilder bulk;

    @InjectMocks
    private Indexer indexer;

    private String change = "{\"seq\":1337,\"id\":\"foo\",\"changes\":[{\"rev\":\"1-23202479633c2b380f79507a776743d5\"}]}";

    @Before
    public void initMocks() {
        given(client.prepareBulk()).willReturn(bulk);
        given(indexConfig.getBulkTimeout()).willReturn(timeValueMillis(10));

        given(lastSeqFormatter.format(anyString())).willCallRealMethod();
    }

    @Test
    public void shouldIssueARequestToUpdateLastSeq() throws Exception {
        // given
        givenReceivedChange();

        String seq = "1337";
        givenProcessedChangeYieldsLastSeqEqualTo(seq);

        // when
        Optional<String> indexedSeq = indexer.index();

        // then verify request added to bulk
        verify(bulk).add(any(IndexRequest.class));

        // and given
        given(bulk.numberOfActions()).willReturn(1);
        givenBulkExecuted();

        // then
        assertThat(indexedSeq.get()).isEqualTo(seq);
    }

    @Test
    public void shouldNotExecuteAnyRequestsIfLastSeqAbsent() throws Exception {
        // given
        givenReceivedChange();
        givenProcessedChangeYieldsLastSeqEqualTo(null);

        // when
        Optional<String> indexedSeq = indexer.index();

        // then
        verify(bulk, never()).add(any(IndexRequest.class));

        // and given
        given(bulk.numberOfActions()).willReturn(0);

        // then
        verify(bulk, never()).execute();
        assertThat(indexedSeq.isPresent()).isFalse();
    }

    private void givenReceivedChange() throws Exception {
        given(changesStream.take()).willReturn(change);
    }

    private void givenProcessedChangeYieldsLastSeqEqualTo(String seq) {
        given(changeProcessor.processChange(change, bulk)).willReturn(seq);
    }

    @SuppressWarnings("unchecked")
    private void givenBulkExecuted() {
        ListenableActionFuture future = mock(ListenableActionFuture.class);
        given(bulk.execute()).willReturn(future);
        BulkResponse bulkResponse = mock(BulkResponse.class);
        given(future.actionGet()).willReturn(bulkResponse);
    }
}
