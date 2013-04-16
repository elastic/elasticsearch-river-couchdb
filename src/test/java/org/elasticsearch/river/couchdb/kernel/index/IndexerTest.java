package org.elasticsearch.river.couchdb.kernel.index;

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
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class IndexerTest {

    @Mock
    private ChangeCollector changeCollector;
    @Mock
    private Client client;
    @Mock
    private LastSeqFormatter lastSeqFormatter;
    @Mock
    private RequestFactory requestFactory;
    @Mock
    private BulkRequestBuilder bulk;

    @InjectMocks
    private Indexer indexer;

    @Before
    public void initMocks() {
        given(client.prepareBulk()).willReturn(bulk);

        given(lastSeqFormatter.format(anyString())).willCallRealMethod();
    }

    @Test
    public void shouldIssueARequestToUpdateLastSeq() throws Exception {
        // given
        String seq = "1337";
        givenReceivedChangeYieldsLastSeqEqualTo(seq);

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
        givenReceivedChangeYieldsLastSeqEqualTo(null);

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

    private void givenReceivedChangeYieldsLastSeqEqualTo(String seq) throws Exception {
        given(changeCollector.collectAndProcessChanges(bulk)).willReturn(seq);
    }

    @SuppressWarnings("unchecked")
    private void givenBulkExecuted() {
        ListenableActionFuture future = mock(ListenableActionFuture.class);
        given(bulk.execute()).willReturn(future);
        BulkResponse bulkResponse = mock(BulkResponse.class);
        given(future.actionGet()).willReturn(bulkResponse);
    }
}
