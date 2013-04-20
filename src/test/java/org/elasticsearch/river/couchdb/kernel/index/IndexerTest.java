package org.elasticsearch.river.couchdb.kernel.index;

import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.BDDMockito.given;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.base.Optional;
import org.elasticsearch.river.couchdb.kernel.shared.ClientWrapper;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class IndexerTest {

    @Mock
    private ChangeCollector changeCollector;
    @Mock
    private ClientWrapper clientWrapper;
    @Mock
    private LastSeqFormatter lastSeqFormatter;
    @Mock
    private RequestFactory requestFactory;
    @Mock
    private BulkRequestBuilder bulk;

    private RetryHandler<IndexCommand> retryHandler = new RetryHandler<IndexCommand>();

    private Indexer indexer;

    @Before
    public void initMocks() {
        indexer = new Indexer("db", changeCollector, clientWrapper, lastSeqFormatter, requestFactory, retryHandler);

        given(clientWrapper.prepareBulkRequest()).willReturn(bulk);

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

    @SuppressWarnings("unchecked")
    @Test
    public void shouldRetryAFailedRequest() throws Exception {
        // given
        givenReceivedChangeYieldsLastSeqEqualTo("1337");

        given(clientWrapper.executeBulkRequest(bulk))
                .willThrow(BulkRequestException.class) // first attempt fails
                .willReturn(okBulkResponse()); // second attempt succeeds

        given(bulk.numberOfActions()).willReturn(1);

        // when
        indexer.singleIteration();

        // then
        assertThat(retryHandler.shouldRetryLastAttempt()).isTrue();

        // when
        indexer.singleIteration();

        // then
        assertThat(retryHandler.shouldRetryLastAttempt()).isFalse();
    }

    private BulkResponse okBulkResponse() {
        BulkResponse okBulkResponse = mock(BulkResponse.class);
        given(okBulkResponse.hasFailures()).willReturn(false);
        return okBulkResponse;
    }

    private void givenReceivedChangeYieldsLastSeqEqualTo(String seq) throws Exception {
        given(changeCollector.collectAndProcessChanges(bulk)).willReturn(seq);
    }
}
