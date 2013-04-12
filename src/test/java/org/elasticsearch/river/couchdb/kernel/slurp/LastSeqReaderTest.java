package org.elasticsearch.river.couchdb.kernel.slurp;

import static org.elasticsearch.common.collect.Maps.newHashMap;
import static org.elasticsearch.river.couchdb.kernel.slurp.LastSeqReader.LAST_SEQ;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.BDDMockito.given;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.base.Optional;
import org.elasticsearch.river.couchdb.CouchdbDatabaseConfig;
import org.elasticsearch.river.couchdb.RiverConfig;
import org.elasticsearch.river.couchdb.kernel.slurp.LastSeqReader;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import java.util.Map;

@RunWith(MockitoJUnitRunner.class)
public class LastSeqReaderTest {

    @Mock
    private CouchdbDatabaseConfig dbConfig;
    @Mock
    private RiverConfig riverConfig;
    @Mock
    private Client client;

    @Mock
    private GetResponse mockedLastSeqResponse;

    @Test
    public void shouldReadAnExistingLastSequence() throws Exception {
        // given
        LastSeqReader lastSeqReader = partiallyMockedLastSeqReader();

        String lastSeqInIndex = "1337";
        givenLastSeqInIndex(lastSeqInIndex);

        // when
        Optional<String> result = lastSeqReader.readLastSequenceFromIndex();

        // then
        assertThat(result.isPresent()).isTrue();
        assertThat(result.get()).isEqualTo(lastSeqInIndex);
    }

    @Test
    public void shouldHandleNoLastSequence() throws Exception {
        // given
        LastSeqReader lastSeqReader = partiallyMockedLastSeqReader();

        givenNoLastSeqInIndex();

        // when
        Optional<String> result = lastSeqReader.readLastSequenceFromIndex();

        // then
        assertThat(result.isPresent()).isFalse();
    }

    public LastSeqReader partiallyMockedLastSeqReader() {
        return new LastSeqReader(dbConfig, riverConfig, client) {
            @Override
            void refreshIndex() {}

            @Override
            GetResponse doReadLastSeq() {
                return mockedLastSeqResponse;
            }
        };
    }

    private void givenLastSeqInIndex(String lastSeqInIndex) {
        given(dbConfig.getDatabase()).willReturn("database");

        given(mockedLastSeqResponse.isExists()).willReturn(true);

        Map<String, Object> lastSeq = newHashMap();
        lastSeq.put(LAST_SEQ, lastSeqInIndex);
        Map<String, Object> source = newHashMap();
        source.put("database", lastSeq);
        given(mockedLastSeqResponse.getSourceAsMap()).willReturn(source);
    }

    private void givenNoLastSeqInIndex() {
        given(mockedLastSeqResponse.isExists()).willReturn(false);
    }
}
