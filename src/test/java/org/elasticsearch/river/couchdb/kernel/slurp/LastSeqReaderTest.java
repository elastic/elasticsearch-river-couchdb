package org.elasticsearch.river.couchdb.kernel.slurp;

import static org.elasticsearch.common.collect.Maps.newHashMap;
import static org.elasticsearch.river.couchdb.kernel.shared.Constants.LAST_SEQ;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.BDDMockito.given;
import static org.mockito.Matchers.anyString;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.common.base.Optional;
import org.elasticsearch.river.RiverName;
import org.elasticsearch.river.couchdb.CouchdbDatabaseConfig;
import org.elasticsearch.river.couchdb.RiverConfig;
import org.elasticsearch.river.couchdb.kernel.shared.ClientWrapper;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
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
    private ClientWrapper clientWrapper;

    @InjectMocks
    private LastSeqReader lastSeqReader;

    @Mock
    private GetResponse mockedLastSeqResponse;

    @Before
    public void initMocks() {
        given(riverConfig.getRiverName()).willReturn(new RiverName("type", "name"));
    }

    @Test
    public void shouldReadAnExistingLastSequence() throws Exception {
        // given
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
        givenNoLastSeqInIndex();

        // when
        Optional<String> result = lastSeqReader.readLastSequenceFromIndex();

        // then
        assertThat(result.isPresent()).isFalse();
    }

    private void givenLastSeqInIndex(String lastSeqInIndex) {
        given(dbConfig.getDatabase()).willReturn("database");

        given(clientWrapper.read(anyString(), anyString(), anyString())).willReturn(mockedLastSeqResponse);
        given(mockedLastSeqResponse.isExists()).willReturn(true);

        Map<String, Object> lastSeq = newHashMap();
        lastSeq.put(LAST_SEQ, lastSeqInIndex);
        Map<String, Object> source = newHashMap();
        source.put("database", lastSeq);
        given(mockedLastSeqResponse.getSourceAsMap()).willReturn(source);
    }

    private void givenNoLastSeqInIndex() {
        given(clientWrapper.read(anyString(), anyString(), anyString())).willReturn(mockedLastSeqResponse);
        given(mockedLastSeqResponse.isExists()).willReturn(false);
    }
}
