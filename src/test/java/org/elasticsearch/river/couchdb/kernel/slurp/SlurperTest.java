package org.elasticsearch.river.couchdb.kernel.slurp;

import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.verify;
import org.elasticsearch.common.base.Optional;
import org.elasticsearch.river.couchdb.kernel.slurp.CouchdbHttpClient;
import org.elasticsearch.river.couchdb.kernel.slurp.LastSeqReader;
import org.elasticsearch.river.couchdb.kernel.slurp.Slurper;
import org.elasticsearch.river.couchdb.kernel.slurp.UrlBuilder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import java.net.URL;

@RunWith(MockitoJUnitRunner.class)
public class SlurperTest {

    @Mock
    private LastSeqReader lastSeqReader;
    @Mock
    private UrlBuilder changesFeedUrlBuilder;
    @Mock
    private CouchdbHttpClient couchdbHttpClient;
    @InjectMocks
    private Slurper slurper;

    @Test
    public void shouldListenForChanges() throws Exception {
        // given
        Optional<String> lastSeq = Optional.of("1337");
        given(lastSeqReader.readLastSequenceFromIndex()).willReturn(lastSeq);

        URL url = new URL("http://doesnt.matter");
        given(changesFeedUrlBuilder.build()).willReturn(url);

        // when
        slurper.slurp();

        // then
        verify(changesFeedUrlBuilder).withLastSeq(lastSeq);
        verify(couchdbHttpClient).listenForChanges(url);
    }
}
