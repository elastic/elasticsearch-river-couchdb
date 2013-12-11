package org.elasticsearch.river.couchdb.kernel.slurp;

import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.BDDMockito.given;
import org.elasticsearch.common.base.Optional;
import org.elasticsearch.river.couchdb.CouchdbConnectionConfig;
import org.elasticsearch.river.couchdb.CouchdbDatabaseConfig;
import org.elasticsearch.river.couchdb.kernel.slurp.UrlBuilder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import java.net.URL;

@RunWith(MockitoJUnitRunner.class)
public class UrlBuilderTest {

    @Mock
    private CouchdbConnectionConfig connectionConfig;
    @Mock
    private CouchdbDatabaseConfig databaseConfig;
    @InjectMocks
    private UrlBuilder urlBuilder;

    @Test
    public void shouldBuildUrlWithNoLastSeq() throws Exception {
        // given
        givenConfigs();

        // when
        URL built = urlBuilder.build();

        // then
        String expectedUrl = "http://some.url/testdb/_changes?feed=continuous&include_docs=true&heartbeat=20000";
        assertThat(built).isEqualTo(new URL(expectedUrl));
    }

    @Test
    public void shouldBuildUrlWithLastSeq() throws Exception {
        // given
        givenConfigs();

        urlBuilder.withLastSeq(Optional.of("1337"));

        // when
        URL built = urlBuilder.build();

        // then
        String expectedUrl = "http://some.url/testdb/_changes?feed=continuous&include_docs=true&heartbeat=20000&since=1337";
        assertThat(built).isEqualTo(new URL(expectedUrl));
    }

    private void givenConfigs() throws Exception {
        given(connectionConfig.getUrl()).willReturn(new URL("http://some.url"));
        given(connectionConfig.getHeartbeatMillis()).willReturn(20000L);
        given(databaseConfig.getDatabase()).willReturn("testdb");
    }
}
