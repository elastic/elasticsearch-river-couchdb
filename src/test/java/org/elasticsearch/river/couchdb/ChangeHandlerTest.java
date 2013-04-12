package org.elasticsearch.river.couchdb;

import static org.mockito.Mockito.verify;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import java.util.concurrent.BlockingQueue;

@RunWith(MockitoJUnitRunner.class)
public class ChangeHandlerTest {

    @Mock
    private BlockingQueue<String> changesQueue;
    @InjectMocks
    private ChangeHandler changeHandler;

    @Test
    public void shouldPutAnIncomingChangeToBlockingQueue() throws Exception {
        // given
        String change = "yello world";
        // when
        changeHandler.handleChange(change);

        // then
        verify(changesQueue).put(change);
    }
}
