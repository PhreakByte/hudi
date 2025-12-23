/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.hudi.reader;

import io.trino.plugin.hudi.util.SynthesizedColumnHandler;
import io.trino.spi.Page;
import io.trino.spi.connector.ConnectorPageSource;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestHudiTrinoReaderContext
{
    @Test
    public void testIteratorHappyPath()
    {
        ConnectorPageSource pageSource = mock(ConnectorPageSource.class);
        Page page = mock(Page.class);
        when(page.getPositionCount()).thenReturn(1);

        when(pageSource.isFinished()).thenReturn(false);
        when(pageSource.getNextPage()).thenReturn(page);

        SynthesizedColumnHandler synthesizedColumnHandler = mock(SynthesizedColumnHandler.class);
        HudiTrinoReaderContext context = new HudiTrinoReaderContext(
                pageSource,
                Collections.emptyList(),
                Collections.emptyList(),
                synthesizedColumnHandler);

        ClosableIterator<IndexedRecord> iterator = context.getFileRecordIterator(
                null, 0, 0, null, null, null);

        // Before hasNext(), currentPage is null. hasNext() loads the page.
        assertThat(iterator.hasNext()).isTrue();

        // Now currentPage is the mocked page, currentPosition is 0.
        // next() calls avroSerializer.serialize(currentPage, currentPosition).
        // Since we have empty column handles, serialize should return an empty record.

        IndexedRecord record = iterator.next();
        assertThat(record).isNotNull();

        // Now currentPosition is 1.
        // hasNext() checks currentPosition (1) >= page.getPositionCount() (1).
        // It tries to get next page.
        when(pageSource.isFinished()).thenReturn(true);
        when(pageSource.getNextPage()).thenReturn(null);

        assertThat(iterator.hasNext()).isFalse();
    }
}
