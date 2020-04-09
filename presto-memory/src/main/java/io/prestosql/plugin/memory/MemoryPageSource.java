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
package io.prestosql.plugin.memory;

import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.prestosql.spi.HostAddress;
import io.prestosql.spi.Page;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.connector.UpdatablePageSource;

import java.io.IOException;
import java.util.BitSet;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static io.prestosql.spi.type.BigintType.BIGINT;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.completedFuture;

public class MemoryPageSource
        implements UpdatablePageSource
{
    private final MemoryPagesStore pagesStore;
    private final HostAddress currentHostAddress;
    private final long tableId;
    private final Iterator<Page> pages;
    private final Map<Integer, BitSet> deleteRowIds = new HashMap<>();

    private long completedBytes;
    private long memoryUsageBytes;
    private boolean closed;

    public MemoryPageSource(MemoryPagesStore pagesStore, HostAddress currentHostAddress, long tableId, List<Page> pages)
    {
        this.pagesStore = requireNonNull(pagesStore, "pagesStore is null");
        this.currentHostAddress = requireNonNull(currentHostAddress, "currentHostAddress is null");
        this.tableId = tableId;
        this.pages = requireNonNull(pages, "pages is null").iterator();

        long memoryUsageBytes = 0;
        for (Page page : pages) {
            memoryUsageBytes += page.getRetainedSizeInBytes();
        }
        this.memoryUsageBytes = memoryUsageBytes;
    }

    @Override
    public long getCompletedBytes()
    {
        return completedBytes;
    }

    @Override
    public long getReadTimeNanos()
    {
        return 0;
    }

    @Override
    public boolean isFinished()
    {
        return closed || !pages.hasNext();
    }

    @Override
    public Page getNextPage()
    {
        if (isFinished()) {
            return null;
        }
        Page page = pages.next();
        completedBytes += page.getSizeInBytes();
        return page;
    }

    @Override
    public long getSystemMemoryUsage()
    {
        return memoryUsageBytes;
    }

    @Override
    public void close() throws IOException
    {
        closed = true;
    }

    @Override
    public void deleteRows(Block rowIds)
    {
        for (int i = 0; i < rowIds.getPositionCount(); i++) {
            long lowId = BIGINT.getLong(rowIds, i);
            int pageIndex = (int) (lowId >> 4);
            int pageRow = (int) (0xffff & lowId);

            deleteRowIds.compute(pageIndex, (index, set) -> {
                if (set == null) {
                    set = new BitSet();
                }
                set.set(pageRow);
                return set;
            });
        }
    }

    @Override
    public CompletableFuture<Collection<Slice>> finish()
    {
        pagesStore.deletePages(tableId, deleteRowIds);
        long deletedRows = deleteRowIds.values().stream()
                .mapToLong(BitSet::cardinality)
                .sum();
        return completedFuture(ImmutableList.of(new MemoryDataFragment(currentHostAddress, -deletedRows).toSlice()));
    }
}
