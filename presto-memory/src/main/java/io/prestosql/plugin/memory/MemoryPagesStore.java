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
import io.prestosql.spi.Page;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.OptionalDouble;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.IntStream;

import static io.prestosql.plugin.memory.MemoryColumnHandle.ROW_ID_COLUMN;
import static io.prestosql.plugin.memory.MemoryErrorCode.MEMORY_LIMIT_EXCEEDED;
import static io.prestosql.plugin.memory.MemoryErrorCode.MISSING_DATA;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static java.lang.String.format;

@ThreadSafe
public class MemoryPagesStore
{
    private final long maxBytes;

    @GuardedBy("this")
    private long currentBytes;

    private final Map<Long, TableData> tables = new HashMap<>();

    @Inject
    public MemoryPagesStore(MemoryConfig config)
    {
        this.maxBytes = config.getMaxDataPerNode().toBytes();
    }

    public synchronized void initialize(long tableId)
    {
        if (!tables.containsKey(tableId)) {
            tables.put(tableId, new TableData());
        }
    }

    public synchronized void add(Long tableId, Page page)
    {
        if (!contains(tableId)) {
            throw new PrestoException(MISSING_DATA, "Failed to find table on a worker.");
        }

        page.compact();

        long newSize = currentBytes + page.getRetainedSizeInBytes();
        if (maxBytes < newSize) {
            throw new PrestoException(MEMORY_LIMIT_EXCEEDED, format("Memory limit [%d] for memory connector exceeded", maxBytes));
        }
        currentBytes = newSize;

        TableData tableData = tables.get(tableId);
        tableData.add(page);
    }

    public synchronized List<Page> getPages(
            Long tableId,
            int partNumber,
            int totalParts,
            List<Integer> columnIndexes,
            long expectedRows,
            OptionalLong limit,
            OptionalDouble sampleRatio)
    {
        if (!contains(tableId)) {
            throw new PrestoException(MISSING_DATA, "Failed to find table on a worker.");
        }
        TableData tableData = tables.get(tableId);
        if (tableData.getRows() < expectedRows) {
            throw new PrestoException(MISSING_DATA,
                    format("Expected to find [%s] rows on a worker, but found [%s].", expectedRows, tableData.getRows()));
        }

        ImmutableList.Builder<Page> partitionedPages = ImmutableList.builder();

        boolean done = false;
        long totalRows = 0;
        for (int i = partNumber; i < tableData.getPages().size() && !done; i += totalParts) {
            if (sampleRatio.isPresent() && ThreadLocalRandom.current().nextDouble() >= sampleRatio.getAsDouble()) {
                continue;
            }

            Page page = tableData.getPages().get(i);
            totalRows += page.getPositionCount();
            if (limit.isPresent() && totalRows > limit.getAsLong()) {
                page = page.getRegion(0, (int) (page.getPositionCount() - (totalRows - limit.getAsLong())));
                done = true;
            }
            partitionedPages.add(getColumns(page, columnIndexes, i));
        }

        return partitionedPages.build();
    }

    public synchronized boolean contains(Long tableId)
    {
        return tables.containsKey(tableId);
    }

    public synchronized void cleanUp(Set<Long> activeTableIds)
    {
        // We have to remember that there might be some race conditions when there are two tables created at once.
        // That can lead to a situation when MemoryPagesStore already knows about a newer second table on some worker
        // but cleanUp is triggered by insert from older first table, which MemoryTableHandle was created before
        // second table creation. Thus activeTableIds can have missing latest ids and we can only clean up tables
        // that:
        // - have smaller value then max(activeTableIds).
        // - are missing from activeTableIds set

        if (activeTableIds.isEmpty()) {
            // if activeTableIds is empty, we cannot determine latestTableId...
            return;
        }
        long latestTableId = Collections.max(activeTableIds);

        for (Iterator<Map.Entry<Long, TableData>> tableDataIterator = tables.entrySet().iterator(); tableDataIterator.hasNext(); ) {
            Map.Entry<Long, TableData> tablePagesEntry = tableDataIterator.next();
            Long tableId = tablePagesEntry.getKey();
            if (tableId < latestTableId && !activeTableIds.contains(tableId)) {
                for (Page removedPage : tablePagesEntry.getValue().getPages()) {
                    currentBytes -= removedPage.getRetainedSizeInBytes();
                }
                tableDataIterator.remove();
            }
        }
    }

    private static Page getColumns(Page page, List<Integer> columnIndexes, int pageNumber)
    {
        Block[] outputBlocks = new Block[columnIndexes.size()];
        int rowIdColumnIndex = ROW_ID_COLUMN.getColumnIndex();

        for (int i = 0; i < columnIndexes.size(); i++) {
            int columnIndex = columnIndexes.get(i);
            if (columnIndex == rowIdColumnIndex) {
                outputBlocks[i] = createRowIdBlock(pageNumber, page.getPositionCount());
            }
            else {
                outputBlocks[i] = page.getBlock(columnIndex);
            }
        }

        return new Page(page.getPositionCount(), outputBlocks);
    }

    private static Block createRowIdBlock(int pageNumber, int positionCount)
    {
        BlockBuilder builder = BIGINT.createBlockBuilder(null, positionCount);
        IntStream.range(0, positionCount)
                .forEach(v -> BIGINT.writeLong(builder, ((long) pageNumber) << 4 | v));
        return builder.build();
    }

    public void deletePages(long tableId, Map<Integer, BitSet> deletePageRowIds)
    {
        TableData tableData = tables.get(tableId);

        for (int pageNumber : deletePageRowIds.keySet()) {
            Page page = tableData.getPages().get(pageNumber);
            BitSet deleteRowIds = deletePageRowIds.get(pageNumber);
            if (deleteRowIds.cardinality() == page.getPositionCount()) {
                // delete all rows
                tableData.set(pageNumber, page.getRegion(0, 0));
                continue;
            }

            int[] positions = new int[page.getPositionCount() - deleteRowIds.cardinality()];

            int index = 0;
            int nextPosition = deleteRowIds.nextClearBit(0);
            while (nextPosition < page.getPositionCount()) {
                positions[index++] = nextPosition;
                nextPosition = deleteRowIds.nextClearBit(nextPosition + 1);
            }
            tableData.set(pageNumber, page.getPositions(positions, 0, positions.length));
        }
    }

    private static final class TableData
    {
        private final List<Page> pages = new ArrayList<>();
        private long rows;

        public void add(Page page)
        {
            pages.add(page);
            rows += page.getPositionCount();
        }

        private void set(int index, Page page)
        {
            Page oldPage = pages.set(index, page);
            if (oldPage != null) {
                rows -= oldPage.getPositionCount();
            }
            rows += page.getPositionCount();
        }

        private List<Page> getPages()
        {
            return pages;
        }

        private long getRows()
        {
            return rows;
        }
    }
}
