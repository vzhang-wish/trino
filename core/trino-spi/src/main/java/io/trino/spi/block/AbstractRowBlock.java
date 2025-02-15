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
package io.trino.spi.block;

import javax.annotation.Nullable;

import java.util.List;

import static io.trino.spi.block.BlockUtil.arraySame;
import static io.trino.spi.block.BlockUtil.checkArrayRange;
import static io.trino.spi.block.BlockUtil.checkValidPositions;
import static io.trino.spi.block.BlockUtil.checkValidRegion;
import static io.trino.spi.block.BlockUtil.compactArray;
import static io.trino.spi.block.BlockUtil.compactOffsets;
import static io.trino.spi.block.RowBlock.createRowBlockInternal;

public abstract class AbstractRowBlock
        implements Block
{
    protected final int numFields;

    @Override
    public final List<Block> getChildren()
    {
        return List.of(getRawFieldBlocks());
    }

    protected abstract Block[] getRawFieldBlocks();

    @Nullable
    protected abstract int[] getFieldBlockOffsets();

    protected abstract int getOffsetBase();

    /**
     * @return the underlying rowIsNull array, or null when all rows are guaranteed to be non-null
     */
    @Nullable
    protected abstract boolean[] getRowIsNull();

    // the offset in each field block, it can also be viewed as the "entry-based" offset in the RowBlock
    protected final int getFieldBlockOffset(int position)
    {
        int[] offsets = getFieldBlockOffsets();
        return offsets != null ? offsets[position + getOffsetBase()] : position + getOffsetBase();
    }

    protected AbstractRowBlock(int numFields)
    {
        if (numFields <= 0) {
            throw new IllegalArgumentException("Number of fields in RowBlock must be positive");
        }
        this.numFields = numFields;
    }

    @Override
    public String getEncodingName()
    {
        return RowBlockEncoding.NAME;
    }

    @Override
    public final Block copyPositions(int[] positions, int offset, int length)
    {
        checkArrayRange(positions, offset, length);

        int[] newOffsets = null;

        int[] fieldBlockPositions = new int[length];
        int fieldBlockPositionCount;
        boolean[] newRowIsNull;
        if (getRowIsNull() == null) {
            // No nulls are present
            newRowIsNull = null;
            for (int i = 0; i < fieldBlockPositions.length; i++) {
                int position = positions[offset + i];
                checkReadablePosition(position);
                fieldBlockPositions[i] = getFieldBlockOffset(position);
            }
            fieldBlockPositionCount = fieldBlockPositions.length;
        }
        else {
            newRowIsNull = new boolean[length];
            newOffsets = new int[length + 1];
            fieldBlockPositionCount = 0;
            for (int i = 0; i < length; i++) {
                newOffsets[i] = fieldBlockPositionCount;
                int position = positions[offset + i];
                if (isNull(position)) {
                    newRowIsNull[i] = true;
                }
                else {
                    fieldBlockPositions[fieldBlockPositionCount++] = getFieldBlockOffset(position);
                }
            }
            // Record last offset position
            newOffsets[length] = fieldBlockPositionCount;
            if (fieldBlockPositionCount == length) {
                // No nulls encountered, discard the null mask and offsets
                newRowIsNull = null;
                newOffsets = null;
            }
        }

        Block[] newBlocks = new Block[numFields];
        Block[] rawBlocks = getRawFieldBlocks();
        for (int i = 0; i < newBlocks.length; i++) {
            newBlocks[i] = rawBlocks[i].copyPositions(fieldBlockPositions, 0, fieldBlockPositionCount);
        }
        return createRowBlockInternal(0, length, newRowIsNull, newOffsets, newBlocks);
    }

    @Override
    public Block getRegion(int position, int length)
    {
        int positionCount = getPositionCount();
        checkValidRegion(positionCount, position, length);

        return createRowBlockInternal(position + getOffsetBase(), length, getRowIsNull(), getFieldBlockOffsets(), getRawFieldBlocks());
    }

    @Override
    public long getRegionSizeInBytes(int position, int length)
    {
        int positionCount = getPositionCount();
        checkValidRegion(positionCount, position, length);

        int startFieldBlockOffset = getFieldBlockOffset(position);
        int endFieldBlockOffset = getFieldBlockOffset(position + length);
        int fieldBlockLength = endFieldBlockOffset - startFieldBlockOffset;

        long regionSizeInBytes = (Integer.BYTES + Byte.BYTES) * (long) length;
        for (int i = 0; i < numFields; i++) {
            regionSizeInBytes += getRawFieldBlocks()[i].getRegionSizeInBytes(startFieldBlockOffset, fieldBlockLength);
        }
        return regionSizeInBytes;
    }

    @Override
    public long getPositionsSizeInBytes(boolean[] positions)
    {
        checkValidPositions(positions, getPositionCount());

        int usedPositionCount = 0;
        boolean[] fieldPositions = new boolean[getRawFieldBlocks()[0].getPositionCount()];
        for (int i = 0; i < positions.length; i++) {
            if (positions[i]) {
                usedPositionCount++;
                int startFieldBlockOffset = getFieldBlockOffset(i);
                int endFieldBlockOffset = getFieldBlockOffset(i + 1);
                for (int j = startFieldBlockOffset; j < endFieldBlockOffset; j++) {
                    fieldPositions[j] = true;
                }
            }
        }
        long sizeInBytes = 0;
        for (int j = 0; j < numFields; j++) {
            sizeInBytes += getRawFieldBlocks()[j].getPositionsSizeInBytes(fieldPositions);
        }
        return sizeInBytes + (Integer.BYTES + Byte.BYTES) * (long) usedPositionCount;
    }

    @Override
    public Block copyRegion(int position, int length)
    {
        int positionCount = getPositionCount();
        checkValidRegion(positionCount, position, length);

        int startFieldBlockOffset = getFieldBlockOffset(position);
        int endFieldBlockOffset = getFieldBlockOffset(position + length);
        int fieldBlockLength = endFieldBlockOffset - startFieldBlockOffset;
        Block[] newBlocks = new Block[numFields];
        for (int i = 0; i < numFields; i++) {
            newBlocks[i] = getRawFieldBlocks()[i].copyRegion(startFieldBlockOffset, fieldBlockLength);
        }
        int[] fieldBlockOffsets = getFieldBlockOffsets();
        int[] newOffsets = fieldBlockOffsets == null ? null : compactOffsets(fieldBlockOffsets, position + getOffsetBase(), length);
        boolean[] rowIsNull = getRowIsNull();
        boolean[] newRowIsNull = rowIsNull == null ? null : compactArray(rowIsNull, position + getOffsetBase(), length);

        if (arraySame(newBlocks, getRawFieldBlocks()) && newOffsets == fieldBlockOffsets && newRowIsNull == rowIsNull) {
            return this;
        }
        return createRowBlockInternal(0, length, newRowIsNull, newOffsets, newBlocks);
    }

    @Override
    public <T> T getObject(int position, Class<T> clazz)
    {
        if (clazz != Block.class) {
            throw new IllegalArgumentException("clazz must be Block.class");
        }
        checkReadablePosition(position);

        return clazz.cast(new SingleRowBlock(getFieldBlockOffset(position), getRawFieldBlocks()));
    }

    @Override
    public Block getSingleValueBlock(int position)
    {
        checkReadablePosition(position);

        int startFieldBlockOffset = getFieldBlockOffset(position);
        int endFieldBlockOffset = getFieldBlockOffset(position + 1);
        int fieldBlockLength = endFieldBlockOffset - startFieldBlockOffset;
        Block[] newBlocks = new Block[numFields];
        for (int i = 0; i < numFields; i++) {
            newBlocks[i] = getRawFieldBlocks()[i].copyRegion(startFieldBlockOffset, fieldBlockLength);
        }
        boolean[] newRowIsNull = isNull(position) ? new boolean[] {true} : null;
        int[] newOffsets = isNull(position) ? new int[] {0, fieldBlockLength} : null;

        return createRowBlockInternal(0, 1, newRowIsNull, newOffsets, newBlocks);
    }

    @Override
    public long getEstimatedDataSizeForStats(int position)
    {
        checkReadablePosition(position);

        if (isNull(position)) {
            return 0;
        }

        Block[] rawFieldBlocks = getRawFieldBlocks();
        long size = 0;
        for (int i = 0; i < numFields; i++) {
            size += rawFieldBlocks[i].getEstimatedDataSizeForStats(getFieldBlockOffset(position));
        }
        return size;
    }

    @Override
    public boolean isNull(int position)
    {
        checkReadablePosition(position);
        boolean[] rowIsNull = getRowIsNull();
        return rowIsNull != null && rowIsNull[position + getOffsetBase()];
    }

    private void checkReadablePosition(int position)
    {
        if (position < 0 || position >= getPositionCount()) {
            throw new IllegalArgumentException("position is not valid");
        }
    }
}
