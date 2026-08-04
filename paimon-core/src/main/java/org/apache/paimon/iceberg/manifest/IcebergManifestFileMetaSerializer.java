/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.iceberg.manifest;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericArray;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.iceberg.manifest.IcebergManifestFileMeta.Content;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.ObjectSerializer;

import java.util.ArrayList;
import java.util.List;

/** Serializer for {@link IcebergManifestFileMeta}. */
public class IcebergManifestFileMetaSerializer extends ObjectSerializer<IcebergManifestFileMeta> {

    private static final long serialVersionUID = 1L;

    private static final String FIRST_ROW_ID_FIELD = "first_row_id";

    private final IcebergPartitionSummarySerializer partitionSummarySerializer;
    // The legacy (Iceberg 1.4 / Athena) manifest list schema has no first_row_id column, so the
    // serializer must follow the schema it was created with.
    private final boolean hasFirstRowId;

    public IcebergManifestFileMetaSerializer(RowType schema) {
        super(schema);
        this.partitionSummarySerializer = new IcebergPartitionSummarySerializer();
        this.hasFirstRowId = schema.getFieldNames().contains(FIRST_ROW_ID_FIELD);
    }

    @Override
    public InternalRow toRow(IcebergManifestFileMeta file) {
        GenericRow row =
                GenericRow.of(
                        BinaryString.fromString(file.manifestPath()),
                        file.manifestLength(),
                        file.partitionSpecId(),
                        file.content().id(),
                        file.sequenceNumber(),
                        file.minSequenceNumber(),
                        file.addedSnapshotId(),
                        file.addedFilesCount(),
                        file.existingFilesCount(),
                        file.deletedFilesCount(),
                        file.addedRowsCount(),
                        file.existingRowsCount(),
                        file.deletedRowsCount(),
                        new GenericArray(
                                file.partitions().stream()
                                        .map(partitionSummarySerializer::toRow)
                                        .toArray(InternalRow[]::new)));
        if (!hasFirstRowId) {
            return row;
        }
        GenericRow rowWithFirstRowId = new GenericRow(row.getFieldCount() + 1);
        for (int i = 0; i < row.getFieldCount(); i++) {
            rowWithFirstRowId.setField(i, row.getField(i));
        }
        rowWithFirstRowId.setField(row.getFieldCount(), file.firstRowId());
        return rowWithFirstRowId;
    }

    @Override
    public IcebergManifestFileMeta fromRow(InternalRow row) {
        return new IcebergManifestFileMeta(
                row.getString(0).toString(),
                row.getLong(1),
                row.getInt(2),
                Content.fromId(row.getInt(3)),
                row.getLong(4),
                row.getLong(5),
                row.getLong(6),
                row.getInt(7),
                row.getInt(8),
                row.getInt(9),
                row.getLong(10),
                row.getLong(11),
                row.getLong(12),
                toPartitionSummaries(row.getArray(13)),
                hasFirstRowId && !row.isNullAt(14) ? row.getLong(14) : null);
    }

    private List<IcebergPartitionSummary> toPartitionSummaries(InternalArray array) {
        List<IcebergPartitionSummary> summaries = new ArrayList<>();
        for (int i = 0; i < array.size(); i++) {
            summaries.add(
                    partitionSummarySerializer.fromRow(
                            array.getRow(i, partitionSummarySerializer.numFields())));
        }
        return summaries;
    }
}
