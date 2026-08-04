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

package org.apache.paimon.iceberg;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.FileSystemCatalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.format.FileFormat;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.iceberg.manifest.IcebergManifestFileMeta;
import org.apache.paimon.iceberg.manifest.IcebergManifestList;
import org.apache.paimon.iceberg.metadata.IcebergMetadata;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.TableCommitImpl;
import org.apache.paimon.table.sink.TableWriteImpl;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for the Iceberg format version 3 <code>first_row_id</code> field (id 520) of manifest list
 * entries. Strict v3 readers (e.g. Snowflake) reject manifest lists whose data manifests carry a
 * null <code>first_row_id</code>.
 */
public class IcebergManifestListFirstRowIdTest {

    @TempDir java.nio.file.Path tempDir;

    @Test
    public void testV3AssignsAndPreservesFirstRowIds() throws Exception {
        FileStoreTable table = createAppendTable(3);
        String commitUser = UUID.randomUUID().toString();
        TableWriteImpl<?> write = table.newWrite(commitUser);
        TableCommitImpl commit = table.newCommit(commitUser);

        write.write(GenericRow.of(1, 10));
        write.write(GenericRow.of(2, 20));
        commit.commit(1, write.prepareCommit(false, 1));

        IcebergMetadata metadata1 = readMetadata(table);
        List<IcebergManifestFileMeta> dataManifests1 = readDataManifests(table, metadata1);
        assertThat(dataManifests1).isNotEmpty();

        // fresh assignment: manifests get consecutive id ranges starting at the snapshot's
        // first-row-id, each advancing by added + existing row counts
        long expectedWatermark = 0;
        for (IcebergManifestFileMeta meta : dataManifests1) {
            assertThat(meta.firstRowId()).isEqualTo(expectedWatermark);
            expectedWatermark += meta.addedRowsCount() + meta.existingRowsCount();
        }
        assertThat(metadata1.currentSnapshot().firstRowId()).isEqualTo(0L);
        assertThat(metadata1.nextRowId()).isEqualTo(expectedWatermark);

        write.write(GenericRow.of(3, 30));
        commit.commit(2, write.prepareCommit(false, 2));

        IcebergMetadata metadata2 = readMetadata(table);
        List<IcebergManifestFileMeta> dataManifests2 = readDataManifests(table, metadata2);
        Map<String, Long> previousAssignment =
                dataManifests1.stream()
                        .collect(
                                Collectors.toMap(
                                        IcebergManifestFileMeta::manifestPath,
                                        IcebergManifestFileMeta::firstRowId));

        long newlyAssignedRows = 0;
        long minNewFirstRowId = Long.MAX_VALUE;
        for (IcebergManifestFileMeta meta : dataManifests2) {
            assertThat(meta.firstRowId()).isNotNull();
            Long previous = previousAssignment.get(meta.manifestPath());
            if (previous != null) {
                // carried-over manifests must keep the id range assigned earlier
                assertThat(meta.firstRowId()).isEqualTo(previous);
            } else {
                newlyAssignedRows += meta.addedRowsCount() + meta.existingRowsCount();
                minNewFirstRowId = Math.min(minNewFirstRowId, meta.firstRowId());
            }
        }
        assertThat(metadata2.currentSnapshot().firstRowId()).isEqualTo(metadata1.nextRowId());
        assertThat(minNewFirstRowId).isEqualTo(metadata1.nextRowId());
        assertThat(metadata2.nextRowId()).isEqualTo(metadata1.nextRowId() + newlyAssignedRows);

        write.close();
        commit.close();
    }

    @Test
    public void testV2LeavesFirstRowIdNull() throws Exception {
        FileStoreTable table = createAppendTable(2);
        String commitUser = UUID.randomUUID().toString();
        TableWriteImpl<?> write = table.newWrite(commitUser);
        TableCommitImpl commit = table.newCommit(commitUser);

        write.write(GenericRow.of(1, 10));
        commit.commit(1, write.prepareCommit(false, 1));

        IcebergMetadata metadata = readMetadata(table);
        for (IcebergManifestFileMeta meta : readDataManifests(table, metadata)) {
            assertThat(meta.firstRowId()).isNull();
        }

        write.close();
        commit.close();
    }

    @Test
    public void testReadManifestListWrittenWithoutFirstRowId() throws Exception {
        // Emulate the upgrade path: a manifest list written by an older Paimon (schema without
        // field 520) must read back with null firstRowId, so the next commit can assign one.
        FileStoreTable table = createAppendTable(3);
        String commitUser = UUID.randomUUID().toString();
        TableWriteImpl<?> write = table.newWrite(commitUser);
        TableCommitImpl commit = table.newCommit(commitUser);
        write.write(GenericRow.of(1, 10));
        commit.commit(1, write.prepareCommit(false, 1));
        write.close();
        commit.close();

        IcebergPathFactory pathFactory =
                new IcebergPathFactory(new Path(table.location(), "metadata"));
        IcebergMetadata metadata = readMetadata(table);
        IcebergManifestList currentList = IcebergManifestList.create(table, pathFactory);
        List<IcebergManifestFileMeta> metas =
                currentList.read(new Path(metadata.currentSnapshot().manifestList()).getName());

        RowType fullSchema = IcebergManifestFileMeta.schema(false);
        List<DataField> legacyFields =
                fullSchema.getFields().stream()
                        .filter(field -> !"first_row_id".equals(field.name()))
                        .collect(Collectors.toList());
        RowType oldSchema = new RowType(false, legacyFields);
        Options avroOptions = new Options();
        avroOptions.set(
                "avro.row-name-mapping",
                "org.apache.paimon.avro.generated.record:manifest_file,"
                        + "iceberg:true,"
                        + "manifest_file_partitions:r508,"
                        + "array_id_r508:508");
        IcebergManifestList oldList =
                new IcebergManifestList(
                        LocalFileIO.create(),
                        FileFormat.fromIdentifier("avro", avroOptions),
                        oldSchema,
                        "snappy",
                        pathFactory.manifestListFactory());

        String oldFileName =
                oldList.writeWithoutRolling(
                        metas.stream()
                                .map(meta -> meta.withFirstRowId(null))
                                .collect(Collectors.toList()));

        List<IcebergManifestFileMeta> readBack = currentList.read(oldFileName);
        assertThat(readBack).hasSameSizeAs(metas);
        for (IcebergManifestFileMeta meta : readBack) {
            assertThat(meta.firstRowId()).isNull();
        }

        // and a round trip through the current schema keeps the assigned value
        String newFileName = currentList.writeWithoutRolling(metas);
        assertThat(currentList.read(newFileName))
                .allSatisfy(meta -> assertThat(meta.firstRowId()).isNotNull());
    }

    private FileStoreTable createAppendTable(int formatVersion) throws Exception {
        LocalFileIO fileIO = LocalFileIO.create();
        Path path = new Path(tempDir.toString());

        RowType rowType =
                RowType.of(
                        new DataType[] {DataTypes.INT(), DataTypes.INT()}, new String[] {"k", "v"});
        Options options = new Options(new HashMap<>());
        options.set(CoreOptions.BUCKET, -1);
        options.set(
                IcebergOptions.METADATA_ICEBERG_STORAGE, IcebergOptions.StorageType.TABLE_LOCATION);
        options.set(CoreOptions.FILE_FORMAT, "avro");
        options.set(IcebergOptions.FORMAT_VERSION, formatVersion);
        Schema schema =
                new Schema(
                        rowType.getFields(),
                        Collections.emptyList(),
                        Collections.emptyList(),
                        options.toMap(),
                        "");

        try (FileSystemCatalog paimonCatalog = new FileSystemCatalog(fileIO, path)) {
            paimonCatalog.createDatabase("mydb", false);
            Identifier paimonIdentifier = Identifier.create("mydb", "t");
            paimonCatalog.createTable(paimonIdentifier, schema, false);
            return (FileStoreTable) paimonCatalog.getTable(paimonIdentifier);
        }
    }

    private IcebergMetadata readMetadata(FileStoreTable table) {
        long snapshotId = table.snapshotManager().latestSnapshotId();
        return IcebergMetadata.fromPath(
                table.fileIO(),
                new Path(table.location(), "metadata/v" + snapshotId + ".metadata.json"));
    }

    private List<IcebergManifestFileMeta> readDataManifests(
            FileStoreTable table, IcebergMetadata metadata) {
        IcebergPathFactory pathFactory =
                new IcebergPathFactory(new Path(table.location(), "metadata"));
        IcebergManifestList manifestList = IcebergManifestList.create(table, pathFactory);
        return manifestList.read(new Path(metadata.currentSnapshot().manifestList()).getName())
                .stream()
                .filter(meta -> meta.content() == IcebergManifestFileMeta.Content.DATA)
                .collect(Collectors.toList());
    }
}
