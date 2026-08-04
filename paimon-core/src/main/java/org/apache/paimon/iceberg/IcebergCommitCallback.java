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
import org.apache.paimon.Snapshot;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.GenericArray;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.factories.FactoryException;
import org.apache.paimon.factories.FactoryUtil;
import org.apache.paimon.fs.Path;
import org.apache.paimon.iceberg.manifest.IcebergConversions;
import org.apache.paimon.iceberg.manifest.IcebergDataFileMeta;
import org.apache.paimon.iceberg.manifest.IcebergManifestEntry;
import org.apache.paimon.iceberg.manifest.IcebergManifestFile;
import org.apache.paimon.iceberg.manifest.IcebergManifestFileMeta;
import org.apache.paimon.iceberg.manifest.IcebergManifestList;
import org.apache.paimon.iceberg.manifest.IcebergPartitionSummary;
import org.apache.paimon.iceberg.metadata.IcebergDataField;
import org.apache.paimon.iceberg.metadata.IcebergMetadata;
import org.apache.paimon.iceberg.metadata.IcebergPartitionField;
import org.apache.paimon.iceberg.metadata.IcebergPartitionSpec;
import org.apache.paimon.iceberg.metadata.IcebergRef;
import org.apache.paimon.iceberg.metadata.IcebergSchema;
import org.apache.paimon.iceberg.metadata.IcebergSnapshot;
import org.apache.paimon.iceberg.metadata.IcebergSnapshotSummary;
import org.apache.paimon.index.DeletionVectorMeta;
import org.apache.paimon.index.IndexFileHandler;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataFilePathFactory;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.manifest.ManifestCommittable;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.options.Options;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.CommitCallback;
import org.apache.paimon.table.sink.TagCallback;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.DeletionFile;
import org.apache.paimon.table.source.RawFile;
import org.apache.paimon.table.source.ScanMode;
import org.apache.paimon.table.source.snapshot.SnapshotReader;
import org.apache.paimon.tag.Tag;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.MultisetType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.DataFilePathFactories;
import org.apache.paimon.utils.FileStorePathFactory;
import org.apache.paimon.utils.ManifestReadThreadPool;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.Preconditions;
import org.apache.paimon.utils.SnapshotManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.apache.paimon.deletionvectors.DeletionVectorsIndexFile.DELETION_VECTORS_INDEX;

/**
 * A {@link CommitCallback} to create Iceberg compatible metadata, so Iceberg readers can read
 * Paimon's {@link RawFile}.
 */
public class IcebergCommitCallback implements CommitCallback, TagCallback {

    private static final Logger LOG = LoggerFactory.getLogger(IcebergCommitCallback.class);

    // see org.apache.iceberg.hadoop.Util
    private static final String VERSION_HINT_FILENAME = "version-hint.text";

    private static final String PUFFIN_FORMAT = "puffin";

    // Snapshot summary metric keys
    private static final String SNAPSHOT_SUMMARY_ADDED_DATA_FILES = "added-data-files";
    private static final String SNAPSHOT_SUMMARY_ADDED_RECORDS = "added-records";
    private static final String SNAPSHOT_SUMMARY_ADDED_FILES_SIZE = "added-files-size";
    private static final String SNAPSHOT_SUMMARY_DELETED_DATA_FILES = "deleted-data-files";
    private static final String SNAPSHOT_SUMMARY_DELETED_RECORDS = "deleted-records";
    private static final String SNAPSHOT_SUMMARY_REMOVED_FILES_SIZE = "removed-files-size";
    private static final String SNAPSHOT_SUMMARY_CHANGED_PARTITION_COUNT =
            "changed-partition-count";
    private static final String SNAPSHOT_SUMMARY_TOTAL_RECORDS = "total-records";
    private static final String SNAPSHOT_SUMMARY_TOTAL_DATA_FILES = "total-data-files";
    private static final String SNAPSHOT_SUMMARY_TOTAL_FILES_SIZE = "total-files-size";
    private static final String SNAPSHOT_SUMMARY_TOTAL_DELETE_FILES = "total-delete-files";
    private static final String SNAPSHOT_SUMMARY_TOTAL_POSITION_DELETES = "total-position-deletes";
    private static final String SNAPSHOT_SUMMARY_TOTAL_EQUALITY_DELETES = "total-equality-deletes";

    private final FileStoreTable table;
    private final String commitUser;

    private final IcebergPathFactory pathFactory;
    private final @Nullable IcebergMetadataCommitter metadataCommitter;

    private final FileStorePathFactory fileStorePathFactory;
    private final IcebergManifestFile manifestFile;
    private final IcebergManifestList manifestList;
    private final int formatVersion;

    private final IndexFileHandler indexFileHandler;
    private final boolean needAddDvToIceberg;
    private final boolean syncFullHistory;

    // -------------------------------------------------------------------------------------
    // Public interface
    // -------------------------------------------------------------------------------------

    public IcebergCommitCallback(FileStoreTable table, String commitUser) {
        this.table = table;
        this.commitUser = commitUser;

        IcebergOptions.StorageType storageType =
                table.coreOptions().toConfiguration().get(IcebergOptions.METADATA_ICEBERG_STORAGE);
        this.pathFactory = new IcebergPathFactory(catalogTableMetadataPath(table));

        IcebergMetadataCommitterFactory metadataCommitterFactory;
        try {
            metadataCommitterFactory =
                    FactoryUtil.discoverFactory(
                            IcebergCommitCallback.class.getClassLoader(),
                            IcebergMetadataCommitterFactory.class,
                            storageType.toString());
        } catch (FactoryException ignore) {
            metadataCommitterFactory = null;
        }
        this.metadataCommitter =
                metadataCommitterFactory == null ? null : metadataCommitterFactory.create(table);

        this.fileStorePathFactory = table.store().pathFactory();
        this.manifestFile = IcebergManifestFile.create(table, pathFactory);
        this.manifestList = IcebergManifestList.create(table, pathFactory);

        this.formatVersion =
                table.coreOptions().toConfiguration().get(IcebergOptions.FORMAT_VERSION);
        Preconditions.checkArgument(
                formatVersion == IcebergMetadata.FORMAT_VERSION_V2
                        || formatVersion == IcebergMetadata.FORMAT_VERSION_V3,
                "Unsupported iceberg format version! Only version 2 or version 3 is valid, but current version is ",
                formatVersion);

        this.indexFileHandler = table.store().newIndexFileHandler();
        this.needAddDvToIceberg = needAddDvToIceberg();
        this.syncFullHistory =
                table.coreOptions().toConfiguration().get(IcebergOptions.SYNC_FULL_HISTORY);
    }

    public static Path catalogTableMetadataPath(FileStoreTable table) {
        Path icebergDBPath = catalogDatabasePath(table);
        return new Path(icebergDBPath, String.format("%s/metadata", table.location().getName()));
    }

    public static Path catalogDatabasePath(FileStoreTable table) {
        Path dbPath = table.location().getParent();
        final String dbSuffix = ".db";

        IcebergOptions.StorageType storageType =
                table.coreOptions().toConfiguration().get(IcebergOptions.METADATA_ICEBERG_STORAGE);

        IcebergOptions.StorageLocation storageLocation =
                table.coreOptions()
                        .toConfiguration()
                        .getOptional(IcebergOptions.METADATA_ICEBERG_STORAGE_LOCATION)
                        .orElse(inferDefaultMetadataLocation(storageType));

        switch (storageLocation) {
            case TABLE_LOCATION:
                // Iceberg metadata is written beside the table, under the database's own location,
                // so no warehouse (<db>.db) layout is required. This lets the table register in any
                // catalog, including a database whose location is not a Paimon warehouse path (e.g.
                // an externally-provisioned / cross-account catalog database).
                return dbPath;
            case CATALOG_STORAGE:
                // Catalog-storage derives a warehouse-relative iceberg/<db>/ path by stripping the
                // ".db" suffix, so it only applies under the Paimon <db>.db warehouse layout.
                if (!dbPath.getName().endsWith(dbSuffix)) {
                    throw new UnsupportedOperationException(
                            String.format(
                                    "Storage type %s with catalog-location Iceberg metadata requires a "
                                            + "Paimon warehouse database (a <db>.db location); set "
                                            + "metadata.iceberg.storage-location=table-location for a "
                                            + "database with a non-warehouse location.",
                                    storageType.name()));
                }
                String dbName =
                        dbPath.getName()
                                .substring(0, dbPath.getName().length() - dbSuffix.length());
                return new Path(dbPath.getParent(), String.format("iceberg/%s/", dbName));
            default:
                throw new UnsupportedOperationException(
                        "Unknown storage location " + storageLocation.name());
        }
    }

    private static IcebergOptions.StorageLocation inferDefaultMetadataLocation(
            IcebergOptions.StorageType storageType) {
        switch (storageType) {
            case TABLE_LOCATION:
                return IcebergOptions.StorageLocation.TABLE_LOCATION;
            case HIVE_CATALOG:
            case HADOOP_CATALOG:
            case REST_CATALOG:
                return IcebergOptions.StorageLocation.CATALOG_STORAGE;
            default:
                throw new UnsupportedOperationException(
                        "Unknown storage type: " + storageType.name());
        }
    }

    @Override
    public void close() throws Exception {}

    @Override
    public void call(Context context) {
        createMetadata(
                context.snapshot,
                (removedFiles, addedFiles) ->
                        collectFileChanges(context.deltaFiles, removedFiles, addedFiles),
                context.indexFiles);
    }

    @Override
    public void retry(ManifestCommittable committable) {
        SnapshotManager snapshotManager = table.snapshotManager();
        Snapshot snapshot =
                snapshotManager
                        .findSnapshotsForIdentifiers(
                                commitUser, Collections.singletonList(committable.identifier()))
                        .stream()
                        .max(Comparator.comparingLong(Snapshot::id))
                        .orElseThrow(
                                () ->
                                        new RuntimeException(
                                                "There is no snapshot for commit user "
                                                        + commitUser
                                                        + " and identifier "
                                                        + committable.identifier()
                                                        + ". This is unexpected."));
        long snapshotId = snapshot.id();
        createMetadata(
                snapshot,
                (removedFiles, addedFiles) ->
                        collectFileChanges(snapshotId, removedFiles, addedFiles),
                indexFileHandler.scan(snapshot, DELETION_VECTORS_INDEX));
    }

    @Override
    public void setTable(FileStoreTable table) {
        // nothing to do
    }

    private void createMetadata(
            Snapshot snapshot,
            FileChangesCollector fileChangesCollector,
            List<IndexManifestEntry> indexFiles) {
        long snapshotId = snapshot.id();
        try {
            if (snapshotId == Snapshot.FIRST_SNAPSHOT_ID) {
                // If Iceberg metadata is stored separately in another directory, dropping the table
                // will not delete old Iceberg metadata. So we delete them here, when the table is
                // created again and the first snapshot is committed.
                table.fileIO().delete(pathFactory.metadataDirectory(), true);
            }

            if (table.fileIO().exists(pathFactory.toMetadataPath(snapshotId))) {
                return;
            }

            Path baseMetadataPath = pathFactory.toMetadataPath(snapshotId - 1);

            if (table.fileIO().exists(baseMetadataPath)) {
                createMetadataWithBase(
                        fileChangesCollector,
                        indexFiles.stream()
                                .filter(
                                        index ->
                                                index.indexFile()
                                                        .indexType()
                                                        .equals(DELETION_VECTORS_INDEX))
                                .collect(Collectors.toList()),
                        snapshot,
                        baseMetadataPath);
            } else {
                recreateMetadata(snapshotId);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Create Iceberg metadata when no usable base metadata exists: either the very first Iceberg
     * commit for this table, or a recovery after the previous metadata became unusable (format
     * version change, missing row lineage, Iceberg-layer commit failure).
     *
     * <p>By default only the current snapshot is exposed to Iceberg. With {@link
     * IcebergOptions#SYNC_FULL_HISTORY} the whole retained Paimon history is replayed instead, so
     * Iceberg readers keep time travel and tags (see <a
     * href="https://github.com/apache/paimon/issues/6107">apache/paimon#6107</a>).
     */
    private void recreateMetadata(long snapshotId) throws IOException {
        if (syncFullHistory) {
            rebuildFullHistory(snapshotId);
        } else {
            createMetadataWithoutBase(snapshotId);
        }
    }

    /**
     * Rebuild Iceberg metadata from every Paimon snapshot that is still retained, ending at {@code
     * currentSnapshotId}: create metadata afresh for the earliest retained snapshot, then replay
     * each following snapshot on top of its predecessor, exactly like live commits would have.
     * Schemas, tags and (for format version 3) the row-id space therefore accumulate consistently
     * across the whole replayed history.
     *
     * <p>Each replay step persists its metadata file, so an interrupted rebuild resumes from the
     * newest already-written metadata on the next commit. Intermediate steps skip the version hint
     * and the external catalog commit; only the final step publishes, so an external catalog sees a
     * single transition. Replayed snapshots keep their original Paimon commit timestamps and are
     * subject to the same retention policy ({@link CoreOptions#SNAPSHOT_NUM_RETAINED_MIN}, {@link
     * CoreOptions#SNAPSHOT_TIME_RETAINED}, ...) that live commits apply.
     */
    private void rebuildFullHistory(long currentSnapshotId) throws IOException {
        SnapshotManager snapshotManager = table.snapshotManager();
        Long earliest = snapshotManager.earliestSnapshotId();
        long startId = earliest == null ? currentSnapshotId : Math.min(earliest, currentSnapshotId);

        // Resume from the newest existing metadata below the current snapshot, if it is usable.
        // Anything older than the newest existing file is stale by definition: live commits only
        // ever read the immediately preceding metadata.
        long baseId = -1;
        for (long id = currentSnapshotId - 1; id >= startId; id--) {
            Path metadataPath = pathFactory.toMetadataPath(id);
            if (table.fileIO().exists(metadataPath)) {
                try {
                    IcebergMetadata metadata =
                            IcebergMetadata.fromPath(table.fileIO(), metadataPath);
                    if (isSameFormatVersion(metadata.formatVersion())
                            && (formatVersion < IcebergMetadata.FORMAT_VERSION_V3
                                    || metadata.nextRowId() != null)
                            && coversRetainedPrefix(metadata, id, startId)) {
                        baseId = id;
                    }
                } catch (Exception e) {
                    LOG.warn(
                            "Failed to read existing Iceberg metadata {}, rebuilding history from scratch",
                            metadataPath,
                            e);
                }
                break;
            }
        }

        long firstWithBase;
        if (baseId == -1) {
            // No usable base. Stale metadata files (e.g. from before a format version change) must
            // be removed from the replay range first: metadata is written with an atomic rename
            // which cannot overwrite, and a leftover file would silently become the base of the
            // next replay step. Clean their manifests before deleting the files themselves.
            expireAllBefore(currentSnapshotId);
            Iterator<Path> stale =
                    pathFactory
                            .getAllMetadataPathBefore(table.fileIO(), currentSnapshotId)
                            .iterator();
            while (stale.hasNext()) {
                table.fileIO().deleteQuietly(stale.next());
            }
            createMetadataWithoutBase(startId, startId != currentSnapshotId);
            firstWithBase = startId + 1;
        } else {
            firstWithBase = baseId + 1;
        }

        for (long id = firstWithBase; id <= currentSnapshotId; id++) {
            long snapshotId = id;
            Snapshot snapshot = snapshotManager.snapshot(snapshotId);
            createMetadataWithBase(
                    (removedFiles, addedFiles) ->
                            collectFileChanges(snapshotId, removedFiles, addedFiles),
                    indexFileHandler.scan(snapshot, DELETION_VECTORS_INDEX),
                    snapshot,
                    pathFactory.toMetadataPath(snapshotId - 1),
                    snapshotId != currentSnapshotId);
        }
    }

    /**
     * Whether a resume candidate for {@link #rebuildFullHistory(long)} really is the prefix of a
     * full-history replay. Metadata written while full-history sync was off (e.g. single-snapshot
     * metadata from a plain rebuild) also passes the format checks, but resuming from it would
     * silently drop the retained snapshots it does not contain. The candidate is only usable if its
     * history reaches back to the earliest retained snapshot, or if the newest snapshot it is
     * missing was already expirable under the snapshot retention policy (i.e. the gap is legitimate
     * retention trimming, not missing history).
     */
    private boolean coversRetainedPrefix(IcebergMetadata base, long baseSnapshotId, long startId) {
        if (base.snapshots().isEmpty()) {
            return false;
        }
        long oldestInBase =
                base.snapshots().stream().mapToLong(IcebergSnapshot::snapshotId).min().getAsLong();
        if (oldestInBase <= startId) {
            return true;
        }
        Snapshot newestMissing = table.snapshotManager().snapshot(oldestInBase - 1);
        return shouldExpire(newestMissing.id(), newestMissing.timeMillis(), baseSnapshotId);
    }

    // -------------------------------------------------------------------------------------
    // Create metadata afresh
    // -------------------------------------------------------------------------------------

    private void createMetadataWithoutBase(long snapshotId) throws IOException {
        createMetadataWithoutBase(snapshotId, false);
    }

    /**
     * @param intermediate whether this metadata is an intermediate step of a {@link
     *     #rebuildFullHistory(long)} replay; intermediate steps skip the version hint and the
     *     external catalog commit, which only the final step publishes.
     */
    private void createMetadataWithoutBase(long snapshotId, boolean intermediate)
            throws IOException {
        SnapshotReader snapshotReader = table.newSnapshotReader().withSnapshot(snapshotId);
        Snapshot paimonSnapshot = table.snapshotManager().snapshot(snapshotId);
        SchemaCache schemaCache = new SchemaCache();
        List<IcebergManifestEntry> dataFileEntries = new ArrayList<>();
        List<IcebergManifestEntry> dvFileEntries = new ArrayList<>();
        SummaryMetrics metrics = new SummaryMetrics();
        Set<BinaryRow> changedPartitions = new HashSet<>();

        List<DataSplit> filteredDataSplits =
                snapshotReader.read().dataSplits().stream()
                        .filter(DataSplit::rawConvertible)
                        .collect(Collectors.toList());
        for (DataSplit dataSplit : filteredDataSplits) {
            changedPartitions.add(dataSplit.partition());
            dataSplitToManifestEntries(
                    dataSplit, snapshotId, schemaCache, dataFileEntries, dvFileEntries);

            for (DataFileMeta paimonFileMeta : dataSplit.dataFiles()) {
                metrics.addedDataFiles++;
                metrics.addedRecords += paimonFileMeta.rowCount();
                metrics.addedFilesSize += paimonFileMeta.fileSize();
            }
        }

        List<IcebergManifestFileMeta> dataManifestFileMetas = new ArrayList<>();
        if (!dataFileEntries.isEmpty()) {
            dataManifestFileMetas.addAll(
                    manifestFile.rollingWrite(dataFileEntries.iterator(), snapshotId));
        }

        List<IcebergManifestFileMeta> dvManifestFileMetas = new ArrayList<>();
        if (!dvFileEntries.isEmpty()) {
            dvManifestFileMetas.addAll(
                    manifestFile.rollingWrite(
                            dvFileEntries.iterator(),
                            snapshotId,
                            IcebergManifestFileMeta.Content.DELETES));
        }

        List<IcebergManifestFileMeta> allManifestFileMetas = new ArrayList<>();
        allManifestFileMetas.addAll(dataManifestFileMetas);
        allManifestFileMetas.addAll(dvManifestFileMetas);

        metrics.changedPartitionCount = changedPartitions.size();
        metrics.totalDataFiles = metrics.addedDataFiles;
        metrics.deletedDataFiles = 0;
        metrics.deletedRecords = 0;
        metrics.deletedFilesSize = 0;
        metrics.totalRecords = metrics.addedRecords;
        metrics.totalFilesSize = metrics.addedFilesSize;
        long totalDeleteFiles = dvFileEntries.stream().filter(IcebergManifestEntry::isLive).count();
        long totalPositionDeleteRecords =
                dvFileEntries.stream()
                        .filter(IcebergManifestEntry::isLive)
                        .mapToLong(entry -> entry.file().recordCount())
                        .sum();
        metrics.totalDeleteFiles = totalDeleteFiles;
        metrics.totalPositionDeletes = totalPositionDeleteRecords;
        metrics.totalEqualityDeletes = 0;

        Long snapshotFirstRowId = computeSnapshotFirstRowId(0L);
        ManifestRowIdAssignment rowIdAssignment =
                assignManifestFirstRowIds(allManifestFileMetas, snapshotFirstRowId);
        allManifestFileMetas = rowIdAssignment.manifests;
        Long addedRows = snapshotFirstRowId == null ? null : rowIdAssignment.assignedRows;
        Long nextRowId =
                snapshotFirstRowId == null
                        ? null
                        : snapshotFirstRowId + rowIdAssignment.assignedRows;
        String manifestListFileName = manifestList.writeWithoutRolling(allManifestFileMetas);

        int schemaId = (int) schemaCache.getLatestSchemaId();
        IcebergSchema icebergSchema = schemaCache.get(schemaId);
        List<IcebergPartitionField> partitionFields =
                getPartitionFields(table.schema().partitionKeys(), icebergSchema);

        IcebergSnapshotSummary snapshotSummary =
                computeSnapshotSummary(
                        IcebergSnapshotSummary.APPEND.operation(), paimonSnapshot, metrics);

        IcebergSnapshot snapshot =
                new IcebergSnapshot(
                        snapshotId,
                        snapshotId,
                        snapshotId == Snapshot.FIRST_SNAPSHOT_ID ? null : (Long) (snapshotId - 1),
                        paimonSnapshot.timeMillis(),
                        snapshotSummary,
                        pathFactory.toManifestListPath(manifestListFileName).toString(),
                        schemaId,
                        snapshotFirstRowId,
                        addedRows);

        // Tags can only be included in Iceberg if they point to an Iceberg snapshot that
        // exists. Otherwise, an Iceberg client fails to parse the metadata and all reads fail.
        // This metadata contains exactly one snapshot, so only tags pointing at it are eligible;
        // that can happen when metadata is rebuilt for an existing snapshot (e.g. the start of a
        // full history replay, see https://github.com/apache/paimon/issues/6107).
        Map<String, IcebergRef> refs =
                table.tagManager().tags().entrySet().stream()
                        .filter(entry -> entry.getKey().id() == snapshotId)
                        .collect(
                                Collectors.toMap(
                                        entry -> entry.getValue().get(0),
                                        entry -> new IcebergRef(entry.getKey().id())));

        String tableUuid = UUID.randomUUID().toString();

        List<IcebergSchema> allSchemas =
                IntStream.rangeClosed(0, schemaId)
                        .mapToObj(schemaCache::get)
                        .collect(Collectors.toList());
        IcebergMetadata metadata =
                new IcebergMetadata(
                        formatVersion,
                        tableUuid,
                        table.location().toString(),
                        snapshotId,
                        icebergSchema.highestFieldId(),
                        allSchemas,
                        schemaId,
                        Collections.singletonList(new IcebergPartitionSpec(partitionFields)),
                        partitionFields.stream()
                                .mapToInt(IcebergPartitionField::fieldId)
                                .max()
                                .orElse(
                                        // not sure why, this is a result tested by hand
                                        IcebergPartitionField.FIRST_FIELD_ID - 1),
                        Collections.singletonList(snapshot),
                        (int) snapshotId,
                        nextRowId,
                        refs);

        Path metadataPath = pathFactory.toMetadataPath(snapshotId);
        table.fileIO().tryToWriteAtomic(metadataPath, metadata.toJson());
        if (!intermediate) {
            table.fileIO()
                    .overwriteFileUtf8(
                            new Path(pathFactory.metadataDirectory(), VERSION_HINT_FILENAME),
                            String.valueOf(snapshotId));
        }

        expireAllBefore(snapshotId);

        if (!intermediate && metadataCommitter != null) {
            switch (metadataCommitter.identifier()) {
                case "hive":
                    metadataCommitter.commitMetadata(metadataPath, null);
                    break;
                case "rest":
                    metadataCommitter.commitMetadata(metadata, null);
                    break;
                default:
                    throw new UnsupportedOperationException(
                            "Unsupported metadata committer: " + metadataCommitter.identifier());
            }
        }
    }

    private void dataSplitToManifestEntries(
            DataSplit dataSplit,
            long snapshotId,
            SchemaCache schemaCache,
            List<IcebergManifestEntry> dataFileEntries,
            List<IcebergManifestEntry> dvFileEntries) {
        List<RawFile> rawFiles = dataSplit.convertToRawFiles().get();

        for (int i = 0; i < dataSplit.dataFiles().size(); i++) {
            DataFileMeta paimonFileMeta = dataSplit.dataFiles().get(i);
            RawFile rawFile = rawFiles.get(i);
            IcebergDataFileMeta fileMeta =
                    IcebergDataFileMeta.create(
                            IcebergDataFileMeta.Content.DATA,
                            rawFile.path(),
                            rawFile.format(),
                            dataSplit.partition(),
                            rawFile.rowCount(),
                            rawFile.fileSize(),
                            schemaCache.get(paimonFileMeta.schemaId()),
                            paimonFileMeta.valueStats(),
                            paimonFileMeta.valueStatsCols());
            dataFileEntries.add(
                    new IcebergManifestEntry(
                            IcebergManifestEntry.Status.ADDED,
                            snapshotId,
                            snapshotId,
                            snapshotId,
                            fileMeta));

            if (needAddDvToIceberg
                    && dataSplit.deletionFiles().isPresent()
                    && dataSplit.deletionFiles().get().get(i) != null) {
                DeletionFile deletionFile = dataSplit.deletionFiles().get().get(i);

                // Iceberg will check the cardinality between deserialized dv and iceberg deletion
                // file, so if deletionFile.cardinality() is null, we should stop synchronizing all
                // dvs.
                Preconditions.checkState(
                        deletionFile.cardinality() != null,
                        "cardinality in DeletionFile is null, stop generating dv for iceberg. "
                                + "dataFile path is {}, deletionFile is {}",
                        rawFile.path(),
                        deletionFile);

                // We can not get the file size of the complete DV index file from the DeletionFile,
                // so we set 'fileSizeInBytes' to -1(default in iceberg)
                IcebergDataFileMeta deleteFileMeta =
                        IcebergDataFileMeta.createForDeleteFile(
                                IcebergDataFileMeta.Content.POSITION_DELETES,
                                deletionFile.path(),
                                PUFFIN_FORMAT,
                                dataSplit.partition(),
                                deletionFile.cardinality(),
                                -1,
                                rawFile.path(),
                                deletionFile.offset(),
                                deletionFile.length());

                dvFileEntries.add(
                        new IcebergManifestEntry(
                                IcebergManifestEntry.Status.ADDED,
                                snapshotId,
                                snapshotId,
                                snapshotId,
                                deleteFileMeta));
            }
        }
    }

    private List<IcebergPartitionField> getPartitionFields(
            List<String> partitionKeys, IcebergSchema icebergSchema) {
        Map<String, IcebergDataField> fields = new HashMap<>();
        for (IcebergDataField field : icebergSchema.fields()) {
            fields.put(field.name(), field);
        }

        List<IcebergPartitionField> result = new ArrayList<>();
        int fieldId = IcebergPartitionField.FIRST_FIELD_ID;
        for (String partitionKey : partitionKeys) {
            result.add(new IcebergPartitionField(fields.get(partitionKey), fieldId));
            fieldId++;
        }
        return result;
    }

    /** VARIANT is an Iceberg format-version-3 type; reject publishing it into v2 metadata. */
    static void checkVariantNotPublishable(RowType rowType) {
        Collection<String> variantFields = new LinkedHashSet<>();
        for (DataField field : rowType.getFields()) {
            collectVariantFields(field.name(), field.type(), variantFields);
        }
        Preconditions.checkArgument(
                variantFields.isEmpty(),
                "Columns %s use the VARIANT type, which requires Iceberg format version 3. "
                        + "Set 'metadata.iceberg.format-version' = '3' to publish this table.",
                variantFields);
    }

    private static void collectVariantFields(
            String path, DataType type, Collection<String> variantFields) {
        switch (type.getTypeRoot()) {
            case VARIANT:
                variantFields.add(path + ": " + type.asSQLString());
                break;
            case ARRAY:
                collectVariantFields(
                        path + ".element", ((ArrayType) type).getElementType(), variantFields);
                break;
            case MULTISET:
                collectVariantFields(
                        path + ".element", ((MultisetType) type).getElementType(), variantFields);
                break;
            case MAP:
                collectVariantFields(path + ".key", ((MapType) type).getKeyType(), variantFields);
                collectVariantFields(
                        path + ".value", ((MapType) type).getValueType(), variantFields);
                break;
            case ROW:
                for (DataField field : ((RowType) type).getFields()) {
                    collectVariantFields(path + "." + field.name(), field.type(), variantFields);
                }
                break;
            default:
                break;
        }
    }

    // -------------------------------------------------------------------------------------
    // Create metadata based on old ones
    // -------------------------------------------------------------------------------------

    private void createMetadataWithBase(
            FileChangesCollector fileChangesCollector,
            List<IndexManifestEntry> indexFiles,
            Snapshot snapshot,
            Path baseMetadataPath)
            throws IOException {
        createMetadataWithBase(fileChangesCollector, indexFiles, snapshot, baseMetadataPath, false);
    }

    /**
     * @param intermediate whether this metadata is an intermediate step of a {@link
     *     #rebuildFullHistory(long)} replay; intermediate steps skip the version hint and the
     *     external catalog commit, which only the final step publishes.
     */
    private void createMetadataWithBase(
            FileChangesCollector fileChangesCollector,
            List<IndexManifestEntry> indexFiles,
            Snapshot snapshot,
            Path baseMetadataPath,
            boolean intermediate)
            throws IOException {
        long snapshotId = snapshot.id();
        IcebergMetadata baseMetadata = IcebergMetadata.fromPath(table.fileIO(), baseMetadataPath);

        if (!isSameFormatVersion(baseMetadata.formatVersion())) {
            // we need to recreate iceberg metadata if format version changed
            recreateFromUnusableBase(snapshot.id(), intermediate);
            return;
        }

        if (formatVersion == IcebergMetadata.FORMAT_VERSION_V3
                && baseMetadata.nextRowId() == null) {
            // v3 base metadata written before Paimon emitted row lineage; recreate to self-heal
            recreateFromUnusableBase(snapshot.id(), intermediate);
            return;
        }

        List<IcebergManifestFileMeta> baseManifestFileMetas =
                manifestList.read(baseMetadata.currentSnapshot().manifestList());

        // base manifest file for data files
        List<IcebergManifestFileMeta> baseDataManifestFileMetas =
                baseManifestFileMetas.stream()
                        .filter(meta -> meta.content() == IcebergManifestFileMeta.Content.DATA)
                        .collect(Collectors.toList());

        // base manifest file for deletion vector index files
        List<IcebergManifestFileMeta> baseDVManifestFileMetas =
                baseManifestFileMetas.stream()
                        .filter(meta -> meta.content() == IcebergManifestFileMeta.Content.DELETES)
                        .collect(Collectors.toList());

        Map<String, Pair<BinaryRow, DataFileMeta>> removedFiles = new LinkedHashMap<>();
        Map<String, Pair<BinaryRow, DataFileMeta>> addedFiles = new LinkedHashMap<>();
        boolean isAddOnly = fileChangesCollector.collect(removedFiles, addedFiles);
        Set<BinaryRow> modifiedPartitionsSet =
                removedFiles.values().stream()
                        .map(Pair::getLeft)
                        .collect(Collectors.toCollection(LinkedHashSet::new));
        addedFiles.values().stream().map(Pair::getLeft).forEach(modifiedPartitionsSet::add);
        List<BinaryRow> modifiedPartitions = new ArrayList<>(modifiedPartitionsSet);

        // Note that this check may be different from `removedFiles.isEmpty()`,
        // because if a file's level is changed, it will first be removed and then added.
        // In this case, if `baseMetadata` already contains this file, we should not add a
        // duplicate.
        List<IcebergManifestFileMeta> newDataManifestFileMetas;
        String operation;
        if (isAddOnly) {
            // Fast case. We don't need to remove files from `baseMetadata`. We only need to append
            // new metadata files.
            newDataManifestFileMetas = new ArrayList<>(baseDataManifestFileMetas);
            newDataManifestFileMetas.addAll(
                    createNewlyAddedManifestFileMetas(addedFiles, snapshotId));
            operation = IcebergSnapshotSummary.APPEND.operation();
        } else {
            Pair<List<IcebergManifestFileMeta>, String> result =
                    createWithDeleteManifestFileMetas(
                            removedFiles,
                            addedFiles,
                            modifiedPartitions,
                            baseDataManifestFileMetas,
                            snapshotId,
                            snapshot.commitKind());
            newDataManifestFileMetas = result.getLeft();
            operation = result.getRight();
        }

        List<IcebergManifestFileMeta> newDVManifestFileMetas = new ArrayList<>();
        if (needAddDvToIceberg) {
            if (!indexFiles.isEmpty()) {
                // reconstruct the dv index
                newDVManifestFileMetas.addAll(createDvManifestFileMetas(snapshot));
            } else {
                // no new dv index, reuse the old one
                newDVManifestFileMetas.addAll(baseDVManifestFileMetas);
            }
        }

        // compact data manifest file if needed
        newDataManifestFileMetas = compactMetadataIfNeeded(newDataManifestFileMetas, snapshotId);

        SummaryMetrics metrics = new SummaryMetrics();
        metrics.addedDataFiles = addedFiles.size();
        metrics.addedRecords =
                addedFiles.values().stream().mapToLong(p -> p.getRight().rowCount()).sum();
        metrics.addedFilesSize =
                addedFiles.values().stream().mapToLong(p -> p.getRight().fileSize()).sum();
        metrics.deletedDataFiles = removedFiles.size();
        metrics.deletedRecords =
                removedFiles.values().stream().mapToLong(p -> p.getRight().rowCount()).sum();
        metrics.deletedFilesSize =
                removedFiles.values().stream().mapToLong(p -> p.getRight().fileSize()).sum();
        metrics.changedPartitionCount = modifiedPartitionsSet.size();

        IcebergSnapshot baseSnapshot = baseMetadata.currentSnapshot();

        Long previousTotalRecordsValue =
                getSummaryLong(baseSnapshot, SNAPSHOT_SUMMARY_TOTAL_RECORDS);
        long previousTotalRecords =
                previousTotalRecordsValue != null
                        ? previousTotalRecordsValue
                        : computeLiveRowCount(baseDataManifestFileMetas);
        metrics.totalRecords =
                Math.max(0, previousTotalRecords + metrics.addedRecords - metrics.deletedRecords);

        Long previousTotalDataFilesValue =
                getSummaryLong(baseSnapshot, SNAPSHOT_SUMMARY_TOTAL_DATA_FILES);
        long previousTotalDataFiles =
                previousTotalDataFilesValue != null
                        ? previousTotalDataFilesValue
                        : computeLiveFileCount(baseDataManifestFileMetas);
        metrics.totalDataFiles =
                Math.max(
                        0,
                        previousTotalDataFiles + metrics.addedDataFiles - metrics.deletedDataFiles);

        Long previousTotalFilesSizeValue =
                getSummaryLong(baseSnapshot, SNAPSHOT_SUMMARY_TOTAL_FILES_SIZE);
        long previousTotalFilesSize =
                previousTotalFilesSizeValue != null
                        ? previousTotalFilesSizeValue
                        : computeTotalFilesSizeFromManifests(baseDataManifestFileMetas);
        metrics.totalFilesSize =
                Math.max(
                        0,
                        previousTotalFilesSize + metrics.addedFilesSize - metrics.deletedFilesSize);

        metrics.totalDeleteFiles = computeLiveFileCount(newDVManifestFileMetas);
        metrics.totalPositionDeletes = computeLiveRowCount(newDVManifestFileMetas);
        metrics.totalEqualityDeletes = 0;

        Long snapshotFirstRowId =
                computeSnapshotFirstRowId(
                        baseMetadata.nextRowId() == null ? 0L : baseMetadata.nextRowId());

        ManifestRowIdAssignment rowIdAssignment =
                assignManifestFirstRowIds(
                        Stream.concat(
                                        newDataManifestFileMetas.stream(),
                                        newDVManifestFileMetas.stream())
                                .collect(Collectors.toList()),
                        snapshotFirstRowId);
        List<IcebergManifestFileMeta> newManifestFileMetasWithRowIds = rowIdAssignment.manifests;
        Long addedRows = snapshotFirstRowId == null ? null : rowIdAssignment.assignedRows;
        Long nextRowId =
                snapshotFirstRowId == null
                        ? null
                        : snapshotFirstRowId + rowIdAssignment.assignedRows;
        String manifestListFileName =
                manifestList.writeWithoutRolling(newManifestFileMetasWithRowIds);

        IcebergSnapshotSummary snapshotSummary =
                computeSnapshotSummary(operation, snapshot, metrics);

        // add new schemas if needed
        SchemaCache schemaCache = new SchemaCache();
        int schemaId = (int) schemaCache.getLatestSchemaId();
        IcebergSchema icebergSchema = schemaCache.get(schemaId);
        List<IcebergSchema> schemas = baseMetadata.schemas();
        if (baseMetadata.currentSchemaId() != schemaId) {
            Preconditions.checkArgument(
                    schemaId > baseMetadata.currentSchemaId(),
                    "currentSchemaId{%s} in paimon should be greater than currentSchemaId{%s} in base metadata.",
                    schemaId,
                    baseMetadata.currentSchemaId());
            schemas = new ArrayList<>(schemas);
            schemas.addAll(
                    IntStream.rangeClosed(baseMetadata.currentSchemaId() + 1, schemaId)
                            .mapToObj(schemaCache::get)
                            .collect(Collectors.toList()));
        }

        List<IcebergSnapshot> snapshots = new ArrayList<>(baseMetadata.snapshots());
        snapshots.add(
                new IcebergSnapshot(
                        snapshotId,
                        snapshotId,
                        snapshotId - 1,
                        snapshot.timeMillis(),
                        snapshotSummary,
                        pathFactory.toManifestListPath(manifestListFileName).toString(),
                        schemaId,
                        snapshotFirstRowId,
                        addedRows));

        // all snapshots in this list, except the last one, need to expire
        List<IcebergSnapshot> toExpireExceptLast = new ArrayList<>();
        for (int i = 0; i + 1 < snapshots.size(); i++) {
            toExpireExceptLast.add(snapshots.get(i));
            // commit callback is called before expire, so we cannot use current earliest snapshot
            // and have to check expire condition by ourselves
            if (!shouldExpire(snapshots.get(i), snapshotId)) {
                snapshots = snapshots.subList(i, snapshots.size());
                break;
            }
        }

        // Tags can only be included in Iceberg if they point to an Iceberg snapshot that
        // exists. Otherwise an Iceberg client fails to parse the metadata and all reads fail.
        Set<Long> snapshotIds =
                snapshots.stream().map(IcebergSnapshot::snapshotId).collect(Collectors.toSet());
        Map<String, IcebergRef> refs =
                table.tagManager().tags().entrySet().stream()
                        .filter(entry -> snapshotIds.contains(entry.getKey().id()))
                        .collect(
                                Collectors.toMap(
                                        entry -> entry.getValue().get(0),
                                        entry -> new IcebergRef(entry.getKey().id())));

        IcebergMetadata metadata =
                new IcebergMetadata(
                        baseMetadata.formatVersion(),
                        baseMetadata.tableUuid(),
                        baseMetadata.location(),
                        snapshotId,
                        icebergSchema.highestFieldId(),
                        schemas,
                        schemaId,
                        baseMetadata.partitionSpecs(),
                        baseMetadata.lastPartitionId(),
                        snapshots,
                        (int) snapshotId,
                        nextRowId,
                        refs);

        Path metadataPath = pathFactory.toMetadataPath(snapshotId);
        table.fileIO().tryToWriteAtomic(metadataPath, metadata.toJson());
        if (!intermediate) {
            table.fileIO()
                    .overwriteFileUtf8(
                            new Path(pathFactory.metadataDirectory(), VERSION_HINT_FILENAME),
                            String.valueOf(snapshotId));
        }

        deleteApplicableMetadataFiles(snapshotId);
        for (int i = 0; i + 1 < toExpireExceptLast.size(); i++) {
            expireManifestList(
                    new Path(toExpireExceptLast.get(i).manifestList()).getName(),
                    new Path(toExpireExceptLast.get(i + 1).manifestList()).getName());
        }

        if (!intermediate && metadataCommitter != null) {
            switch (metadataCommitter.identifier()) {
                case "hive":
                    metadataCommitter.commitMetadata(metadataPath, baseMetadataPath);
                    break;
                case "rest":
                    metadataCommitter.commitMetadata(metadata, baseMetadata);
                    break;
                default:
                    throw new UnsupportedOperationException(
                            "Unsupported metadata committer: " + metadataCommitter.identifier());
            }
        }
    }

    /**
     * Recreate metadata when the base metadata of a commit turned out to be unusable. At the head
     * of the history this honors {@link IcebergOptions#SYNC_FULL_HISTORY}; in the middle of a
     * {@link #rebuildFullHistory(long)} replay (where an unusable base should be impossible, since
     * the replay itself validates or writes every base) it falls back to single-snapshot metadata
     * instead of recursing into another replay.
     */
    private void recreateFromUnusableBase(long snapshotId, boolean intermediate)
            throws IOException {
        if (intermediate) {
            createMetadataWithoutBase(snapshotId, true);
        } else {
            recreateMetadata(snapshotId);
        }
    }

    private interface FileChangesCollector {
        boolean collect(
                Map<String, Pair<BinaryRow, DataFileMeta>> removedFiles,
                Map<String, Pair<BinaryRow, DataFileMeta>> addedFiles)
                throws IOException;
    }

    private boolean collectFileChanges(
            List<ManifestEntry> manifestEntries,
            Map<String, Pair<BinaryRow, DataFileMeta>> removedFiles,
            Map<String, Pair<BinaryRow, DataFileMeta>> addedFiles) {
        boolean isAddOnly = true;
        DataFilePathFactories factories = new DataFilePathFactories(fileStorePathFactory);
        for (ManifestEntry entry : manifestEntries) {
            DataFilePathFactory dataFilePathFactory =
                    factories.get(entry.partition(), entry.bucket());
            String path = dataFilePathFactory.toPath(entry).toString();
            switch (entry.kind()) {
                case ADD:
                    if (shouldAddFileToIceberg(entry.file())) {
                        removedFiles.remove(path);
                        addedFiles.put(path, Pair.of(entry.partition(), entry.file()));
                    }
                    break;
                case DELETE:
                    isAddOnly = false;
                    addedFiles.remove(path);
                    removedFiles.put(path, Pair.of(entry.partition(), entry.file()));
                    break;
                default:
                    throw new UnsupportedOperationException(
                            "Unknown ManifestEntry FileKind " + entry.kind());
            }
        }
        return isAddOnly;
    }

    private boolean collectFileChanges(
            long snapshotId,
            Map<String, Pair<BinaryRow, DataFileMeta>> removedFiles,
            Map<String, Pair<BinaryRow, DataFileMeta>> addedFiles) {
        return collectFileChanges(
                table.store()
                        .newScan()
                        .withKind(ScanMode.DELTA)
                        .withSnapshot(snapshotId)
                        .plan()
                        .files(),
                removedFiles,
                addedFiles);
    }

    private boolean shouldAddFileToIceberg(DataFileMeta meta) {
        if (table.primaryKeys().isEmpty()) {
            return true;
        } else {
            if (needAddDvToIceberg) {
                return meta.level() > 0;
            }
            int maxLevel = table.coreOptions().numLevels() - 1;
            return meta.level() == maxLevel;
        }
    }

    private List<IcebergManifestFileMeta> createNewlyAddedManifestFileMetas(
            Map<String, Pair<BinaryRow, DataFileMeta>> addedFiles, long currentSnapshotId)
            throws IOException {
        if (addedFiles.isEmpty()) {
            return Collections.emptyList();
        }

        SchemaCache schemaCache = new SchemaCache();
        return manifestFile.rollingWrite(
                addedFiles.entrySet().stream()
                        .map(
                                e -> {
                                    DataFileMeta paimonFileMeta = e.getValue().getRight();
                                    IcebergDataFileMeta icebergFileMeta =
                                            IcebergDataFileMeta.create(
                                                    IcebergDataFileMeta.Content.DATA,
                                                    e.getKey(),
                                                    paimonFileMeta.fileFormat(),
                                                    e.getValue().getLeft(),
                                                    paimonFileMeta.rowCount(),
                                                    paimonFileMeta.fileSize(),
                                                    schemaCache.get(paimonFileMeta.schemaId()),
                                                    paimonFileMeta.valueStats(),
                                                    paimonFileMeta.valueStatsCols());
                                    return new IcebergManifestEntry(
                                            IcebergManifestEntry.Status.ADDED,
                                            currentSnapshotId,
                                            currentSnapshotId,
                                            currentSnapshotId,
                                            icebergFileMeta);
                                })
                        .iterator(),
                currentSnapshotId);
    }

    private Pair<List<IcebergManifestFileMeta>, String> createWithDeleteManifestFileMetas(
            Map<String, Pair<BinaryRow, DataFileMeta>> removedFiles,
            Map<String, Pair<BinaryRow, DataFileMeta>> addedFiles,
            List<BinaryRow> modifiedPartitions,
            List<IcebergManifestFileMeta> baseManifestFileMetas,
            long currentSnapshotId,
            Snapshot.CommitKind commitKind)
            throws IOException {
        String operation = IcebergSnapshotSummary.APPEND.operation();
        List<IcebergManifestFileMeta> newManifestFileMetas = new ArrayList<>();

        RowType partitionType = table.schema().logicalPartitionType();
        PartitionPredicate predicate =
                PartitionPredicate.fromMultiple(partitionType, modifiedPartitions);

        for (IcebergManifestFileMeta fileMeta : baseManifestFileMetas) {
            // use partition predicate to only check modified partitions
            int numFields = partitionType.getFieldCount();
            GenericRow minValues = new GenericRow(numFields);
            GenericRow maxValues = new GenericRow(numFields);
            long[] nullCounts = new long[numFields];
            for (int i = 0; i < numFields; i++) {
                IcebergPartitionSummary summary = fileMeta.partitions().get(i);
                DataType fieldType = partitionType.getTypeAt(i);
                minValues.setField(
                        i, IcebergConversions.toPaimonObject(fieldType, summary.lowerBound()));
                maxValues.setField(
                        i, IcebergConversions.toPaimonObject(fieldType, summary.upperBound()));
                // IcebergPartitionSummary only has `containsNull` field and does not have the
                // exact number of nulls.
                nullCounts[i] = summary.containsNull() ? 1 : 0;
            }

            if (predicate == null
                    || predicate.test(
                            fileMeta.liveRowsCount(),
                            minValues,
                            maxValues,
                            new GenericArray(nullCounts))) {
                // check if any IcebergManifestEntry in this manifest file meta is removed
                List<IcebergManifestEntry> entries =
                        manifestFile.read(new Path(fileMeta.manifestPath()).getName());
                boolean canReuseFile = true;
                for (IcebergManifestEntry entry : entries) {
                    if (entry.isLive()) {
                        String path = entry.file().filePath();
                        if (addedFiles.containsKey(path)) {
                            // added file already exists (most probably due to level changes),
                            // remove it to not add a duplicate.
                            addedFiles.remove(path);
                        } else if (removedFiles.containsKey(path)) {
                            canReuseFile = false;
                        }
                    }
                }

                if (canReuseFile) {
                    // nothing is removed, use this file meta again
                    newManifestFileMetas.add(fileMeta);
                } else {
                    // some file is removed, rewrite this file meta
                    operation =
                            commitKind == Snapshot.CommitKind.COMPACT
                                    ? IcebergSnapshotSummary.REPLACE.operation()
                                    : IcebergSnapshotSummary.OVERWRITE.operation();
                    List<IcebergManifestEntry> sourceEntries =
                            materializeFirstRowIds(fileMeta, entries);
                    List<IcebergManifestEntry> newEntries = new ArrayList<>();
                    for (IcebergManifestEntry entry : sourceEntries) {
                        if (entry.isLive()) {
                            boolean removed = removedFiles.containsKey(entry.file().filePath());
                            newEntries.add(
                                    new IcebergManifestEntry(
                                            removed
                                                    ? IcebergManifestEntry.Status.DELETED
                                                    : IcebergManifestEntry.Status.EXISTING,
                                            // a deleted entry records the snapshot that
                                            // deleted the file, not the one that added it
                                            removed ? currentSnapshotId : entry.snapshotId(),
                                            entry.sequenceNumber(),
                                            entry.fileSequenceNumber(),
                                            entry.file()));
                        }
                    }
                    newManifestFileMetas.addAll(
                            manifestFile.rollingWrite(newEntries.iterator(), currentSnapshotId));
                }
            } else {
                // partition of this file meta is not modified in this snapshot,
                // use this file meta again
                newManifestFileMetas.add(fileMeta);
            }
        }

        newManifestFileMetas.addAll(
                createNewlyAddedManifestFileMetas(addedFiles, currentSnapshotId));
        return Pair.of(newManifestFileMetas, operation);
    }

    // -------------------------------------------------------------------------------------
    // Compact
    // -------------------------------------------------------------------------------------

    private List<IcebergManifestFileMeta> compactMetadataIfNeeded(
            List<IcebergManifestFileMeta> toCompact, long currentSnapshotId) throws IOException {
        List<IcebergManifestFileMeta> result = new ArrayList<>();
        long targetSizeInBytes = table.coreOptions().manifestTargetSize().getBytes();

        List<IcebergManifestFileMeta> candidates = new ArrayList<>();
        long totalSizeInBytes = 0;
        for (IcebergManifestFileMeta meta : toCompact) {
            if (meta.manifestLength() < targetSizeInBytes * 2 / 3) {
                candidates.add(meta);
                totalSizeInBytes += meta.manifestLength();
            } else {
                result.add(meta);
            }
        }

        Options options = new Options(table.options());
        if (candidates.size() < options.get(IcebergOptions.COMPACT_MIN_FILE_NUM)) {
            return toCompact;
        }
        if (candidates.size() < options.get(IcebergOptions.COMPACT_MAX_FILE_NUM)
                && totalSizeInBytes < targetSizeInBytes) {
            return toCompact;
        }

        Function<IcebergManifestFileMeta, List<IcebergManifestEntry>> processor =
                meta -> {
                    List<IcebergManifestEntry> sourceEntries =
                            materializeFirstRowIds(
                                    meta,
                                    IcebergManifestFile.create(table, pathFactory)
                                            .read(new Path(meta.manifestPath()).getName()));
                    List<IcebergManifestEntry> entries = new ArrayList<>();
                    for (IcebergManifestEntry entry : sourceEntries) {
                        // a deletion made by this commit is recorded against the current
                        // snapshot but keeps the file sequence number of the older snapshot
                        // that added the file, so it has to be recognised by snapshot id
                        if (entry.fileSequenceNumber() == currentSnapshotId
                                || entry.snapshotId() == currentSnapshotId
                                || entry.status() == IcebergManifestEntry.Status.EXISTING) {
                            entries.add(entry);
                        } else {
                            // rewrite status if this entry is from an older snapshot
                            IcebergManifestEntry.Status newStatus;
                            if (entry.status() == IcebergManifestEntry.Status.ADDED) {
                                newStatus = IcebergManifestEntry.Status.EXISTING;
                            } else if (entry.status() == IcebergManifestEntry.Status.DELETED) {
                                continue;
                            } else {
                                throw new UnsupportedOperationException(
                                        "Unknown IcebergManifestEntry.Status " + entry.status());
                            }
                            entries.add(
                                    new IcebergManifestEntry(
                                            newStatus,
                                            entry.snapshotId(),
                                            entry.sequenceNumber(),
                                            entry.fileSequenceNumber(),
                                            entry.file()));
                        }
                    }
                    if (meta.sequenceNumber() == currentSnapshotId) {
                        // this file is created for this snapshot, so it is not recorded in any
                        // iceberg metas, we need to clean it
                        table.fileIO().deleteQuietly(new Path(meta.manifestPath()));
                    }
                    return entries;
                };
        Iterable<IcebergManifestEntry> newEntries =
                ManifestReadThreadPool.sequentialBatchedExecute(processor, candidates, null);
        result.addAll(manifestFile.rollingWrite(newEntries.iterator(), currentSnapshotId));
        return result;
    }

    // -------------------------------------------------------------------------------------
    // Expire
    // -------------------------------------------------------------------------------------

    private boolean shouldExpire(IcebergSnapshot snapshot, long currentSnapshotId) {
        return shouldExpire(snapshot.snapshotId(), snapshot.timestampMs(), currentSnapshotId);
    }

    private boolean shouldExpire(long snapshotId, long timestampMs, long currentSnapshotId) {
        Options options = new Options(table.options());
        if (snapshotId > currentSnapshotId - options.get(CoreOptions.SNAPSHOT_NUM_RETAINED_MIN)) {
            return false;
        }
        if (snapshotId <= currentSnapshotId - options.get(CoreOptions.SNAPSHOT_NUM_RETAINED_MAX)) {
            return true;
        }
        return timestampMs
                < System.currentTimeMillis()
                        - options.get(CoreOptions.SNAPSHOT_TIME_RETAINED).toMillis();
    }

    private void expireManifestList(String toExpire, String next) {
        Set<IcebergManifestFileMeta> metaInUse = new HashSet<>(manifestList.read(next));
        for (IcebergManifestFileMeta meta : manifestList.read(toExpire)) {
            if (metaInUse.contains(meta)) {
                continue;
            }
            table.fileIO().deleteQuietly(new Path(meta.manifestPath()));
        }
        table.fileIO().deleteQuietly(pathFactory.toManifestListPath(toExpire));
    }

    private void expireAllBefore(long snapshotId) throws IOException {
        Set<String> expiredManifestLists = new HashSet<>();
        Set<String> expiredManifestFileMetas = new HashSet<>();
        Iterator<Path> it =
                pathFactory.getAllMetadataPathBefore(table.fileIO(), snapshotId).iterator();

        while (it.hasNext()) {
            Path path = it.next();
            IcebergMetadata metadata = IcebergMetadata.fromPath(table.fileIO(), path);

            for (IcebergSnapshot snapshot : metadata.snapshots()) {
                Path listPath = new Path(snapshot.manifestList());
                String listName = listPath.getName();
                if (expiredManifestLists.contains(listName)) {
                    continue;
                }
                expiredManifestLists.add(listName);

                for (IcebergManifestFileMeta meta : manifestList.read(listName)) {
                    String metaName = new Path(meta.manifestPath()).getName();
                    if (expiredManifestFileMetas.contains(metaName)) {
                        continue;
                    }
                    expiredManifestFileMetas.add(metaName);
                    table.fileIO().deleteQuietly(new Path(meta.manifestPath()));
                }
                table.fileIO().deleteQuietly(listPath);
            }
        }
        deleteApplicableMetadataFiles(snapshotId);
    }

    private void deleteApplicableMetadataFiles(long snapshotId) throws IOException {
        Options options = new Options(table.options());
        if (options.get(IcebergOptions.METADATA_DELETE_AFTER_COMMIT)) {
            long earliestMetadataId =
                    snapshotId - options.get(IcebergOptions.METADATA_PREVIOUS_VERSIONS_MAX);
            if (earliestMetadataId > 0) {
                Iterator<Path> it =
                        pathFactory
                                .getAllMetadataPathBefore(table.fileIO(), earliestMetadataId)
                                .iterator();
                while (it.hasNext()) {
                    Path path = it.next();
                    table.fileIO().deleteQuietly(path);
                }
            }
        }
    }

    @Override
    public void notifyCreation(String tagName) {
        // The base TagCallback API does not carry a snapshot id, but Iceberg refs
        // require one. The tag is persisted by TagManager before this callback
        // fires, so resolve the snapshot the tag points to and delegate to the
        // snapshot aware overload.
        Optional<Tag> tag = table.tagManager().get(tagName);
        if (!tag.isPresent()) {
            LOG.info(
                    "Tag {} not found in Paimon TagManager when creating Iceberg ref. Unable to create tag.",
                    tagName);
            return;
        }
        notifyCreation(tagName, tag.get().id());
    }

    @Override
    public void notifyCreation(String tagName, long snapshotId) {
        try {
            Snapshot latestSnapshot = table.snapshotManager().latestSnapshot();
            if (latestSnapshot == null) {
                LOG.info(
                        "Latest Iceberg snapshot not found when creating tag {} for snapshot {}. Unable to create tag.",
                        tagName,
                        snapshotId);
                return;
            }

            Path baseMetadataPath = pathFactory.toMetadataPath(latestSnapshot.id());
            if (!table.fileIO().exists(baseMetadataPath)) {
                LOG.info(
                        "Iceberg metadata file {} not found when creating tag {} for snapshot {}. Unable to create tag.",
                        baseMetadataPath,
                        tagName,
                        snapshotId);
                return;
            }

            IcebergMetadata baseMetadata =
                    IcebergMetadata.fromPath(table.fileIO(), baseMetadataPath);

            // Tags can only be included in Iceberg if they point to an Iceberg snapshot that
            // exists. Otherwise an Iceberg client fails to parse the metadata and all reads fail.
            boolean tagSnapshotInIceberg = false;
            for (IcebergSnapshot snapshot : baseMetadata.snapshots()) {
                if (snapshot.snapshotId() == snapshotId) {
                    tagSnapshotInIceberg = true;
                    break;
                }
            }

            if (!tagSnapshotInIceberg) {
                LOG.warn(
                        "Snapshot {} does not exist in Iceberg metadata. Unable to create tag {}.",
                        snapshotId,
                        tagName);
                return;
            }

            baseMetadata.refs().put(tagName, new IcebergRef(snapshotId));

            IcebergMetadata metadata =
                    new IcebergMetadata(
                            baseMetadata.formatVersion(),
                            baseMetadata.tableUuid(),
                            baseMetadata.location(),
                            baseMetadata.currentSnapshotId(),
                            baseMetadata.lastColumnId(),
                            baseMetadata.schemas(),
                            baseMetadata.currentSchemaId(),
                            baseMetadata.partitionSpecs(),
                            baseMetadata.lastPartitionId(),
                            baseMetadata.snapshots(),
                            baseMetadata.currentSnapshotId(),
                            baseMetadata.nextRowId(),
                            baseMetadata.refs());

            /*
            Overwrite the latest metadata file
            Currently the Paimon table snapshot id value is the same as the Iceberg metadata
            version number. Tag creation overwrites the latest metadata file to maintain this.
            There is no need to update the catalog after overwrite.
             */
            table.fileIO().overwriteFileUtf8(baseMetadataPath, metadata.toJson());
            LOG.info(
                    "Iceberg metadata file {} overwritten to add tag {} for snapshot {}.",
                    baseMetadataPath,
                    tagName,
                    snapshotId);

        } catch (IOException e) {
            throw new UncheckedIOException("Failed to create tag " + tagName, e);
        }
    }

    @Override
    public void notifyDeletion(String tagName) {
        try {
            Snapshot latestSnapshot = table.snapshotManager().latestSnapshot();
            if (latestSnapshot == null) {
                LOG.info(
                        "Latest Iceberg snapshot not found when deleting tag {}. Unable to delete tag.",
                        tagName);
                return;
            }

            Path baseMetadataPath = pathFactory.toMetadataPath(latestSnapshot.id());
            if (!table.fileIO().exists(baseMetadataPath)) {
                LOG.info(
                        "Iceberg metadata file {} not found when deleting tag {}. Unable to delete tag.",
                        baseMetadataPath,
                        tagName);
                return;
            }

            IcebergMetadata baseMetadata =
                    IcebergMetadata.fromPath(table.fileIO(), baseMetadataPath);

            baseMetadata.refs().remove(tagName);

            IcebergMetadata metadata =
                    new IcebergMetadata(
                            baseMetadata.formatVersion(),
                            baseMetadata.tableUuid(),
                            baseMetadata.location(),
                            baseMetadata.currentSnapshotId(),
                            baseMetadata.lastColumnId(),
                            baseMetadata.schemas(),
                            baseMetadata.currentSchemaId(),
                            baseMetadata.partitionSpecs(),
                            baseMetadata.lastPartitionId(),
                            baseMetadata.snapshots(),
                            baseMetadata.currentSnapshotId(),
                            baseMetadata.nextRowId(),
                            baseMetadata.refs());

            /*
            Overwrite the latest metadata file
            Currently the Paimon table snapshot id value is the same as the Iceberg metadata
            version number. Tag creation overwrites the latest metadata file to maintain this.
            There is no need to update the catalog after overwrite.
             */
            table.fileIO().overwriteFileUtf8(baseMetadataPath, metadata.toJson());
            LOG.info(
                    "Iceberg metadata file {} overwritten to delete tag {}.",
                    baseMetadataPath,
                    tagName);

        } catch (IOException e) {
            throw new UncheckedIOException("Failed to create tag " + tagName, e);
        }
    }

    // -------------------------------------------------------------------------------------
    // Deletion vectors
    // -------------------------------------------------------------------------------------

    private boolean needAddDvToIceberg() {
        CoreOptions options = table.coreOptions();
        // there may be dv indexes using bitmap32 in index files even if 'deletion-vectors.bitmap64'
        // is true, but analyzing all deletion vectors is very costly, so we do not check exactly
        // currently.
        return options.deletionVectorsEnabled()
                && options.deletionVectorBitmap64()
                && formatVersion == IcebergMetadata.FORMAT_VERSION_V3;
    }

    private List<IcebergManifestFileMeta> createDvManifestFileMetas(Snapshot snapshot) {
        List<IcebergManifestEntry> icebergDvEntries = new ArrayList<>();

        long snapshotId = snapshot.id();
        List<IndexManifestEntry> newIndexes =
                indexFileHandler.scan(snapshot, DELETION_VECTORS_INDEX);
        if (newIndexes.isEmpty()) {
            return Collections.emptyList();
        }
        for (IndexManifestEntry entry : newIndexes) {
            LinkedHashMap<String, DeletionVectorMeta> dvMetas = entry.indexFile().dvRanges();
            Path bucketPath = fileStorePathFactory.bucketPath(entry.partition(), entry.bucket());
            if (dvMetas != null) {
                for (DeletionVectorMeta dvMeta : dvMetas.values()) {

                    // Iceberg will check the cardinality between deserialized dv and iceberg
                    // deletion file, so if deletionFile.cardinality() is null, we should stop
                    // synchronizing all dvs.
                    Preconditions.checkState(
                            dvMeta.cardinality() != null,
                            "cardinality in DeletionVector is null, stop generate dv for iceberg. "
                                    + "dataFile path is {}, indexFile path is {}",
                            new Path(bucketPath, dvMeta.dataFileName()),
                            indexFileHandler.filePath(entry).toString());

                    IcebergDataFileMeta deleteFileMeta =
                            IcebergDataFileMeta.createForDeleteFile(
                                    IcebergDataFileMeta.Content.POSITION_DELETES,
                                    indexFileHandler.filePath(entry).toString(),
                                    PUFFIN_FORMAT,
                                    entry.partition(),
                                    dvMeta.cardinality(),
                                    entry.indexFile().fileSize(),
                                    new Path(bucketPath, dvMeta.dataFileName()).toString(),
                                    (long) dvMeta.offset(),
                                    (long) dvMeta.length());

                    icebergDvEntries.add(
                            new IcebergManifestEntry(
                                    IcebergManifestEntry.Status.ADDED,
                                    snapshotId,
                                    snapshotId,
                                    snapshotId,
                                    deleteFileMeta));
                }
            }
        }

        if (icebergDvEntries.isEmpty()) {
            return Collections.emptyList();
        }

        return manifestFile.rollingWrite(
                icebergDvEntries.iterator(), snapshotId, IcebergManifestFileMeta.Content.DELETES);
    }

    // -------------------------------------------------------------------------------------
    // Snapshot Summary Computation
    // -------------------------------------------------------------------------------------

    private static class SummaryMetrics {
        long addedDataFiles;
        long addedRecords;
        long addedFilesSize;
        long deletedDataFiles;
        long deletedRecords;
        long deletedFilesSize;
        long changedPartitionCount;
        long totalDataFiles;
        long totalRecords;
        long totalFilesSize;
        long totalDeleteFiles;
        long totalPositionDeletes;
        long totalEqualityDeletes;
    }

    private IcebergSnapshotSummary computeSnapshotSummary(
            String operation, Snapshot snapshot, SummaryMetrics metrics) {

        IcebergSnapshotSummary summary = new IcebergSnapshotSummary(operation);

        long addedDataFiles = Math.max(0, metrics.addedDataFiles);
        long addedRecords = Math.max(0, metrics.addedRecords);
        long addedFilesSize = Math.max(0, metrics.addedFilesSize);
        long deletedDataFiles = Math.max(0, metrics.deletedDataFiles);
        long deletedRecords = Math.max(0, metrics.deletedRecords);
        long deletedFilesSize = Math.max(0, metrics.deletedFilesSize);
        long changedPartitionCount = Math.max(0, metrics.changedPartitionCount);
        long totalRecords = Math.max(0, metrics.totalRecords);
        long totalDataFiles = Math.max(0, metrics.totalDataFiles);
        long totalFilesSize = Math.max(0, metrics.totalFilesSize);
        long totalDeleteFiles = Math.max(0, metrics.totalDeleteFiles);
        long totalPositionDeletes = Math.max(0, metrics.totalPositionDeletes);
        long totalEqualityDeletes = Math.max(0, metrics.totalEqualityDeletes);

        summary.put(SNAPSHOT_SUMMARY_ADDED_DATA_FILES, Long.toString(addedDataFiles));
        summary.put(SNAPSHOT_SUMMARY_ADDED_RECORDS, Long.toString(addedRecords));
        summary.put(SNAPSHOT_SUMMARY_ADDED_FILES_SIZE, Long.toString(addedFilesSize));
        summary.put(SNAPSHOT_SUMMARY_DELETED_DATA_FILES, Long.toString(deletedDataFiles));
        summary.put(SNAPSHOT_SUMMARY_DELETED_RECORDS, Long.toString(deletedRecords));
        summary.put(SNAPSHOT_SUMMARY_REMOVED_FILES_SIZE, Long.toString(deletedFilesSize));
        summary.put(SNAPSHOT_SUMMARY_CHANGED_PARTITION_COUNT, Long.toString(changedPartitionCount));
        summary.put(SNAPSHOT_SUMMARY_TOTAL_RECORDS, Long.toString(totalRecords));
        summary.put(SNAPSHOT_SUMMARY_TOTAL_DATA_FILES, Long.toString(totalDataFiles));
        summary.put(SNAPSHOT_SUMMARY_TOTAL_FILES_SIZE, Long.toString(totalFilesSize));
        summary.put(SNAPSHOT_SUMMARY_TOTAL_DELETE_FILES, Long.toString(totalDeleteFiles));
        summary.put(SNAPSHOT_SUMMARY_TOTAL_POSITION_DELETES, Long.toString(totalPositionDeletes));
        summary.put(SNAPSHOT_SUMMARY_TOTAL_EQUALITY_DELETES, Long.toString(totalEqualityDeletes));

        Map<String, String> properties = snapshot.properties();
        if (properties != null) {
            properties.forEach(
                    (key, value) -> {
                        if (value != null) {
                            summary.put(key, value);
                        }
                    });
        }

        return summary;
    }

    private long computeLiveFileCount(List<IcebergManifestFileMeta> manifestMetas) {
        return manifestMetas.stream()
                .mapToLong(
                        meta ->
                                meta.addedFilesCount()
                                        + meta.existingFilesCount()
                                        - meta.deletedFilesCount())
                .sum();
    }

    private long computeLiveRowCount(List<IcebergManifestFileMeta> manifestMetas) {
        return manifestMetas.stream()
                .mapToLong(
                        meta ->
                                meta.addedRowsCount()
                                        + meta.existingRowsCount()
                                        - meta.deletedRowsCount())
                .sum();
    }

    @Nullable
    private Long getSummaryLong(@Nullable IcebergSnapshot snapshot, String key) {
        if (snapshot == null) {
            return null;
        }
        Map<String, String> summaryMap = snapshot.summary().getSummary();
        String value = summaryMap.get(key);
        if (value == null) {
            return null;
        }
        try {
            return Long.parseLong(value);
        } catch (NumberFormatException e) {
            LOG.warn(
                    "Unable to parse snapshot summary field {}={} as long. The value will be recomputed.",
                    key,
                    value);
            return null;
        }
    }

    private long computeTotalFilesSizeFromManifests(List<IcebergManifestFileMeta> manifestMetas)
            throws IOException {
        long total = 0;
        for (IcebergManifestFileMeta meta : manifestMetas) {
            for (IcebergManifestEntry entry :
                    manifestFile.read(new Path(meta.manifestPath()).getName())) {
                if (entry.isLive()) {
                    total += entry.file().fileSizeInBytes();
                }
            }
        }
        return total;
    }

    // -------------------------------------------------------------------------------------
    // Utils
    // -------------------------------------------------------------------------------------

    private boolean isSameFormatVersion(int baseFormatVersion) {
        if (baseFormatVersion != formatVersion) {
            Preconditions.checkArgument(
                    formatVersion > baseFormatVersion,
                    "format version in base metadata is {}, and it's bigger than the current format version {}, "
                            + "this is not allowed!");

            LOG.info(
                    "format version in base metadata is {}, and it's different from the current format version {}. "
                            + "New metadata will be recreated using format version {}.",
                    baseFormatVersion,
                    formatVersion,
                    formatVersion);
            return false;
        }
        return true;
    }

    /**
     * Row-lineage bookkeeping for a new snapshot, mandatory in Iceberg format version 3: the
     * snapshot's first-row-id starts at the base metadata's next-row-id watermark. The snapshot's
     * added-rows and the table's next-row-id are NOT derived here: they depend on how many rows
     * {@link #assignManifestFirstRowIds} actually assigns (which can exceed this commit's added
     * records when a carried-over manifest is assigned for the first time, e.g. a Layer-1-written
     * manifest being upgraded), so callers must recompute them from the assignment's result. For
     * format version 2 the field stays null so nothing is written.
     */
    @Nullable
    private Long computeSnapshotFirstRowId(long baseNextRowId) {
        return formatVersion >= IcebergMetadata.FORMAT_VERSION_V3 ? baseNextRowId : null;
    }

    /**
     * Result of {@link #assignManifestFirstRowIds}: the manifests with first_row_id assigned, and
     * the total number of rows actually consumed from the row-id space by that assignment (which
     * may be larger than this commit's added-records count; see the class-level note there).
     */
    private static class ManifestRowIdAssignment {
        private final List<IcebergManifestFileMeta> manifests;
        private final long assignedRows;

        private ManifestRowIdAssignment(
                List<IcebergManifestFileMeta> manifests, long assignedRows) {
            this.manifests = manifests;
            this.assignedRows = assignedRows;
        }
    }

    /**
     * Iceberg v3: assign first_row_id (field 520) to data manifests that do not have one yet.
     * Manifests carried over from base metadata that are already assigned keep their value; delete
     * manifests stay null. The watermark starts at the snapshot's first-row-id and advances by each
     * newly-assigned manifest's TRUE inheriting-rows count (see {@link #trueInheritingRowsCount}),
     * returned as {@link ManifestRowIdAssignment#assignedRows}.
     *
     * <p>A manifest written entirely by Layer 2 (this commit or a later one) satisfies "null-142
     * rows == ADDED rows", so {@code addedRowsCount()} is exact for it. But a manifest carried over
     * from before manifest-level assignment existed (a "Layer-1" manifest) may reach here
     * unassigned with existing/deleted entries whose per-file field 142 is also still null; for
     * those, {@code addedRowsCount()} alone would undercount the rows this assignment must cover,
     * silently shrinking the range handed out and colliding with the next commit's ids. Callers
     * MUST use {@code assignedRows} (not this commit's added-records count) to advance the
     * snapshot's added-rows / table next-row-id, precisely because of that mismatch.
     */
    private ManifestRowIdAssignment assignManifestFirstRowIds(
            List<IcebergManifestFileMeta> manifests, @Nullable Long snapshotFirstRowId) {
        if (snapshotFirstRowId == null) {
            return new ManifestRowIdAssignment(manifests, 0L);
        }
        List<IcebergManifestFileMeta> result = new ArrayList<>();
        long watermark = snapshotFirstRowId;
        for (IcebergManifestFileMeta meta : manifests) {
            if (meta.content() == IcebergManifestFileMeta.Content.DATA
                    && meta.firstRowId() == null) {
                result.add(meta.withFirstRowId(watermark));
                watermark += trueInheritingRowsCount(meta);
            } else {
                result.add(meta);
            }
        }
        return new ManifestRowIdAssignment(result, watermark - snapshotFirstRowId);
    }

    /**
     * The true number of rows an unassigned manifest needs from the row-id space: the sum of {@code
     * recordCount()} over entries whose per-file first_row_id (field 142) is null.
     *
     * <p>Fast path: when the manifest has no existing/deleted entries ({@code existingFilesCount()
     * + deletedFilesCount() == 0}), every entry is ADDED and, by the Layer-2 invariant, has a null
     * field 142, so {@code addedRowsCount()} already equals this sum without having to read the
     * manifest file.
     *
     * <p>Otherwise (a manifest that may carry Layer-1-era existing/deleted entries whose field 142
     * was never materialized) the manifest is actually read and entries are inspected one by one,
     * since {@code addedRowsCount()} alone would not include those entries' rows.
     */
    private long trueInheritingRowsCount(IcebergManifestFileMeta meta) {
        if (meta.existingFilesCount() + meta.deletedFilesCount() == 0) {
            return meta.addedRowsCount();
        }
        long sum = 0;
        for (IcebergManifestEntry entry :
                manifestFile.read(new Path(meta.manifestPath()).getName())) {
            if (entry.file().firstRowId() == null) {
                sum += entry.file().recordCount();
            }
        }
        return sum;
    }

    /**
     * Iceberg v3 requires the inherited first_row_id to be written into file metadata when entries
     * are copied into a rewritten manifest. Computes each entry's effective id in base manifest
     * order (explicit field 142, or inherited from the manifest's first_row_id) and returns entries
     * with the id materialized. No-op for delete manifests and for base manifests without an
     * assigned first_row_id (v2 metadata, or v3 metadata written before manifest-level assignment
     * existed — those stay in the spec's upgraded-table state).
     */
    private static List<IcebergManifestEntry> materializeFirstRowIds(
            IcebergManifestFileMeta baseMeta, List<IcebergManifestEntry> entries) {
        if (baseMeta.content() != IcebergManifestFileMeta.Content.DATA
                || baseMeta.firstRowId() == null) {
            return entries;
        }
        List<IcebergManifestEntry> result = new ArrayList<>();
        long watermark = baseMeta.firstRowId();
        for (IcebergManifestEntry entry : entries) {
            if (entry.file().firstRowId() == null) {
                result.add(entry.withFile(entry.file().withFirstRowId(watermark)));
                watermark += entry.file().recordCount();
            } else {
                result.add(entry);
            }
        }
        return result;
    }

    private class SchemaCache {

        SchemaManager schemaManager = new SchemaManager(table.fileIO(), table.location());
        Map<Long, IcebergSchema> schemas = new HashMap<>();

        private IcebergSchema get(long schemaId) {
            return schemas.computeIfAbsent(
                    schemaId,
                    id -> {
                        TableSchema schema = schemaManager.schema(id);
                        if (formatVersion < IcebergMetadata.FORMAT_VERSION_V3) {
                            // VARIANT is an Iceberg format-version-3 type; v2 metadata cannot
                            // represent it
                            checkVariantNotPublishable(schema.logicalRowType());
                        }
                        return IcebergSchema.create(schema);
                    });
        }

        private long getLatestSchemaId() {
            return schemaManager.latest().get().id();
        }
    }
}
