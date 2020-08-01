/*
 * Copyright (C) 2020. OpenLattice, Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 * You can contact the owner of the copyright at support@openlattice.com
 *
 *
 */

package com.openlattice.shuttle.destinations

import com.dataloom.mappers.ObjectMappers
import com.fasterxml.jackson.module.kotlin.readValue
import com.google.common.base.Stopwatch
import com.google.common.collect.ImmutableList
import com.openlattice.data.DataEdgeKey
import com.openlattice.data.EntityDataKey
import com.openlattice.data.EntityKey
import com.openlattice.data.UpdateType
import com.openlattice.data.integration.Association
import com.openlattice.data.integration.Entity
import com.openlattice.data.storage.partitions.getPartition
import com.openlattice.data.storage.updateEntitySql
import com.openlattice.data.storage.updateVersionsForPropertyTypesInEntitiesInEntitySet
import com.openlattice.data.storage.upsertPropertyValueSql
import com.openlattice.data.util.PostgresDataHasher
import com.openlattice.edm.EntitySet
import com.openlattice.edm.type.EntityType
import com.openlattice.edm.type.PropertyType
import com.openlattice.graph.EDGES_UPSERT_SQL
import com.openlattice.graph.bindColumnsForEdge
import com.openlattice.postgres.JsonDeserializer
import com.openlattice.postgres.PostgresArrays
import com.zaxxer.hikari.HikariDataSource
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind
import org.postgresql.util.PSQLException
import org.slf4j.LoggerFactory
import java.security.InvalidParameterException
import java.sql.Connection
import java.sql.PreparedStatement
import java.util.*
import java.util.concurrent.TimeUnit

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class PostgresDestination(
        private val entitySets: Map<UUID, EntitySet>,
        private val entityTypes: Map<UUID, EntityType>,
        private val propertyTypes: Map<UUID, PropertyType>,
        private val hds: HikariDataSource
) : IntegrationDestination {
    companion object {
        private val logger = LoggerFactory.getLogger(PostgresDestination::class.java)
        private val mapper = ObjectMappers.newJsonMapper()
    }

    override fun integrateEntities(
            data: Collection<Entity>,
            entityKeyIds: Map<EntityKey, UUID>,
            updateTypes: Map<UUID, UpdateType>
    ): Long {

        return hds.connection.use { connection ->
            val sw = Stopwatch.createStarted()
            val upsertPropertyValues = mutableMapOf<UUID, PreparedStatement>()
            val updatePropertyValueVersion = connection.prepareStatement(
                    updateVersionsForPropertyTypesInEntitiesInEntitySet()
            )
            val count = data
                    .groupBy({ it.entitySetId }, { normalize(entityKeyIds, it) })
                    .map { (entitySetId, entities) ->
                        logger.info("Integrating entity set {}", entitySets.getValue(entitySetId).name)
                        val esSw = Stopwatch.createStarted()
                        val entitySet = entitySets.getValue(entitySetId)

                        val partitions = entitySet.partitions.toList()

                        val baseVersion = System.currentTimeMillis()
                        val tombstoneVersion = baseVersion
                        val writeVersion = baseVersion + 1
                        val relevantPropertyTypes = entityTypes
                                .getValue(entitySets.getValue(entitySetId).entityTypeId)
                                .properties
                                .associateWith(propertyTypes::getValue)
                        val propertyTypeIdsArr = when (updateTypes.getValue(entitySetId)) {
                            UpdateType.Replace -> PostgresArrays.createUuidArray(connection, relevantPropertyTypes.keys)
                            UpdateType.PartialReplace -> PostgresArrays.createUuidArray(
                                    connection, data.flatMap { it.details.keys }.toSet()
                            )
                            else -> PostgresArrays.createUuidArray(connection, setOf())
                        }

                        val writeVersionArray = PostgresArrays.createLongArray(connection, writeVersion)
                        logger.info(
                                "Preparing queries for entity set {} took {} ms",
                                entitySets.getValue(entitySetId).name,
                                esSw.elapsed(TimeUnit.MILLISECONDS)
                        )
                        val esCount = entities.groupBy { getPartition(it.first, partitions) }
                                .toSortedMap()
                                .map { (partition, entityPairs) ->
                                    val partSw = Stopwatch.createStarted()
                                    val entityMap = entityPairs.toMap()
                                    val entityKeyIdsArr = PostgresArrays.createUuidArray(connection, entityMap.keys)
                                    val partitionArr = PostgresArrays.createIntArray(connection, listOf(partition))

                                    when (updateTypes.getValue(entitySetId)) {
                                        UpdateType.Replace, UpdateType.PartialReplace -> tombstone(
                                                updatePropertyValueVersion,
                                                entitySet,
                                                entityKeyIdsArr,
                                                partitionArr,
                                                propertyTypeIdsArr,
                                                tombstoneVersion
                                        )
                                    }

                                    val committedProperties = upsertEntities(
                                            connection,
                                            upsertPropertyValues,
                                            entitySet,
                                            partition,
                                            entityMap,
                                            relevantPropertyTypes,
                                            writeVersionArray,
                                            writeVersion
                                    )

                                    logger.info(
                                            "Upserted $committedProperties properties for partition $partition and entity set {} in {} ms ",
                                            partition,

                                            partSw.elapsed(TimeUnit.MILLISECONDS)
                                    )

                                    commitEntities(
                                            connection,
                                            entitySetId,
                                            partition,
                                            entityMap.keys,
                                            entityKeyIdsArr,
                                            writeVersionArray,
                                            writeVersion
                                    )
                                    val committed = entityMap.size.toLong()
                                    logger.info(
                                            "Integrated $committed entities and $committedProperties properties for partition $partition and entity set {} in {} ms",
                                            entitySets.getValue(entitySetId).name,
                                            partSw.elapsed(TimeUnit.MILLISECONDS)
                                    )
                                    committed
                                }.sum()
                        logger.info(
                                "Integrated $esCount entities for entity set {} in {} ms",
                                entitySets.getValue(entitySetId).name,
                                esSw.elapsed(TimeUnit.MILLISECONDS)
                        )
                        esCount
                    }.sum()

            logger.info(
                    "Integrated ${data.size} entities and update $count rows in ${sw.elapsed(
                            TimeUnit.MILLISECONDS
                    )} ms."
            )
            data.size.toLong()
        }
    }

    override fun integrateAssociations(
            data: Collection<Association>,
            entityKeyIds: Map<EntityKey, UUID>,
            updateTypes: Map<UUID, UpdateType>
    ): Long {
        val sw = Stopwatch.createStarted()
        val dataEdgeKeys = data.map {
            val srcDataKey = EntityDataKey(it.src.entitySetId, entityKeyIds.getValue(it.src))
            val dstDataKey = EntityDataKey(it.dst.entitySetId, entityKeyIds.getValue(it.dst))
            val edgeDataKey = EntityDataKey(it.key.entitySetId, entityKeyIds.getValue(it.key))
            DataEdgeKey(srcDataKey, dstDataKey, edgeDataKey)
        }.toSet()
        val numCreatedEdges = createEdges(dataEdgeKeys)

        logger.info(
                "Integrated {} edges in {} ms.",
                numCreatedEdges,
                sw.elapsed(TimeUnit.MILLISECONDS)
        )

        return data.size.toLong()
    }

    override fun accepts(): StorageDestination {
        return StorageDestination.POSTGRES
    }

    internal fun createEdges(keys: Set<DataEdgeKey>): Long {
        val partitionsByEntitySet = keys
                .flatMap { listOf(it.src.entitySetId, it.dst.entitySetId, it.edge.entitySetId) }
                .toSet()
                .associateWith { entitySetId -> entitySets.getValue(entitySetId).partitions.toList() }

        return hds.connection.use { connection ->
            val ps = connection.prepareStatement(EDGES_UPSERT_SQL)
            val version = System.currentTimeMillis()
            val versions = PostgresArrays.createLongArray(connection, ImmutableList.of(version))

            ps.use {
                keys.forEach { dataEdgeKey ->
                    bindColumnsForEdge(ps, dataEdgeKey, version, versions, partitionsByEntitySet)
                }

                ps.executeBatch().sum().toLong()
            }
        }
    }


    private fun normalize(entityKeyIds: Map<EntityKey, UUID>, entity: Entity): Pair<UUID, Map<UUID, Set<Any>>> {
        val sw = Stopwatch.createStarted()
        val propertyValues = mapper.readValue<Map<UUID, Set<Any>>>(mapper.writeValueAsBytes(entity.details))
        val validatedPropertyValues = JsonDeserializer.validateFormatAndNormalize(propertyValues, propertyTypes) {
            "Error validating during integration"
        }
        logger.debug("Normalizing took {} ms", sw.elapsed(TimeUnit.MILLISECONDS))
        return entityKeyIds.getValue(entity.key) to validatedPropertyValues
    }

    private fun upsertEntities(
            connection: Connection,
            upsertPropertyValues: MutableMap<UUID, PreparedStatement>,
            entitySet: EntitySet,
            partition: Int,
            entities: Map<UUID, Map<UUID, Set<Any>>>,
            propertyTypes: Map<UUID, PropertyType>,
            versionArray: java.sql.Array,
            version: Long,
            entitySetId: UUID = entitySet.id
    ): Long {

        /*
         * We do not need entity level locking as our version field ensures that data is consistent even across
         * transactions in all cases, except deletes (clear is fine) as long entity version is not bumped until
         * all properties are written.
         *
         */

        //Update property values. We use multiple prepared statements in batch while re-using ARRAY[version].


        return entities.entries.map { (entityKeyId, entityData) ->
            entityData.map { (propertyTypeId, values) ->
                val upsertPropertyValue = upsertPropertyValues.getOrPut(propertyTypeId) {
                    val pt = propertyTypes[propertyTypeId] ?: abortInsert(entitySetId, entityKeyId)
                    connection.prepareStatement(upsertPropertyValueSql(pt))
                }

                values.map { value ->
                    val dataType = propertyTypes.getValue(propertyTypeId).datatype

                    val (propertyHash, insertValue) = getPropertyHash(
                            value,
                            dataType
                    )

                    upsertPropertyValue.setObject(1, entitySetId)
                    upsertPropertyValue.setObject(2, entityKeyId)
                    upsertPropertyValue.setInt(3, partition)
                    upsertPropertyValue.setObject(4, propertyTypeId)
                    upsertPropertyValue.setObject(5, propertyHash)
                    upsertPropertyValue.setObject(6, version)
                    upsertPropertyValue.setArray(7, versionArray)
                    upsertPropertyValue.setObject(8, insertValue)
                    upsertPropertyValue.addBatch()
                }
            }
            upsertPropertyValues.values.map { it.executeBatch().sum().toLong() }.sum()
        }.sum()
    }

    private fun tombstone(
            updatePropertyValueVersion: PreparedStatement,
            entitySet: EntitySet,
            entityKeyIdsArr: java.sql.Array,
            partitionsArr: java.sql.Array,
            propertyTypeIdsArr: java.sql.Array,
            version: Long,
            entitySetId: UUID = entitySet.id
    ) {

        updatePropertyValueVersion.setLong(1, -version)
        updatePropertyValueVersion.setLong(2, -version)
        updatePropertyValueVersion.setLong(3, -version)
        updatePropertyValueVersion.setObject(4, entitySetId)
        updatePropertyValueVersion.setArray(5, partitionsArr)
        updatePropertyValueVersion.setArray(6, propertyTypeIdsArr)
        updatePropertyValueVersion.setArray(7, entityKeyIdsArr)

        val numUpdated = updatePropertyValueVersion.executeUpdate()

        logger.info("Tombstoned $numUpdated properties in ${entitySet.name} with version $version ")

    }

    private fun commitEntities(
            connection: Connection,
            entitySetId: UUID,
            partition: Int,
            entityKeyIds: Set<UUID>,
            entityKeyIdsArr: java.sql.Array,
            versionArray: java.sql.Array,
            version: Long
    ): Int {

            val ps = connection.prepareStatement(updateEntitySql)

            entityKeyIds.sorted().forEach { entityKeyId ->
                ps.setArray(1, versionArray)
                ps.setObject(2, version)
                ps.setObject(3, version)
                ps.setObject(4, entitySetId)
                ps.setObject(5, entityKeyId)
                ps.setInt(6, partition)
                ps.addBatch()
            }
            val numUpdates = ps.executeBatch().sum()

            logger.info("Updated $numUpdates entities as part of insert.")
            return numUpdates
    }

    private fun getPropertyHash(
            value: Any,
            dataType: EdmPrimitiveTypeKind
    ): Pair<ByteArray, Any> {
        return PostgresDataHasher.hashObject(value, dataType) to value
    }
}

private fun abortInsert(entitySetId: UUID, entityKeyId: UUID): Nothing {
    throw InvalidParameterException(
            "Cannot insert property type not in authorized property types for entity $entityKeyId from entity set $entitySetId."
    )
}
