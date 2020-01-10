package com.openlattice.shuttle

import com.dataloom.mappers.ObjectMappers
import com.google.common.base.Preconditions.checkState
import com.google.common.util.concurrent.MoreExecutors
import com.hazelcast.core.HazelcastInstance
import com.hazelcast.query.Predicates
import com.openlattice.authorization.HazelcastAclKeyReservationService
import com.openlattice.authorization.Principals
import com.openlattice.client.ApiClient
import com.openlattice.data.DataIntegrationApi
import com.openlattice.data.EntityKeyIdService
import com.openlattice.data.S3Api
import com.openlattice.data.integration.IntegrationDestination
import com.openlattice.data.integration.StorageDestination
import com.openlattice.data.integration.destinations.PostgresDestination
import com.openlattice.data.integration.destinations.PostgresS3Destination
import com.openlattice.datastore.services.EdmManager
import com.openlattice.datastore.services.EntitySetManager
import com.openlattice.edm.EntitySet
import com.openlattice.edm.type.EntityType
import com.openlattice.edm.type.PropertyType
import com.openlattice.hazelcast.HazelcastMap
import com.openlattice.hazelcast.HazelcastQueue
import com.openlattice.retrofit.RhizomeByteConverterFactory
import com.openlattice.retrofit.RhizomeCallAdapterFactory
import com.openlattice.retrofit.RhizomeJacksonConverterFactory
import com.openlattice.rhizome.proxy.RetrofitBuilders
import com.openlattice.shuttle.control.*
import com.openlattice.shuttle.hazelcast.processors.UpdateIntegrationEntryProcessor
import com.openlattice.shuttle.logs.Blackbox
import com.openlattice.shuttle.mapstore.INTEGRATION_STATUS
import com.openlattice.shuttle.payload.JdbcPayload
import com.openlattice.shuttle.payload.Payload
import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import org.apache.olingo.commons.api.edm.FullQualifiedName
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import retrofit2.Retrofit
import java.util.*
import java.util.concurrent.Executors
import java.util.concurrent.Semaphore

private val logger = LoggerFactory.getLogger(IntegrationService::class.java)
private const val fetchSize = 10000
private const val readRateLimit = 1000
private const val uploadBatchSize = 10000
private val nThreads = 2 * Runtime.getRuntime().availableProcessors()

private lateinit var logEntityType: EntityType

@Service
class IntegrationService(
        private val hazelcastInstance: HazelcastInstance,
        private val missionParameters: MissionParameters,
        private val idService: EntityKeyIdService,
        private val entitySetManager: EntitySetManager,
        private val reservationService: HazelcastAclKeyReservationService,
        private val blackbox: Blackbox
) {

    private val integrations = hazelcastInstance.getMap<String, Integration>(HazelcastMap.INTEGRATIONS.name)
    private val entitySets = hazelcastInstance.getMap<UUID, EntitySet>(HazelcastMap.ENTITY_SETS.name)
    private val entityTypes = hazelcastInstance.getMap<UUID, EntityType>(HazelcastMap.ENTITY_TYPES.name)
    private val propertyTypes = hazelcastInstance.getMap<UUID, PropertyType>(HazelcastMap.PROPERTY_TYPES.name)
    private val integrationJobs = hazelcastInstance.getMap<UUID, IntegrationJob>(HazelcastMap.INTEGRATION_JOBS.name)
    private val integrationQueue = hazelcastInstance.getQueue<Pair<UUID, String>>(HazelcastQueue.INTEGRATION_JOBS.name)
    private val creds = missionParameters.auth.credentials
    private val semaphore = Semaphore(nThreads)
    private val executor = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(nThreads))
    private val statusPredicate = Predicates.or(
            Predicates.equal(INTEGRATION_STATUS, IntegrationStatus.IN_PROGRESS),
            Predicates.equal(INTEGRATION_STATUS, IntegrationStatus.QUEUED)
    )

    companion object {
        @JvmStatic
        fun init(blackbox: Blackbox, dataModelService: EdmManager) {
            if (blackbox.enabled) {
                logEntityType = dataModelService.getEntityType(FullQualifiedName(blackbox.entityTypeFqn))
            }
        }
    }

    init {
        executor.execute {
            semaphore.acquire()

            //load any jobs that were in progress or queued
            integrationJobs.entrySet(statusPredicate).forEach{
                integrationQueue.put(it.key to it.value.integrationName)
            }

            while (true) {
                if (semaphore.availablePermits() > 0) {
                    loadCargo()
                }
            }
        }
    }

    fun enqueueIntegrationJob(integrationName: String): UUID {
        checkState(integrations.containsKey(integrationName), "Integration with name $integrationName does not exist")
        val jobId = generateIntegrationJobId()
        integrationQueue.put(Pair(jobId, integrationName))
        integrationJobs[jobId] = IntegrationJob(integrationName, IntegrationStatus.QUEUED)
        return jobId
    }

    private final fun loadCargo() {
        val integrationJob = integrationQueue.take()
        val jobId = integrationJob.first
        val integration = integrations.getValue(integrationJob.second)
        //val token = MissionControl.getIdToken(creds.getProperty("email"), creds.getProperty("password"))
        val apiClient = ApiClient(integration.environment) { "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJlbWFpbCI6InBpcGVyQG9wZW5sYXR0aWNlLmNvbSIsImVtYWlsX3ZlcmlmaWVkIjp0cnVlLCJ1c2VyX21ldGFkYXRhIjp7fSwiYXBwX21ldGFkYXRhIjp7InJvbGVzIjpbIkF1dGhlbnRpY2F0ZWRVc2VyIiwiYWRtaW4iXSwiYWN0aXZhdGVkIjoiYWN0aXZhdGVkIn0sIm5pY2tuYW1lIjoicGlwZXIiLCJyb2xlcyI6WyJBdXRoZW50aWNhdGVkVXNlciIsImFkbWluIl0sInVzZXJfaWQiOiJnb29nbGUtb2F1dGgyfDExNDczMDU3NjI0MjQ4MTc3NTE4MiIsImlzcyI6Imh0dHBzOi8vb3BlbmxhdHRpY2UuYXV0aDAuY29tLyIsInN1YiI6Imdvb2dsZS1vYXV0aDJ8MTE0NzMwNTc2MjQyNDgxNzc1MTgyIiwiYXVkIjoiS1R6Z3l4czZLQmNKSEI4NzJlU01lMmNwVEh6aHhTOTkiLCJpYXQiOjE1Nzg2MTI4MjksImV4cCI6MTU3ODY5OTIyOX0.-TbgsunB0-HVzft-vX5r_0KgPlUBNZ4ezUAsKbB35UE" }
        val dataIntegrationApi = apiClient.dataIntegrationApi

        val flightPlan = mutableMapOf<Flight, Payload>()
        val tableColsToPrint = mutableMapOf<Flight, List<String>>()
        integration.flightPlanParameters.values.forEach {
            val srcDataSource = getSrcDataSource(it.source)
            val payload = JdbcPayload(readRateLimit.toDouble(), srcDataSource, it.sql, fetchSize, readRateLimit != 0)
            flightPlan[it.flight!!] = payload
            tableColsToPrint[it.flight!!] = it.sourcePrimaryKeyColumns
        }
        val destinationsMap = generateDestinationsMap(integration, missionParameters, dataIntegrationApi)

        val shuttle = Shuttle(
                integration.environment,
                flightPlan,
                entitySets.values.associateBy { it.name },
                entityTypes.values.associateBy { it.id },
                propertyTypes.values.associateBy { it.type },
                destinationsMap,
                dataIntegrationApi,
                tableColsToPrint,
                missionParameters,
                StorageDestination.S3,
                blackbox,
                Optional.of(entitySets.getValue(integration.logEntitySetId.get())),
                Optional.of(jobId),
                Optional.of(integrationJob.second),
                idService,
                hazelcastInstance
        )

        executor.submit {
            semaphore.acquire()
            shuttle.launch(uploadBatchSize)
        }.addListener(Runnable { semaphore.release() }, executor)
    }

    fun pollIntegrationStatus(jobId: UUID): IntegrationStatus {
        checkState(integrationJobs.containsKey(jobId), "Job Id $jobId is not assigned to an existing integration job")
        val status = integrationJobs.getValue(jobId).integrationStatus
        if (status == IntegrationStatus.SUCCEEDED || status == IntegrationStatus.FAILED) {
            integrationJobs.remove(jobId)
        }
        return status
    }

    fun pollAllIntegrationStatuses(): Map<UUID, IntegrationJob> {
        return integrationJobs
    }

    fun createIntegrationDefinition(integrationName: String, integration: Integration) {
        checkState(!integrations.containsKey(integrationName), "An integration with name $integrationName already exists.")
        val logEntitySetId = createLogEntitySet(integrationName, integration)
        integration.logEntitySetId = Optional.of(logEntitySetId)
        integrations[integrationName] = integration
    }

    fun readIntegrationDefinition(integrationName: String): Integration {
        checkState(integrations.containsKey(integrationName), "Integration with name $integrationName does not exist.")
        return integrations.getValue(integrationName)
    }

    fun updateIntegrationDefinition(integrationName: String, integrationUpdate: IntegrationUpdate) {
        checkState(integrations.containsKey(integrationName), "Integration with name $integrationName does not exist.")
        integrations.executeOnKey(integrationName, UpdateIntegrationEntryProcessor(integrationUpdate))
    }

    fun deleteIntegrationDefinition(integrationName: String) {
        checkState(integrations.containsKey(integrationName), "Integration with name $integrationName does not exist.")
        val integration = integrations.getValue(integrationName)
        integrations.remove(integrationName)
        entitySetManager.deleteEntitySet(integration.logEntitySetId.get())
    }

    private fun getSrcDataSource(source: Properties): HikariDataSource {
        return HikariDataSource(HikariConfig(source))
    }

    private fun generateDestinationsMap(integration: Integration, missionParameters: MissionParameters, dataIntegrationApi: DataIntegrationApi): Map<StorageDestination, IntegrationDestination> {
        val s3BucketUrl = integration.s3bucket
        val dstDataSource = HikariDataSource(HikariConfig(missionParameters.postgres.config))
        integration.maxConnections.ifPresent { dstDataSource.maximumPoolSize = it }

        val pgDestination = PostgresDestination(
                entitySets.mapKeys { it.value.id },
                entityTypes,
                propertyTypes.mapKeys { it.value.id },
                dstDataSource
        )

        if (s3BucketUrl.isNotBlank()) {
            val s3Api = Retrofit.Builder()
                    .baseUrl(s3BucketUrl)
                    .addConverterFactory(RhizomeByteConverterFactory())
                    .addConverterFactory(RhizomeJacksonConverterFactory(ObjectMappers.getJsonMapper()))
                    .addCallAdapterFactory(RhizomeCallAdapterFactory())
                    .client(RetrofitBuilders.okHttpClient().build())
                    .build().create(S3Api::class.java)
            val s3Destination = PostgresS3Destination(pgDestination, s3Api, dataIntegrationApi)
            return mapOf(StorageDestination.POSTGRES to pgDestination, StorageDestination.S3 to s3Destination)
        }
        return mapOf(StorageDestination.POSTGRES to pgDestination)
    }

    private fun createLogEntitySet(integrationName: String, integration: Integration): UUID {
        val name = buildLogEntitySetName(integrationName)
        val contacts = integration.contacts
        val description = buildLogEntitySetDescription(integration.flightPlanParameters)
        val logEntitySet = EntitySet(
                logEntityType.id,
                name,
                name,
                Optional.of(description),
                contacts
        )
        return entitySetManager.createEntitySet(Principals.getCurrentUser(), logEntitySet)
    }

    private fun buildLogEntitySetName(integrationName: String): String {
        var name = "Integration logs for $integrationName"
        var nameAttempt = name
        var count = 1

        while (reservationService.isReserved(nameAttempt)) {
            nameAttempt = name + "_" + count
            count++
        }

        return nameAttempt
    }

    private fun buildLogEntitySetDescription(flightPlanParameters: Map<String, FlightPlanParameters>): String {
        var entitySetDescription = "Auto-generated entity set containing logs of the following flights: "
        flightPlanParameters.values.forEach {
            val flight = it.flight!!
            entitySetDescription += flight.name
            if (flight.tags.isNotEmpty()) entitySetDescription += " with tags ${flight.tags.joinToString(", ")}"
        }

        return entitySetDescription
    }

    private fun generateIntegrationJobId(): UUID {
        var jobId = UUID.randomUUID()
        while (integrationJobs.containsKey(jobId)) {
            jobId = UUID.randomUUID()
        }
        return jobId
    }

}