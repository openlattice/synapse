package com.openlattice.shuttle

import com.google.common.base.Preconditions.checkState
import com.google.common.util.concurrent.MoreExecutors
import com.hazelcast.core.HazelcastInstance
import com.hazelcast.query.Predicates
import com.openlattice.authorization.HazelcastAclKeyReservationService
import com.openlattice.authorization.Principals
import com.openlattice.client.RetrofitFactory
import com.openlattice.client.RetrofitFactory.decorateWithOpenLatticeFactories
import com.openlattice.data.EntityKeyIdService
import com.openlattice.data.S3Api
import com.openlattice.data.integration.S3EntityData
import com.openlattice.data.storage.aws.AwsDataSinkService
import com.openlattice.shuttle.destinations.IntegrationDestination
import com.openlattice.shuttle.destinations.StorageDestination
import com.openlattice.shuttle.destinations.PostgresDestination
import com.openlattice.shuttle.destinations.PostgresS3Destination
import com.openlattice.datastore.services.EdmManager
import com.openlattice.datastore.services.EntitySetManager
import com.openlattice.edm.EntitySet
import com.openlattice.edm.type.EntityType
import com.openlattice.hazelcast.HazelcastMap
import com.openlattice.hazelcast.HazelcastQueue
import com.openlattice.hazelcast.processors.shuttle.UpdateIntegrationEntryProcessor
import com.openlattice.shuttle.logs.Blackbox
import com.openlattice.hazelcast.mapstores.shuttle.INTEGRATION_STATUS
import com.openlattice.shuttle.payload.JdbcPayload
import com.openlattice.shuttle.payload.Payload
import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import okhttp3.FormBody
import okhttp3.OkHttpClient
import okhttp3.Request
import org.apache.olingo.commons.api.edm.FullQualifiedName
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import retrofit2.Retrofit
import java.io.IOException
import java.lang.IllegalStateException
import java.net.URL
import java.util.*
import java.util.concurrent.Executors
import java.util.concurrent.Semaphore

private val logger = LoggerFactory.getLogger(IntegrationService::class.java)
private const val fetchSize = 10_000
private const val readRateLimit = 1_000
private const val uploadBatchSize = 10_000
private val threadCount = 2 * Runtime.getRuntime().availableProcessors()

private lateinit var logEntityType: EntityType
private lateinit var httpClient: OkHttpClient

@Service
class IntegrationService(
        private val hazelcastInstance: HazelcastInstance,
        private val missionParameters: MissionParameters,
        private val idService: EntityKeyIdService,
        private val entitySetManager: EntitySetManager,
        private val reservationService: HazelcastAclKeyReservationService,
        private val awsDataSinkService: AwsDataSinkService,
        private val blackbox: Blackbox
) {

    private val integrations = HazelcastMap.INTEGRATIONS.getMap(hazelcastInstance)
    private val entitySets = HazelcastMap.ENTITY_SETS.getMap(hazelcastInstance)
    private val entityTypes = HazelcastMap.ENTITY_TYPES.getMap(hazelcastInstance)
    private val propertyTypes = HazelcastMap.PROPERTY_TYPES.getMap(hazelcastInstance)
    private val integrationJobs = HazelcastMap.INTEGRATION_JOBS.getMap(hazelcastInstance)
    private val jobQueue = HazelcastQueue.QUEUED_INTEGRATION_JOBS.getQueue(hazelcastInstance)
    private val semaphore = Semaphore(threadCount)
    private val executor = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(threadCount))

    private val statusPredicate = Predicates.or<UUID, IntegrationJob>(
            Predicates.equal<UUID, IntegrationJob>(INTEGRATION_STATUS, IntegrationStatus.IN_PROGRESS),
            Predicates.equal<UUID, IntegrationJob>(INTEGRATION_STATUS, IntegrationStatus.QUEUED)
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
        httpClient = OkHttpClient()

        executor.execute {
            semaphore.acquire()

            //load any jobs that were in progress or queued
            integrationJobs.entrySet(statusPredicate).forEach {
                jobQueue.put(QueuedIntegrationJob(it.key, it.value))
            }

            while (true) {
                if (semaphore.availablePermits() > 0) {
                    val job = jobQueue.take()
                    try {
                        loadCargo(job.jobId)
                    } catch (ex: Exception) {
                        logger.info(
                                "Encountered exception $ex when trying to start integration job with id ${job.jobId}"
                        )

                        job.integrationJob.integrationStatus = IntegrationStatus.FAILED_TO_START
                        integrationJobs[job.jobId] = job.integrationJob

                        integrations.getValue(job.integrationJob.integrationName).callbackUrls.ifPresent {
                            submitCallback(
                                    job.jobId,
                                    it,
                                    "Integration job with id ${job.jobId} failed with an exception $ex before starting"
                            )
                        }
                    }
                }
            }
        }
    }

    fun enqueueIntegrationJob(integrationName: String, integrationKey: UUID): UUID {
        val integration = integrations[integrationName] ?: throw IllegalStateException(
                "Integration with name $integrationName does not exist"
        )
        checkState(integrationKey == integration.key, "Integration key $integrationKey is incorrect")
        val integrationJob = IntegrationJob(integrationName, IntegrationStatus.QUEUED)
        val jobId = generateIntegrationJobId(integrationJob)
        jobQueue.put(QueuedIntegrationJob(jobId, integrationJob))
        integrationJobs[jobId] = integrationJob
        return jobId
    }

    private final fun loadCargo(jobId: UUID) {
        val integrationJob = integrationJobs.getValue(jobId)
        val integration = integrations.getValue(integrationJob.integrationName)

        //an integration object is expected to have a non-empty logEntitySetId,
        //a non-null flight, and non-null key in order for the integration to run successfully
        val flightPlan = mutableMapOf<Flight, Payload>()
        val tableColsToPrint = mutableMapOf<Flight, List<String>>()
        integration.flightPlanParameters.values.forEach {
            val srcDataSourceProperties = Properties()
            srcDataSourceProperties.putAll(it.source)
            val srcDataSource = getSrcDataSource(srcDataSourceProperties)
            val rateLimited = readRateLimit != 0
            val payload = JdbcPayload(readRateLimit.toDouble(), srcDataSource, it.sql, fetchSize, rateLimited)
            flightPlan[it.flight!!] = payload
            tableColsToPrint[it.flight!!] = it.sourcePrimaryKeyColumns
        }

        val generatePresignedUrlsFun: (List<S3EntityData>) -> List<String> = {
            val propertyTypesByEntitySetId = it.map { elem ->
                elem.entitySetId to entitySetManager.getPropertyTypesForEntitySet(elem.entitySetId)
            }.toMap()
            awsDataSinkService.generatePresignedUrls(it, propertyTypesByEntitySetId)
        }

        val destinationsMap = generateDestinationsMap(integration, missionParameters, generatePresignedUrlsFun)

        val shuttle = Shuttle(
                integration.environment,
                true,
                flightPlan,
                entitySets.values.associateBy { it.name },
                entityTypes.values.associateBy { it.id },
                propertyTypes.values.associateBy { it.type },
                destinationsMap,
                null,
                tableColsToPrint,
                missionParameters,
                StorageDestination.S3,
                blackbox,
                Optional.of(entitySets.getValue(integration.logEntitySetId.get())),
                Optional.of(jobId),
                idService,
                hazelcastInstance
        )

        executor.submit {
            semaphore.acquire()
            shuttle.launch(uploadBatchSize)
        }.addListener(Runnable {
            integration.callbackUrls.ifPresent {
                submitCallback(jobId, it, "Integration job with id $jobId succeeded! :D")
            }
            semaphore.release()
        }, executor)
    }

    fun pollIntegrationStatus(jobId: UUID): IntegrationStatus {
        return integrationJobs[jobId]?.integrationStatus ?: throw IllegalStateException(
                "Job Id $jobId is not assigned to an existing integration job"
        )
    }

    fun pollAllIntegrationStatuses(): Map<UUID, IntegrationJob> {
        return integrationJobs
    }

    fun deleteIntegrationJobStatus(jobId: UUID) {
        checkIntegrationJobExists(jobId)
        integrationJobs.delete(jobId)
    }

    fun createIntegrationDefinition(integrationName: String, integration: Integration): UUID {
        checkIntegrationDoesNotExist(integrationName)
        integration.callbackUrls.ifPresent {
            try {
                it.forEach { url ->
                    URL(url).toURI()
                }
            } catch (ex: Exception) {
                logger.error("Callback URL was not properly formatted")
            }
        }
        val logEntitySetId = createLogEntitySet(integrationName, integration)
        integration.logEntitySetId = Optional.of(logEntitySetId)
        integrations[integrationName] = integration
        return integration.key!!
    }

    fun readIntegrationDefinition(integrationName: String): Integration {
        return integrations[integrationName] ?: throw IllegalStateException(
                "Integration with name $integrationName does not exist."
        )
    }

    fun updateIntegrationDefinition(integrationName: String, integrationUpdate: IntegrationUpdate) {
        checkIntegrationExists(integrationName)
        integrations.executeOnKey(integrationName, UpdateIntegrationEntryProcessor(integrationUpdate))
    }

    fun deleteIntegrationDefinition(integrationName: String) {
        checkIntegrationExists(integrationName)
        integrations.remove(integrationName)
    }

    private fun checkIntegrationJobExists(jobId: UUID) {
        checkState(integrationJobs.containsKey(jobId), "Job Id $jobId is not assigned to an existing integration job")
    }

    private fun checkIntegrationExists(integrationName: String) {
        checkState(integrations.containsKey(integrationName), "Integration with name $integrationName does not exist.")
    }

    private fun checkIntegrationDoesNotExist(integrationName: String) {
        checkState(
                !integrations.containsKey(integrationName), "An integration with name $integrationName already exists."
        )

    }

    private fun getSrcDataSource(source: Properties): HikariDataSource {
        return HikariDataSource(HikariConfig(source))
    }

    private fun generateDestinationsMap(
            integration: Integration,
            missionParameters: MissionParameters,
            generatePresignedUrlsFun: (List<S3EntityData>) -> List<String>
    ): Map<StorageDestination, IntegrationDestination> {
        val s3BucketUrl = integration.s3bucket
        val dstDataSource = HikariDataSource(HikariConfig(missionParameters.postgres.config))
        integration.maxConnections.ifPresent { dstDataSource.maximumPoolSize = it }

        val pgDestination = PostgresDestination(
                entitySets.mapKeys { it.value.id },
                entityTypes,
                propertyTypes.mapKeys { it.value.id },
                dstDataSource
        )

        if (s3BucketUrl.isBlank()) {
            return mapOf(StorageDestination.POSTGRES to pgDestination)
        }

        val s3Api = decorateWithOpenLatticeFactories(Retrofit.Builder())
                .baseUrl(s3BucketUrl)
                .client(RetrofitFactory.okHttpClient().build())
                .build().create(S3Api::class.java)
        val s3Destination = PostgresS3Destination(pgDestination, s3Api, generatePresignedUrlsFun)
        return mapOf(StorageDestination.POSTGRES to pgDestination, StorageDestination.S3 to s3Destination)
    }

    private fun createLogEntitySet(integrationName: String, integration: Integration): UUID {
        val name = buildLogEntitySetName(integrationName)
        val contacts = integration.contacts
        val orgId = integration.organizationId
        val description = buildLogEntitySetDescription(integration.flightPlanParameters)
        val logEntitySet = EntitySet(
                entityTypeId = logEntityType.id,
                name = name,
                _title = name,
                _description = description,
                contacts = contacts.toMutableSet(),
                organizationId = orgId
        )
        return entitySetManager.createEntitySet(Principals.getCurrentUser(), logEntitySet)
    }

    private fun buildLogEntitySetName(integrationName: String): String {
        val name = "Integration logs for $integrationName"
        var nameAttempt = name
        var count = 1

        while (reservationService.isReserved(nameAttempt)) {
            nameAttempt = "${name}_$count"
            count++
        }

        return nameAttempt
    }

    private fun buildLogEntitySetDescription(flightPlanParameters: Map<String, FlightPlanParameters>): String {
        val flightDescriptionsClause = flightPlanParameters.values.joinToString(", ") {
            val flight = it.flight!!
            val tags = if (flight.tags.isEmpty()) {
                ""
            } else {
                " with tags [${flight.tags.joinToString(", ")}]"
            }
            return@joinToString "${flight.name}$tags"
        }

        return "Auto-generated entity set containing logs of the following flights: $flightDescriptionsClause"
    }

    private fun generateIntegrationJobId(integrationJob: IntegrationJob): UUID {
        var jobId = UUID.randomUUID()
        while (integrationJobs.putIfAbsent(jobId, integrationJob) != null) {
            jobId = UUID.randomUUID()
        }
        return jobId
    }

    private fun submitCallback(jobId: UUID, urls: List<String>, message: String) {
        urls.forEach {
            val url = URL(it)
            val body = FormBody.Builder()
                    .add("message", message)
                    .add("jobId", "$jobId")
                    .build()
            val request = Request.Builder()
                    .url(url)
                    .post(body)
                    .build()
            try {
                val response = httpClient.newCall(request).execute()
                response.body()?.close()
            } catch (ex: IOException) {
                logger.info(
                        "Encountered exception $ex when submitting callback to url $url for integration job with id $jobId"
                )
            }
        }
    }

}