/*
 * Copyright (C) 2018. OpenLattice, Inc.
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

package com.openlattice.shuttle

import com.dataloom.mappers.ObjectMappers
import com.google.common.base.Preconditions
import com.google.common.base.Stopwatch
import com.google.common.collect.ImmutableList
import com.google.common.collect.Queues
import com.openlattice.ApiUtil
import com.openlattice.client.RetrofitFactory
import com.openlattice.data.DataIntegrationApi
import com.openlattice.data.EntityKey
import com.openlattice.data.integration.*
import com.openlattice.edm.EntitySet
import com.openlattice.edm.type.EntityType
import com.openlattice.edm.type.PropertyType
import com.openlattice.shuttle.ShuttleCli.Companion.CONFIGURATION
import com.openlattice.shuttle.ShuttleCli.Companion.CREATE
import com.openlattice.shuttle.ShuttleCli.Companion.CSV
import com.openlattice.shuttle.ShuttleCli.Companion.DATASOURCE
import com.openlattice.shuttle.ShuttleCli.Companion.ENVIRONMENT
import com.openlattice.shuttle.ShuttleCli.Companion.FETCHSIZE
import com.openlattice.shuttle.ShuttleCli.Companion.FLIGHT
import com.openlattice.shuttle.ShuttleCli.Companion.FROM_EMAIL
import com.openlattice.shuttle.ShuttleCli.Companion.FROM_EMAIL_PASSWORD
import com.openlattice.shuttle.ShuttleCli.Companion.HELP
import com.openlattice.shuttle.ShuttleCli.Companion.NOTIFICATION_EMAILS
import com.openlattice.shuttle.ShuttleCli.Companion.PASSWORD
import com.openlattice.shuttle.ShuttleCli.Companion.S3
import com.openlattice.shuttle.ShuttleCli.Companion.SMTP_SERVER
import com.openlattice.shuttle.ShuttleCli.Companion.SMTP_SERVER_PORT
import com.openlattice.shuttle.ShuttleCli.Companion.SQL
import com.openlattice.shuttle.ShuttleCli.Companion.TOKEN
import com.openlattice.shuttle.ShuttleCli.Companion.UPLOAD_SIZE
import com.openlattice.shuttle.ShuttleCli.Companion.USER
import com.openlattice.shuttle.config.IntegrationConfig
import com.openlattice.shuttle.payload.JdbcPayload
import com.openlattice.shuttle.payload.Payload
import com.openlattice.shuttle.payload.SimplePayload
import jodd.mail.Email
import jodd.mail.EmailAddress
import jodd.mail.MailServer
import org.apache.commons.cli.CommandLine
import org.apache.commons.lang3.StringUtils
import org.apache.commons.lang3.exception.ExceptionUtils
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind
import org.apache.olingo.commons.api.edm.FullQualifiedName
import org.slf4j.LoggerFactory
import java.io.File
import java.lang.System.exit
import java.util.*
import java.util.concurrent.BlockingQueue
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.locks.ReentrantLock
import java.util.function.Supplier
import java.util.stream.Stream
import kotlin.streams.asSequence
import kotlin.streams.asStream

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */

private val logger = LoggerFactory.getLogger(ShuttleCli::class.java)
const val DEFAULT_UPLOAD_SIZE = 100000

fun main(args: Array<String>) {

    val configuration: IntegrationConfig
    val environment: RetrofitFactory.Environment
    val cl = ShuttleCli.parseCommandLine(args)
    val payload: Payload
    val flight: Flight
    val createEntitySets: Boolean
    val contacts: Set<String>


    if (cl.hasOption(HELP)) {
        ShuttleCli.printHelp()
        return
    }

    if (cl.hasOption(FLIGHT)) {
        flight = ObjectMappers.getYamlMapper().readValue(File(cl.getOptionValue(FLIGHT)), Flight::class.java)

    } else {
        System.err.println("A flight is required in order to run shuttle.")
        ShuttleCli.printHelp()
        return
    }

    //You can have a configuration without any JDBC datasrouces
    when {
        cl.hasOption(CONFIGURATION) -> {
            configuration = ObjectMappers.getYamlMapper()
                    .readValue(File(cl.getOptionValue(CONFIGURATION)), IntegrationConfig::class.java)

            if (!cl.hasOption(DATASOURCE)) {
                // check datasource presence
                System.out.println("Datasource must be specified when doing a JDBC datasource based integration.")
                ShuttleCli.printHelp()
                return
            }
            if (!cl.hasOption(SQL)) {
                // check SQL presence
                System.out.println("SQL expression must be specified when doing a JDBC datasource based integration.")
                ShuttleCli.printHelp()
                return
            }
            if (cl.hasOption(CSV)) {
                // check csv ABsence
                System.out.println("Cannot specify CSV datasource and JDBC datasource simultaneously.")
                ShuttleCli.printHelp()
                return
            }

            // get JDBC payload
            val hds = configuration.getHikariDatasource(cl.getOptionValue(DATASOURCE))
            val sql = cl.getOptionValue(SQL)

            payload = if (cl.hasOption(FETCHSIZE)) {
                val fetchSize = cl.getOptionValue(FETCHSIZE).toInt()
                logger.info("Using a fetch size of $fetchSize")
                JdbcPayload(hds, sql, fetchSize)
            } else {
                JdbcPayload(hds, sql)
            }

        }
        cl.hasOption(CSV) -> {// get csv payload
            payload = SimplePayload(cl.getOptionValue(CSV))
        }
        else -> {
            System.err.println("At least one valid data source must be specified.")
            ShuttleCli.printHelp()
            exit(1)
            return
        }
    }


    environment = if (cl.hasOption(ENVIRONMENT)) {
        RetrofitFactory.Environment.valueOf(cl.getOptionValue(ENVIRONMENT).toUpperCase())
    } else {
        RetrofitFactory.Environment.PRODUCTION
    }


    val s3BucketUrl = if (cl.hasOption(S3)) {
        val bucketCategory = cl.getOptionValue(S3)
        check(bucketCategory.toUpperCase() in setOf("TEST", "PRODUCTION")) { "Invalid option $bucketCategory for $S3." }
        when (bucketCategory) {
            "TEST" -> "https://tempy-media-storage.s3-website-us-gov-west-1.amazonaws.com"
            "PRODUCTION" -> "http://openlattice-media-storage.s3-website-us-gov-west-1.amazonaws.com"
            else -> "https://tempy-media-storage.s3-website-us-gov-west-1.amazonaws.com"
        }
    } else {
        "https://tempy-media-storage.s3-website-us-gov-west-1.amazonaws.com"
    }

    //TODO: Use the right method to select the JWT token for the appropriate environment.

    val missionControl = when {
        cl.hasOption(TOKEN) -> {
            Preconditions.checkArgument(!cl.hasOption(PASSWORD))
            val jwt = cl.getOptionValue(TOKEN)
            MissionControl(environment, Supplier { jwt }, s3BucketUrl)
        }
        cl.hasOption(USER) -> {
            Preconditions.checkArgument(cl.hasOption(PASSWORD))
            val user = cl.getOptionValue(USER)
            val password = cl.getOptionValue(PASSWORD)
            MissionControl(environment, user, password, s3BucketUrl)
        }
        else -> {
            System.err.println("User or token must be provided for authentication.")
            ShuttleCli.printHelp()
            return
        }
    }

    createEntitySets = cl.hasOption(CREATE)
    if (createEntitySets) {
        if (environment == RetrofitFactory.Environment.PRODUCTION) {
            throw IllegalArgumentException(
                    "It is not allowed to automatically create entity sets on " +
                            "${RetrofitFactory.Environment.PRODUCTION} environment"
            )
        }

        contacts = cl.getOptionValues(CREATE).toSet()
        if (contacts.isEmpty()) {
            System.err.println("Can't create entity sets automatically without contacts provided")
            ShuttleCli.printHelp()
            return
        }
    } else {
        contacts = setOf()
    }

    val uploadBatchSize = if (cl.hasOption(UPLOAD_SIZE)) {
        cl.getOptionValue(UPLOAD_SIZE).toInt()
    } else {
        DEFAULT_UPLOAD_SIZE
    }

    val emailConfiguration = getEmailConfiguration(cl)

    val flightPlan = mapOf(flight to payload)

    try {
        val shuttle = missionControl.prepare(flightPlan, createEntitySets, contacts)
        shuttle.launch(uploadBatchSize)
        MissionControl.succeed()
    } catch (ex: Exception) {
        emailConfiguration.ifPresentOrElse(
                { emailConfiguration ->
                    logger.error("An error occurred during the integration sending e-mail notification.", ex)
                    val stackTraceText = ExceptionUtils.getStackTrace(ex)
                    val errorEmail = "An error occurred while running an integration. The integration name is $flight.name. \n" +
                            "The cause is ${ex.message} \n The stack trace is $stackTraceText"
                    val emailAddresses = emailConfiguration.notificationEmails
                            .map(EmailAddress::of)
                            .toTypedArray()
                    val email = Email.create()
                            .from(emailConfiguration.fromEmail)
                            .subject("Integration error in $flight.name")
                            .textMessage(errorEmail)
                    emailConfiguration.notificationEmails
                            .map(EmailAddress::of)
                            .forEach { emailAddress -> email.to(emailAddress) }

                    val smtpServer = MailServer.create()
                            .ssl(true)
                            .host(emailConfiguration.smtpServer)
                            .port(emailConfiguration.smtpServerPort)
                            .auth(
                                    emailConfiguration.fromEmail,
                                    emailConfiguration.fromEmailPassword
                            )
                            .buildSmtpMailServer()

                    val session = smtpServer.createSession()
                    session.open()
                    session.sendMail(email)
                    session.close()

                }, { logger.error("An error occurred during the integration.", ex) })
        MissionControl.fail()
    }
}

fun getEmailConfiguration(cl: CommandLine): Optional<EmailConfiguration> {
    return when {
        cl.hasOption(SMTP_SERVER) -> {
            val smtpServer = cl.getOptionValue(SMTP_SERVER)
            val smtpServerPort = if (cl.hasOption(SMTP_SERVER_PORT)) {
                cl.getOptionValue(SMTP_SERVER_PORT).toInt()
            } else {
                System.err.println("No smtp server port was specified")
                ShuttleCli.printHelp()
                kotlin.system.exitProcess(1)
            }

            val notificationEmails = cl.getOptionValues(NOTIFICATION_EMAILS).toSet()
            if (notificationEmails.isEmpty()) {
                System.err.println("No notification e-mails were actually specified.")
                ShuttleCli.printHelp()
                kotlin.system.exitProcess(1)
            }

            val fromEmail = if (cl.hasOption(FROM_EMAIL)) {
                cl.getOptionValue(FROM_EMAIL)
            } else {
                System.err.println("If notification e-mails are specified must also specify a sending account.")
                ShuttleCli.printHelp()
                kotlin.system.exitProcess(1)
            }

            val fromEmailPassword = if (cl.hasOption(FROM_EMAIL_PASSWORD)) {
                cl.getOptionValue(FROM_EMAIL_PASSWORD)
            } else {
                System.err.println(
                        "If notification e-mails are specified must also specify an e-mail password for sending account."
                )
                ShuttleCli.printHelp()
                kotlin.system.exitProcess(1)
            }
            Optional.of(
                    EmailConfiguration(fromEmail, fromEmailPassword, notificationEmails, smtpServer, smtpServerPort)
            )
        }
        cl.hasOption(SMTP_SERVER_PORT) -> {
            System.err.println("Port was specified, no smtp server was specified")
            ShuttleCli.printHelp()
            kotlin.system.exitProcess(1)
        }
        cl.hasOption(FROM_EMAIL) -> {
            System.err.println("From e-mail was specified, no smtp server was specified")
            ShuttleCli.printHelp()
            kotlin.system.exitProcess(1)
        }
        cl.hasOption(FROM_EMAIL_PASSWORD) -> {
            System.err.println("From e-mail password was specified, no smtp server was specified")
            ShuttleCli.printHelp()
            kotlin.system.exitProcess(1)
        }
        cl.hasOption(NOTIFICATION_EMAILS) -> {
            System.err.println("Notification e-mails were specified, no smtp server was specified")
            ShuttleCli.printHelp()
            kotlin.system.exitProcess(1)
        }
        else -> Optional.empty()
    }

}


/**
 *
 * This is the primary class for driving an integration. It is designed to cache all
 */
class Shuttle(
        private val flightPlan: Map<Flight, Payload>,
        private val entitySets: Map<String, EntitySet>,
        private val entityTypes: Map<UUID, EntityType>,
        private val propertyTypes: Map<FullQualifiedName, PropertyType>,
        private val propertyTypesById: Map<UUID, PropertyType>,
        private val integrationDestinations: Map<StorageDestination, IntegrationDestination>,
        private val dataIntegrationApi: DataIntegrationApi
) {
    companion object {
        private val logger = LoggerFactory.getLogger(Shuttle::class.java)
        private val uploadingExecutor = Executors.newSingleThreadExecutor()
    }

    private val updateTypes = flightPlan.keys.flatMap { flight ->
        flight.entities.map { entitySets[it.entitySetName]!!.id to it.updateType } +
                flight.associations.map { entitySets[it.entitySetName]!!.id to it.updateType }
    }.toMap()


    fun launch(uploadBatchSize: Int): Long {
        val sw = Stopwatch.createStarted()
        val total = flightPlan.entries.map { entry ->
            logger.info("Launching flight: {}", entry.key.name)
            val count = takeoff(entry.key, entry.value.payload, uploadBatchSize)
            logger.info("Finished flight: {}", entry.key.name)
            count
        }.sum()
        logger.info("Executed {} entity writes in flight plan in {} ms.", total, sw.elapsed(TimeUnit.MILLISECONDS))
        return total
    }

    /**
     * This function works under the assumption that the set returned from key is a unmodifiable linked hash set.
     */
    private fun getKeys(entitySetName: String): Set<UUID> {
        return entityTypes[entitySets[entitySetName]!!.entityTypeId]!!.key
    }

    /**
     * By default, the entity id is generated as a concatenation of the entity set id and all the key property values.
     * This is guaranteed to be unique for each unique set of primary key values. For this to work correctly it is very
     * important that Stream remain ordered. Ordered != sequential vs parallel.
     *
     * @param key A stable set of ordered primary key property type ids to use for default entity key generation.
     */
    private fun generateDefaultEntityId(
            key: Set<UUID>,
            properties: Map<UUID, Set<Any>>
    ): String {
        return ApiUtil.generateDefaultEntityId(key.stream(), properties)
    }

    private fun takeoff(flight: Flight, payload: Stream<Map<String, Any>>, uploadBatchSize: Int): Long {
        val integratedEntities = mutableMapOf<StorageDestination, AtomicLong>().withDefault{AtomicLong(0L)}
        val integratedEdges = mutableMapOf<StorageDestination, AtomicLong>().withDefault{AtomicLong(0L)}
        val integrationQueue = Queues
                .newArrayBlockingQueue<List<Map<String, Any>>>(
                        Math.max(2, 2 * (Runtime.getRuntime().availableProcessors() - 2))
                )
        val sw = Stopwatch.createStarted()

        uploadingExecutor.execute {
            try {
                Stream.generate { integrationQueue.take() }.parallel()
                        .map { batch -> impulse(flight, batch) }
                        .forEach { batch ->
                            val entityKeys = (batch.entities.flatMap { e -> e.value.map { it.key } }
                                    + batch.associations.flatMap { it.value.map { assoc -> assoc.key } }).toSet()
                            val entityKeyIds = entityKeys.zip(dataIntegrationApi.getEntityKeyIds(entityKeys)).toMap()


                            integrationDestinations.forEach { (storageDestination, integrationDestination) ->
                                if (batch.entities.containsKey(storageDestination)) {
                                    integratedEntities.getOrPut(storageDestination) { AtomicLong(0) }.addAndGet(
                                            integrationDestination.integrateEntities(
                                                    batch.entities.getValue(storageDestination),
                                                    entityKeyIds,
                                                    updateTypes
                                            )
                                    )
                                }

                                if (batch.associations.containsKey(storageDestination)) {
                                    integratedEdges.getOrPut(storageDestination) { AtomicLong(0) }.addAndGet(
                                            integrationDestination.integrateAssociations(
                                                    batch.associations.getValue(storageDestination),
                                                    entityKeyIds,
                                                    updateTypes
                                            )
                                    )
                                }
                            }
                            logger.info("Current entities progress: {}", integratedEntities)
                            logger.info("Current edges progress: {}", integratedEdges)
                        }
            } catch (ex: Exception) {
                logger.error("Integration failure. ", ex)
                MissionControl.fail(1)
            }
        }

        payload.asSequence()
                .chunked(uploadBatchSize)
                .forEach {
                    integrationQueue.put(it)
                }
        //Wait on upload thread to finish emptying queue.
        while( integrationQueue.isNotEmpty() ) {
            Thread.sleep(1000)
        }

        return StorageDestination.values().map {
            logger.info(
                    "Integrated {} entities and {} edges in {} ms for flight {} to {}",
                    integratedEntities.getValue(it),
                    integratedEdges.getValue(it),
                    sw.elapsed(TimeUnit.MILLISECONDS),
                    flight.name,
                    it.name
            )
            integratedEntities.getValue(it).get() + integratedEdges.getValue(it).get()
        }.sum()
    }

    private fun impulse(flight: Flight, batch: List<Map<String, Any>>): AddressedDataHolder {
        val addressedDataHolder = AddressedDataHolder(mutableMapOf(), mutableMapOf())

        batch.forEach { row ->
            val aliasesToEntityKey = mutableMapOf<String, EntityKey>()
            val wasCreated = mutableMapOf<String, Boolean>()
            if (flight.condition.isPresent && !(flight.valueMapper.apply(row) as Boolean)) {
                return@forEach
            }
            for (entityDefinition in flight.entities) {
                val condition = if (entityDefinition.condition.isPresent) {
                    entityDefinition.valueMapper.apply(row) as Boolean
                } else {
                    true
                }

                val entitySetId = entitySets[entityDefinition.entitySetName]!!.id
                val properties = mutableMapOf<UUID, MutableSet<Any>>()
                val addressedProperties = mutableMapOf<StorageDestination, MutableMap<UUID, MutableSet<Any>>>()
                for (propertyDefinition in entityDefinition.properties) {
                    val propertyValue = propertyDefinition.propertyValue.apply(row)
                    if (propertyValue != null &&
                            ((propertyValue !is String) || propertyValue.isNotBlank())) {
                        val storageDestination = propertyDefinition.storageDestination.orElseGet {
                            when (propertyTypes[propertyDefinition.fullQualifiedName]!!.datatype) {
                                EdmPrimitiveTypeKind.Binary -> StorageDestination.S3
                                else -> StorageDestination.REST
                            }
                        }

                        val propertyId = propertyTypes[propertyDefinition.fullQualifiedName]!!.id

                        val propertyValueAsCollection: Collection<Any> =
                                if (propertyValue is Collection<*>) propertyValue as Collection<Any>
                                else ImmutableList.of(propertyValue)

                        addressedProperties
                                .getOrPut(storageDestination) { mutableMapOf() }
                                .getOrPut(propertyId) { mutableSetOf() }
                                .addAll(propertyValueAsCollection)
                        properties.getOrPut(propertyId) { mutableSetOf() }.addAll(propertyValueAsCollection)
                    }
                }

                /*
                 * For entityId generation to work correctly it is very important that Stream remain ordered.
                 * Ordered != sequential vs parallel.
                 */

                val entityId = entityDefinition.generator
                        .map { it.apply(row) }
                        .orElseGet {
                            generateDefaultEntityId(getKeys(entityDefinition.entitySetName), properties)
                        }

                if (StringUtils.isNotBlank(entityId) and condition and properties.isNotEmpty()) {
                    val key = EntityKey(entitySetId, entityId)
                    aliasesToEntityKey[entityDefinition.alias] = key
                    addressedProperties.forEach { storageDestination, data ->
                        addressedDataHolder.entities
                                .getOrPut(storageDestination) { mutableSetOf() }
                                .add(Entity(key, data))
                    }
                    wasCreated[entityDefinition.alias] = true
                } else {
                    wasCreated[entityDefinition.alias] = false
                }
            }

            for (associationDefinition in flight.associations) {

                if (associationDefinition.condition.isPresent &&
                        !(associationDefinition.valueMapper.apply(row) as Boolean)) {
                    continue
                }

                if (!wasCreated.containsKey(associationDefinition.dstAlias)) {
                    logger.error(
                            "Destination " + associationDefinition.dstAlias
                                    + " cannot be found to construct association " + associationDefinition.alias
                    )
                }

                if (!wasCreated.containsKey(associationDefinition.srcAlias)) {
                    logger.error(
                            ("Source " + associationDefinition.srcAlias
                                    + " cannot be found to construct association " + associationDefinition.alias)
                    )
                }
                if ((wasCreated[associationDefinition.srcAlias]!! && wasCreated[associationDefinition.dstAlias]!!)) {

                    val entitySetId = entitySets.getValue(associationDefinition.entitySetName).id
                    val properties = mutableMapOf<UUID, MutableSet<Any>>()
                    val addressedProperties = mutableMapOf<StorageDestination, MutableMap<UUID, MutableSet<Any>>>()

                    for (propertyDefinition in associationDefinition.properties) {
                        val propertyValue = propertyDefinition.propertyValue.apply(row)
                        if (propertyValue != null &&
                                ((propertyValue !is String) || propertyValue.isNotBlank())) {

                            val storageDestination = propertyDefinition.storageDestination.orElseGet {
                                when (propertyTypes.getValue(propertyDefinition.fullQualifiedName).datatype) {
                                    EdmPrimitiveTypeKind.Binary -> StorageDestination.S3
                                    else -> StorageDestination.REST
                                }
                            }

                            val propertyId = propertyTypes.getValue(propertyDefinition.fullQualifiedName).id

                            val propertyValueAsCollection: Collection<Any> =
                                    if (propertyValue is Collection<*>) propertyValue as Collection<Any>
                                    else ImmutableList.of(propertyValue)

                            addressedProperties
                                    .getOrPut(storageDestination) { mutableMapOf() }
                                    .getOrPut(propertyId) { mutableSetOf() }
                                    .addAll(propertyValueAsCollection)
                            properties.getOrPut(propertyId) { mutableSetOf() }.addAll(propertyValueAsCollection)
                        }
                    }

                    val entityId = associationDefinition.generator
                            .map { it.apply(row) }
                            .orElseGet {
                                ApiUtil.generateDefaultEntityId(
                                        getKeys(associationDefinition.entitySetName).stream(),
                                        properties
                                )
                            }

                    if (StringUtils.isNotBlank(entityId)) {
                        val key = EntityKey(entitySetId, entityId)
                        val src = aliasesToEntityKey[associationDefinition.srcAlias]
                        val dst = aliasesToEntityKey[associationDefinition.dstAlias]
                        addressedProperties.forEach { storageDestination, data ->
                            addressedDataHolder.associations
                                    .getOrPut(storageDestination) { mutableSetOf() }
                                    .add(Association(key, src, dst, data))

                        }
                    } else {
                        logger.error(
                                "Encountered blank entity id for entity set {}",
                                associationDefinition.entitySetName
                        )
                    }
                }
            }
        }
        return addressedDataHolder
    }
}
