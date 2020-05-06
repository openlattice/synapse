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

import com.auth0.client.auth.AuthAPI
import com.auth0.exception.Auth0Exception
import com.dataloom.mappers.ObjectMappers
import com.google.common.annotations.VisibleForTesting
import com.google.common.base.Suppliers
import com.google.common.collect.ImmutableMap
import com.openlattice.client.ApiClient
import com.openlattice.client.RetrofitFactory
import com.openlattice.data.S3Api
import com.openlattice.shuttle.destinations.IntegrationDestination
import com.openlattice.shuttle.destinations.StorageDestination
import com.openlattice.shuttle.destinations.PostgresDestination
import com.openlattice.shuttle.destinations.PostgresS3Destination
import com.openlattice.shuttle.destinations.RestDestination
import com.openlattice.shuttle.destinations.S3Destination
import com.openlattice.data.serializers.FullQualifiedNameJacksonSerializer
import com.openlattice.edm.EntitySet
import com.openlattice.edm.type.EntityType
import com.openlattice.edm.type.PropertyType
import com.openlattice.retrofit.RhizomeByteConverterFactory
import com.openlattice.retrofit.RhizomeCallAdapterFactory
import com.openlattice.retrofit.RhizomeJacksonConverterFactory
import com.openlattice.retrofit.RhizomeRetrofitCallException
import com.openlattice.shuttle.logs.Blackbox
import com.openlattice.shuttle.payload.Payload
import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import jodd.mail.Email
import jodd.mail.EmailAddress
import jodd.mail.MailServer
import org.apache.commons.lang3.exception.ExceptionUtils
import org.apache.olingo.commons.api.edm.FullQualifiedName
import org.slf4j.LoggerFactory
import retrofit2.Retrofit
import java.util.*
import java.util.concurrent.ExecutorService
import java.util.concurrent.TimeUnit
import java.util.function.Supplier
import kotlin.NoSuchElementException


private const val AUTH0_CLIENT_ID = "o8Y2U2zb5Iwo01jdxMN1W2aiN8PxwVjh"
private const val AUTH0_CLIENT_DOMAIN = "openlattice.auth0.com"
private const val AUTH0_CONNECTION = "Username-Password-Authentication"
private const val AUTH0_SCOPES = "openid email nickname roles user_id organizations"

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class MissionControl(
        private val environment: RetrofitFactory.Environment,
        authToken: Supplier<String>,
        s3BucketUrl: String,
        private val parameters: MissionParameters
) {

    constructor(
            environment: RetrofitFactory.Environment,
            username: String,
            password: String,
            s3BucketUrl: String,
            parameters: MissionParameters = MissionParameters.empty()
    ) : this(
            environment,
            Suppliers.memoizeWithExpiration({ getIdToken(username, password) }, 1, TimeUnit.HOURS),
            s3BucketUrl,
            parameters
    )

    companion object {
        private val logger = LoggerFactory.getLogger(MissionControl::class.java)
        private val client = buildClient(AUTH0_CLIENT_ID)
        private var emailConfiguration: Optional<EmailConfiguration> = Optional.empty()
        private var terminateOnSuccess = true

        init {
            Runtime.getRuntime().addShutdownHook(object : Thread() {
                override fun run() {
                    logger.info("The JVM is going through its normal shutdown process.")
                }
            })
        }

        @JvmStatic
        fun failWithBadInputs( message: String, ex: Throwable ) {
            logger.error("Shuttle encountered a problematic input!")
            logger.error(message)
            logger.error("Stacktrace: ", ex)
            ShuttleCliOptions.printHelp()
            kotlin.system.exitProcess(0)
        }

        @JvmStatic
        @Throws(Auth0Exception::class)
        fun getIdToken(username: String, password: String): String {
            return getIdToken(client, AUTH0_CONNECTION, username, password)
        }

        @JvmStatic
        fun continueAfterSuccess() {
            this.terminateOnSuccess = false
        }

        @JvmStatic
        @VisibleForTesting
        fun buildClient(clientId: String): AuthAPI {
            return AuthAPI(AUTH0_CLIENT_DOMAIN, clientId, "")
        }

        @JvmStatic
        @VisibleForTesting
        @Throws(Auth0Exception::class)
        fun getIdToken(auth0: AuthAPI, realm: String, username: String, password: String): String {
            return auth0
                    .login(username, password, realm)
                    .setScope(AUTH0_SCOPES)
                    .setAudience("https://api.openlattice.com")
                    .execute()
                    .idToken
        }

        @JvmStatic
        fun setEmailConfiguration(emailConfiguration: Optional<EmailConfiguration>) {
            this.emailConfiguration = emailConfiguration
        }

        @JvmStatic
        fun succeed() {
            logger.info(
                    "\n _____ _   _ _____  _____  _____ _____ _____ \n" +
                            "/  ___| | | /  __ \\/  __ \\|  ___/  ___/  ___|\n" +
                            "\\ `--.| | | | /  \\/| /  \\/| |__ \\ `--.\\ `--. \n" +
                            " `--. \\ | | | |    | |    |  __| `--. \\`--. \\\n" +
                            "/\\__/ / |_| | \\__/\\| \\__/\\| |___/\\__/ /\\__/ /\n" +
                            "\\____/ \\___/ \\____/ \\____/\\____/\\____/\\____/"
            )
            if (terminateOnSuccess) {
                kotlin.system.exitProcess(0)
            }
        }

        @JvmStatic
        fun fail(code: Int, flight: Flight, ex: Throwable, executors: List<ExecutorService> = listOf()): Nothing {
            logger.error(
                    "\n______ ___  _____ _     _   _______ _____ \n" +
                            "|  ___/ _ \\|_   _| |   | | | | ___ \\  ___|\n" +
                            "| |_ / /_\\ \\ | | | |   | | | | |_/ / |__  \n" +
                            "|  _||  _  | | | | |   | | | |    /|  __| \n" +
                            "| |  | | | |_| |_| |___| |_| | |\\ \\| |___ \n" +
                            "\\_|  \\_| |_/\\___/\\_____/\\___/\\_| \\_\\____/ \n" +
                            "                                         ",
                    ex
            )

            val errorInfo = if (ex is RhizomeRetrofitCallException) {
                "Server returned ${ex.code} with body: ${ex.body}."
            } else {
                "Something went wrong during client side processing. "
            }

            logger.error(errorInfo)

            emailConfiguration.ifPresentOrElse(
                    { emailConfiguration ->
                        logger.error(
                                "An error occurred during the integration sending e-mail notification.", ex
                        )
                        val stackTraceText = ExceptionUtils.getStackTrace(ex)

                        val errorEmail = "An error occurred while running integration ${flight.name}. $errorInfo \n" +
                                "The cause is ${ex.message} \n The stack trace is $stackTraceText"
                        val emailAddresses = emailConfiguration.notificationEmails
                                .map { address -> EmailAddress.of(address) }
                                .toTypedArray()
                        val email = Email.create()
                                .from(emailConfiguration.fromEmail)
                                .subject("Integration error in $flight.name")
                                .textMessage(errorEmail)
                        emailConfiguration.notificationEmails
                                .map { address -> EmailAddress.of(address) }
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

            executors.forEach { it.shutdownNow() }

            kotlin.system.exitProcess(code)
        }
    }

    //TODO: We need to figure out why this isn't registered or a better way of controlling watch ObjectMapper is used
    //by retrofit clients.
    init {
        FullQualifiedNameJacksonSerializer.registerWithMapper(ObjectMappers.getJsonMapper())
        if (environment == RetrofitFactory.Environment.PRODUCTION) {
            fail(
                    -999, Flight.newFlight().done(), Throwable(
                    "PRODUCTION is not a valid integration environment. The valid environments are PROD_INTEGRATION and LOCAL"
                )
            )
        }
    }

    private val apiClient = ApiClient(environment) { authToken.get() }
    private val edmApi = apiClient.edmApi
    private val entitySetsApi = apiClient.entitySetsApi
    private val dataApi = apiClient.dataApi
    private val dataIntegrationApi = apiClient.dataIntegrationApi

    private val s3Api = if (s3BucketUrl.isBlank()) null else Retrofit.Builder()
            .baseUrl(s3BucketUrl)
            .addConverterFactory(RhizomeByteConverterFactory())
            .addConverterFactory(RhizomeJacksonConverterFactory(ObjectMappers.getJsonMapper()))
            .addCallAdapterFactory(RhizomeCallAdapterFactory())
            .client(RetrofitFactory.okHttpClient().build())
            .build().create(S3Api::class.java)

    private val entitySets: MutableMap<String, EntitySet>
    private val entityTypes: MutableMap<UUID, EntityType>
    private val propertyTypes: MutableMap<FullQualifiedName, PropertyType>
    private val propertyTypesById: Map<UUID, PropertyType>

    private val binaryStorageDestination = if (s3BucketUrl.isBlank()) {
        StorageDestination.REST
    } else {
        StorageDestination.S3
    }

    private val integrationDestinations: Map<StorageDestination, IntegrationDestination>

    init {
        try {
            entitySets = entitySetsApi.getEntitySets().filter { it as EntitySet? != null }.map { it.name to it }.toMap().toMutableMap()
            entityTypes = edmApi.entityTypes.map { it.id to it }.toMap().toMutableMap()
            propertyTypes = edmApi.propertyTypes.map { it.type to it }.toMap().toMutableMap()
            propertyTypesById = propertyTypes.mapKeys { it.value.id }
        } catch ( thrown: Throwable ) {
            MissionControl.fail(1, Flight.newFlight().done(), thrown)
        }
        val destinations = mutableMapOf<StorageDestination, IntegrationDestination>()
        destinations[StorageDestination.REST] = RestDestination(dataApi)
        val generatePresignedUrlsFun = dataIntegrationApi::generatePresignedUrls


        if (parameters.postgres.enabled) {
            val pgDestination = PostgresDestination(
                    entitySets.mapKeys { it.value.id },
                    entityTypes,
                    propertyTypes.mapKeys { it.value.id },
                    HikariDataSource(HikariConfig(parameters.postgres.config))
            )

            destinations[StorageDestination.POSTGRES] = pgDestination

            if (s3BucketUrl.isNotBlank()) {
                destinations[StorageDestination.S3] = PostgresS3Destination(
                        pgDestination, s3Api!!, generatePresignedUrlsFun
                )
            }
        } else {
            destinations[StorageDestination.REST] = RestDestination(dataApi)

            if (s3BucketUrl.isNotBlank()) {
                destinations[StorageDestination.S3] = S3Destination(dataApi, s3Api!!, generatePresignedUrlsFun)
            }
        }

        integrationDestinations = destinations.toMap()
    }


    fun prepare(
            flightPlan: Map<Flight, Payload>,
            createEntitySets: Boolean = false,
            primaryKeyCols: Map<Flight, List<String>> = mapOf(),
            contacts: Set<String> = setOf()
    ): Shuttle {
        if (createEntitySets) {
            createMissingEntitySets(flightPlan, contacts)
        }
        ensureValidIntegration(flightPlan)

        return Shuttle(
                environment,
                false,
                flightPlan,
                entitySets,
                entityTypes,
                propertyTypes,
                integrationDestinations,
                dataIntegrationApi,
                primaryKeyCols,
                parameters,
                binaryStorageDestination,
                Blackbox.empty(),
                Optional.empty(),
                Optional.empty(),
                null,
                null
        )
    }

    private fun createMissingEntitySets(flightPlan: Map<Flight, Payload>, contacts: Set<String>) {
        flightPlan.keys.forEach {
            check(it.organizationId.isPresent) { "Flight ${it.name} cannot create missing entity sets because organizationId is not present" }
            val organizationId = it.organizationId.get()
            it.entities.forEach { entityDefinition ->
                if (!entitySets.containsKey(entityDefinition.entitySetName)) {
                    val fqn = entityDefinition.getEntityTypeFqn()
                    val entitySet = EntitySet(
                            entityDefinition.getId().orElse(UUID.randomUUID()),
                            edmApi.getEntityTypeId(fqn.namespace, fqn.name),
                            entityDefinition.getEntitySetName(),
                            entityDefinition.getEntitySetName(),
                            entityDefinition.getEntitySetName(),
                            contacts.toMutableSet(),
                            mutableSetOf(),
                            organizationId
                    )
                    val entitySetId = entitySetsApi
                            .createEntitySets(setOf(entitySet))
                            .getValue(entityDefinition.entitySetName)
                    check(entitySetId == entitySet.id) { "Submitted entity set id does not match return." }
                    entitySets[entityDefinition.entitySetName] = entitySet
                }
            }
            it.associations.forEach { associationDefinition ->
                if (!entitySets.containsKey(associationDefinition.entitySetName)) {
                    val fqn = associationDefinition.getEntityTypeFqn()
                    val entityTypeId = edmApi.getEntityTypeId(fqn.namespace, fqn.name)
                    val entitySet = EntitySet(
                            associationDefinition.getId().orElse(UUID.randomUUID()),
                            entityTypeId,
                            associationDefinition.getEntitySetName(),
                            associationDefinition.getEntitySetName(),
                            associationDefinition.getEntitySetName(),
                            contacts.toMutableSet(),
                            mutableSetOf(),
                            organizationId
                    )
                    val entitySetId = entitySetsApi
                            .createEntitySets(setOf(entitySet))
                            .getValue(associationDefinition.entitySetName)
                    check(entitySetId == entitySet.id) { "Submitted entity set id does not match return." }
                    entitySets[associationDefinition.entitySetName] = entitySet
                }
            }
        }
    }


    private fun ensureValidIntegration(flight: Map<Flight, Payload>) {
        flight.keys.forEach { flight ->

            flight
                    .entities
                    .forEach { entityDefinition ->
                        assertPropertiesMatchEdm(entityDefinition.entitySetName, entityDefinition.properties)
                    }

            flight
                    .associations
                    .forEach { associationDefinition ->
                        assertPropertiesMatchEdm(associationDefinition.entitySetName, associationDefinition.properties)
                    }
        }
    }

    private fun assertPropertiesMatchEdm(entitySetName: String, properties: Collection<PropertyDefinition>) {
        check(entitySets.contains(entitySetName)) { "Entity set $entitySetName does not exist." }
        val missingProperties = properties.filter { !propertyTypes.contains(it.fullQualifiedName) }
        if (missingProperties.isNotEmpty()) {
            missingProperties.forEach {
                logger.error("The fqn ${it.fullQualifiedName}is not associated with any property type ")
            }
            throw NoSuchElementException("The following property definition fqn were not found: $missingProperties")
        }

        val requiredPropertyTypes = properties.map { propertyTypes[it.fullQualifiedName]!!.id }.toSet()
        val actualPropertyTypes = entityTypes[entitySets[entitySetName]!!.entityTypeId]!!.properties

        val missingPropertyTypes = requiredPropertyTypes - actualPropertyTypes
        missingPropertyTypes.forEach {
            logger.error(
                    "Entity set {} does not contain any property type with FQN: {}",
                    entitySetName,
                    propertyTypesById.getValue(it).type
            )
            throw IllegalStateException(
                    "Property types $missingPropertyTypes not defined for entity set $entitySetName"
            )
        }
    }
}

