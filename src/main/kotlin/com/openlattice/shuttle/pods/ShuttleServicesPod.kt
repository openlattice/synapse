package com.openlattice.shuttle.pods

import com.amazonaws.services.s3.AmazonS3
import com.auth0.client.mgmt.ManagementAPI
import com.codahale.metrics.MetricRegistry
import com.dataloom.mappers.ObjectMappers
import com.geekbeast.hazelcast.HazelcastClientProvider
import com.google.common.eventbus.EventBus
import com.hazelcast.core.HazelcastInstance
import com.kryptnostic.rhizome.configuration.ConfigurationConstants
import com.kryptnostic.rhizome.configuration.amazon.AmazonLaunchConfiguration
import com.kryptnostic.rhizome.configuration.service.ConfigurationService
import com.openlattice.ResourceConfigurationLoader
import com.openlattice.assembler.Assembler
import com.openlattice.assembler.AssemblerConfiguration
import com.openlattice.assembler.AssemblerConnectionManager
import com.openlattice.assembler.pods.AssemblerConfigurationPod
import com.openlattice.auditing.AuditingConfiguration
import com.openlattice.auth0.Auth0Pod
import com.openlattice.auth0.Auth0TokenProvider
import com.openlattice.auth0.AwsAuth0TokenProvider
import com.openlattice.authentication.Auth0Configuration
import com.openlattice.authorization.*
import com.openlattice.authorization.initializers.AuthorizationInitializationDependencies
import com.openlattice.authorization.initializers.AuthorizationInitializationTask
import com.openlattice.authorization.mapstores.ResolvedPrincipalTreesMapLoader
import com.openlattice.authorization.mapstores.SecurablePrincipalsMapLoader
import com.openlattice.conductor.rpc.ConductorConfiguration
import com.openlattice.data.ids.PostgresEntityKeyIdService
import com.openlattice.data.storage.ByteBlobDataManager
import com.openlattice.data.storage.aws.AwsDataSinkService
import com.openlattice.data.storage.partitions.PartitionManager
import com.openlattice.datastore.services.EdmService
import com.openlattice.datastore.services.EntitySetService
import com.openlattice.edm.properties.PostgresTypeManager
import com.openlattice.edm.schemas.manager.HazelcastSchemaManager
import com.openlattice.hazelcast.mapstores.shuttle.IntegrationJobsMapstore
import com.openlattice.hazelcast.mapstores.shuttle.IntegrationsMapstore
import com.openlattice.ids.HazelcastIdGenerationService
import com.openlattice.ids.HazelcastLongIdService
import com.openlattice.notifications.sms.PhoneNumberService
import com.openlattice.organizations.HazelcastOrganizationService
import com.openlattice.organizations.OrganizationMetadataEntitySetsService
import com.openlattice.organizations.roles.HazelcastPrincipalService
import com.openlattice.organizations.roles.SecurePrincipalsManager
import com.openlattice.organizations.tasks.OrganizationsInitializationTask
import com.openlattice.postgres.external.ExternalDatabaseConnectionManager
import com.openlattice.postgres.external.ExternalDatabasePermissionsManager
import com.openlattice.shuttle.IntegrationService
import com.openlattice.shuttle.MissionParameters
import com.openlattice.shuttle.logs.Blackbox
import com.openlattice.tasks.PostConstructInitializerTaskDependencies
import com.openlattice.users.Auth0UserListingService
import com.openlattice.users.LocalUserListingService
import com.openlattice.users.UserListingService
import com.openlattice.users.export.Auth0ApiExtension
import com.zaxxer.hikari.HikariDataSource
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Import
import org.springframework.context.annotation.Profile
import java.io.IOException
import javax.annotation.PostConstruct
import javax.inject.Inject

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
@Configuration
@Import(
        AssemblerConfigurationPod::class,
        Auth0Pod::class
)
class ShuttleServicesPod {
    private val logger = LoggerFactory.getLogger(ShuttleServicesPod::class.java)

    @Inject
    private lateinit var hds: HikariDataSource

    @Inject
    private lateinit var hazelcastInstance: HazelcastInstance

    @Inject
    private lateinit var eventBus: EventBus

    @Inject
    private lateinit var missionParametersConfiguration: MissionParameters

    @Inject
    private lateinit var hazelcastClientProvider: HazelcastClientProvider

    @Inject
    private lateinit var auditingConfiguration: AuditingConfiguration

    @Inject
    private lateinit var blackbox: Blackbox

    @Inject
    private lateinit var spml: SecurablePrincipalsMapLoader

    @Inject
    private lateinit var rptml: ResolvedPrincipalTreesMapLoader

    @Inject
    private lateinit var metricRegistry: MetricRegistry

    @Inject
    private lateinit var auth0Configuration: Auth0Configuration

    @Inject
    private lateinit var configurationService: ConfigurationService

    @Inject
    private lateinit var assemblerConfiguration: AssemblerConfiguration

    @Inject
    private lateinit var byteBlobDataManager: ByteBlobDataManager

    @Inject
    private lateinit var externalDbConnMan: ExternalDatabaseConnectionManager

    @Inject
    private lateinit var extDbPermManager: ExternalDatabasePermissionsManager

    @Inject
    private lateinit var principalService: SecurePrincipalsManager

    @Inject
    private lateinit var aclKeyReservationService: HazelcastAclKeyReservationService

    @Inject
    private lateinit var authorizationManager: AuthorizationManager

    @Autowired(required = false)
    private var s3: AmazonS3? = null

    @Autowired(required = false)
    private var awsLaunchConfig: AmazonLaunchConfiguration? = null

    @Bean(name = ["conductorConfiguration"])
    @Profile(ConfigurationConstants.Profiles.LOCAL_CONFIGURATION_PROFILE)
    @Throws(IOException::class)
    fun getLocalConductorConfiguration(): ConductorConfiguration {
        val config = configurationService.getConfiguration(ConductorConfiguration::class.java)!!
        logger.info("Using local conductor configuration: {}", config)
        return config
    }

    @Bean(name = ["conductorConfiguration"])
    @Profile(
            ConfigurationConstants.Profiles.AWS_CONFIGURATION_PROFILE,
            ConfigurationConstants.Profiles.AWS_TESTING_PROFILE
    )
    @Throws(IOException::class)
    fun getAwsConductorConfiguration(): ConductorConfiguration {
        val checked = awsLaunchConfig!!
        val config = ResourceConfigurationLoader.loadConfigurationFromS3(
                s3,
                checked.bucket,
                checked.folder,
                ConductorConfiguration::class.java
        )

        logger.info("Using aws conductor configuration: {}", config)
        return config
    }

    @Bean
    fun defaultObjectMapper() = ObjectMappers.getJsonMapper()

    @Bean
    fun phoneNumberService(): PhoneNumberService {
        return PhoneNumberService(hazelcastInstance)
    }

    @Bean
    fun longIdService(): HazelcastLongIdService {
        return HazelcastLongIdService(hazelcastClientProvider)
    }

    @Bean
    fun dbcs(): DbCredentialService {
        return DbCredentialService(hazelcastInstance, longIdService())
    }

    @Bean
    fun organizationMetadataEntitySetsService(): OrganizationMetadataEntitySetsService {
        return OrganizationMetadataEntitySetsService(dataModelService())
    }

    @Bean
    fun assembler(): Assembler {
        return Assembler(
                dbcs(),
                hds,
                authorizationManager,
                principalService,
                metricRegistry,
                hazelcastInstance,
                eventBus
        )
    }

    @Bean
    fun organizationsManager(): HazelcastOrganizationService {
        return HazelcastOrganizationService(
                hazelcastInstance,
                aclKeyReservationService,
                authorizationManager,
                principalService,
                phoneNumberService(),
                partitionManager(),
                assembler(),
                organizationMetadataEntitySetsService()
        )
    }

    @Bean
    fun auth0TokenProvider(): Auth0TokenProvider {
        return AwsAuth0TokenProvider(auth0Configuration)
    }

    @Bean
    fun managementAPI(): ManagementAPI {
        return ManagementAPI(auth0Configuration.domain, auth0TokenProvider().token)
    }

    @Bean
    fun userListingService(): UserListingService {
        val config = auth0Configuration
        return if (config.managementApiUrl.contains(Auth0Configuration.NO_SYNC_URL)) {
            LocalUserListingService(config)
        } else {
            val auth0Token = auth0TokenProvider().token
            Auth0UserListingService(
                    ManagementAPI(config.domain, auth0Token),
                    Auth0ApiExtension(config.domain, auth0Token)
            )
        }
    }

    @Bean
    fun postInitializerDependencies(): PostConstructInitializerTaskDependencies {
        return PostConstructInitializerTaskDependencies()
    }

    @Bean
    fun postInitializerTask(): PostConstructInitializerTaskDependencies.PostConstructInitializerTask {
        return PostConstructInitializerTaskDependencies.PostConstructInitializerTask()
    }

    @Bean
    fun assemblerConnectionManager(): AssemblerConnectionManager {
        return AssemblerConnectionManager(
                assemblerConfiguration,
                externalDbConnMan,
                principalService,
                organizationsManager(),
                dbcs(),
                extDbPermManager,
                eventBus,
                metricRegistry
        )
    }

    @Bean
    fun organizationBootstrap(): OrganizationsInitializationTask {
        return OrganizationsInitializationTask()
    }

    @Bean
    fun authorizationBootstrapDependencies(): AuthorizationInitializationDependencies {
        return AuthorizationInitializationDependencies(principalService)
    }

    @Bean
    fun authorizationBootstrap(): AuthorizationInitializationTask {
        return AuthorizationInitializationTask()
    }

    @Bean
    fun integrationsMapstore() = IntegrationsMapstore(hds)

    @Bean
    fun integrationJobsMapstore() = IntegrationJobsMapstore(hds)

    @Bean
    fun idGenerationService() = HazelcastIdGenerationService(hazelcastClientProvider)

    @Bean
    internal fun partitionManager() = PartitionManager(hazelcastInstance, hds)

    @Bean
    fun idService() = PostgresEntityKeyIdService(
            hds,
            idGenerationService(),
            partitionManager()
    )

    @Bean
    fun entityTypeManager() = PostgresTypeManager(hds, hazelcastInstance)

    @Bean
    fun schemaQueryService() = entityTypeManager()

    @Bean
    fun schemaManager() = HazelcastSchemaManager(hazelcastInstance, schemaQueryService())

    @Bean
    fun dataModelService() = EdmService(
            hazelcastInstance,
            aclKeyReservationService,
            authorizationManager,
            entityTypeManager(),
            schemaManager()
    )

    @Bean
    fun entitySetManager() = EntitySetService(
            hazelcastInstance,
            eventBus,
            aclKeyReservationService,
            authorizationManager,
            partitionManager(),
            dataModelService(),
            hds,
            organizationMetadataEntitySetsService(),
            auditingConfiguration
    )

    @Bean
    internal fun awsDataSinkService(): AwsDataSinkService {
        return AwsDataSinkService(
                partitionManager(),
                byteBlobDataManager,
                hds,
                hds
        )
    }

    @Bean
    fun integrationService(): IntegrationService {
        return IntegrationService(
                hazelcastInstance,
                missionParametersConfiguration,
                idService(),
                entitySetManager(),
                aclKeyReservationService,
                awsDataSinkService(),
                blackbox
        )
    }

    @PostConstruct
    internal fun initPrincipals() {
        Principals.init(principalService, hazelcastInstance)
        IntegrationService.init(blackbox, dataModelService())
    }

}