package com.openlattice.shuttle

import com.amazonaws.auth.InstanceProfileCredentialsProvider
import com.amazonaws.regions.RegionUtils
import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import com.dataloom.mappers.ObjectMappers
import com.fasterxml.jackson.core.JsonParseException
import com.fasterxml.jackson.databind.JsonMappingException
import com.google.common.base.Preconditions
import com.openlattice.ResourceConfigurationLoader
import com.openlattice.client.RetrofitFactory
import com.openlattice.shuttle.ShuttleCliOptions.Companion.CONFIGURATION
import com.openlattice.shuttle.ShuttleCliOptions.Companion.CREATE
import com.openlattice.shuttle.ShuttleCliOptions.Companion.CSV
import com.openlattice.shuttle.ShuttleCliOptions.Companion.DATASOURCE
import com.openlattice.shuttle.ShuttleCliOptions.Companion.DATA_ORIGIN
import com.openlattice.shuttle.ShuttleCliOptions.Companion.ENVIRONMENT
import com.openlattice.shuttle.ShuttleCliOptions.Companion.FETCHSIZE
import com.openlattice.shuttle.ShuttleCliOptions.Companion.FLIGHT
import com.openlattice.shuttle.ShuttleCliOptions.Companion.FROM_EMAIL
import com.openlattice.shuttle.ShuttleCliOptions.Companion.FROM_EMAIL_PASSWORD
import com.openlattice.shuttle.ShuttleCliOptions.Companion.HELP
import com.openlattice.shuttle.ShuttleCliOptions.Companion.LOCAL_ORIGIN_EXPECTED_ARGS_COUNT
import com.openlattice.shuttle.ShuttleCliOptions.Companion.NOTIFICATION_EMAILS
import com.openlattice.shuttle.ShuttleCliOptions.Companion.PASSWORD
import com.openlattice.shuttle.ShuttleCliOptions.Companion.POSTGRES
import com.openlattice.shuttle.ShuttleCliOptions.Companion.PROFILES
import com.openlattice.shuttle.ShuttleCliOptions.Companion.READ_RATE_LIMIT
import com.openlattice.shuttle.ShuttleCliOptions.Companion.S3
import com.openlattice.shuttle.ShuttleCliOptions.Companion.S3_ORIGIN_MAXIMUM_ARGS_COUNT
import com.openlattice.shuttle.ShuttleCliOptions.Companion.S3_ORIGIN_MINIMUM_ARGS_COUNT
import com.openlattice.shuttle.ShuttleCliOptions.Companion.SERVER
import com.openlattice.shuttle.ShuttleCliOptions.Companion.SMTP_SERVER
import com.openlattice.shuttle.ShuttleCliOptions.Companion.SMTP_SERVER_PORT
import com.openlattice.shuttle.ShuttleCliOptions.Companion.SQL
import com.openlattice.shuttle.ShuttleCliOptions.Companion.TOKEN
import com.openlattice.shuttle.ShuttleCliOptions.Companion.UPLOAD_SIZE
import com.openlattice.shuttle.ShuttleCliOptions.Companion.USER
import com.openlattice.shuttle.ShuttleCliOptions.Companion.XML
import com.openlattice.shuttle.config.IntegrationConfig
import com.openlattice.shuttle.payload.*
import com.openlattice.shuttle.source.LocalFileOrigin
import com.openlattice.shuttle.source.S3BucketOrigin
import org.apache.commons.cli.CommandLine
import org.slf4j.LoggerFactory
import java.io.File
import java.io.IOException
import java.nio.file.Paths
import java.util.*
import java.util.function.Supplier
import kotlin.system.exitProcess

private val logger = LoggerFactory.getLogger(ShuttleCli::class.java)

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */

fun main(args: Array<String>) {

    val configuration: IntegrationConfig
    val environment: RetrofitFactory.Environment
    val cl = ShuttleCliOptions.parseCommandLine(args)
    val payload: Payload
    val flight: Flight
    val createEntitySets: Boolean
    val contacts: Set<String>
    val rowColsToPrint: Map<Flight, List<String>>

    if (cl.hasOption(HELP)) {
        ShuttleCliOptions.printHelp()
        return
    }

    if (cl.hasOption(SERVER)) {
        if (cl.hasOption(PROFILES)) {
            val shuttleServer = ShuttleServer()
            println("Server mode specifying ignoring other arguments and starting server.")
            shuttleServer.start(*cl.getOptionValues(PROFILES))
            return
        } else {
            println("Server mode was specified but not profiles were provided.")
        }
    }

    if (!cl.hasOption(FLIGHT)) {
        System.err.println("A flight is required in order to run shuttle.")
        ShuttleCliOptions.printHelp()
        return
    }

    flight = try {
         ObjectMappers.getYamlMapper().readValue(File(cl.getOptionValue(FLIGHT)), Flight::class.java)
    } catch (io: IOException) {
        MissionControl.failWithBadInputs("IOException encountered converting yaml file into java flight objects", io)
        Flight.newFlight("fail").done() // only here for compiler, above statement exits process
    } catch (jp: JsonParseException) {
        MissionControl.failWithBadInputs("Shuttle was unable to parse the flight yaml file", jp)
        Flight.newFlight("fail").done() // only here for compiler, above statement exits process
    } catch (jm: JsonMappingException) {
        MissionControl.failWithBadInputs( "Shuttle was unable to map the flight yaml objects into java flight objects", jm)
        Flight.newFlight("fail").done() // only here for compiler, above statement exits process
    }


    //You can have a configuration without any JDBC datasources
    when {
        cl.hasOption(CONFIGURATION) -> {
            configuration = ObjectMappers.getYamlMapper()
                    .readValue(File(cl.getOptionValue(CONFIGURATION)), IntegrationConfig::class.java)

            if (!cl.hasOption(DATASOURCE)) {
                // check datasource presence
                println("Datasource must be specified when doing a JDBC datasource based integration.")
                ShuttleCliOptions.printHelp()
                return
            }
            if (!cl.hasOption(SQL)) {
                // check SQL presence
                println("SQL expression must be specified when doing a JDBC datasource based integration.")
                ShuttleCliOptions.printHelp()
                return
            }
            if (cl.hasOption(CSV)) {
                // check csv ABsence
                println("Cannot specify CSV datasource and JDBC datasource simultaneously.")
                ShuttleCliOptions.printHelp()
                return
            }
            if (cl.hasOption(XML)) {
                // check xml Absence
                println("Cannot specify XML datasource and JDBC datasource simultaneously.")
                ShuttleCliOptions.printHelp()
                return
            }
            if (cl.hasOption(DATA_ORIGIN)) {
                println("SQL cannot be specified when performing a data origin integration")
                ShuttleCliOptions.printHelp()
                return
            }

            // get JDBC payload
            val hds = configuration.getHikariDatasource(cl.getOptionValue(DATASOURCE))
            val sql = cl.getOptionValue(SQL)
            rowColsToPrint = mapOf(flight to configuration.primaryKeyColumns)
            val readRateLimit = if (cl.hasOption(READ_RATE_LIMIT)) {
                cl.getOptionValue(READ_RATE_LIMIT).toInt()
            } else {
                DEFAULT_PERMITS_PER_SECOND.toInt()
            }

            payload = if (cl.hasOption(FETCHSIZE)) {
                val fetchSize = cl.getOptionValue(FETCHSIZE).toInt()
                logger.info("Using a fetch size of $fetchSize")
                JdbcPayload(readRateLimit.toDouble(), hds, sql, fetchSize, readRateLimit != 0)
            } else {
                JdbcPayload(readRateLimit.toDouble(), hds, sql, 0, readRateLimit != 0)
            }
        }
        cl.hasOption(CSV) -> {// get csv payload
            if (cl.hasOption(DATA_ORIGIN)) {
                println("CSV cannot be specified when performing a data origin integration")
                ShuttleCliOptions.printHelp()
                return
            }
            rowColsToPrint = mapOf()
            payload = CsvPayload(cl.getOptionValue(CSV))
        }
        cl.hasOption(XML) -> {// get xml payload
            rowColsToPrint = mapOf()
            if (cl.hasOption(DATA_ORIGIN)) {
                val arguments = cl.getOptionValues(DATA_ORIGIN)
                val dataOrigin = when (arguments[0]) {
                    "S3" -> {
                        if (arguments.size < S3_ORIGIN_MINIMUM_ARGS_COUNT) {
                            println("Not enough arguments provided for S3 data origin, provide AWS region, S3 URL and bucket name")
                            ShuttleCliOptions.printHelp()
                            exitProcess(1)
                        }
                        val filePrefix = if ( arguments.size == S3_ORIGIN_MAXIMUM_ARGS_COUNT) {
                            arguments[3]
                        } else {
                            ""
                        }
                        S3BucketOrigin(arguments[2], makeAWSS3Client(arguments[1]), filePrefix)
                    }
                    "local" -> {
                        if (arguments.size < LOCAL_ORIGIN_EXPECTED_ARGS_COUNT) {
                            println("Not enough arguments provided for local data origin, provide a local file path")
                            ShuttleCliOptions.printHelp()
                            exitProcess(1)
                        }
                        LocalFileOrigin(Paths.get(arguments[1]))
                    }
                    else -> {
                        println("The specified configuration is invalid ${cl.getOptionValues(DATA_ORIGIN)}")
                        ShuttleCliOptions.printHelp()
                        exitProcess(1)
                    }
                }
                payload = XmlFilesPayload(dataOrigin)
            } else {
                payload = XmlFilesPayload(cl.getOptionValue(XML))
            }
        }
        else -> {
            System.err.println("At least one valid data origin must be specified.")
            ShuttleCliOptions.printHelp()
            exitProcess(1)
        }
    }

    environment = if (cl.hasOption(ENVIRONMENT)) {
        val env = cl.getOptionValue(ENVIRONMENT).toUpperCase()
        if ("PRODUCTION" == env) {
            MissionControl.fail(
                    -999, flight, Throwable(
                    "PRODUCTION is not a valid integration environment. The valid environments are PROD_INTEGRATION and LOCAL"
            )
            )
        }
        RetrofitFactory.Environment.valueOf(env)
    } else {
        RetrofitFactory.Environment.PROD_INTEGRATION
    }


    val s3BucketUrl = if (cl.hasOption(S3)) {
        val bucketCategory = cl.getOptionValue(S3)
        require(
                bucketCategory.toUpperCase() in setOf(
                        "TEST", "PRODUCTION"
                )
        ) { "Invalid option $bucketCategory for $S3." }
        when (bucketCategory) {
            "TEST" -> "https://tempy-media-storage.s3-website-us-gov-west-1.amazonaws.com"
            "PRODUCTION" -> "http://openlattice-media-storage.s3-website-us-gov-west-1.amazonaws.com"
            else -> "https://tempy-media-storage.s3-website-us-gov-west-1.amazonaws.com"
        }
    } else {
        ""
    }

    val shuttleConfig = if (cl.hasOption(POSTGRES)) {
        val pgCfg = cl.getOptionValues(POSTGRES)
        require(pgCfg.size == 2) { "Must specify in format <bucket>,<region>" }
        val bucket = pgCfg[0]
        val region = pgCfg[1]
        val s3Client = AmazonS3ClientBuilder.standard().withCredentials(
                InstanceProfileCredentialsProvider.createAsyncRefreshingProvider(true)
        ).withRegion(RegionUtils.getRegion(region).name).build()
        ResourceConfigurationLoader.loadConfigurationFromS3(s3Client, bucket, "shuttle/", MissionParameters::class.java)
    } else {
        MissionParameters.empty()
    }

    //TODO: Use the right method to select the JWT token for the appropriate environment.

    val missionControl = when {
        cl.hasOption(TOKEN) -> {
            Preconditions.checkArgument(!cl.hasOption(PASSWORD))
            val jwt = cl.getOptionValue(TOKEN)
            MissionControl(environment, Supplier { jwt }, s3BucketUrl, shuttleConfig)
        }
        cl.hasOption(USER) -> {
            Preconditions.checkArgument(cl.hasOption(PASSWORD))
            val user = cl.getOptionValue(USER)
            val password = cl.getOptionValue(PASSWORD)
            MissionControl(environment, user, password, s3BucketUrl, shuttleConfig)
        }
        else -> {
            System.err.println("User or token must be provided for authentication.")
            ShuttleCliOptions.printHelp()
            return
        }
    }

    createEntitySets = cl.hasOption(CREATE)
    if (createEntitySets) {
        require(environment != RetrofitFactory.Environment.PRODUCTION) {
            "It is not allowed to automatically create entity sets on " +
                    "${RetrofitFactory.Environment.PRODUCTION} environment"
        }

        contacts = cl.getOptionValues(CREATE).toSet()
        if (contacts.isEmpty()) {
            System.err.println("Can't create entity sets automatically without contacts provided")
            ShuttleCliOptions.printHelp()
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
        MissionControl.setEmailConfiguration(emailConfiguration)
        logger.info("Preparing flight plan.")
        val shuttle = missionControl.prepare(flightPlan, createEntitySets, rowColsToPrint, contacts)
        logger.info("Pre-flight check list complete. ")
        shuttle.launch(uploadBatchSize)
        MissionControl.succeed()
    } catch (ex: Throwable) {
        MissionControl.fail(1, flight, ex)
    }
}

fun makeAWSS3Client(region: String): AmazonS3 {
    return AmazonS3ClientBuilder
            .standard()
            .withPathStyleAccessEnabled(true)
            .withRegion(region)
            .build()
}

fun getEmailConfiguration(cl: CommandLine): Optional<EmailConfiguration> {
    return when {
        cl.hasOption(SMTP_SERVER) -> {
            val smtpServer = cl.getOptionValue(SMTP_SERVER)
            val smtpServerPort = if (cl.hasOption(SMTP_SERVER_PORT)) {
                cl.getOptionValue(SMTP_SERVER_PORT).toInt()
            } else {
                System.err.println("No smtp server port was specified")
                ShuttleCliOptions.printHelp()
                exitProcess(1)
            }

            val notificationEmails = cl.getOptionValues(NOTIFICATION_EMAILS).toSet()
            if (notificationEmails.isEmpty()) {
                System.err.println("No notification e-mails were actually specified.")
                ShuttleCliOptions.printHelp()
                exitProcess(1)
            }

            val fromEmail = if (cl.hasOption(FROM_EMAIL)) {
                cl.getOptionValue(FROM_EMAIL)
            } else {
                System.err.println("If notification e-mails are specified must also specify a sending account.")
                ShuttleCliOptions.printHelp()
                exitProcess(1)
            }

            val fromEmailPassword = if (cl.hasOption(FROM_EMAIL_PASSWORD)) {
                cl.getOptionValue(FROM_EMAIL_PASSWORD)
            } else {
                System.err.println(
                        "If notification e-mails are specified must also specify an e-mail password for sending account."
                )
                ShuttleCliOptions.printHelp()
                exitProcess(1)
            }
            Optional.of(
                    EmailConfiguration(fromEmail, fromEmailPassword, notificationEmails, smtpServer, smtpServerPort)
            )
        }
        cl.hasOption(SMTP_SERVER_PORT) -> {
            System.err.println("Port was specified, no smtp server was specified")
            ShuttleCliOptions.printHelp()
            exitProcess(1)
        }
        cl.hasOption(FROM_EMAIL) -> {
            System.err.println("From e-mail was specified, no smtp server was specified")
            ShuttleCliOptions.printHelp()
            exitProcess(1)
        }
        cl.hasOption(FROM_EMAIL_PASSWORD) -> {
            System.err.println("From e-mail password was specified, no smtp server was specified")
            ShuttleCliOptions.printHelp()
            exitProcess(1)
        }
        cl.hasOption(NOTIFICATION_EMAILS) -> {
            System.err.println("Notification e-mails were specified, no smtp server was specified")
            ShuttleCliOptions.printHelp()
            exitProcess(1)
        }
        else -> Optional.empty()
    }

}

class ShuttleCli {
}