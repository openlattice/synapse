package com.openlattice.shuttle.hazelcast.serializers

import com.hazelcast.nio.ObjectDataInput
import com.hazelcast.nio.ObjectDataOutput
import com.openlattice.client.RetrofitFactory
import com.openlattice.data.integration.StorageDestination
import com.openlattice.hazelcast.StreamSerializerTypeIds
import com.openlattice.hazelcast.serializers.ListStreamSerializer
import com.openlattice.hazelcast.serializers.OptionalStreamSerializers
import com.openlattice.hazelcast.serializers.TestableSelfRegisteringStreamSerializer
import com.openlattice.hazelcast.serializers.UUIDStreamSerializer
import com.openlattice.shuttle.control.Integration
import org.springframework.stereotype.Component

@Component
class IntegrationStreamSerializer : TestableSelfRegisteringStreamSerializer<Integration> {

    companion object {
        private val environments = RetrofitFactory.Environment.values()

        fun serialize(output: ObjectDataOutput, obj: Integration) {
            UUIDStreamSerializer.serialize(output, obj.key)
            output.writeInt(obj.environment.ordinal)
            output.writeUTF(obj.s3bucket)
            output.writeUTFArray(obj.contacts.toTypedArray())
            OptionalStreamSerializers.serialize(output, obj.logEntitySetId, UUIDStreamSerializer::serialize)
            OptionalStreamSerializers.serialize(output, obj.maxConnections, ObjectDataOutput::writeInt)
            OptionalStreamSerializers.serializeList(output, obj.callbackUrls, ObjectDataOutput::writeUTF)
            output.writeUTFArray(obj.flightPlanParameters.keys.toTypedArray())
            obj.flightPlanParameters.values.forEach { FlightPlanParametersStreamSerializer.serialize(output, it) }
        }

        fun deserialize(input: ObjectDataInput): Integration {
            val key = UUIDStreamSerializer.deserialize(input)
            val environment = environments[input.readInt()]
            val s3bucket = input.readUTF()
            val contacts = input.readUTFArray().toSet()
            val logEntitySetId = OptionalStreamSerializers.deserialize(input, UUIDStreamSerializer::deserialize)
            val maxConnections = OptionalStreamSerializers.deserialize(input, ObjectDataInput::readInt)
            val callbackUrls = OptionalStreamSerializers.deserializeList(input, ObjectDataInput::readUTF)
            val flightPlanParamsKeys = input.readUTFArray()
            val flightPlanParamsValues = flightPlanParamsKeys.map{
                FlightPlanParametersStreamSerializer.deserialize(input)
            }.toList()
            val flightPlanParams = flightPlanParamsKeys.zip(flightPlanParamsValues).toMap().toMutableMap()
            return Integration(
                    key,
                    environment,
                    s3bucket,
                    contacts,
                    logEntitySetId,
                    maxConnections,
                    callbackUrls,
                    flightPlanParams
            )
        }
    }

    override fun write(output: ObjectDataOutput, obj: Integration) {
        serialize(output, obj)
    }

    override fun read(input: ObjectDataInput): Integration {
        return deserialize(input)
    }

    override fun getClazz(): Class<out Integration> {
        return Integration::class.java
    }

    override fun getTypeId(): Int {
        return StreamSerializerTypeIds.INTEGRATION.ordinal
    }

    override fun generateTestValue(): Integration {
        return Integration.testData()
    }

}