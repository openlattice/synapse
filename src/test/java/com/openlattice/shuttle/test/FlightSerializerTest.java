/*
 * Copyright (C) 2017. OpenLattice, Inc
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
 */

package com.openlattice.shuttle.test;

import com.dataloom.mappers.ObjectMappers;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.openlattice.data.serializers.FullQualifiedNameJacksonDeserializer;
import com.openlattice.data.serializers.FullQualifiedNameJacksonSerializer;
import com.openlattice.serializer.AbstractJacksonSerializationTest;
import com.openlattice.shuttle.Flight;
import java.io.IOException;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class FlightSerializerTest extends AbstractJacksonSerializationTest<Flight> {
    private static ObjectMapper yaml = ObjectMappers.getYamlMapper();
    @Override
    protected Flight getSampleData() {
        return ShuttleTest.getFlight();
    }

    @Override
    protected Class<Flight> getClazz() {
        return Flight.class;
    }

    @Test
    public void testYaml() throws IOException {
        Flight expected = getSampleData();

        String yml = yaml.writeValueAsString( expected );
        Flight actual = yaml.readValue( yml , getClazz());
        logger.info("Yaml: {}", yml );
        Assert.assertEquals( expected,actual );
    }
    @BeforeClass
    public static void registerModules() {
        registerModule( FullQualifiedNameJacksonSerializer::registerWithMapper );
        registerModule( FullQualifiedNameJacksonDeserializer::registerWithMapper );
        FullQualifiedNameJacksonSerializer.registerWithMapper( yaml );
        FullQualifiedNameJacksonDeserializer.registerWithMapper( yaml );
    }
}
