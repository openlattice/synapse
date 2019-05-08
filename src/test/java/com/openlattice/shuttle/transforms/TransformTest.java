package com.openlattice.shuttle.transforms;

import com.openlattice.shuttle.transformations.Transformations;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import transforms.*;

import javax.management.AttributeList;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.*;

public class TransformTest {

    String lat               = "36.23452";
    String lon               = "30.34573";
    String sex               = "f";
    String first             = "John";
    String last              = "Doe";
    String family            = "Joanna Doe (mother)";
    String DOB               = "03/05/1998 10:00";
    String address           = "560 Scott Street, San Francisco, CA 94117";
    String dateArrest        = "10/01/92";
    String dateRelease       = "10-01-25";
    String datetimeCommitted = "03/05/00 10:00";

    public Map<String, String> getTestRow() {
        Map<String, String> testrow = new HashMap<String, String>();
        testrow.put( "FirstName", first );
        testrow.put( "LastName", last );
        testrow.put( "Family", family );
        testrow.put( "DOB", DOB );
        testrow.put( "ArrestedDate", dateArrest );
        testrow.put( "ReleasedDate", dateRelease );
        testrow.put( "SSN", null );
        testrow.put( "Sex", sex);
        testrow.put( "Address", address );
        testrow.put( "CommittedDateTime", datetimeCommitted );
        testrow.put( "Lat", lat );
        testrow.put( "Long", lon );
        return testrow;
    }

    //==================//
    // HELPER FUNCTIONS //
    //==================//

    public Transformations getTrueTestTransforms() {
        Transformations transfos = new Transformations();
        transfos.add( new ValueTransform( "yup" ) );
        return transfos;
    }

    public Transformations getFalseTestTransforms() {
        Transformations transfos = new Transformations();
        transfos.add( new ValueTransform( "nope" ) );
        return transfos;
    }

    //==================//
    // BOOLEAN TESTS    //
    //==================//

    @Test
    public void testGeographyPointTransform() {
        Transformations latTransfos = new Transformations();
        latTransfos.add( new ColumnTransform( "Lat" ) );
        Transformations lonTransfos = new Transformations();
        lonTransfos.add( new ColumnTransform( "Long" ) );
        Object geographypointTest1 = new GeographyPointTransform(
                latTransfos, lonTransfos
        ).apply( getTestRow() );
        Assert.assertEquals( lat + "," + lon, geographypointTest1 );

    }

    @Test
    public void testBooleanContainsTransform() {
        Optional<Boolean> optrue = Optional.of( true );
        Optional<Boolean> opfals = Optional.of( false );
        Optional<Transformations> truetransfo = Optional.of( getTrueTestTransforms() );
        Optional<Transformations> falsetransfo = Optional.of( getFalseTestTransforms() );
        Object booleanContainsTest1 = new BooleanContainsTransform(
                "DOB",
                "1998",
                optrue,
                truetransfo,
                falsetransfo
        ).apply( getTestRow() );
        Assert.assertEquals( "yup", booleanContainsTest1 );

        Object booleanContainsTest2 = new BooleanContainsTransform(
                "DOB",
                "1999",
                optrue,
                truetransfo,
                falsetransfo
        ).apply( getTestRow() );
        Assert.assertEquals( "nope", booleanContainsTest2 );

        Object booleanContainsTest3 = new BooleanContainsTransform(
                "FirstName",
                "jOHN",
                opfals,
                truetransfo,
                falsetransfo
        ).apply( getTestRow() );
        Assert.assertEquals( "nope", booleanContainsTest3 );

    }

    @Test
    public void testBooleanIsNullTransform() {
        Optional<Boolean> blue = Optional.of( true );
        Optional<Transformations> truetransfo = Optional.of( getTrueTestTransforms() );
        Optional<Transformations> falsetransfo = Optional.of( getFalseTestTransforms() );
        Object booleanIsNullTest1 = new BooleanIsNullTransform(
                "SSN",
                truetransfo,
                falsetransfo
        ).apply( getTestRow() );
        Assert.assertEquals( "yup", booleanIsNullTest1 );

        Object booleanIsNullTest2 = new BooleanIsNullTransform(
                "DOB",
                truetransfo,
                falsetransfo
        ).apply( getTestRow() );
        Assert.assertEquals( "nope", booleanIsNullTest2 );
    }

    @Test
    public void testBooleanPrefixTransform() {

        Optional<Boolean> optrue = Optional.of( true );
        Optional<Boolean> opfals = Optional.of( false );
        Optional<Transformations> truetransfo = Optional.of( getTrueTestTransforms() );
        Optional<Transformations> falsetransfo = Optional.of( getFalseTestTransforms() );

        // test true with ignorecase = true
        Object booleanPrefixTest1 = new BooleanPrefixTransform(
                "jo",
                "FirstName",
                optrue,
                truetransfo,
                falsetransfo
        ).apply( getTestRow() );
        Assert.assertEquals( "yup", booleanPrefixTest1 );

        // test true with ignorecase = true
        Object booleanPrefixTest2 = new BooleanPrefixTransform(
                "jo",
                "FirstName",
                opfals,
                truetransfo,
                falsetransfo
        ).apply( getTestRow() );
        Assert.assertEquals( "nope", booleanPrefixTest2 );

        // test false with ignorecase = true
        Object booleanPrefixTest3 = new BooleanPrefixTransform(
                "Nobody",
                "FirstName",
                opfals,
                truetransfo,
                falsetransfo
        ).apply( getTestRow() );
        Assert.assertEquals( "nope", booleanPrefixTest3 );

    }

    @Test
    public void testBooleanRegexTransform() {
        Optional<Transformations> truetransfo = Optional.of( getTrueTestTransforms() );
        Optional<Transformations> falsetransfo = Optional.of( getFalseTestTransforms() );

        // test true with ignorecase = true
        Object booleanRegexTest1 = new BooleanRegexTransform(
                "FirstName",
                "nobody|Jo",
                truetransfo,
                falsetransfo
        ).apply( getTestRow() );
        Assert.assertEquals( "yup", booleanRegexTest1 );
    }

    //==================//
    // PARSING TESTS    //
    //==================//

    @Test
    public void testParseIntTransform() {
        Object parseIntTest1 = new ParseIntTransform().apply( "3" );
        Assert.assertEquals( "3", parseIntTest1 );
    }

    @Test
    public void testParseBooleanTransform() {
        Object parseBoolTest1 = new ParseBoolTransform().apply( "1" );
        Assert.assertEquals( "true", parseBoolTest1 );
        Object parseBoolTest2 = new ParseBoolTransform().apply( "0" );
        Assert.assertEquals( "false", parseBoolTest2 );
        Object parseBoolTest3 = new ParseBoolTransform().apply( "true" );
        Assert.assertEquals( "true", parseBoolTest3 );
        Object parseBoolTest4 = new ParseBoolTransform().apply( "false" );
        Assert.assertEquals( "false", parseBoolTest4 );
    }

    //==================//
    // DATETIME TESTS   //
    //==================//

    @Test
    public void testDateTimeTransform() {
        String[] patterns = { "MM/dd/yyyy HH:mm", "MM/dd/yy HH:mm" };
        OffsetDateTime expected1 = OffsetDateTime
                .of( LocalDateTime.of( 1998, 03, 05, 10, 0 ), ZoneOffset.ofHours( -5 ) );
        Object dateTimeTest1 = new DateTimeTransform( patterns ).apply( getTestRow().get( "DOB" ) );
        Assert.assertEquals( expected1.toString(), dateTimeTest1 );
        OffsetDateTime expected2 = OffsetDateTime
                .of( LocalDateTime.of( 2000, 03, 05, 10, 0 ), ZoneOffset.ofHours( -5 ) );
        Object dateTimeTest2 = new DateTimeTransform( patterns ).apply( getTestRow().get( "CommittedDateTime" ) );
        Assert.assertEquals( expected1.toString(), dateTimeTest1 );
        Assert.assertEquals( expected2.toString(), dateTimeTest2 );
    }

    @Test
    public void testDateTransform() {
        String[] patterns = { "MM/dd/yy", "MM-dd-yy" };
        LocalDate expected1 = LocalDate.of( 1992, 10, 01 );
        Object dateTimeTest1 = new DateTransform( patterns ).apply( getTestRow().get( "ArrestedDate" ) );
        Assert.assertEquals( expected1.toString(), dateTimeTest1 );
        LocalDate expected2 = LocalDate.of( 2025, 10, 01 );
        Object dateTimeTest2 = new DateTransform( patterns ).apply( getTestRow().get( "ReleasedDate" ) );
        Assert.assertEquals( expected2.toString(), dateTimeTest2 );
    }

    //==================//
    // OTHER TESTS      //
    //==================//

    @Test
    public void testPrefixTransform() {
        Object prefixTest1 = new PrefixTransform( "prefix_" ).apply( "name" );
        Assert.assertEquals( "prefix_name", prefixTest1 );

        Object prefixTest2 = new PrefixTransform( "prefix_" ).apply( null );
        Assert.assertEquals( null, prefixTest2 );
    }

    @Test
    public void testCasingTransform() {
        Object casingTest1 = new CaseTransform( null ).apply( "JANINE" );
        Assert.assertEquals( "Janine", casingTest1 );
        Object casingTest2 = new CaseTransform( null ).apply( "123JANINE" );
        Assert.assertEquals( "123janine", casingTest2 );
        Object casingTest3 = new CaseTransform( CaseTransform.CaseType.lower ).apply( "JAniNE" );
        Assert.assertEquals( "janine", casingTest3 );
        Object casingTest4 = new CaseTransform( CaseTransform.CaseType.upper ).apply( "JAniNE" );
        Assert.assertEquals( "JANINE", casingTest4 );
        Object casingTest5 = new CaseTransform( CaseTransform.CaseType.sentence ).apply( "janine vanderginst" );
        Assert.assertEquals( "Janine vanderginst", casingTest5 );
        Object casingTest6 = new CaseTransform( CaseTransform.CaseType.name ).apply( "janine vanderginst" );
        Assert.assertEquals( "Janine Vanderginst", casingTest6 );
    }


    @Test
    public void testConcatTransform() {
        String expected = "John Doe";
        List<String> cols = Arrays.asList( "FirstName", "LastName" );
        Object concatTest1 = new ConcatTransform( cols, " " ).apply( getTestRow() );
        Assert.assertEquals( expected, concatTest1 );
    }

    @Test(expected = IllegalStateException.class)
    public void testColumnAbsence() {
        String unknownColumn = "thisdoesnotexist";
        Object concatTest1 = new ColumnTransform( unknownColumn ).apply( getTestRow() );
    }

    @Test
    public void testReplaceRegexTransform() {
        String target = ".*\\(|\\)";
        String goal = "";

        Object replaceRegexTest1 = new ReplaceRegexTransform( target, goal )
                .apply(getTestRow().get("Family"));
        Assert.assertEquals( "mother", replaceRegexTest1);
    }

    @Test
    public void testReplaceTransform() {
        Optional<Boolean> optrue = Optional.of( true );
        Optional<Boolean> opfals = Optional.of( false );

        List<String> target = Arrays.asList( "F" );
        List<String> goal = Arrays.asList( "female" );
        // not case sensitive
        Object replaceTest1 = new ReplaceTransform( target, optrue, opfals, goal, "null" )
                .apply( getTestRow().get( "Sex" ) );
        Assert.assertEquals( "female", replaceTest1 );

        // case sensitive
        Object replaceTest2 = new ReplaceTransform( target, opfals, opfals, goal, "null" )
                .apply( getTestRow().get( "Sex" ) );
        Assert.assertEquals( null, replaceTest2 );

        // return original when valueElse is not specified
        Object replaceTest3 = new ReplaceTransform( target, opfals, opfals, goal, null )
                .apply( getTestRow().get( "Sex" ) );
        Assert.assertEquals( "f", replaceTest3 );

        List<String> target4 = Arrays.asList( "F", "e" );
        List<String> goal4 = Arrays.asList( "female", "erel" );
        Object replaceTest4 = new ReplaceTransform( target4, optrue, optrue, goal4, null )
                .apply( getTestRow().get( "Sex" ) );
        Assert.assertEquals( "female", replaceTest4 );

        List<String> target5 = Arrays.asList( "Android", "a" );
        List<String> goal5 = Arrays.asList( "Windows", "u" );
        Object replaceTest5 = new ReplaceTransform( target5, optrue, optrue, goal5, null )
                .apply( "Android gave new life to java" );
        Assert.assertEquals( "Windows guve new life to juvu", replaceTest5 );

    }

    @Test(timeout=4000)
    public void testGeocoderTransform() {
        String expectedStreet = "Scott Street";
        Object geocoderTest1 = new GeocoderTransform( "road", Optional.empty() )
                .applyValue( getTestRow().get( "Address" ) );

        String expectedNo = "560";
        Object geocoderTest2 = new GeocoderTransform( "house_number", Optional.empty() )
                .applyValue( getTestRow().get( "Address" ) );

        String expectedType = "house";
        Object geocoderTest3 = new GeocoderTransform( "type", Optional.empty() )
                .applyValue( getTestRow().get( "Address" ) );

        String expectedGeoPoint = "37.7748313877551,-122.435947714286";
        Object geocoderTest4 = new GeocoderTransform( "geographypoint", Optional.empty() )
                .applyValue( getTestRow().get( "Address" ) );

        Assert.assertEquals( expectedStreet, geocoderTest1 );
        Assert.assertEquals( expectedNo, geocoderTest2 );
        Assert.assertEquals( expectedType, geocoderTest3 );
        Assert.assertEquals( expectedGeoPoint, geocoderTest4 );
    }


    @Test(expected = IllegalStateException.class)
    public void testArithmeticTransformInvalid() {
        // Tests with problematic input

        Transformations leftTransfos = new Transformations();
        leftTransfos.add( new ColumnTransform( "Lat" ) );

        Transformations rightTransfos = new Transformations();
        rightTransfos.add( new ColumnTransform( "Long" ) );

        Object arithmeticTest = new ArithmeticTransform(
                leftTransfos, rightTransfos, ":", Optional.empty()
        ).apply( getTestRow() );
    }


    @Test
    public void testArithmeticTransform() {
        double expectedSum = 36.23452 + 30.34573;
        double expectedDiff = 36.23452 - 30.34573;
        double expectedProd = 36.23452 * 30.34573;
        double expectedQuo = 36.23452 / 30.34573;
        double expectedSumLeft = 36.23452;

        Transformations leftTransfos = new Transformations();
        leftTransfos.add( new ColumnTransform( "Lat" ) );

        Transformations rightTransfos = new Transformations();
        rightTransfos.add( new ColumnTransform( "Long" ) );

        Object arithmeticTest1 = new ArithmeticTransform(
                leftTransfos, rightTransfos, "+", Optional.empty()
        ).apply( getTestRow() );

        if (!(arithmeticTest1 instanceof Double))
        {
            Assert.fail("Arithmetic transform with addition did not return a Double.");
        }
        Double value = (Double)arithmeticTest1;

        Assert.assertTrue( expectedSum - .00001 < value && expectedSum + .00001 > value );


        Object arithmeticTest2 = new ArithmeticTransform(
                leftTransfos, rightTransfos, "-", Optional.empty()
        ).apply( getTestRow() );

        if (!(arithmeticTest2 instanceof Double))
        {
            Assert.fail("Arithmetic transform with subtraction did not return a Double.");
        }
        value = (Double)arithmeticTest2;

        Assert.assertTrue( expectedDiff - .00001 < value && expectedDiff + .00001 > value );



        Object arithmeticTest3 = new ArithmeticTransform(
                leftTransfos, rightTransfos, "*", Optional.empty()
        ).apply( getTestRow() );

        if (!(arithmeticTest3 instanceof Double))
        {
            Assert.fail("Arithmetic transform with multiplication did not return a Double.");
        }
        value = (Double)arithmeticTest3;

        Assert.assertTrue( expectedProd - .00001 < value && expectedProd + .00001 > value );



        Object arithmeticTest4 = new ArithmeticTransform(
                leftTransfos, rightTransfos, "/", Optional.empty()
        ).apply( getTestRow() );

        if (!(arithmeticTest4 instanceof Double))
        {
            Assert.fail("Arithmetic transform with division did not return a Double.");
        }
        value = (Double)arithmeticTest4;

        Assert.assertTrue( expectedQuo - .00001 < value && expectedQuo + .00001 > value );


        // invalid value returning 0 as alternative

        Transformations rightTransfos_string = new Transformations();
        rightTransfos.add( new ValueTransform( "Long" ) );

        Object arithmeticTest5 = new ArithmeticTransform(
                leftTransfos, rightTransfos, "+", Optional.of(0.0)
        ).apply( getTestRow() );

        if (!(arithmeticTest5 instanceof Double))
        {
            Assert.fail("Arithmetic transform with sum did not return a Double.");
        }
        value = (Double)arithmeticTest5;

        Assert.assertTrue( expectedSumLeft - .00001 < value && expectedSumLeft + .00001 > value );

        // invalid value returning null as alternative

        Object arithmeticTest = new ArithmeticTransform(
                leftTransfos, rightTransfos_string, "+", Optional.empty()
        ).apply( getTestRow() );
        Assert.assertEquals( arithmeticTest , null );

    }

}
