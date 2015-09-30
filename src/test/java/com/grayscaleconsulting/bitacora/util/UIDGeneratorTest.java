package com.grayscaleconsulting.bitacora.util;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class UIDGeneratorTest {
    private UIDGenerator generator;
    
    @Before
    public void setup() {
        generator = UIDGenerator.getInstance();
    }
    
    @After
    public void teardown() {
        generator = null;
    }

    @Test
    public void testGetNextSequence() throws Exception {
        long num = generator.getNextSequence();
        assertEquals(num + 1, generator.getNextSequence());
        assertEquals(num + 2, generator.getNextSequence());
        assertEquals(num + 3, generator.getNextSequence());
        
        UIDGenerator anotherInstance = UIDGenerator.getInstance();
        assertEquals(num + 4, anotherInstance.getNextSequence());
    }
    
    @Test
    public void testIdsAreSequential() throws Exception {
        generator.setNodeName("nodeName");
        long id1 = generator.getUID();
        long id2 = generator.getUID();
        long id3 = generator.getUID();
        long id4 = generator.getUID();
        long id5 = generator.getUID();
        
        assertTrue(id1 < id5);
        assertTrue(id4 < id5);
        assertTrue(id1 < id3);
        assertTrue(id3 < id4);
        assertTrue(id2 < id4);
    }
    
    @Test
    public void testReturnsDifferentIdsForSameTimeButDifferentNode() {
        StubUIDGenerator generator1 = StubUIDGenerator.getInstance();
        StubUIDGenerator generator2 = StubUIDGenerator.getInstance();
        
        generator1.setNodeName("node1");
        generator2.setNodeName("node2");
        
        generator1.setTime(100000);
        generator2.setTime(100000);

        long id1 = generator1.getUID();
        long id2 = generator2.getUID();
        
        assertTrue(id1 != id2);
    }

    @Test
    public void testReturnsDifferentIdsForSameTimeAndSameNode() {
        StubUIDGenerator generator1 = StubUIDGenerator.getInstance();
        generator1.setNodeName("node1");

        generator1.setTime(100000);
        long id1 = generator1.getUID();

        generator1.setTime(100000);
        long id2 = generator1.getUID();

        assertTrue(id1 != id2);

    }

    @Test (expected = NullPointerException.class)
    public void testNodeNameRequired() {
        StubUIDGenerator generator1 = StubUIDGenerator.getInstance();
        generator1.getUID();
    }
    
}

// Private class that removes "singletoness" and allows setting values for testing
class StubUIDGenerator extends UIDGenerator{
    long currentTime = -1;

    public static StubUIDGenerator getInstance() {
        return new StubUIDGenerator();
    }

    protected StubUIDGenerator() {
        super();
    }
    
    @Override
    public void setNodeName(String nodeName) {
        this.nodeName = nodeName;
    }
    
    public void setTime(long time) {
        currentTime = time;
    }

    protected long getCurrentTime() {
        if(-1 == currentTime) {
            return super.getCurrentTime();
        } else {
            return currentTime;
        }
    }
}
