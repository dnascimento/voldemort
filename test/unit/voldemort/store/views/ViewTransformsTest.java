package voldemort.store.views;

import java.util.Arrays;
import java.util.List;

import junit.framework.TestCase;

import org.junit.Before;
import org.junit.Test;

import voldemort.VoldemortTestConstants;
import voldemort.client.MockStoreClientFactory;
import voldemort.client.StoreClient;

public class ViewTransformsTest extends TestCase {

    private static String storesXml = VoldemortTestConstants.getViewStoreDefinitionXml();
    private StoreClient<String, String> upperCaseClient;
    private StoreClient<Integer, List<Integer>> rangeFilterClient;

    @Override
    @Before
    public void setUp() throws Exception {
        MockStoreClientFactory factory = new MockStoreClientFactory(null,
                                                                    null,
                                                                    null,
                                                                    null,
                                                                    storesXml);
        upperCaseClient = factory.getStoreClient("test-view");
        rangeFilterClient = factory.getStoreClient("range-view");
        Integer[] values1 = { 1, 2, 3, 4, 5, 6, 7, 8 };
        Integer[] values2 = { 100, 200, 300, 400, 500, 600, 700 };
        rangeFilterClient.put(1, Arrays.asList(values1), null, 0L);
        rangeFilterClient.put(100, Arrays.asList(values2), null, 0L);
    }

    @Test
    public void testPutNegative() {
        try {
            upperCaseClient.put("test", "test2", "concat", 0L);
        } catch(UnsupportedViewOperationException ve) {} catch(Exception e) {
            fail("UnsuportedViewOperationException expected");
        }
    }

    @Test
    public void testGet() {
        assertEquals(8, rangeFilterClient.get(1, 0L).getValue().size());
        assertEquals(7, rangeFilterClient.get(100, 0L).getValue().size());
    }

    @Test
    public void testGetWithTransforms() {
        Integer[] filter1 = { 1, 10 };
        assertEquals(8, rangeFilterClient.get(1, Arrays.asList(filter1), 0L).getValue().size());
        Integer[] filter2 = { 100, 1000 };
        assertEquals(7, rangeFilterClient.get(100, Arrays.asList(filter2), 0L).getValue().size());
    }

    @Test
    public void testPut() {
        Integer[] values1 = { 9, 90, 10, 15, 25, 106 };

        rangeFilterClient.put(1, Arrays.asList(values1), 0L);

        assertEquals(14, rangeFilterClient.get(1, 0L).getValue().size());
    }

    @Test
    public void testPutWithTransforms() {
        Integer[] values1 = { 9, 90, 10, 15, 25, 106 };
        Integer[] filter1 = { 1, 10 };

        rangeFilterClient.put(1, Arrays.asList(values1), Arrays.asList(filter1), 0L);

        assertEquals(10, rangeFilterClient.get(1, Arrays.asList(filter1), 0L).getValue().size());

        Integer[] filter2 = { 5, 10 };
        assertEquals(6, rangeFilterClient.get(1, Arrays.asList(filter2), 0L).getValue().size());

        Integer[] filter3 = { 1, 50 };

        Integer[] values2 = { 90, 15, 25, 106 };
        rangeFilterClient.put(1, Arrays.asList(values2), Arrays.asList(filter3), 0L);

        assertEquals(12, rangeFilterClient.get(1, Arrays.asList(filter3), 0L).getValue().size());
    }
}