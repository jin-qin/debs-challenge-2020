package clusterdata.exercise2;

import clusterdata.datatypes.EventType;
import clusterdata.datatypes.TaskEvent;
import clusterdata.testing.TaskEventTestBase;
import com.google.common.collect.Lists;
import org.apache.flink.api.java.tuple.Tuple5;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;

public class PerMachineTaskStatisticsTest extends TaskEventTestBase<TaskEvent> {

	static Testable javaExercise = () -> PerMachineTaskStatistics.main(new String[]{});

	@Test
	public void testBusyMachines() throws Exception {

		// Window 0: (1, 1, 1, 0), (2, 1, 0, 0), (3, 1, 2, 0)
		TaskEvent a1_1 = testEvent(1, EventType.FINISH, 10000);
		TaskEvent b1_1 = testEvent(2, EventType.FINISH, 11000);
		TaskEvent c1_1 = testEvent(3, EventType.FINISH, 23000);
		TaskEvent a1_2 = testEvent(1, EventType.FAIL, 40000);
		TaskEvent b1_2 = testEvent(3, EventType.FAIL, 58000);
		TaskEvent c1_2 = testEvent(3, EventType.FAIL, 59000);

		// Window 60000: (4, 0, 0, 1), (2, 0, 0, 2)
		TaskEvent a2_1 = testEvent(4, EventType.KILL, 60000);
		TaskEvent b2_1 = testEvent(4, EventType.SUBMIT, 65000);
		TaskEvent c2_1 = testEvent(2, EventType.SUBMIT, 86000);
		TaskEvent a2_2 = testEvent(2, EventType.KILL, 92000);
		TaskEvent b2_2 = testEvent(2, EventType.KILL, 110000);

		// Window 120000: (1, 1, 1, 0), (5, 0, 1, 1), (9, 0, 0, 1)
		TaskEvent a3_1 = testEvent(1, EventType.FAIL, 123000);
		TaskEvent b3_1 = testEvent(5, EventType.KILL, 130000);
		TaskEvent c3_1 = testEvent(5, EventType.FAIL, 140000);
		TaskEvent d3_1 = testEvent(9, EventType.KILL, 170000);
		TaskEvent e3_1 = testEvent(1, EventType.FINISH, 174000);

		// Window 180000: (99, 1, 0, 0)
		TaskEvent a4_1 = testEvent(99, EventType.FINISH, 220000);


		TestTaskEventSource source = new TestTaskEventSource(
				a1_1, b1_1, c1_1, a1_2, b1_2, c1_2,
				a2_1, b2_1, c2_1, a2_2, b2_2,
				a3_1, b3_1, c3_1, d3_1, e3_1,
				a4_1
		);

		List<?> results = results(source);

		assertEquals(9, results.size());

		assertEquals(true, results.contains(Tuple5.of(0l, 1l, 1, 1, 0)));
		assertEquals(true, results.contains(Tuple5.of(0l, 2l, 1, 0, 0)));
		assertEquals(true, results.contains(Tuple5.of(0l, 3l, 1, 2, 0)));
		assertEquals(true, results.contains(Tuple5.of(60000l, 4l, 0, 0, 1)));
		assertEquals(true, results.contains(Tuple5.of(60000l, 2l, 0, 0, 2)));
		assertEquals(true, results.contains(Tuple5.of(120000l, 1l, 1, 1, 0)));
		assertEquals(true, results.contains(Tuple5.of(120000l, 5l, 0, 1, 1)));
		assertEquals(true, results.contains(Tuple5.of(120000l, 9l, 0, 0, 1)));
		assertEquals(true, results.contains(Tuple5.of(180000l, 99l, 1, 0, 0)));

	}

	private TaskEvent testEvent(long machineId, EventType t, long timestamp) {
		return new TaskEvent(42, 9, timestamp, machineId, t, "Kate",
				2, 1, 0d, 0d, 0d, false, "123");
	}

	protected List<?> results(TestTaskEventSource source) throws Exception {
		return runApp(source, new TestSink<>(), javaExercise);
	}

}