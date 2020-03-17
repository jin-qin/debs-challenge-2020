package clusterdata.exercise2;

import clusterdata.datatypes.EventType;
import clusterdata.datatypes.TaskEvent;
import clusterdata.exercise2.BusyMachines;
import clusterdata.exercise2.GlobalTaskStatistics;
import clusterdata.testing.TaskEventTestBase;
import com.google.common.collect.Lists;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;

public class GlobalTaskStatisticsTest extends TaskEventTestBase<TaskEvent> {

	static Testable javaExercise = () -> GlobalTaskStatistics.main(new String[]{});

	@Test
	public void testBusyMachines() throws Exception {

		// Window 0: (3, 3, 0)
		TaskEvent a = testEvent(1, EventType.FINISH, 10000);
		TaskEvent b = testEvent(1, EventType.FINISH, 11000);
		TaskEvent c = testEvent(1, EventType.FINISH, 23000);
		TaskEvent d = testEvent(1, EventType.FAIL, 40000);
		TaskEvent e = testEvent(1, EventType.FAIL, 58000);
		TaskEvent f = testEvent(1, EventType.FAIL, 59000);

		// Window 60000: (0, 0, 3)
		TaskEvent g = testEvent(2, EventType.KILL, 60000);
		TaskEvent h = testEvent(2, EventType.SUBMIT, 65000);
		TaskEvent i = testEvent(2, EventType.SUBMIT, 86000);
		TaskEvent j = testEvent(2, EventType.KILL, 92000);
		TaskEvent k = testEvent(2, EventType.KILL, 110000);

		// Window 120000: (1, 2, 2)
		TaskEvent l = testEvent(3, EventType.FAIL, 123000);
		TaskEvent m = testEvent(3, EventType.KILL, 130000);
		TaskEvent n = testEvent(3, EventType.FAIL, 140000);
		TaskEvent o = testEvent(3, EventType.KILL, 170000);
		TaskEvent p = testEvent(3, EventType.FINISH, 174000);

		// Window 180000: (1, 0, 0)
		TaskEvent q = testEvent(2, EventType.FINISH, 220000);

		TestTaskEventSource source = new TestTaskEventSource(
				a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q
		);

		List<?> results = results(source);

		assertEquals(4, results.size());

		assertEquals(true, results.contains(Tuple4.of(0l, 3, 3, 0)));
		assertEquals(true, results.contains(Tuple4.of(60000l, 0, 0, 3)));
		assertEquals(true, results.contains(Tuple4.of(120000l, 1, 2, 2)));
		assertEquals(true, results.contains(Tuple4.of(180000l, 1, 0, 0)));
	}

	private TaskEvent testEvent(long machineId, EventType t, long timestamp) {
		return new TaskEvent(42, 9, timestamp, machineId, t, "Kate",
				2, 1, 0d, 0d, 0d, false, "123");
	}

	protected List<?> results(TestTaskEventSource source) throws Exception {
		return runApp(source, new TestSink<>(), javaExercise);
	}

}