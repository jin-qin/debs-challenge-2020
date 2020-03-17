package clusterdata.exercise2;

import clusterdata.datatypes.EventType;
import clusterdata.datatypes.TaskEvent;
import clusterdata.exercise2.BusyMachines;
import clusterdata.testing.TaskEventTestBase;
import com.google.common.collect.Lists;
import org.apache.flink.api.java.tuple.Tuple2;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;

public class BusyMachinesTest extends TaskEventTestBase<TaskEvent> {

	static Testable javaExercise = () -> BusyMachines.main(new String[]{"--threshold", "3"});

	@Test
	public void testBusyMachines() throws Exception {

		// Window 300000: (1, 3)
		// machine #1: busy
		TaskEvent a1_submit = testEvent(1, EventType.SUBMIT, 100000);
		TaskEvent b1_submit = testEvent(1, EventType.SUBMIT, 110000);
		TaskEvent c1_submit = testEvent(1, EventType.SUBMIT, 130000);
		TaskEvent a1_schedule = testEvent(1, EventType.SCHEDULE, 200000);
		TaskEvent b1_schedule = testEvent(1, EventType.SCHEDULE, 220000);
		TaskEvent c1_schedule = testEvent(1, EventType.SCHEDULE, 240000);

		// machine #2: not busy
		TaskEvent a2_submit = testEvent(2, EventType.SUBMIT, 105000);
		TaskEvent b2_submit = testEvent(2, EventType.SUBMIT, 115000);
		TaskEvent c2_submit = testEvent(2, EventType.SUBMIT, 135000);
		TaskEvent a2_schedule = testEvent(2, EventType.SCHEDULE, 210000);
		TaskEvent b2_schedule = testEvent(2, EventType.SCHEDULE, 260000);

		// machine #3: busy
		TaskEvent a3_schedule = testEvent(3, EventType.SCHEDULE, 215000);
		TaskEvent b3_schedule = testEvent(3, EventType.SCHEDULE, 265000);
		TaskEvent c3_schedule = testEvent(3, EventType.SCHEDULE, 266000);
		TaskEvent d3_schedule = testEvent(3, EventType.SCHEDULE, 267000);
		TaskEvent e3_schedule = testEvent(3, EventType.SCHEDULE, 268000);

		// Window 600000: (1, 2, 3)
		// machine #2: busy
		TaskEvent c2_schedule = testEvent(2, EventType.SCHEDULE, 320000);

		// Window 900000: (1, 2, 3, 4)
		// machine #4: busy
		TaskEvent a4_schedule = testEvent(4, EventType.SCHEDULE, 715000);
		TaskEvent b4_schedule = testEvent(4, EventType.SCHEDULE, 765000);
		TaskEvent c4_schedule = testEvent(4, EventType.SCHEDULE, 866000);

		// Window 1200000: (4)
		TaskEvent a5_schedule = testEvent(5, EventType.SCHEDULE, 1115000);

		// Window 1500000: (4, 5)
		TaskEvent b5_schedule = testEvent(5, EventType.SCHEDULE, 1465000);
		TaskEvent c5_schedule = testEvent(5, EventType.SCHEDULE, 1466000);

		TestTaskEventSource source = new TestTaskEventSource(
				a1_submit, b1_submit, c1_submit, a1_schedule, b1_schedule, c1_schedule,
				a2_submit, b2_submit, c2_submit, a2_schedule, b2_schedule,
				a3_schedule, b3_schedule, c3_schedule, d3_schedule, e3_schedule,
				c2_schedule,
				a4_schedule, b4_schedule, c4_schedule,
				a5_schedule,
				b5_schedule, c5_schedule
		);

		assertEquals(Lists.newArrayList(new Tuple2<Long, Integer>(300000L, 2),
				new Tuple2<Long, Integer>(600000L, 3),
				new Tuple2<Long, Integer>(900000L, 4),
				new Tuple2<Long, Integer>(1200000L, 1),
				new Tuple2<Long, Integer>(1500000L, 2),
				new Tuple2<Long, Integer>(1800000L, 1)), results(source));
	}

	private TaskEvent testEvent(long machineId, EventType t, long timestamp) {
		return new TaskEvent(42, 9, timestamp, machineId, t, "Kate",
				2, 1, 0d, 0d, 0d, false, "123");
	}

	protected List<?> results(TestTaskEventSource source) throws Exception {
		return runApp(source, new TestSink<>(), javaExercise);
	}

}