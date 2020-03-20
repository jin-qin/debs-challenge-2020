// package clusterdata.exercise2;

// import clusterdata.datatypes.EventType;
// import clusterdata.datatypes.JobEvent;
// import clusterdata.datatypes.TaskEvent;
// import clusterdata.testing.JobEventTestBase;
// import clusterdata.testing.TaskEventTestBase;
// import org.apache.flink.api.java.tuple.Tuple2;
// import org.apache.flink.api.java.tuple.Tuple5;
// import org.junit.Test;

// import java.util.List;

// import static org.junit.Assert.assertEquals;

// public class LongestSessionPerJobTest extends TaskEventTestBase<TaskEvent> {

// 	static Testable javaExercise = () -> LongestSessionPerJob.main(new String[]{});

// 	@Test
// 	public void testBusyMachines() throws Exception {

// 		// Job_1: sessions with 3, 5, and 2 events
// 		JobEvent j1_start = jobTestEvent(1, 123, EventType.SUBMIT);
// 		TaskEvent a = taskTestEvent(1, 2, EventType.SUBMIT, 10000);
// 		TaskEvent b = taskTestEvent(1, 3, EventType.SUBMIT, 11000);
// 		TaskEvent c = taskTestEvent(1, 4, EventType.SUBMIT, 23000);

// 		TaskEvent d = taskTestEvent(1, 5, EventType.SUBMIT, 640000);
// 		TaskEvent e = taskTestEvent(1, 6, EventType.SUBMIT, 658000);
// 		TaskEvent f = taskTestEvent(1, 7, EventType.SUBMIT, 659000);
// 		TaskEvent g = taskTestEvent(1, 7, EventType.SUBMIT, 659000);
// 		TaskEvent h = taskTestEvent(1, 7, EventType.SUBMIT, 659000);

// 		TaskEvent i = taskTestEvent(1, 7, EventType.SUBMIT, 1459000);
// 		TaskEvent j = taskTestEvent(1, 7, EventType.SUBMIT, 1459000);

// 		JobEvent j1_finish = jobTestEvent(1, 1559000, EventType.FINISH);

// 		// Job_2: sessions with 2 and 1 events
// 		JobEvent j2_start = jobTestEvent(2, 4242, EventType.SUBMIT);
// 		TaskEvent k = taskTestEvent(2, 2, EventType.SUBMIT, 30000);
// 		TaskEvent l = taskTestEvent(2, 3, EventType.SUBMIT, 42000);

// 		TaskEvent m = taskTestEvent(2, 5, EventType.SUBMIT, 842000);
// 		TaskEvent n = taskTestEvent(2, 2, EventType.KILL, 9002000);
// 		TaskEvent o = taskTestEvent(2, 3, EventType.FINISH, 1100000);

// 		JobEvent j2_finish = jobTestEvent(2, 1332000, EventType.FINISH);

// 		// Job_3: 1 session with 5 events
// 		JobEvent j3_start = jobTestEvent(3, 999, EventType.SUBMIT);

// 		TaskEvent p = taskTestEvent(3, 5, EventType.SUBMIT, 1200);
// 		TaskEvent q = taskTestEvent(3, 6, EventType.SUBMIT, 13000);
// 		TaskEvent r = taskTestEvent(3, 7, EventType.SUBMIT, 14000);
// 		TaskEvent s = taskTestEvent(3, 8, EventType.SUBMIT, 17000);
// 		TaskEvent t = taskTestEvent(3, 9, EventType.SUBMIT, 17400);

// 		JobEvent j3_finish = jobTestEvent(3, 9000000, EventType.FINISH);


// 		TestTaskEventSource source1 = new TestTaskEventSource(
// 				a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t
// 		);

// 		JobEventTestBase.TestJobEventSource source2 = new JobEventTestBase.TestJobEventSource(
// 				j1_start, j2_start, j3_start, j1_finish, j2_finish, j3_finish
// 		);

// 		List<?> results = results(source1, source2);

// 		assertEquals(3, results.size());

// 		assertEquals(true, results.contains(Tuple2.of(1l, 5)));
// 		assertEquals(true, results.contains(Tuple2.of(2l, 2)));
// 		assertEquals(true, results.contains(Tuple2.of(3l, 5)));
// 	}

// 	private TaskEvent taskTestEvent(long jobId, int taskIndex, EventType t, long timestamp) {
// 		return new TaskEvent(jobId, taskIndex, timestamp, 42, t, "Kate",
// 				2, 1, 0d, 0d, 0d, false, "123");
// 	}

// 	private JobEvent jobTestEvent(long id, long timestamp, EventType et) {
// 		return new JobEvent(id, timestamp, et, "Kate",
// 				"job42", "logicalJob42", 0, "");
// 	}

// 	protected List<?> results(TestTaskEventSource source1, JobEventTestBase.TestJobEventSource source2) throws Exception {
// 		return runApp(source1, source2, new TestSink<>(), javaExercise);
// 	}

// }