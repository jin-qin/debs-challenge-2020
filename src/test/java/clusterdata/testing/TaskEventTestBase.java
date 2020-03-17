package clusterdata.testing;

import clusterdata.datatypes.TaskEvent;
import clusterdata.utils.AppBase;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.util.ArrayList;
import java.util.List;

public abstract class TaskEventTestBase<OUT> {

	public static class TestTaskEventSource extends TestSource implements ResultTypeQueryable<TaskEvent> {
		public TestTaskEventSource(Object ... eventsOrWatermarks) {
			this.testStream = eventsOrWatermarks;
		}

		@Override
		public TypeInformation<TaskEvent> getProducedType() {
			return TypeInformation.of(TaskEvent.class);
		}
	}

	public static class TestStringSource extends TestSource implements ResultTypeQueryable<String> {
		public TestStringSource(Object ... eventsOrWatermarks) {
			this.testStream = eventsOrWatermarks;
		}

		@Override
		public TypeInformation<String> getProducedType() {
			return TypeInformation.of(String.class);
		}
	}

	public static class TestSink<OUT> implements SinkFunction<OUT> {

		public static final List values = new ArrayList<>();

		@Override
		public void invoke(OUT value, Context context) throws Exception {
			values.add(value);
		}
	}

	public interface Testable {
		public abstract void main() throws Exception;
	}

	protected List<OUT> runApp(TestTaskEventSource source, TestSink<OUT> sink, Testable exercise, Testable solution) throws Exception {
		AppBase.taskEvents = source;

		return execute(sink, exercise, solution);
	}

	protected List<OUT> runApp(TestTaskEventSource source, TestSink<OUT> sink, Testable solution) throws Exception {
		AppBase.taskEvents = source;

		return execute(sink, solution);
	}

	protected List<OUT> runApp(TestTaskEventSource source1, JobEventTestBase.TestJobEventSource source2, TestSink<OUT> sink, Testable solution) throws Exception {
		AppBase.taskEvents = source1;
		AppBase.jobEvents = source2;

		return execute(sink, solution);
	}

	protected List<OUT> runApp(TestTaskEventSource source, TestStringSource strings, TestSink<OUT> sink, Testable exercise, Testable solution) throws Exception {
		AppBase.taskEvents = source;
		AppBase.strings = strings;

		return execute(sink, exercise, solution);
	}

	private List<OUT> execute(TestSink<OUT> sink, Testable exercise, Testable solution) throws Exception {
		sink.values.clear();

		AppBase.out = sink;
		AppBase.parallelism = 1;

		try {
			exercise.main();
		} catch (JobExecutionException e) {
				throw e;
		}

		return sink.values;
	}

	private List<OUT> execute(TestSink<OUT> sink, Testable solution) throws Exception {
		sink.values.clear();

		AppBase.out = sink;
		AppBase.parallelism = 1;

		solution.main();

		return sink.values;
	}
}
