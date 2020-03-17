package clusterdata.testing;

import clusterdata.datatypes.JobEvent;
import clusterdata.utils.AppBase;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.util.ArrayList;
import java.util.List;

public abstract class JobEventTestBase<OUT> {
	public static class TestJobEventSource extends TestSource implements ResultTypeQueryable<JobEvent> {
		public TestJobEventSource(Object ... eventsOrWatermarks) {
			this.testStream = eventsOrWatermarks;
		}

		@Override
		public TypeInformation<JobEvent> getProducedType() {
			return TypeInformation.of(JobEvent.class);
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

		// must be static
		public static final List values = new ArrayList<>();

		@Override
		public void invoke(OUT value, Context context) throws Exception {
			values.add(value);
		}
	}

	public interface Testable {
		public abstract void main() throws Exception;
	}

	protected List<OUT> runApp(TestJobEventSource source, TestSink<OUT> sink, Testable exercise, Testable solution) throws Exception {
		AppBase.jobEvents = source;

		return execute(sink, exercise, solution);
	}

	protected List<OUT> runApp(TestJobEventSource jobs, TestSink<OUT> sink, Testable solution) throws Exception {
		AppBase.jobEvents = jobs;

		return execute(sink, solution);
	}

	protected List<OUT> runApp(TestJobEventSource jobs, TestStringSource strings, TestSink<OUT> sink, Testable exercise, Testable solution) throws Exception {
		AppBase.jobEvents = jobs;
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
