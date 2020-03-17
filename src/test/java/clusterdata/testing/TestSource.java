package clusterdata.testing;
import clusterdata.datatypes.JobEvent;
import clusterdata.datatypes.TaskEvent;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;

public abstract class TestSource implements SourceFunction {
	private volatile boolean running = true;
	protected Object[] testStream;

	@Override
	public void run(SourceContext ctx) throws Exception {
		for (int i = 0; (i < testStream.length) && running; i++) {
			if (testStream[i] instanceof JobEvent) {
				JobEvent e = (JobEvent) testStream[i];
				ctx.collectWithTimestamp(e, e.timestamp);
			} else if (testStream[i] instanceof TaskEvent) {
				TaskEvent e = (TaskEvent) testStream[i];
				ctx.collectWithTimestamp(e, e.timestamp);
			} else if (testStream[i] instanceof String) {
				String s = (String) testStream[i];
				ctx.collectWithTimestamp(s, 0);
			} else if (testStream[i] instanceof Long) {
				Long ts = (Long) testStream[i];
				ctx.emitWatermark(new Watermark(ts));
			} else {
				throw new RuntimeException(testStream[i].toString());
			}
		}
		// test sources are finite, so they have a Long.MAX_VALUE watermark when they finish
	}

	@Override
	public void cancel() {
		running = false;
	}
}
