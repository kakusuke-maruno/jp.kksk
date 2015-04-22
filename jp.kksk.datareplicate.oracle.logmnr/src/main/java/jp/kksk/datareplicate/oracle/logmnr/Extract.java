package jp.kksk.datareplicate.oracle.logmnr;

public abstract class Extract implements Runnable {
	protected abstract void execute(Trail trail);

	@Override
	public void run() {
		try {
			execute(Trail.LOG.take());
		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		}
	}
}
