package com.flinkfasc.core.control;

/**
 * Callback invoked by {@link ControlTopicConsumer} for each {@link ControlSignal}
 * that passes the per-app filter.
 *
 * <p>Implementations must be <em>non-blocking</em>.  The consumer polling loop calls
 * {@link #handle} on its dedicated background thread; any long-running work should be
 * dispatched to a separate executor.
 *
 * <p>This is a {@link FunctionalInterface} so it can be implemented with a lambda or
 * method reference:
 * <pre>{@code
 * SignalHandler handler = signal -> {
 *     switch (signal.getSignalType()) {
 *         case PROMOTE: ...
 *         case DEMOTE:  ...
 *     }
 * };
 * }</pre>
 *
 * @see ControlTopicConsumer
 * @see com.flinkfasc.core.ShadowSink
 */
@FunctionalInterface
public interface SignalHandler {

    /**
     * Processes a {@link ControlSignal} received from the FASC control topic.
     *
     * <p>The signal has already been filtered by {@link ControlTopicConsumer}: only
     * signals whose {@code targetAppId} matches this application's {@code appId} or
     * equals {@link ControlSignal#BROADCAST} are delivered here.
     *
     * @param signal the received and pre-filtered control signal; never {@code null}
     */
    void handle(ControlSignal signal);
}
