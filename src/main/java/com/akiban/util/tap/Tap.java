/**
 * END USER LICENSE AGREEMENT (“EULA”)
 *
 * READ THIS AGREEMENT CAREFULLY (date: 9/13/2011):
 * http://www.akiban.com/licensing/20110913
 *
 * BY INSTALLING OR USING ALL OR ANY PORTION OF THE SOFTWARE, YOU ARE ACCEPTING
 * ALL OF THE TERMS AND CONDITIONS OF THIS AGREEMENT. YOU AGREE THAT THIS
 * AGREEMENT IS ENFORCEABLE LIKE ANY WRITTEN AGREEMENT SIGNED BY YOU.
 *
 * IF YOU HAVE PAID A LICENSE FEE FOR USE OF THE SOFTWARE AND DO NOT AGREE TO
 * THESE TERMS, YOU MAY RETURN THE SOFTWARE FOR A FULL REFUND PROVIDED YOU (A) DO
 * NOT USE THE SOFTWARE AND (B) RETURN THE SOFTWARE WITHIN THIRTY (30) DAYS OF
 * YOUR INITIAL PURCHASE.
 *
 * IF YOU WISH TO USE THE SOFTWARE AS AN EMPLOYEE, CONTRACTOR, OR AGENT OF A
 * CORPORATION, PARTNERSHIP OR SIMILAR ENTITY, THEN YOU MUST BE AUTHORIZED TO SIGN
 * FOR AND BIND THE ENTITY IN ORDER TO ACCEPT THE TERMS OF THIS AGREEMENT. THE
 * LICENSES GRANTED UNDER THIS AGREEMENT ARE EXPRESSLY CONDITIONED UPON ACCEPTANCE
 * BY SUCH AUTHORIZED PERSONNEL.
 *
 * IF YOU HAVE ENTERED INTO A SEPARATE WRITTEN LICENSE AGREEMENT WITH AKIBAN FOR
 * USE OF THE SOFTWARE, THE TERMS AND CONDITIONS OF SUCH OTHER AGREEMENT SHALL
 * PREVAIL OVER ANY CONFLICTING TERMS OR CONDITIONS IN THIS AGREEMENT.
 */

package com.akiban.util.tap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.*;
import java.lang.management.ManagementFactory;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;

/**
 * Loosely inspired by SystemTap, this class implements a generic mechanism for
 * timing and counting executions of code sections. Unlike SystemTap, you must
 * add explicit method calls to your code to delimit measurement start and end
 * points. However, HotSpot appears to optimize disabled instances of these
 * method calls away, so there is little or no cost in the added calls other
 * than noise in the source code.
 * <p/>
 * Application code should do something like the following:
 * <p/>
 * <pre>
 *   // As a static initializer
 *   private final static Tap MY_TAP = Tap.add("myTap");
 *   ...
 *      // Within a code body
 *   	MY_TAP.in();
 *   	...
 *   	... The code section to measure
 *   	...
 *   	MY_TAP.out();
 * </pre>
 * <p/>* The static {@link #add(Tap)} method creates an instance of an initially
 * disabled {@link Dispatch}. when enabled, this {@link Dispatch} will
 * invoke a {@link TimeAndCount} instance to count and time the invocations
 * of {@link #in()} and {@link #out()}. You may instantiate other Tap subclasses
 * with the {@link #add(Tap)} method, for example,
 * <p/>
 * <pre>
 * private final static Tap COUNT_ONLY_TAP = Tap.add(new Tap.Count(&quot;counter&quot;));
 * </pre>
 * <p/>
 * <p/>
 * When needed, call {@link #setEnabled(String, boolean)} to turn on/off
 * performance monitoring for one or more Dispatch objects by name. The String
 * argument is a regular expression to be applied to the name; for example
 * <code>Tap.setEnabled("my.*", true)</code> will enable monitoring of the code
 * section in the example above.
 * <p/>
 * The results are available in String or object form. Call {@link #report()} to
 * get a printable string representation of the results. Note that for
 * convenience when using jconsole, the {@link #report()} method also dumps the
 * report string to the log at INFO level. Use MY_TAP.getReports() to get a
 * {@link TapReport} object.
 * <p/>
 * Note that the TimeAndCount subclass of {@link Tap} uses
 * {@link System#nanoTime()} to measure elapsed time. The System#nanoTime() call
 * itself takes several hundred nanoseconds, so don't attempt to use this class
 * to measure very short code segments.
 * <p/>
 * The {@link #in()} and {@link #out()} methods are meant to be paired, but it
 * is not necessary to implement a try/finally block to guarantee that
 * <tt>out</tt> is called after <tt>in</tt>. Generally this makes the tool
 * easier to use, but it means that if you instrument a body of code that
 * sometimes or always throws an Exception, those instances will not be timed.
 * You can detect this issue in the most of the Tap subclasses by comparing the
 * reported <tt>inCount</tt> and <tt>outCount</tt> values.
 * <p/>
 * Currently the following Tap subclasses are defined:
 * <dl>
 * <dt>{@link TimeAndCount}</dt>
 * <dd>Count and measure the elapsed time between each pair of calls to the
 * {@link #in()} and {@link #out()} methods.</dd>
 * <dt>{@link Count}</dt>
 * <dd>Simply count the number of alls to {@link #in()} and {@link #out()}.
 * Faster because {@link System#nanoTime()} is not called</dd>
 * <dt>{@link PerThread}</dt>
 * <dd>Sub-dispatches each {@link #in()} and {@link #out()} call to a
 * subordinate {@link Tap} on private to the current Thread. Results are
 * reported by thread name.</dd>
 * </dl>
 * {@link PerThread} requires another Tap subclass to dispatch to, as shown
 * here:
 * <p/>
 * <pre>
 *   // As a static initializer
 *   private final static Tap ANOTHER_TAP =
 *   	Tap.add(new PerThread("anotherTap",  Tap.TimeStampLog.class));
 *   ...
 *      // Within a multi-threaded code body
 *   	ANOTHER_TAP.in();
 *   	...
 *   	... The code section to measure
 *   	...
 *   	ANOTHER_TAP.out();
 *      ...
 *      // To see a formatted text report in the log.
 * </pre>
 * <p/>
 * In this example, a {@link Count} instance will be created for each thread
 * that calls either {@link Tap#in()} or {@link Tap#out()}.
 *
 * @author peter
 */
public abstract class Tap
{
    // Tap interface

    /**
     * @return the name of the Tap
     */
    public final String getName()
    {
        return name;
    }

    /**
     * @return duration of time spent in section of code to be timed
     */
    public abstract long getDuration();

    /**
     * Reset any accumulated statistics to zero.
     */
    public abstract void reset();

    public void disable()
    {}

    public InOutTap createSubsidiaryTap(String name, InOutTap outermostRecursiveTapWrapper)
    {
        Dispatch outermostPerDispatch = (Dispatch) outermostRecursiveTapWrapper.internal();
        PerThread outermostPerThread = (PerThread) outermostPerDispatch.enabledTap();
        return new InOutTap(PerThread.createRecursivePerThread(name, outermostPerThread));
    }

    public static PointTap createCount(String name)
    {
        PointTap tap = new PointTap(add(PerThread.createPerThread(name, Count.class)));
        Tap.setEnabled(name, defaultOn(name));
        return tap;
    }

    public static InOutTap createTimer(String name)
    {
        InOutTap tap = new InOutTap(add(PerThread.createPerThread(name, TimeAndCount.class)));
        Tap.setEnabled(name, defaultOn(name));
        return tap;
    }
    
    public static InOutTap createRecursiveTimer(String name)
    {
        return new InOutTap(add(PerThread.createPerThread(name, RecursiveTap.Outermost.class)));
    }

    public static void defaultToOn(boolean defaultIsOn)
    {
        System.getProperties().setProperty(DEFAULT_ON_PROPERTY, Boolean.toString(defaultIsOn));
    }

    /**
     * Re-initialize counters and timers for selected {@link Dispatch}es.
     *
     * @param regExPattern regular expression for names of Dispatches to reset.
     * @param enabled      desired state of the Dispatches.
     */
    public static void setEnabled(String regExPattern, boolean enabled)
    {
        Pattern pattern = Pattern.compile(regExPattern);
        for (Dispatch tap : dispatchesCopy()) {
            if (pattern.matcher(tap.getName()).matches()) {
                tap.setEnabled(enabled);
            }
        }
    }

    public static void enableInitial()
    {
        if (initiallyEnabledPattern != null) {
            for (Dispatch dispatch : DISPATCHES.values()) {
                dispatch.setEnabled(initiallyEnabledPattern.matcher(dispatch.getName()).find());
            }
        }
    }
    
    public static void setInitiallyEnabled(String initiallyEnabledRegex)
    {
        initiallyEnabledPattern = Pattern.compile(initiallyEnabledRegex);
        // Enable any taps already created
        for (Dispatch dispatch : DISPATCHES.values()) {
            if (initiallyEnabledPattern.matcher(dispatch.getName()).find()) {
                dispatch.setEnabled(true);
            }
        }
    }

    /**
     * Re-initialize counters and timers for selected {@link Dispatch}es.
     *
     * @param regExPattern regular expression for names of Dispatches to reset.
     */
    public static void reset(String regExPattern)
    {
        Pattern pattern = Pattern.compile(regExPattern);
        for (Dispatch tap : dispatchesCopy()) {
            if (pattern.matcher(tap.getName()).matches()) {
                tap.reset();
            }
        }
    }

    /**
     * Fetch an array of current report statuses of {@link Tap} selected by
     * name.
     *
     * @param regExPattern regular expression for names of Dispatches to report
     * @return array of TapReport objects.
     */
    public static TapReport[] getReport(String regExPattern)
    {
        List<TapReport> reports = new ArrayList<TapReport>();
        Pattern pattern = Pattern.compile(regExPattern);
        for (Dispatch tap : dispatchesCopy()) {
            if (pattern.matcher(tap.getName()).matches()) {
                if (tap.getReports() != null) {
                    for (TapReport tapReport : tap.getReports()) {
                        reports.add(tapReport);
                    }
                }
            }
        }
        Collections.sort(reports, TAPS_BY_NAME_COMPARATOR);
        return reports.toArray(new TapReport[reports.size()]);
    }

    /**
     * Return a String containing a formatted report generated by traversing and
     * appending results from all registered Taps. This method also logs the
     * report in a human-readable format at INFO level.
     *
     * @return Formated report
     */
    public static String report()
    {
        StringBuilder sb = new StringBuilder();
        for (Tap tap : dispatchesCopy()) {
            int length = sb.length();
            tap.appendReport(null, sb);
            if (sb.length() > length) {
                sb.append(NEW_LINE);
            }
        }
        String result = sb.toString();
        LOG.info("Tap Report" + NEW_LINE + NEW_LINE + result + NEW_LINE);
        return result;
    }

    /**
     * Register an MXBean to make methods of this class available remotely from
     * JConsole or other JMX client. Does nothing if there already is a
     * registered MXBean.
     *
     * @throws NullPointerException
     * @throws MalformedObjectNameException
     * @throws NotCompliantMBeanException
     * @throws MBeanRegistrationException
     * @throws InstanceAlreadyExistsException
     * @throws Exception
     */
    public synchronized static void registerMXBean()
        throws MalformedObjectNameException,
               NullPointerException,
               InstanceAlreadyExistsException,
               MBeanRegistrationException,
               NotCompliantMBeanException
    {
        if (!registered) {
            ObjectName mxbeanName = new ObjectName("com.akiban:type=Tap");
            MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
            mbs.registerMBean(new TapMXBeanImpl(), mxbeanName);
            registered = true;
        }
    }

    /**
     * Unregister the MXBean created by {@link #registerMXBean()}. Does nothing
     * if there is no registered MXBean.
     *
     * @throws NullPointerException
     * @throws MalformedObjectNameException
     * @throws InstanceNotFoundException
     * @throws MBeanRegistrationException
     * @throws Exception
     */
    public synchronized static void unregisterMXBean()
        throws MalformedObjectNameException,
               NullPointerException,
               MBeanRegistrationException,
               InstanceNotFoundException
    {
        if (registered) {
            ObjectName mxbeanName = new ObjectName("com.akiban:type=Tap");
            MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
            mbs.unregisterMBean(mxbeanName);
            registered = false;
        }
    }
    
    public synchronized static void registerBadNestingHandler(BadNestingHandler badNestingHandler)
    {
        Tap.badNestingHandler = badNestingHandler;
    }

    // For use by this package

    abstract void in();

    abstract void out();

    abstract void appendReport(String label, StringBuilder buffer);

    abstract TapReport[] getReports();

    // For use by subclasses

    protected boolean checkNesting()
    {
        if (inCount != outCount) {
            Tap.badNestingHandler.handleBadNesting(this);
            return false;
        } else {
            return true;
        }
    }

    protected Tap(String name)
    {
        this.name = name;
    }

    // For use by this class

    private static Dispatch add(Tap tap)
    {
        Dispatch dispatch = new Dispatch(tap.getName(), tap);
        synchronized (DISPATCHES) {
            Dispatch previous = DISPATCHES.put(tap.getName(), dispatch);
            // TODO: Someday - breaks tests (e.g. PostgresServerSessionIT)
            // assert previous == null;
        }
        return dispatch;
    }

    private static boolean defaultOn(String name)
    {
        return Boolean.getBoolean(DEFAULT_ON_PROPERTY) ||
               initiallyEnabledPattern != null && initiallyEnabledPattern.matcher(name).find();
    }

    private static Collection<Dispatch> dispatchesCopy()
    {
        synchronized (DISPATCHES) {
            return new ArrayList<Dispatch>(DISPATCHES.values());
        }
    }

    // Class state

    static final Logger LOG = LoggerFactory.getLogger(Tap.class.getName());
    static final String NEW_LINE = System.getProperty("line.separator");
    private static final String DEFAULT_ON_PROPERTY = "taps_default_on";
    private static final Comparator<TapReport> TAPS_BY_NAME_COMPARATOR = new Comparator<TapReport>()
    {
        @Override
        public int compare(TapReport x, TapReport y)
        {
            return x.getName().compareTo(y.getName());
        }
    };
    static final Map<String, Dispatch> DISPATCHES = new TreeMap<String, Dispatch>();
    static volatile BadNestingHandler badNestingHandler =
        new BadNestingHandler()
        {
            @Override
            public void handleBadNesting(Tap tap)
            {
/* TODO: Re-enable warnings
                if ((count.getAndIncrement() % 1000) == 0) {
                    LOG.warn("Bad nesting encountered for tap {}, in: {}, out: {}",
                             new Object[]{tap.getName(), tap.inCount, tap.outCount});
                    LOG.warn("you are here", new Exception());
                }
*/
            }
            
            private final AtomicLong count = new AtomicLong(0);
        };
    private static boolean registered;
    private static Pattern initiallyEnabledPattern;

    // Object state

    protected final String name;
    protected volatile long inCount = 0;
    protected volatile long outCount = 0;

    // Inner classes
    
    public interface BadNestingHandler
    {
        void handleBadNesting(Tap tap);
    }
}
