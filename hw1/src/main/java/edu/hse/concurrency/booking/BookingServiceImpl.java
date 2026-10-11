package edu.hse.concurrency.booking;

import java.time.DateTimeException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;

public class BookingServiceImpl implements BookingService {

    private final Hall hall;
    private final Duration holdDuration;
    private final SeatCell[] cells;
    private final Object control = new Object();
    // коллекции и счётчики ниже защищены монитором control
    private final TreeSet<HoldEntry> expirations = new TreeSet<>(
            Comparator.comparing((HoldEntry entry) -> entry.hold.expiresAt())
                    .thenComparingLong(entry -> entry.hold.id()));
    private final Set<Waiter> waitingCalls = new HashSet<>();
    private long nextId;
    private int activeOperations;
    private volatile boolean stopped;
    private final Thread expirationThread;

    public BookingServiceImpl(Hall hall, Duration holdDuration) {
        this.hall = Objects.requireNonNull(hall, "hall");
        this.holdDuration = Objects.requireNonNull(holdDuration, "holdDuration");
        if (holdDuration.isZero() || holdDuration.isNegative()) {
            throw new IllegalArgumentException("holdDuration must be positive");
        }
        cells = new SeatCell[hall.capacity()];
        Arrays.setAll(cells, ignored -> new SeatCell());
        expirationThread = new Thread(this::expireHolds, "booking-expiration");
        expirationThread.setDaemon(true);
        expirationThread.start();
    }

    @Override
    public Optional<Hold> hold(String clientId, Set<Seat> seats) {
        Selection selection = validateRequest(clientId, seats);
        enter(null);
        try {
            return withSeats(selection.indices, 0, () -> {
                requireRunning();
                if (availability(selection.indices, Instant.now()) != SeatStatus.FREE) {
                    return Optional.empty();
                }
                Hold issued = issue(clientId, selection, null).hold;
                if (issued == null) {
                    throw new IllegalStateException("Service is stopped");
                }
                return Optional.of(issued);
            });
        } finally {
            leave(null);
        }
    }

    @Override
    public Optional<Hold> holdOrWait(String clientId, Set<Seat> seats, Duration timeout)
            throws InterruptedException {
        Objects.requireNonNull(timeout, "timeout");
        Selection selection = validateRequest(clientId, seats);
        if (timeout.isNegative()) {
            throw new IllegalArgumentException("timeout must not be negative");
        }
        long started = System.nanoTime();
        Waiter waiter = new Waiter(timeout, started);
        enter(waiter);
        try {
            while (true) {
                if (Thread.interrupted()) {
                    throw new InterruptedException();
                }
                long observed;
                synchronized (waiter) {
                    observed = waiter.version;
                }
                Attempt attempt = withSeats(selection.indices, 0, () -> {
                    if (Thread.currentThread().isInterrupted()) {
                        return Attempt.INTERRUPTED;
                    }
                    if (stopped || (!timeout.isZero() && expired(timeout, started))) {
                        return Attempt.DONE;
                    }
                    SeatStatus state = availability(selection.indices, Instant.now());
                    if (state == SeatStatus.SOLD) {
                        return Attempt.DONE;
                    }
                    if (state == SeatStatus.FREE) {
                        // перед выдачей брони повторно проверяем прерыв потока
                        if (Thread.currentThread().isInterrupted()) {
                            return Attempt.INTERRUPTED;
                        }
                        return issue(clientId, selection, waiter);
                    }
                    if (timeout.isZero()) {
                        return Attempt.DONE;
                    }
                    for (int index : selection.indices) {
                        SeatCell cell = cells[index];
                        if (cell.waiters == null) {
                            cell.waiters = new HashSet<>();
                        }
                        cell.waiters.add(waiter);
                    }
                    return Attempt.WAIT;
                });
                if (attempt.interrupted) {
                    Thread.interrupted();
                    throw new InterruptedException();
                }
                if (!attempt.waiting) {
                    return Optional.ofNullable(attempt.hold);
                }
                // удерживая монитор ожидающего запроса, нельзя захватывать мониторы мест
                synchronized (waiter) {
                    while (waiter.version == observed && !stopped) {
                        Duration remaining = remaining(timeout, started);
                        if (remaining.isNegative() || remaining.isZero()) {
                            break;
                        }
                        timedWait(waiter, remaining);
                    }
                }
            }
        } finally {
            withSeats(selection.indices, 0, () -> {
                for (int index : selection.indices) {
                    SeatCell cell = cells[index];
                    if (cell.waiters != null) {
                        cell.waiters.remove(waiter);
                        if (cell.waiters.isEmpty()) {
                            cell.waiters = null;
                        }
                    }
                }
                return null;
            });
            leave(waiter);
        }
    }

    @Override
    public boolean confirm(Hold hold) {
        return finishHold(hold, true);
    }

    @Override
    public boolean release(Hold hold) {
        return finishHold(hold, false);
    }

    @Override
    public SeatStatus status(Seat seat) {
        SeatCell cell = cells[indexOf(seat)];
        synchronized (cell) {
            expireCell(cell, Instant.now());
            return cell.sold ? SeatStatus.SOLD
                    : cell.owner == null ? SeatStatus.FREE : SeatStatus.HELD;
        }
    }

    @Override
    public void shutdown() {
        List<Waiter> toWake;
        synchronized (control) {
            stopped = true;
            toWake = new ArrayList<>(waitingCalls);
            expirations.clear();
            control.notifyAll();
        }
        toWake.forEach(BookingServiceImpl::signal);

        boolean interrupted = false;
        synchronized (control) {
            while (activeOperations != 0) {
                try {
                    control.wait();
                } catch (InterruptedException e) {
                    interrupted = true;
                }
            }
        }
        // каждый вызов shutdown дожидается завершения фонового потока
        while (true) {
            try {
                expirationThread.join();
                break;
            } catch (InterruptedException e) {
                interrupted = true;
            }
        }
        if (interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    private boolean finishHold(Hold hold, boolean sell) {
        Objects.requireNonNull(hold, "hold");
        Selection selection = validateRequest(hold.clientId(), hold.seats());
        enter(null);
        try {
            return withSeats(selection.indices, 0, () -> {
                requireRunning();
                HoldEntry entry = cells[selection.indices[0]].owner;
                if (entry == null || entry.hold != hold
                        || !Instant.now().isBefore(hold.expiresAt())) {
                    return false;
                }
                for (int index : selection.indices) {
                    if (cells[index].owner != entry) {
                        return false;
                    }
                }
                Set<Waiter> toWake = new HashSet<>();
                for (int index : selection.indices) {
                    SeatCell cell = cells[index];
                    cell.owner = null;
                    cell.sold = sell;
                    if (cell.waiters != null) {
                        toWake.addAll(cell.waiters);
                    }
                }
                synchronized (control) {
                    if (expirations.remove(entry)) {
                        control.notifyAll();
                    }
                }
                // при продаже будим ожидающие запросы, чтобы они сразк вернули empty
                toWake.forEach(BookingServiceImpl::signal);
                return true;
            });
        } finally {
            leave(null);
        }
    }

    private Attempt issue(String clientId, Selection selection, Waiter waiter) {
        HoldEntry entry;
        synchronized (control) {
            if (stopped) {
                return Attempt.DONE;
            }
            if (waiter != null) {
                if (Thread.currentThread().isInterrupted()) {
                    return Attempt.INTERRUPTED;
                }
                if (!waiter.timeout.isZero() && expired(waiter.timeout, waiter.started)) {
                    return Attempt.DONE;
                }
            }
            Hold hold = new Hold(++nextId, clientId, selection.seats, expirationTime());
            entry = new HoldEntry(hold, selection.indices);
            expirations.add(entry);
            if (expirations.first() == entry) {
                control.notifyAll();
            }
        }
        // все мониторы запрошенных мест здесь ещё удерживаются
        for (int index : selection.indices) {
            cells[index].owner = entry;
        }
        return new Attempt(entry.hold, false, false);
    }

    private SeatStatus availability(int[] indices, Instant now) {
        boolean held = false;
        for (int index : indices) {
            SeatCell cell = cells[index];
            expireCell(cell, now);
            if (cell.sold) {
                return SeatStatus.SOLD;
            }
            held |= cell.owner != null;
        }
        return held ? SeatStatus.HELD : SeatStatus.FREE;
    }

    private void expireCell(SeatCell cell, Instant now) {
        if (cell.owner != null && !now.isBefore(cell.owner.hold.expiresAt())) {
            cell.owner = null;
            if (cell.waiters != null) {
                cell.waiters.forEach(BookingServiceImpl::signal);
            }
        }
    }

    private void expireHolds() {
        while (true) {
            HoldEntry entry;
            try {
                entry = nextExpiration();
            } catch (InterruptedException e) {
                // shutdown будит поток через notifyAll. прерывание не останавливает обработку
                continue;
            }
            if (entry == null) {
                return;
            }
            // перед захватом мониторов мест монитор control уже освобождён
            withSeats(entry.indices, 0, () -> {
                Instant now = Instant.now();
                if (now.isBefore(entry.hold.expiresAt())) {
                    // на случай, если после извлечения события из очереди системное время сдвинулось назад
                    synchronized (control) {
                        if (!stopped && hasOwner(entry)) {
                            expirations.add(entry);
                            control.notifyAll();
                        }
                    }
                    return null;
                }
                for (int index : entry.indices) {
                    SeatCell cell = cells[index];
                    if (cell.owner == entry) {
                        expireCell(cell, now);
                    }
                }
                return null;
            });
        }
    }

    private boolean hasOwner(HoldEntry entry) {
        for (int index : entry.indices) {
            if (cells[index].owner == entry) {
                return true;
            }
        }
        return false;
    }

    private HoldEntry nextExpiration() throws InterruptedException {
        synchronized (control) {
            while (!stopped) {
                if (expirations.isEmpty()) {
                    control.wait();
                } else {
                    Duration delay = Duration.between(Instant.now(), expirations.first().hold.expiresAt());
                    if (delay.isNegative() || delay.isZero()) {
                        return expirations.pollFirst();
                    }
                    timedWait(control, delay);
                }
            }
            return null;
        }
    }

    private void enter(Waiter waiter) {
        synchronized (control) {
            requireRunning();
            activeOperations++;
            if (waiter != null) {
                waitingCalls.add(waiter);
            }
        }
    }

    private void leave(Waiter waiter) {
        synchronized (control) {
            if (waiter != null) {
                waitingCalls.remove(waiter);
            }
            activeOperations--;
            if (stopped && activeOperations == 0) {
                control.notifyAll();
            }
        }
    }

    private void requireRunning() {
        if (stopped) {
            throw new IllegalStateException("Service is stopped");
        }
    }

    private Selection validateRequest(String clientId, Set<Seat> seats) {
        Objects.requireNonNull(clientId, "clientId");
        Set<Seat> copy = Set.copyOf(Objects.requireNonNull(seats, "seats"));
        if (clientId.isBlank()) {
            throw new IllegalArgumentException("clientId must not be blank");
        }
        if (copy.isEmpty() || copy.size() > MAX_SEATS_PER_HOLD) {
            throw new IllegalArgumentException("A hold requires 1 to 10 seats");
        }
        int[] indices = new int[copy.size()];
        int i = 0;
        for (Seat seat : copy) {
            indices[i++] = indexOf(seat);
        }
        Arrays.sort(indices);
        return new Selection(copy, indices);
    }

    private int indexOf(Seat seat) {
        Objects.requireNonNull(seat, "seat");
        if (!hall.contains(seat)) {
            throw new IllegalArgumentException("Seat is outside the hall: " + seat);
        }
        return (seat.row() - 1) * hall.seatsPerRow() + seat.number() - 1;
    }

    private <T> T withSeats(int[] indices, int offset, Action<T> action) {
        if (offset == indices.length) {
            return action.run();
        }
        synchronized (cells[indices[offset]]) {
            return withSeats(indices, offset + 1, action);
        }
    }

    private Instant expirationTime() {
        try {
            return Instant.now().plus(holdDuration);
        } catch (DateTimeException | ArithmeticException e) {
            // срок брони мож вых за range Instant
            return Instant.MAX;
        }
    }

    private static boolean expired(Duration timeout, long started) {
        Duration left = remaining(timeout, started);
        return left.isNegative() || left.isZero();
    }

    private static Duration remaining(Duration timeout, long started) {
        return timeout.minusNanos(System.nanoTime() - started);
    }

    private static void timedWait(Object monitor, Duration duration) throws InterruptedException {
        long millis;
        int nanos = duration.getNano() % 1_000_000;
        try {
            millis = duration.toMillis();
        } catch (ArithmeticException e) {
            millis = Long.MAX_VALUE;
            nanos = 0;
        }
        monitor.wait(millis, nanos);
    }

    private static void signal(Waiter waiter) {
        synchronized (waiter) {
            waiter.version++;
            waiter.notifyAll();
        }
    }

    private interface Action<T> {
        T run();
    }

    // для мониторов мест и ожидающих запросов исп внутр объекты
    private static final class SeatCell {
        private boolean sold;
        private HoldEntry owner;
        private Set<Waiter> waiters;
    }

    private static final class Waiter {
        private final Duration timeout;
        private final long started;
        private long version;

        private Waiter(Duration timeout, long started) {
            this.timeout = timeout;
            this.started = started;
        }
    }

    private record Selection(Set<Seat> seats, int[] indices) { }

    private record HoldEntry(Hold hold, int[] indices) { }

    private record Attempt(Hold hold, boolean waiting, boolean interrupted) {
        private static final Attempt WAIT = new Attempt(null, true, false);
        private static final Attempt DONE = new Attempt(null, false, false);
        private static final Attempt INTERRUPTED = new Attempt(null, false, true);
    }
}
