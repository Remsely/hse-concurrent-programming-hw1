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
    private final Duration holdDur;
    private final SeatState[] seats;
    private final Object monitor = new Object();
    private final TreeSet<Reservation> expirations = new TreeSet<>(Comparator
            .comparing((Reservation reservation) -> reservation.hold.expiresAt())
            .thenComparingLong(reservation -> reservation.hold.id()));
    private final Set<Waiter> waitersSet = new HashSet<>();
    private final Thread endThread;
    private volatile boolean isStop;
    private long nextId;
    private int usedOperations;

    public BookingServiceImpl(Hall hall, Duration holdDur) {
        this.hall = Objects.requireNonNull(hall, "Зал не задан");
        this.holdDur = Objects.requireNonNull(holdDur, "Длительность брони не задана");
        if (holdDur.isZero() || holdDur.isNegative()) {
            throw new IllegalArgumentException("Длительность брони должна быть положительной");
        }
        seats = new SeatState[hall.capacity()];
        for (int i = 0; i < seats.length; i++) {
            seats[i] = new SeatState();
        }
        endThread = new Thread(this::expireReservations, "booking-expiration");
        endThread.setDaemon(true);
        endThread.start();
    }

    @Override
    public Optional<Hold> hold(String clientId, Set<Seat> seats) {
        Set<Seat> requested = validateRequest(clientId, seats);
        int[] indices = indices(requested);
        beginOperation(null);
        try {
            return locked(indices, 0, () -> {
                checkRunning();
                return Optional.ofNullable(tryHold(clientId, requested, indices));
            });
        } finally {
            endOperation(null);
        }
    }

    @Override
    public Optional<Hold> holdOrWait(String clientId, Set<Seat> seats, Duration timeout)
            throws InterruptedException {
        Objects.requireNonNull(timeout, "Таймаут ожидания не задан");
        Set<Seat> requested = validateRequest(clientId, seats);
        if (timeout.isNegative()) {
            throw new IllegalArgumentException("Таймаут ожидания не может быть отрицательным");
        }
        int[] indices = indices(requested);
        long budget = nanos(timeout);
        long started = System.nanoTime();
        Waiter waiter = new Waiter();
        beginOperation(waiter);
        try {
            boolean firstAttempt = true;
            while (true) {
                if (Thread.interrupted()) {
                    throw new InterruptedException("Ожидание бронирования прервано");
                }
                if (isStop || (!firstAttempt && remaining(started, budget) <= 0)) {
                    return Optional.empty();
                }
                firstAttempt = false;
                Hold acquired = locked(indices, 0, () -> {
                    if (isStop || Thread.currentThread().isInterrupted()
                            || (budget != 0 && remaining(started, budget) <= 0)) {
                        return null;
                    }
                    for (int index : indices) {
                        if (this.seats[index].sold) {
                            waiter.sold = true;
                            return null;
                        }
                    }
                    synchronized (waiter) {
                        waiter.changed = false;
                    }
                    Hold result = tryHold(clientId, requested, indices);
                    if (result == null) {
                        for (int index : indices) {
                            SeatState state = this.seats[index];
                            if (state.waiters == null) {
                                state.waiters = new HashSet<>();
                            }
                            state.waiters.add(waiter);
                        }
                    }
                    return result;
                });
                if (acquired != null) {
                    return Optional.of(acquired);
                }
                if (Thread.interrupted()) {
                    throw new InterruptedException("Ожидание бронирования прервано");
                }
                if (waiter.sold) {
                    return Optional.empty();
                }
                synchronized (waiter) {
                    long left;
                    while (!waiter.changed && !isStop && (left = remaining(started, budget)) > 0) {
                        waitNanos(waiter, left);
                    }
                }
            }
        } finally {
            locked(indices, 0, () -> {
                for (int index : indices) {
                    SeatState state = this.seats[index];
                    if (state.waiters != null) {
                        state.waiters.remove(waiter);
                        if (state.waiters.isEmpty()) {
                            state.waiters = null;
                        }
                    }
                }
                return null;
            });
            endOperation(waiter);
        }
    }

    @Override
    public boolean confirm(Hold hold) {
        return finish(hold, true);
    }

    @Override
    public boolean release(Hold hold) {
        return finish(hold, false);
    }

    @Override
    public SeatStatus status(Seat seat) {
        int index = index(Objects.requireNonNull(seat, "Место не задано"));
        synchronized (seats[index]) {
            SeatState state = seats[index];
            if (state.sold) {
                return SeatStatus.SOLD;
            }
            return held(state, Instant.now()) ? SeatStatus.HELD : SeatStatus.FREE;
        }
    }

    @Override
    public void shutdown() {
        List<Waiter> toWake;
        synchronized (monitor) {
            isStop = true;
            toWake = new ArrayList<>(waitersSet);
            monitor.notifyAll();
        }
        for (Waiter waiter : toWake) {
            signal(waiter);
        }
        boolean interrupted = false;
        while (endThread.isAlive()) {
            try {
                endThread.join();
            } catch (InterruptedException exception) {
                interrupted = true;
            }
        }
        synchronized (monitor) {
            while (usedOperations != 0) {
                try {
                    monitor.wait();
                } catch (InterruptedException exception) {
                    interrupted = true;
                }
            }
            expirations.clear();
        }
        if (interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    private boolean finish(Hold hold, boolean sell) {
        Objects.requireNonNull(hold, "Бронь не задана");
        beginOperation(null);
        try {
            if (hold.seats().isEmpty() || hold.seats().size() > MAX_SEATS_PER_HOLD) {
                return false;
            }
            for (Seat seat : hold.seats()) {
                if (!hall.contains(seat)) {
                    return false;
                }
            }
            int[] indices = indices(hold.seats());
            return locked(indices, 0, () -> {
                checkRunning();
                Reservation reservation = seats[indices[0]].reservation;
                if (reservation == null || reservation.hold != hold
                        || !Instant.now().isBefore(hold.expiresAt())) {
                    return false;
                }
                for (int index : indices) {
                    if (seats[index].reservation != reservation) {
                        return false;
                    }
                }
                clear(reservation, sell);
                synchronized (monitor) {
                    expirations.remove(reservation);
                    monitor.notifyAll();
                }
                return true;
            });
        } finally {
            endOperation(null);
        }
    }

    private Hold tryHold(String clientId, Set<Seat> requested, int[] indices) {
        Instant now = Instant.now();
        for (int index : indices) {
            SeatState state = seats[index];
            if (state.sold || held(state, now)) {
                return null;
            }
        }
        long id;
        synchronized (monitor) {
            id = ++nextId;
        }
        Instant expiresAt;
        try {
            expiresAt = now.plus(holdDur);
        } catch (DateTimeException | ArithmeticException exception) {
            expiresAt = Instant.MAX;
        }
        Hold hold = new Hold(id, clientId, requested, expiresAt);
        Reservation reservation = new Reservation(hold, indices);
        for (int index : indices) {
            seats[index].reservation = reservation;
        }
        synchronized (monitor) {
            expirations.add(reservation);
            monitor.notifyAll();
        }
        return hold;
    }

    private void expireReservations() {
        while (true) {
            Reservation reservation;
            try {
                reservation = nextExpiration();
            } catch (InterruptedException exception) {
                continue;
            }
            if (reservation == null) {
                return;
            }
            locked(reservation.indices, 0, () -> {
                if (!Instant.now().isBefore(reservation.hold.expiresAt())) {
                    clear(reservation, false);
                } else {
                    synchronized (monitor) {
                        expirations.add(reservation);
                    }
                }
                return null;
            });
        }
    }

    private Reservation nextExpiration() throws InterruptedException {
        synchronized (monitor) {
            while (!isStop) {
                if (expirations.isEmpty()) {
                    monitor.wait();
                } else {
                    Reservation first = expirations.first();
                    Duration left = Duration.between(Instant.now(), first.hold.expiresAt());
                    if (left.isNegative() || left.isZero()) {
                        return expirations.pollFirst();
                    }
                    waitNanos(monitor, nanos(left));
                }
            }
            return null;
        }
    }

    private void clear(Reservation reservation, boolean sell) {
        for (int index : reservation.indices) {
            SeatState state = seats[index];
            if (state.reservation == reservation) {
                state.reservation = null;
                state.sold = sell;
                if (state.waiters != null) {
                    for (Waiter waiter : state.waiters) {
                        signal(waiter);
                    }
                }
            }
        }
    }

    private static void signal(Waiter waiter) {
        synchronized (waiter) {
            waiter.changed = true;
            waiter.notifyAll();
        }
    }

    private static boolean held(SeatState state, Instant now) {
        return state.reservation != null && now.isBefore(state.reservation.hold.expiresAt());
    }

    private Set<Seat> validateRequest(String clientId, Set<Seat> requested) {
        Objects.requireNonNull(clientId, "Идентификатор покупателя не задан");
        Set<Seat> copy = Set.copyOf(Objects.requireNonNull(requested, "Набор мест не задан"));
        if (clientId.isBlank()) {
            throw new IllegalArgumentException("Идентификатор покупателя не должен быть пустым или состоять из пробелов");
        }
        if (copy.isEmpty() || copy.size() > MAX_SEATS_PER_HOLD) {
            throw new IllegalArgumentException("Бронь должна содержать от 1 до 10 мест");
        }
        for (Seat seat : copy) {
            index(seat);
        }
        return copy;
    }

    private int index(Seat seat) {
        if (!hall.contains(seat)) {
            throw new IllegalArgumentException("Место находится за пределами зала: " + seat);
        }
        return (seat.row() - 1) * hall.seatsPerRow() + seat.number() - 1;
    }

    private int[] indices(Set<Seat> requested) {
        int[] indices = new int[requested.size()];
        int position = 0;
        for (Seat seat : requested) {
            indices[position++] = index(seat);
        }
        Arrays.sort(indices);
        return indices;
    }

    private <T> T locked(int[] indices, int position, Action<T> action) {
        if (position == indices.length) {
            return action.run();
        }
        synchronized (seats[indices[position]]) {
            return locked(indices, position + 1, action);
        }
    }

    private void beginOperation(Waiter waiter) {
        synchronized (monitor) {
            checkRunning();
            usedOperations++;
            if (waiter != null) {
                waitersSet.add(waiter);
            }
        }
    }

    private void endOperation(Waiter waiter) {
        synchronized (monitor) {
            if (waiter != null) {
                waitersSet.remove(waiter);
            }
            usedOperations--;
            if (isStop && usedOperations == 0) {
                monitor.notifyAll();
            }
        }
    }

    private void checkRunning() {
        if (isStop) {
            throw new IllegalStateException("Сервис бронирования остановлен");
        }
    }

    private static long nanos(Duration duration) {
        try {
            return duration.toNanos();
        } catch (ArithmeticException exception) {
            return Long.MAX_VALUE;
        }
    }

    private static long remaining(long started, long budget) {
        return budget - (System.nanoTime() - started);
    }

    private static void waitNanos(Object monitor, long nanos) throws InterruptedException {
        monitor.wait(nanos / 1_000_000, (int) (nanos % 1_000_000));
    }

    private static final class SeatState {
        private Reservation reservation;
        private boolean sold;
        private Set<Waiter> waiters;
    }

    private record Reservation(Hold hold, int[] indices) { }

    private static final class Waiter {
        private boolean changed;
        private boolean sold;
    }

    private interface Action<T> {
        T run();
    }
}
