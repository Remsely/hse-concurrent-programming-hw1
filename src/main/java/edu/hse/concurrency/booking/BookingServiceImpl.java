package edu.hse.concurrency.booking;

import java.time.DateTimeException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;

public class BookingServiceImpl implements BookingService {
    private final Hall hall;
    private final Duration holdDuration;
    private final SeatData[] places;
    private final Object changeMonitor = new Object();
    private final Object expirationMonitor = new Object();
    private final Thread expirationThread;
    private volatile boolean stopped;
    private long nextId = 1;
    private long changes;
    private int activeOperations;

    private final TreeSet<Hold> expirations = new TreeSet<>((a, b) -> {
        int byTime = a.expiresAt().compareTo(b.expiresAt());
        return byTime != 0 ? byTime : Long.compare(a.id(), b.id());
    });

    public BookingServiceImpl(Hall hall, Duration holdDuration) {
        this.hall = Objects.requireNonNull(hall);
        this.holdDuration = Objects.requireNonNull(holdDuration);
        if (holdDuration.isZero() || holdDuration.isNegative()){
            throw new IllegalArgumentException("Срок брони должен быть положительным");
        }
        places = new SeatData[hall.capacity()];
        for(int i = 0; i < places.length; i++) {
            places[i] = new SeatData();
        }
        expirationThread = new Thread(this::expireHolds, "booking-expiration");
        expirationThread.setDaemon(true);
        expirationThread.start();
    }

    @Override
    public Optional<Hold> hold(String clientId, Set<Seat> seats) {
        List<Seat> ordered = validateRequest(clientId, seats);
        beginOperation();
        try{
            return Optional.ofNullable(reserve(clientId, ordered, 0, false, 0));
        } finally {
            endOperation();
        }
    }

    @Override
    public Optional<Hold> holdOrWait(String clientId, Set<Seat> seats, Duration timeout)
            throws InterruptedException {
        Objects.requireNonNull(timeout);
        List<Seat> ordered = validateRequest(clientId, seats);
        if (timeout.isNegative()){
            throw new IllegalArgumentException("Отрицательный таймаут");
        }
        long nanos = toNanos(timeout);
        long deadline = nanos == 0 ? 0 : System.nanoTime() + nanos;
        beginOperation();
        try{
            while(true){
                if (Thread.interrupted()){
                    throw new InterruptedException();
                }
                long observed;
                synchronized (changeMonitor) {
                    observed = changes;
                }
                Hold hold = reserve(clientId, ordered, 0, true, deadline);
                if (hold != null){
                    return Optional.of(hold);
                }
                if (Thread.interrupted()){
                    throw new InterruptedException();
                }
                if (stopped || nanos == 0 || deadline - System.nanoTime() <= 0 || hasSoldSeat(ordered)){
                    return Optional.empty();
                }
                synchronized (changeMonitor) {
                    while(changes == observed && !stopped){
                        long remaining = deadline - System.nanoTime();
                        if (remaining <= 0){
                            return Optional.empty();
                        }
                        waitNanos(changeMonitor, remaining);
                    }
                }
            }
        } finally {
            endOperation();
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
        validateSeat(seat);
        SeatData place = getPlace(seat);
        synchronized (place) {
            if (place.sold){
                return SeatStatus.SOLD;
            }
            return isActive(place.hold, Instant.now()) ? SeatStatus.HELD : SeatStatus.FREE;
        }
    }

    @Override
    public void shutdown() {
        synchronized (changeMonitor) {
            stopped = true;
            changes++;
            changeMonitor.notifyAll();
        }
        synchronized (expirationMonitor) {
            expirationMonitor.notifyAll();
        }
        boolean interrupted = false;
        while(expirationThread.isAlive()){
            try{
                expirationThread.join();
            } catch (InterruptedException exception) {
                interrupted = true;
            }
        }

        synchronized (changeMonitor) {
            while(activeOperations != 0){
                try{
                    changeMonitor.wait();
                } catch (InterruptedException exception) {
                    interrupted = true;
                }
            }
        }
        if (interrupted){
            Thread.currentThread().interrupt();
        }
    }

    private List<Seat> validateRequest(String clientId, Set<Seat> seats) {
        Objects.requireNonNull(clientId);
        Objects.requireNonNull(seats);

        if (clientId.isBlank() || seats.isEmpty() || seats.size() > MAX_SEATS_PER_HOLD){
            throw new IllegalArgumentException("Некорректный покупатель или набор мест");
        }

        List<Seat> ordered = sortSeats(seats);

        for(Seat seat : ordered) {
            validateSeat(seat);
        }

        return ordered;
    }

    private List<Seat> sortSeats(Set<Seat> seats) {

        List<Seat> result = new ArrayList<>(seats);

        for(Seat seat : result) {
            Objects.requireNonNull(seat);
        }

        result.sort((a, b) -> {
            int byRow = Integer.compare(a.row(), b.row());
            return byRow != 0 ? byRow : Integer.compare(a.number(), b.number());
        });

        return result;
    }

    private void validateSeat(Seat seat) {
        Objects.requireNonNull(seat);
        if (!hall.contains(seat)){
            throw new IllegalArgumentException("Места нет в зале: " + seat);
        }
    }

    private SeatData getPlace(Seat seat) {
        return places[(seat.row() - 1) * hall.seatsPerRow() + seat.number() - 1];
    }

    private void beginOperation() {
        synchronized (changeMonitor) {
            if (stopped){
                throw new IllegalStateException("Сервис остановлен");
            }
            activeOperations++;
        }
    }

    private void endOperation() {
        synchronized (changeMonitor) {
            activeOperations--;
            if (activeOperations == 0){
                changeMonitor.notifyAll();
            }
        }
    }


    private Hold reserve(String clientId, List<Seat> seats, int index, boolean interruptible, long deadline) {
        if (index < seats.size()){
            synchronized (getPlace(seats.get(index))) {
                return reserve(clientId, seats, index + 1, interruptible, deadline);
            }
        }

        Instant now = Instant.now();
        long id;

        for(Seat seat : seats) {
            SeatData place = getPlace(seat);
            if (place.sold || isActive(place.hold, now)){
                return null;
            }
        }

        synchronized (changeMonitor) {
            if (stopped || (interruptible && Thread.currentThread().isInterrupted())
                    || (deadline != 0 && deadline - System.nanoTime() <= 0)){
                return null;
            }
            id = nextId++;
        }

        Hold hold = new Hold(id, clientId, new HashSet<>(seats), expirationTime(Instant.now()));

        for(Seat seat : seats) {
            getPlace(seat).hold = hold;
        }

        synchronized (expirationMonitor) {
            expirations.add(hold);
            expirationMonitor.notifyAll();
        }

        return hold;
    }

    private boolean hasSoldSeat(List<Seat> seats) {
        for(Seat seat : seats) {
            if (status(seat) == SeatStatus.SOLD){
                return true;
            }
        }
        return false;
    }

    private boolean isActive(Hold hold, Instant now) {
        return hold != null && now.isBefore(hold.expiresAt());
    }

    private boolean finishHold(Hold hold, boolean sell) {
        Objects.requireNonNull(hold);
        beginOperation();
        try{
            if (hold.seats().isEmpty()){
                return false;
            }
            Seat first = hold.seats().iterator().next();
            if (!hall.contains(first)){
                return false;
            }
            SeatData place = getPlace(first);
            synchronized (place) {

                if (place.hold != hold){
                    return false;
                }
            }
            return finish(hold, sortSeats(hold.seats()), 0, sell, false);
        } finally {
            endOperation();
        }
    }

    private boolean finish(Hold hold, List<Seat> seats, int index, boolean sell, boolean expiration) {

        if (index < seats.size()){
            synchronized (getPlace(seats.get(index))) {
                return finish(hold, seats, index + 1, sell, expiration);
            }
        }

        if (!expiration && stopped){
            throw new IllegalStateException("Сервис остановлен");
        }

        boolean valid = Instant.now().isBefore(hold.expiresAt());
        boolean changed = false;

        if (expiration && valid){
            synchronized (expirationMonitor) {
                expirations.add(hold);
                expirationMonitor.notifyAll();
            }
            return false;
        }

        for(Seat seat : seats) {
            SeatData place = getPlace(seat);
            if (place.hold == hold){
                place.hold = null;
                if (sell && valid && !expiration){
                    place.sold = true;
                }
                changed = true;
            }
        }

        synchronized (expirationMonitor) {
            expirations.remove(hold);
            expirationMonitor.notifyAll();
        }

        if (changed){
            synchronized (changeMonitor) {
                changes++;
                changeMonitor.notifyAll();
            }
        }

        return changed && valid && !expiration;
    }

    private void expireHolds() {
        while(!stopped){
            try{
                Hold hold;
                synchronized (expirationMonitor) {
                    while(true){
                        if (stopped){
                            return;
                        }
                        if (expirations.isEmpty()){
                            expirationMonitor.wait();
                        } else{
                            hold = expirations.first();
                            long remaining = toNanos(Duration.between(Instant.now(), hold.expiresAt()));
                            if (remaining <= 0){
                                expirations.remove(hold);
                                break;
                            }
                            waitNanos(expirationMonitor, remaining);
                        }
                    }
                }

                finish(hold, sortSeats(hold.seats()), 0, false, true);
            } catch (InterruptedException exception) {

            }
        }
    }

    private Instant expirationTime(Instant now) {

        try{
            return now.plus(holdDuration);
        } catch (DateTimeException | ArithmeticException exception) {
            return Instant.MAX;
        }
    }

    private static long toNanos(Duration duration) {
        try{
            return duration.toNanos();
        } catch (ArithmeticException exception) {
            return duration.isNegative() ? Long.MIN_VALUE : Long.MAX_VALUE;
        }
    }

    private static void waitNanos(Object monitor, long nanos) throws InterruptedException {
        monitor.wait(nanos / 1_000_000, (int) (nanos % 1_000_000));
    }

    private static class SeatData {
        private boolean sold;
        private Hold hold;
    }
}
