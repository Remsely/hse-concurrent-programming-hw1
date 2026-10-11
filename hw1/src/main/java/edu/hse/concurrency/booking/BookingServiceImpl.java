package edu.hse.concurrency.booking;

import java.time.Duration;
import java.util.Optional;
import java.util.Set;

public class BookingServiceImpl implements BookingService {

    public BookingServiceImpl(Hall hall, Duration holdDuration) {
        // TODO
    }

    @Override
    public Optional<Hold> hold(String clientId, Set<Seat> seats) {
        throw new UnsupportedOperationException("TODO");
    }

    @Override
    public Optional<Hold> holdOrWait(String clientId, Set<Seat> seats, Duration timeout)
            throws InterruptedException {
        throw new UnsupportedOperationException("TODO");
    }

    @Override
    public boolean confirm(Hold hold) {
        throw new UnsupportedOperationException("TODO");
    }

    @Override
    public boolean release(Hold hold) {
        throw new UnsupportedOperationException("TODO");
    }

    @Override
    public SeatStatus status(Seat seat) {
        throw new UnsupportedOperationException("TODO");
    }

    @Override
    public void shutdown() {
        throw new UnsupportedOperationException("TODO");
    }
}
