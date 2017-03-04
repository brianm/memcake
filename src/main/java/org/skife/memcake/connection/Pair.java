package org.skife.memcake.connection;

final class Pair<L, R> {
    private final L left;
    private final R right;

    Pair(L left, R right) {
        this.left = left;
        this.right = right;
    }

    public L left() {
        return left;
    }

    public R right() {
        return right;
    }

    static <L, R> Pair<L, R> of(L l, R r) {
        return new Pair(l, r);
    }
}
