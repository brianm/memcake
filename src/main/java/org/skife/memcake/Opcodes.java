package org.skife.memcake;

class Opcodes {
    static final byte get = 0x00;
    static final byte set = 0x01;
    static final byte add = 0x02;
    static final byte replace = 0x03;
    static final byte delete = 0x04;
    static final byte increment = 0x05;
    static final byte decrement = 0x06;
    static final byte quit = 0x07;
    static final byte flush = 0x08;
    static final byte getq = 0x09;
    static final byte noop = 0x0a;
    static final byte version = 0x0b;

    static final byte deleteq = 0x14;
}
