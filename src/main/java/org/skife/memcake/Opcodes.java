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
    static final byte getk = 0x0c;
    static final byte getkq = 0x0d;
    static final byte append = 0x0e;
    static final byte prepend = 0x0f;
    static final byte stat = 0x10;
    static final byte setq = 0x11;
    static final byte addq = 0x12;
    static final byte replaceq = 0x13;
    static final byte deleteq = 0x14;
    static final byte incrementq = 0x15;
    static final byte decrementq = 0x16;
    static final byte quitq = 0x17;
    static final byte flushq = 0x18;
    static final byte appendq = 0x19;
    static final byte prependq = 0x1a;
}
