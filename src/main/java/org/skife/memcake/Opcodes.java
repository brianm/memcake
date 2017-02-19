package org.skife.memcake;

class Opcodes {
    static final byte get = 0x00;
    static final byte set = 0x01;
    static final byte add = 0x02;
    static final byte replace = 0x03;
    static final byte delete = 0x04;
    static final byte increment = 0x05;
    static final byte deleteq = 0x14;
    static final byte flush = 0x08;
    static final byte getq = 0x09;
}
