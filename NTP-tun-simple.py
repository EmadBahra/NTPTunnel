#!/usr/bin/env python3
import argparse
import asyncio
import socket
import struct
import time
from typing import Optional

# Precompiled NTP struct: LI/VN/Mode + rest of header
# This is a simplified header for tunneling purposes.
NTP_STRUCT = struct.Struct("!BBBb11I")
NTP_EPOCH = 2208988800  # 1970-1900 in seconds

MAX_PAYLOAD = 468  # keep NTP packet size reasonable
BATCH_SIZE = 8     # how many queued payloads to send at once


def build_ntp_packet(payload: bytes) -> bytes:
    """
    Build an NTP-like packet with payload embedded in the 'transmit timestamp' area.
    This is a simplified covert channel, not a full NTP implementation.
    """
    now = time.time() + NTP_EPOCH
    seconds = int(now)
    fraction = int((now - seconds) * (1 << 32))

    # Basic header fields (LI=0, VN=4, Mode=4 (server-like))
    li_vn_mode = (0 << 6) | (4 << 3) | 4
    stratum = 1
    poll = 4
    precision = -20

    # 11 integers: root delay, root dispersion, ref id, ref ts (2), orig ts (2), recv ts (2), xmit ts (2)
    # We'll stuff payload into the last two integers (xmit timestamp) as raw bytes (truncated/padded).
    # This is just an example; adapt to your original encoding scheme.
    root_delay = 0
    root_dispersion = 0
    ref_id = 0
    ref_ts_sec = seconds
    ref_ts_frac = fraction
    orig_ts_sec = 0
    orig_ts_frac = 0
    recv_ts_sec = 0
    recv_ts_frac = 0

    # Encode up to 8 bytes of payload into two 32-bit ints
    p = payload[:8].ljust(8, b"\0")
    xmit_ts_sec = int.from_bytes(p[:4], "big")
    xmit_ts_frac = int.from_bytes(p[4:], "big")

    header = NTP_STRUCT.pack(
        li_vn_mode,
        stratum,
        poll,
        precision,
        root_delay,
        root_dispersion,
        ref_id,
        ref_ts_sec,
        ref_ts_frac,
        orig_ts_sec,
        orig_ts_frac,
        recv_ts_sec,
        recv_ts_frac,
        xmit_ts_sec,
        xmit_ts_frac,
    )
    return header


def extract_payload(packet: bytes) -> bytes:
    """
    Extract payload from the xmit timestamp fields.
    """
    if len(packet) < NTP_STRUCT.size:
        return b""
    fields = NTP_STRUCT.unpack(packet[:NTP_STRUCT.size])
    xmit_ts_sec = fields[-2]
    xmit_ts_frac = fields[-1]
    return xmit_ts_sec.to_bytes(4, "big") + xmit_ts_frac.to_bytes(4, "big")


class UdpTunnelEndpoint(asyncio.DatagramProtocol):
    def __init__(self, loop: asyncio.AbstractEventLoop, tcp_reader: asyncio.StreamReader,
                 tcp_writer: asyncio.StreamWriter, is_server: bool):
        self.loop = loop
        self.transport: Optional[asyncio.DatagramTransport] = None
        self.tcp_reader = tcp_reader
        self.tcp_writer = tcp_writer
        self.is_server = is_server
        self.send_queue = asyncio.Queue()
        self.sender_task: Optional[asyncio.Task] = None

    def connection_made(self, transport: asyncio.DatagramTransport) -> None:
        self.transport = transport
        self.sender_task = self.loop.create_task(self._sender_loop())

    def datagram_received(self, data: bytes, addr) -> None:
        payload = extract_payload(data)
        if payload:
            # Forward to TCP side
            self.loop.create_task(self._forward_to_tcp(payload))

    def error_received(self, exc: Exception) -> None:
        # You can add logging here if desired
        pass

    def connection_lost(self, exc: Optional[Exception]) -> None:
        if self.sender_task:
            self.sender_task.cancel()

    async def _forward_to_tcp(self, payload: bytes) -> None:
        try:
            self.tcp_writer.write(payload)
            await self.tcp_writer.drain()
        except ConnectionError:
            if self.transport:
                self.transport.close()

    async def _sender_loop(self) -> None:
        """
        Batch payloads from TCP and send them as NTP packets.
        """
        try:
            while True:
                batch = []
                # Always wait for at least one item
                item = await self.send_queue.get()
                batch.append(item)
                # Try to gather more without blocking too long
                for _ in range(BATCH_SIZE - 1):
                    try:
                        item = self.send_queue.get_nowait()
                        batch.append(item)
                    except asyncio.QueueEmpty:
                        break

                if not self.transport:
                    continue

                for payload in batch:
                    pkt = build_ntp_packet(payload)
                    self.transport.sendto(pkt)
        except asyncio.CancelledError:
            pass

    async def pump_tcp_to_udp(self) -> None:
        """
        Read from TCP and enqueue payloads for UDP sending.
        """
        try:
            while True:
                data = await self.tcp_reader.read(MAX_PAYLOAD)
                if not data:
                    break
                await self.send_queue.put(data)
        except ConnectionError:
            pass
        finally:
            if self.transport:
                self.transport.close()
            self.tcp_writer.close()
            await self.tcp_writer.wait_closed()


async def run_server(loop, listen_ntp, listen_tcp):
    # TCP side: accept a single client and tunnel it over UDP
    server = await asyncio.start_server(
        lambda r, w: handle_tcp_connection(loop, r, w, listen_ntp, is_server=True),
        host="0.0.0.0",
        port=listen_tcp,
    )
    async with server:
        await server.serve_forever()


async def handle_tcp_connection(loop, reader, writer, ntp_addr, is_server: bool):
    # UDP side
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.setblocking(False)
    # Bind to NTP port if server, else use ephemeral
    if is_server:
        sock.bind(ntp_addr)
    else:
        sock.bind(("0.0.0.0", 0))

    transport, protocol = await loop.create_datagram_endpoint(
        lambda: UdpTunnelEndpoint(loop, reader, writer, is_server=is_server),
        sock=sock,
        remote_addr=None if is_server else ntp_addr,
    )

    # Start TCP→UDP pump
    await protocol.pump_tcp_to_udp()
    transport.close()


async def run_client(loop, server_host, server_ntp_port, local_tcp_port):
    # Local TCP server that apps connect to; traffic is tunneled to remote NTP server
    async def client_handler(reader, writer):
        await handle_tcp_connection(
            loop,
            reader,
            writer,
            (server_host, server_ntp_port),
            is_server=False,
        )

    server = await asyncio.start_server(
        client_handler,
        host="127.0.0.1",
        port=local_tcp_port,
    )
    async with server:
        await server.serve_forever()


def main():
    parser = argparse.ArgumentParser(description="Async NTP tunnel (single-process).")
    sub = parser.add_subparsers(dest="mode", required=True)

    srv = sub.add_parser("server", help="Run in server mode")
    srv.add_argument("--ntp-port", type=int, default=123, help="UDP port to listen on (NTP-like)")
    srv.add_argument("--tcp-port", type=int, default=8080, help="TCP port to expose")

    cli = sub.add_parser("client", help="Run in client mode")
    cli.add_argument("server_host", help="Server hostname/IP")
    cli.add_argument("--server-ntp-port", type=int, default=123, help="Server NTP-like UDP port")
    cli.add_argument("--local-tcp-port", type=int, default=8080, help="Local TCP port to expose")

    args = parser.parse_args()
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    if args.mode == "server":
        loop.run_until_complete(
            run_server(loop, ("0.0.0.0", args.ntp_port), args.tcp_port)
        )
    else:
        loop.run_until_complete(
            run_client(loop, args.server_host, args.server_ntp_port, args.local_tcp_port)
        )


if __name__ == "__main__":
    main()
