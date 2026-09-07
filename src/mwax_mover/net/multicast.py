"""UDP multicast sending and local interface IP resolution.

get_ip_address() resolves a local network interface name to its IPv4 address
(via SIOCGIFADDR), typically to determine the outbound interface IP that
send_multicast() then binds to for sending a UDP datagram to a multicast
group.
"""

import fcntl
import socket
import struct


def send_multicast(
    multicast_interface_ip: str,
    dest_multicast_ip: str,
    dest_multicast_port: int,
    message: bytes,
    ttl_hops: int,
):
    """
    Send a UDP datagram to an IP multicast group.

    Creates a UDP socket, configures the outbound multicast interface and
    TTL, then transmits ``message`` to the specified group address and port.
    The socket is closed in a finally block regardless of outcome.

    Args:
        multicast_interface_ip: IP address of the local network interface to
            use for outbound multicast traffic. Must be associated with a
            multicast-capable interface.
        dest_multicast_ip: Destination multicast group IP address
            (e.g. ``'239.255.0.1'``).
        dest_multicast_port: Destination UDP port number.
        message: The raw bytes payload to transmit.
        ttl_hops: IP multicast TTL / hop limit. Controls how many router hops
            the datagram may traverse.

    Raises:
        Exception: If ``sendto`` sends zero bytes, or if any socket operation
            raises an unexpected error.
    """

    # Create the datagram socket
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)

    # Disable loopback so you do not receive your own datagrams.
    # loopback = 0
    # if sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_LOOP,
    #                    loopback) != 0:
    #    raise Exception("Error setsockopt IP_MULTICAST_LOOP failed")

    # Set the time-to-live for messages.
    hops = struct.pack("b", ttl_hops)
    sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, hops)

    # Set local interface for outbound multicast datagrams.
    # The IP address specified must be associated with a local,
    # multicast - capable interface.
    sock.setsockopt(
        socket.IPPROTO_IP,
        socket.IP_MULTICAST_IF,
        socket.inet_aton(multicast_interface_ip),
    )

    try:
        # Send data to the multicast group
        if sock.sendto(message, (dest_multicast_ip, dest_multicast_port)) == 0:
            raise Exception("Error sock.sendto() sent 0 bytes")
    finally:
        sock.close()


def get_ip_address(ifname: str) -> str:
    """
    Return the IPv4 address assigned to a named network interface.

    Uses the ``SIOCGIFADDR`` ioctl to query the kernel directly.

    Args:
        ifname: The network interface name (e.g. ``'eth0'``, ``'bond0'``).
            Only the first 15 characters are used.

    Returns:
        The IPv4 address as a dotted-decimal string (e.g. ``'192.168.1.10'``).

    Raises:
        OSError: If the ioctl call fails (e.g. the interface does not exist).
    """
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    return socket.inet_ntoa(
        fcntl.ioctl(
            sock.fileno(),
            0x8915,  # SIOCGIFADDR
            struct.pack("256s", bytes(ifname[:15], "utf-8")),
        )[20:24]
    )
