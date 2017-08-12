package pt.uminho.haslab.smcoprocessors;

/*This test aims to validate the integration of the DiscoveryService and the PeerConnectionManager. This two components
* are essential for the correctness of the IORelay as they enables players in different clusters to communicate. This
* is made transparently without the SMPC having to know how the connections are being made.
* The connection of the two classes (DiscoveryService and PeerConnectionManager) is made in the function getTargetClient
* on the IORelay class.
*
* As input the test should receive a List of bindingAddress and ports, create regionServers and have this regionServers
* connect to each other and exchange messages. Thus each region server should receive a list of target clients and
* messages to send.
* */
public class MessageExchangeDistributedClusterTest {
}
