//package zio.memberlist
//
//import zio._
//import zio.logging.{Logging, log}
//import zio.memberlist.transport.{Bind, Channel, ConnectionLessTransport}
//import zio.stream.Take
//
///**
// * Logic for connection handler for udp transport
// */
//object ConnectionHandler {
//
//  private def read(connection: Channel, messages: Queue[Take[Error, (NodeAddress, Chunk[Byte])]]): IO[Error, Unit] =
//    Take
//      .fromEffect(
//        connection.address.flatMap(NodeAddress.fromSocketAddress) <*> connection.read
//      )
//      .flatMap {
//        messages.offer(_)
//      }
//      .unit
//
//  def bind(
//    local: NodeAddress,
//    transport: ConnectionLessTransport,
//    messages: Queue[Take[Error, (NodeAddress, Chunk[Byte])]]
//  ): ZManaged[Logging, TransportError, Bind] =
//    for {
//      localAddress <- local.socketAddress.toManaged_
//      _            <- log.info("bind to " + localAddress).toManaged_
//      logger       <- ZManaged.environment[Logging]
//      bind         <- transport
//                        .bind(localAddress) { conn =>
//                          read(conn, messages)
//                            .catchAll(ex => log.error("fail to read", Cause.fail(ex)).unit.provide(logger))
//                        }
//    } yield bind
//
//}
